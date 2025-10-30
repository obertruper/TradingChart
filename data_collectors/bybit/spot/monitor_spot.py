#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous Data Monitor for Bybit Spot Market
==============================================

This module provides continuous monitoring and automatic filling of missing candle data.
It monitors the database for gaps and fills them using the existing collection infrastructure.
"""

import sys
import os
import yaml
import logging
import time
import datetime
import argparse
import json
import pytz
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal
from tqdm import tqdm
from pathlib import Path

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from project root
    env_path = Path(__file__).resolve().parents[3] / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass  # dotenv not installed, will use system environment variables

# Add path to Bybit API client
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", "api", "bybit"))

from bybit_api_client import get_bybit_client
from database import DatabaseManager, CandleDataManager
from time_utils import TimeManager
from config_validator import ConfigValidator
from data_loader_spot import SpotCollector


class Monitor:
    """
    Continuous monitoring and gap-filling system for candle data.

    Features:
    - Automatic detection of missing data gaps
    - Smart filling of recent gaps only
    - Rate-limited API usage
    - Error recovery and retry logic
    - Multiple operation modes (daemon, one-shot, single symbol)
    """

    def __init__(self, config_path: str = None, verbose: bool = False, quiet: bool = False):
        """
        Initialize the continuous monitor.

        Args:
            config_path: Path to configuration file
            verbose: Enable verbose (DEBUG) mode
            quiet: Enable quiet mode (minimal output)
        """
        # Load configuration
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "..", "..", "monitor_config_spot.yaml")

        self.config_path = config_path
        self.config = self._load_config()

        # Initialize components
        self.time_manager = TimeManager(self.config)
        self.db_manager = DatabaseManager(self.config)
        self.candle_manager = CandleDataManager(self.db_manager, self.config)

        # Log configured symbols for debugging
        monitoring_symbols = self.config.get("monitoring", {}).get("symbols", [])
        if monitoring_symbols:
            print(f"[CONFIG] Loaded {len(monitoring_symbols)} symbols from config: {monitoring_symbols}")

        # Initialize Bybit API client for data fetching
        self.bybit_client = get_bybit_client(
            api_key=self.config.get("api", {}).get("bybit", {}).get("api_key"),
            api_secret=self.config.get("api", {}).get("bybit", {}).get("api_secret"),
            testnet=self.config.get("api", {}).get("bybit", {}).get("testnet", False),
        )

        # Monitoring state
        self.state = {
            "last_check_time": None,
            "symbol_errors": {},  # Track consecutive errors per symbol
            "total_gaps_filled": 0,
            "last_successful_symbols": [],
        }

        # Load saved state if exists
        self._load_state()

        # Setup logging
        self._setup_logging(verbose=verbose, quiet=quiet)
        self.logger = logging.getLogger("Monitor")

        # Progress tracking
        self.progress_bar = None
        self.batch_progress = None

        # Only log initialization in debug mode
        if not self.compact_mode:
            self.logger.info("Monitor initialized")
            self.logger.info(f"Monitoring {len(self.config.get('monitoring', {}).get('symbols', []))} symbols")

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file and merge with environment variables."""
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)

            # Override database password from environment variable if available
            if os.getenv('DB_WRITER_PASSWORD'):
                if 'database' not in config:
                    config['database'] = {}
                config['database']['password'] = os.getenv('DB_WRITER_PASSWORD')
                config['database']['user'] = os.getenv('DB_WRITER_USER', config['database'].get('user', 'trading_writer'))
                config['database']['host'] = os.getenv('DB_HOST', config['database'].get('host', '82.25.115.144'))
                config['database']['port'] = int(os.getenv('DB_PORT', config['database'].get('port', 5432)))
                config['database']['database'] = os.getenv('DB_NAME', config['database'].get('database', 'trading_data'))

            # Override Bybit API credentials from environment if available
            if os.getenv('BYBIT_API_KEY'):
                if 'api' not in config:
                    config['api'] = {'bybit': {}}
                elif 'bybit' not in config['api']:
                    config['api']['bybit'] = {}
                config['api']['bybit']['api_key'] = os.getenv('BYBIT_API_KEY')
                config['api']['bybit']['api_secret'] = os.getenv('BYBIT_API_SECRET', '')

            # Validate configuration
            validator = ConfigValidator()
            validation_result = validator.validate_config(config)

            if not validation_result["is_valid"]:
                raise ValueError(f"Configuration validation failed: {validation_result['errors']}")

            if validation_result["warnings"]:
                print(f"Configuration warnings: {validation_result['warnings']}")

            return validation_result.get("corrected_config", config)

        except Exception as e:
            raise RuntimeError(f"Failed to load configuration from {self.config_path}: {e}")

    def _setup_logging(self, verbose: bool = False, quiet: bool = False):
        """Setup logging configuration with flexible levels.

        Args:
            verbose: Enable verbose (DEBUG) mode
            quiet: Enable quiet mode (minimal output)
        """
        log_config = self.config.get("logging", {})
        console_config = log_config.get("console_output", {})

        # Determine logging levels
        debug_mode = log_config.get("debug_mode", False) or verbose

        if debug_mode:
            file_level = "DEBUG"
            console_level = "DEBUG"
        elif quiet:
            file_level = log_config.get("level", "INFO").upper()
            console_level = "WARNING"
        else:
            file_level = log_config.get("level", "INFO").upper()
            console_level = console_config.get("level", "INFO").upper()

        log_file = log_config.get("file", "logs/monitor.log")
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all
        root_logger.handlers = []  # Clear existing handlers

        # File handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, file_level))
        file_handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(file_handler)

        # Console handler
        if console_config.get("enabled", True) and not quiet:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, console_level))

            # Use compact format for console if specified
            if console_config.get("compact_mode", True) and not debug_mode:
                console_format = "%(asctime)s - %(message)s"
                console_handler.setFormatter(logging.Formatter(console_format, datefmt="%H:%M:%S"))
            else:
                console_handler.setFormatter(logging.Formatter(log_format))

            root_logger.addHandler(console_handler)

        # Set component-specific log levels
        component_levels = log_config.get("component_levels", {})
        for component, level in component_levels.items():
            if not debug_mode:  # Don't override in debug mode
                component_logger = logging.getLogger(component)
                component_logger.setLevel(getattr(logging, level.upper()))

        # Store settings for later use
        self.show_progress_bar = console_config.get("show_progress_bar", True) and not debug_mode and not quiet
        self.compact_mode = console_config.get("compact_mode", True) and not debug_mode

    def _load_state(self):
        """Load monitoring state from checkpoint file."""
        state_file = self.config.get("advanced", {}).get("state_file", "data_collectors/checkpoints/monitor_state.json")

        try:
            if os.path.exists(state_file):
                with open(state_file, "r") as f:
                    saved_state = json.load(f)
                self.state.update(saved_state)
        except Exception as e:
            print(f"Warning: Could not load state file: {e}")

    def _save_state(self):
        """Save monitoring state to checkpoint file."""
        state_file = self.config.get("advanced", {}).get("state_file", "data_collectors/checkpoints/monitor_state.json")

        try:
            # Create checkpoint directory if it doesn't exist
            state_dir = os.path.dirname(state_file)
            if state_dir and not os.path.exists(state_dir):
                os.makedirs(state_dir, exist_ok=True)

            # Save current state
            self.state["last_check_time"] = datetime.datetime.now().isoformat()

            with open(state_file, "w") as f:
                json.dump(self.state, f, indent=2, default=str)

        except Exception as e:
            self.logger.warning(f"Could not save state file: {e}")

    def get_last_timestamp(self, symbol: str, interval: str = "1") -> Optional[int]:
        """
        Get the last timestamp for a symbol from the database.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Timeframe interval

        Returns:
            Last timestamp in milliseconds or None if no data exists
        """
        try:
            data_range = self.candle_manager.get_existing_data_range(symbol, interval)
            if data_range:
                return data_range[1]  # Return max timestamp
            return None

        except Exception as e:
            self.logger.error(f"Failed to get last timestamp for {symbol}: {e}")
            return None

    def calculate_missing_range(
        self, symbol: str, current_time: Optional[datetime.datetime] = None
    ) -> Optional[Tuple[int, int]]:
        """
        Calculate the time range of missing data for a symbol.
        Start always from last_timestamp_in_db + 1 minute (in ms).
        If gap is tiny — skip. If last_timestamp is in the future — adjust to now-1min.
        """
        if current_time is None:
            current_time = datetime.datetime.now(pytz.UTC)

        monitoring_config = self.config.get("monitoring", {})
        max_lookback_hours = monitoring_config.get("max_lookback_hours", 24)
        min_gap_minutes = monitoring_config.get("min_gap_minutes", 2)

        # Получаем последний timestamp (ожидаем миллисекунды)
        last_timestamp_ms = self.get_last_timestamp(symbol)

        if last_timestamp_ms is None:
            self.logger.info(f"No existing data for {symbol}, cannot calculate missing range")
            return None

        # Защита: если в БД случайно будущая метка времени — корректируем
        now_ms = int(current_time.timestamp() * 1000)
        if last_timestamp_ms >= now_ms:
            self.logger.warning(
                f"{symbol}: last_timestamp in DB ({last_timestamp_ms}) >= now ({now_ms}). Adjusting to now-1min."
            )
            last_timestamp_ms = now_ms - 60_000

        last_time = datetime.datetime.fromtimestamp(last_timestamp_ms / 1000, tz=pytz.UTC)
        gap_duration = current_time - last_time
        gap_minutes = gap_duration.total_seconds() / 60.0

        self.logger.debug(f"{symbol}: Last data at {last_time.isoformat()}, gap {gap_minutes:.1f} minutes")

        if gap_minutes < min_gap_minutes:
            self.logger.debug(f"{symbol}: Gap too small ({gap_minutes:.1f} min), skipping")
            return None

        # Новый важный момент: стартуем ОТ last + 1 минуту (даже если gap >> max_lookback_hours)
        start_timestamp_ms = last_timestamp_ms + 60_000

        # ИЗМЕНЕНО: Загружаем данные порциями в хронологическом порядке
        # Вместо загрузки до текущего времени, загружаем максимум 1000 свечей вперед
        # Это обеспечит последовательную загрузку от старых к новым данным
        max_candles_per_iteration = 1000

        # Конечное время для этой итерации - через 1000 минут от начала
        # Но не дальше текущего времени минус 1 минута
        end_time_iteration = datetime.datetime.fromtimestamp(
            start_timestamp_ms / 1000 + (max_candles_per_iteration * 60), tz=pytz.UTC
        )
        current_time_safe = current_time - datetime.timedelta(minutes=1)

        # Берем минимум из двух времен
        if end_time_iteration > current_time_safe:
            end_time = current_time_safe
        else:
            end_time = end_time_iteration

        end_timestamp_ms = int(end_time.timestamp() * 1000)

        if start_timestamp_ms >= end_timestamp_ms:
            self.logger.debug(f"{symbol}: No meaningful gap to fill after normalization (start >= end)")
            return None

        # Если разрыв очень большой — логируем предупреждение, но НЕ меняем старт.
        if gap_minutes > (max_lookback_hours * 60):
            self.logger.warning(
                f"{symbol}: Gap too large ({gap_minutes/60:.1f} hours). "
                f"Will start from last DB timestamp +1min ({datetime.datetime.fromtimestamp(start_timestamp_ms/1000, tz=pytz.UTC)}) "
                f"and fill forward in chunks limited by API client."
            )

        candles_to_load = (end_timestamp_ms - start_timestamp_ms) // 60000
        # Simplified logging for compact mode with date information
        if self.compact_mode:
            # Calculate total gap
            current_time = datetime.datetime.now(pytz.UTC)
            total_gap_minutes = int((current_time.timestamp() * 1000 - last_timestamp_ms) / 60000)

            # Format the date range being loaded
            start_date = datetime.datetime.fromtimestamp(start_timestamp_ms / 1000, tz=pytz.UTC).strftime(
                "%Y-%m-%d %H:%M"
            )
            end_date = datetime.datetime.fromtimestamp(end_timestamp_ms / 1000, tz=pytz.UTC).strftime("%Y-%m-%d %H:%M")

            # Don't log if using progress bar
            if not self.show_progress_bar:
                self.logger.info(
                    f"{symbol}: Loading {candles_to_load:,}/{total_gap_minutes:,} | Period: {start_date} to {end_date}"
                )
        else:
            self.logger.info(
                f"{symbol}: Will fill chronologically from {datetime.datetime.fromtimestamp(start_timestamp_ms/1000, tz=pytz.UTC)} "
                f"to {datetime.datetime.fromtimestamp(end_timestamp_ms/1000, tz=pytz.UTC)} (~{candles_to_load} candles, "
                f"max {max_candles_per_iteration} per iteration)"
            )

        return (start_timestamp_ms, end_timestamp_ms)

    def _fetch_candles_api(self, symbol: str, start_timestamp: int, end_timestamp: int) -> List[List[Any]]:
        """
        Chunked fetch that loads data in batches of 1000 candles from Bybit API.
        Continues until all data in the time range is loaded or API returns < 1000 candles.
        """
        try:
            per_request_limit = int(self.config.get("exchange", {}).get("max_candles_per_request", 1000))
            api_cooldown = float(self.config.get("monitoring", {}).get("api_cooldown_seconds", 1))
            # Remove artificial limit - load as much as possible in chunks of 1000
            max_candles_per_run = float("inf")  # No limit for large gaps

            all_candles: List[List[Any]] = []
            current_start = start_timestamp
            total_fetched = 0

            # Calculate total expected candles for progress bar
            total_expected = int((end_timestamp - start_timestamp) / 60000)

            # Don't create individual batch progress bar anymore - we use overall progress
            pbar = None

            # loop until we reach end_timestamp
            while current_start <= end_timestamp:
                # request a batch starting at current_start, limit=per_request_limit
                if not self.show_progress_bar:
                    self.logger.debug(f"Requesting batch for {symbol}: start={current_start} limit={per_request_limit}")

                batch = self._fetch_candles_batch(symbol, current_start, end_timestamp)

                if not batch:
                    # нет данных дальше — выходим
                    if not self.show_progress_bar:
                        self.logger.debug(f"{symbol}: received empty batch at start={current_start} — stopping.")
                    break

                # Store original batch size for end-of-data detection
                original_batch_size = len(batch)

                all_candles.extend(batch)
                total_fetched += len(batch)

                # Update overall progress bar if exists
                if hasattr(self, "overall_progress") and self.overall_progress:
                    # Update the total if we're getting more data than expected
                    if self.overall_progress.n + len(batch) > self.overall_progress.total:
                        # Adjust total to match actual data
                        self.overall_progress.total = self.overall_progress.n + len(batch)
                        self.overall_progress.refresh()

                    self.overall_progress.update(len(batch))
                    # Update postfix with latest loaded timestamp
                    if batch:
                        latest_ts = int(batch[-1][0])  # Last candle's timestamp (convert to int)
                        latest_date = datetime.datetime.fromtimestamp(latest_ts / 1000, tz=pytz.UTC).strftime(
                            "%Y-%m-%d %H:%M"
                        )
                        self.overall_progress.set_postfix_str(f"Latest: {latest_date}")

                # Save progress: last fetched timestamp for symbol
                try:
                    last_ts = int(batch[-1][0])
                    self.state.setdefault("progress", {})[symbol] = last_ts
                    # периодически сохраняем прогресс на диск
                    if total_fetched % 5000 == 0:
                        self._save_state()
                except Exception:
                    pass

                # Log progress only if not using progress bar and not in compact mode
                if not self.show_progress_bar and not self.compact_mode:
                    remaining_time_ms = end_timestamp - current_start
                    remaining_candles = remaining_time_ms // 60_000
                    progress_percent = (
                        (total_fetched / (total_fetched + remaining_candles)) * 100 if remaining_candles > 0 else 100
                    )

                    current_date = datetime.datetime.fromtimestamp(current_start / 1000, tz=pytz.UTC).strftime(
                        "%Y-%m-%d %H:%M"
                    )

                    self.logger.info(
                        f"{symbol}: fetched {len(batch)} candles (total: {total_fetched:,}, "
                        f"progress: {progress_percent:.1f}%, current date: {current_date})"
                    )

                # Если API вернул меньше, чем лимит — данных дальше нет в этом диапазоне
                if original_batch_size < per_request_limit:
                    if not self.show_progress_bar:
                        self.logger.info(
                            f"{symbol}: received {original_batch_size} < {per_request_limit} candles — reached end of available data"
                        )
                    break

                # Продвигаем старт на last_ts + 1 минута (чтобы избежать перекрытия)
                last_fetched_ts = int(batch[-1][0])
                current_start = last_fetched_ts + 60_000

                # cooldown (можно настроить в конфиге)
                try:
                    time.sleep(api_cooldown)
                except KeyboardInterrupt:
                    if pbar:
                        pbar.close()
                    self.logger.warning("KeyboardInterrupt during cooldown sleep — returning partial results")
                    break

            # Don't close overall progress bar here - it spans multiple batches

            # dedupe & sort
            if all_candles:
                seen = set()
                unique = []
                for c in all_candles:
                    ts = int(c[0])
                    if ts not in seen:
                        seen.add(ts)
                        unique.append(c)
                unique.sort(key=lambda x: int(x[0]))
                all_candles = unique

            # Don't log the loaded message in compact mode
            if not self.compact_mode and not self.show_progress_bar:
                self.logger.info(f"{symbol}: chunked fetch finished — total_candles_fetched={len(all_candles)}")

            return all_candles

        except KeyboardInterrupt:
            if "pbar" in locals() and pbar:
                pbar.close()
            self.logger.warning("KeyboardInterrupt in _fetch_candles_api — returning partial results")
            return all_candles if "all_candles" in locals() else []
        except Exception as e:
            if "pbar" in locals() and pbar:
                pbar.close()
            self.logger.error(f"Failed to fetch candles for {symbol}: {e}")
            return []

    def _fetch_candles_batch(self, symbol: str, start_timestamp: int, end_timestamp: int) -> List[List[Any]]:
        """
        Fetch a single batch of candles from Bybit API with retries.
        Returns candles oldest-first (sorted).
        """
        max_retries = int(self.config.get("exchange", {}).get("retry_attempts", 3))
        retry_delay = float(self.config.get("exchange", {}).get("retry_delay", 2))
        per_request_limit = int(self.config.get("exchange", {}).get("max_candles_per_request", 1000))

        tries = 0
        while tries <= max_retries:
            try:
                result = self.bybit_client.get_kline(
                    category=self.config.get("exchange", {}).get("category", "linear"),
                    symbol=symbol,
                    interval="1",
                    start=start_timestamp,
                    end=end_timestamp,
                    limit=per_request_limit,
                )

                if result.get("retCode") != 0:
                    self.logger.error(
                        f"Bybit API error for {symbol}: {result.get('retMsg', 'Unknown error')} (try {tries+1}/{max_retries})"
                    )
                    tries += 1
                    time.sleep(retry_delay)
                    continue

                candles = result.get("result", {}).get("list", [])
                if not candles:
                    self.logger.debug(f"No candles returned for {symbol} in period {start_timestamp}-{end_timestamp}")
                    return []

                # Bybit returns newest first; reverse => oldest first
                candles.reverse()
                self.logger.debug(f"Fetched {len(candles)} candles for {symbol} (batch)")
                return candles

            except KeyboardInterrupt:
                self.logger.warning("KeyboardInterrupt during _fetch_candles_batch")
                raise
            except Exception as e:
                tries += 1
                self.logger.error(f"Exception fetching batch for {symbol}: {e} (try {tries}/{max_retries})")
                time.sleep(retry_delay)

        self.logger.error(f"Failed to fetch candles batch for {symbol} after {max_retries} retries")
        return []

    def fill_missing_data(self, symbol: str, start_timestamp: int, end_timestamp: int) -> bool:
        """
        Fill missing data for a symbol in the specified time range.

        Args:
            symbol: Trading pair symbol
            start_timestamp: Start time in milliseconds
            end_timestamp: End time in milliseconds

        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert timestamps to readable format for logging
            start_time = datetime.datetime.fromtimestamp(start_timestamp / 1000, tz=pytz.UTC)
            end_time = datetime.datetime.fromtimestamp(end_timestamp / 1000, tz=pytz.UTC)

            expected_candles = (end_timestamp - start_timestamp) / 60000  # 1-minute intervals

            # Only log in non-compact mode
            if not self.compact_mode:
                self.logger.info(
                    f"Filling missing data for {symbol}: {start_time} to {end_time} (~{expected_candles:.0f} candles)"
                )

            # Check if this is a dry run
            if self.config.get("advanced", {}).get("dry_run", False):
                self.logger.info(f"DRY RUN: Would fill {expected_candles:.0f} candles for {symbol}")
                return True

            # Fetch candles using our own method
            candles = self._fetch_candles_api(symbol, start_timestamp, end_timestamp)

            if not candles:
                self.logger.warning(f"No candles received for {symbol} in range {start_time} to {end_time}")
                return False

            # Insert candles into database
            inserted_count = self.candle_manager.insert_candles_batch(candles, symbol)

            if inserted_count > 0:
                # Log success with total progress info
                if self.compact_mode:
                    # Get total gap size for better progress tracking
                    last_timestamp = self.get_last_timestamp(symbol)
                    current_time = datetime.datetime.now(pytz.UTC)
                    if last_timestamp:
                        gap_minutes = int((current_time.timestamp() * 1000 - last_timestamp) / 60000)
                        # Show the latest date we have data for
                        latest_date = datetime.datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC).strftime(
                            "%Y-%m-%d %H:%M"
                        )

                        # Update overall progress bar if exists
                        if hasattr(self, "overall_progress") and self.overall_progress:
                            self.overall_progress.set_postfix_str(f"Latest: {latest_date}")

                        # Don't log if using progress bar
                        if not self.show_progress_bar:
                            self.logger.info(
                                f"{symbol}: +{inserted_count:,} candles | Latest: {latest_date} | Remaining: ~{gap_minutes:,}"
                            )
                    else:
                        if not self.show_progress_bar:
                            self.logger.info(f"{symbol}: +{inserted_count:,} candles")
                else:
                    self.logger.info(f"Successfully filled {inserted_count} missing candles for {symbol}")
                self.state["total_gaps_filled"] += inserted_count
                return True
            else:
                self.logger.info(f"No new candles inserted for {symbol} (data may already exist)")
                return True

        except Exception as e:
            self.logger.error(f"Failed to fill missing data for {symbol}: {e}")
            return False

    def check_symbol(self, symbol: str) -> tuple:
        """
        Check a single symbol for missing data and fill gaps if found.

        Args:
            symbol: Trading pair symbol to check

        Returns:
            Tuple of (success, has_more_data) where:
            - success: True if successful (or no gaps), False if error occurred
            - has_more_data: True if there are more historical gaps to fill
        """
        try:
            # Always log symbol being checked (even in compact mode)
            if self.compact_mode:
                self.logger.info(f"Checking {symbol}...")
            else:
                self.logger.debug(f"Checking {symbol} for missing data...")

            # Calculate missing range
            missing_range = self.calculate_missing_range(symbol)

            if missing_range is None:
                # Log status even in compact mode
                if self.compact_mode:
                    self.logger.info(f"{symbol}: ✓ Up to date (no gaps)")
                else:
                    self.logger.debug(f"{symbol}: No missing data detected")
                # Close overall progress bar if exists and no more data
                if hasattr(self, "overall_progress") and self.overall_progress:
                    self.overall_progress.close()
                    delattr(self, "overall_progress")
                return True, False  # Success, no more data

            start_timestamp, end_timestamp = missing_range

            # Calculate if there's more data after this batch
            current_time = datetime.datetime.now(pytz.UTC)
            end_time = datetime.datetime.fromtimestamp(end_timestamp / 1000, tz=pytz.UTC)
            time_diff = (current_time - end_time).total_seconds() / 60  # in minutes
            has_more_data = time_diff > 1000  # More than 1000 minutes (candles) remaining

            # Setup overall progress bar if this is the first batch and progress bar is enabled
            if self.show_progress_bar and not hasattr(self, "overall_progress"):
                # Calculate the actual number of candles we expect to load in this batch
                expected_candles = int((end_timestamp - start_timestamp) / 60000)

                # If we're loading a small batch (< 1000 candles), use exact count
                # Otherwise, estimate based on total gap
                if expected_candles < 1000:
                    total_to_load = expected_candles
                else:
                    # For large gaps, show the full gap size
                    first_timestamp = self.get_last_timestamp(symbol)
                    if first_timestamp:
                        total_to_load = int((current_time.timestamp() * 1000 - first_timestamp) / 60000)
                    else:
                        total_to_load = expected_candles

                self.overall_progress = tqdm(
                    total=total_to_load,
                    desc=f"{symbol}",
                    unit="candles",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
                    leave=True,
                )
                self.total_loaded = 0

            # Don't log gap detection in compact mode - it's redundant
            if has_more_data and not self.compact_mode:
                self.logger.info(
                    f"{symbol}: Large gap detected (~{time_diff:.0f} minutes remaining), continuing without delay"
                )

            # Fill missing data
            success = self.fill_missing_data(symbol, start_timestamp, end_timestamp)

            if success:
                # Reset error count for this symbol
                if symbol in self.state["symbol_errors"]:
                    del self.state["symbol_errors"][symbol]
                return True, has_more_data
            else:
                # Increment error count
                self.state["symbol_errors"][symbol] = self.state["symbol_errors"].get(symbol, 0) + 1
                return False, has_more_data

        except Exception as e:
            self.logger.error(f"Error checking {symbol}: {e}")
            self.state["symbol_errors"][symbol] = self.state["symbol_errors"].get(symbol, 0) + 1
            return False, False

    def check_all_symbols(self) -> tuple:
        """
        Check all configured symbols for missing data.

        Returns:
            Tuple of (results_dict, has_more_gaps) where:
            - results_dict: Dictionary mapping symbol names to success/failure status
            - has_more_gaps: True if any symbol has more historical data to load
        """
        monitoring_config = self.config.get("monitoring", {})
        symbols = monitoring_config.get("symbols", [])
        symbol_delay = monitoring_config.get("symbol_delay_seconds", 2)
        max_consecutive_errors = monitoring_config.get("max_consecutive_errors", 3)

        results = {}
        successful_symbols = []
        has_any_gaps = False

        # Always log summary of symbols being checked
        if self.compact_mode:
            self.logger.info(f"Checking {len(symbols)} symbols: {', '.join(symbols)}")
        else:
            self.logger.info(f"Starting check of {len(symbols)} symbols...")

        for i, symbol in enumerate(symbols):
            # Check if symbol has too many consecutive errors
            error_count = self.state["symbol_errors"].get(symbol, 0)
            if error_count >= max_consecutive_errors:
                self.logger.warning(f"Skipping {symbol} due to {error_count} consecutive errors")
                results[symbol] = False
                continue

            # Check the symbol
            try:
                success, has_more_data = self.check_symbol(symbol)
                results[symbol] = success

                if has_more_data:
                    has_any_gaps = True

                if success:
                    successful_symbols.append(symbol)

                # Clean up progress bar after each symbol
                if hasattr(self, "overall_progress") and self.overall_progress:
                    self.overall_progress.close()
                    delattr(self, "overall_progress")

            except Exception as e:
                self.logger.error(f"Unexpected error checking {symbol}: {e}")
                results[symbol] = False
                # Clean up progress bar on error too
                if hasattr(self, "overall_progress") and self.overall_progress:
                    self.overall_progress.close()
                    delattr(self, "overall_progress")

            # Add delay between symbols only if NOT loading historical data
            if i < len(symbols) - 1 and not has_any_gaps:
                time.sleep(symbol_delay)

        # Update state
        self.state["last_successful_symbols"] = successful_symbols

        # Always log summary
        successful_count = sum(1 for success in results.values() if success)
        if self.compact_mode:
            # Compact summary with symbol status
            status_summary = []
            for symbol, success in results.items():
                status_summary.append(f"{symbol}:{'✓' if success else '✗'}")
            self.logger.info(f"Check complete: {successful_count}/{len(symbols)} successful [{', '.join(status_summary)}]")
        else:
            self.logger.info(f"Symbol check complete: {successful_count}/{len(symbols)} successful")
            if has_any_gaps:
                self.logger.info("Historical gaps detected, continuing without delay...")

        return results, has_any_gaps

    def run_single_check(self, symbol: str = None) -> bool:
        """
        Run a single check (either one symbol or all symbols).

        Args:
            symbol: Optional specific symbol to check

        Returns:
            True if all checks successful, False otherwise
        """
        if symbol:
            self.logger.info(f"Running single check for symbol: {symbol}")
            success, _ = self.check_symbol(symbol)
            return success
        else:
            self.logger.info(f"Running single check for all configured symbols")
            results, _ = self.check_all_symbols()
            return all(results.values())

    def run_continuous(self):
        """
        Run continuous monitoring loop with smart delay logic.
        """
        monitoring_config = self.config.get("monitoring", {})
        check_interval_minutes = monitoring_config.get("check_interval_minutes", 5)

        if not self.compact_mode:
            self.logger.info("Starting continuous monitoring mode")
            self.logger.info(f"Check interval: {check_interval_minutes} minutes")

        consecutive_loads = 0  # Track consecutive historical data loads

        try:
            while True:
                start_time = time.time()

                # Run symbol checks and check if there are more gaps
                results, has_more_gaps = self.check_all_symbols()

                # Save state periodically
                self._save_state()

                # Smart delay logic based on data availability
                elapsed_time = time.time() - start_time

                if has_more_gaps:
                    # Historical data loading mode - minimal or no delay
                    consecutive_loads += 1

                    if consecutive_loads > 100:  # Safety check after 100 consecutive loads
                        sleep_time = 5  # Small delay to prevent overload
                        self.logger.info(f"Safety pause: {sleep_time}s after {consecutive_loads} consecutive loads")
                        consecutive_loads = 0
                    else:
                        sleep_time = 0.5  # Minimal delay between batches
                        # Don't log the delay message in compact mode
                else:
                    # Real-time monitoring mode - normal delay
                    consecutive_loads = 0

                    # Close overall progress bar if exists
                    if hasattr(self, "overall_progress") and self.overall_progress:
                        self.overall_progress.close()
                        delattr(self, "overall_progress")

                    sleep_time = max(0, (check_interval_minutes * 60) - elapsed_time)

                    if sleep_time > 0:
                        self.logger.info(
                            f"All data up-to-date. Sleeping for {sleep_time:.1f} seconds until next check..."
                        )
                    else:
                        self.logger.warning(
                            f"Check cycle took {elapsed_time:.1f}s, longer than {check_interval_minutes}m interval"
                        )

                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("Continuous monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"Continuous monitoring failed: {e}")
            raise


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(description="Continuous Data Monitor for Bybit Futures")

    # Configuration
    parser.add_argument("--config", "-c", help="Path to configuration file")

    # Operation modes
    parser.add_argument("--check-once", action="store_true", help="Run a single check and exit")

    parser.add_argument("--daemon", action="store_true", help="Run in continuous monitoring mode")

    parser.add_argument("--symbol", "-s", help="Check specific symbol only")

    parser.add_argument("--hours", type=int, help="Override max lookback hours for single symbol check")

    # Logging control
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose (DEBUG) output")

    parser.add_argument("--quiet", "-q", action="store_true", help="Enable quiet mode (minimal output)")

    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars")

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )

    args = parser.parse_args()

    try:
        # Initialize monitor with logging options
        monitor = Monitor(config_path=args.config, verbose=args.verbose, quiet=args.quiet)

        # Override logging level if specified
        if args.log_level:
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, args.log_level))
            for handler in root_logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    handler.setLevel(getattr(logging, args.log_level))

        # Disable progress bars if requested
        if args.no_progress:
            monitor.show_progress_bar = False

        # Override lookback hours if specified
        if args.hours and args.symbol:
            monitor.config["monitoring"]["max_lookback_hours"] = args.hours

        # Run appropriate mode
        if args.daemon:
            monitor.run_continuous()
        else:
            success = monitor.run_single_check(args.symbol)
            sys.exit(0 if success else 1)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
