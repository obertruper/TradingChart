#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Futures Candle Data Collector for Bybit Exchange
==================================================

This module implements historical candle data collection for Bybit futures.
It collects 1-minute OHLCV data for specified trading pairs and time ranges.
"""

import sys
import os
import yaml
import logging
import time
import datetime
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal
from tqdm import tqdm

# Add path to Bybit API client
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", "api", "bybit"))

from bybit_api_client import get_bybit_client
from database import DatabaseManager, CandleDataManager
from time_utils import TimeManager
from config_validator import ConfigValidator


class FuturesCollector:
    """
    Collects historical 1-minute candle data from Bybit futures.

    Features:
    - Rate limiting and retry logic
    - Progress tracking and checkpoints
    - Data validation and duplicate checking
    - Configurable time ranges and symbols
    - Memory and performance monitoring
    """

    def __init__(self, config_path: str = "../../../data_collectors/data_collector_config.yaml"):
        """
        Initialize the futures collector.

        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()

        # Validate configuration
        self.config_validator = ConfigValidator()
        validation_result = self.config_validator.validate_config(self.config)

        # Use corrected config if validation made improvements
        if validation_result["corrected_config"]:
            self.config = validation_result["corrected_config"]

        self.logger = self._setup_logging()

        # Log validation results
        if not validation_result["is_valid"]:
            for error in validation_result["errors"]:
                self.logger.error(f"Config error: {error}")
            raise ValueError("Configuration validation failed")

        if validation_result["warnings"]:
            for warning in validation_result["warnings"]:
                self.logger.warning(f"Config warning: {warning}")

        if validation_result["recommendations"]:
            self.logger.info("Configuration recommendations:")
            for rec in validation_result["recommendations"]:
                self.logger.info(f"  • {rec}")

        self.bybit_client = None

        # Initialize managers
        self.db_manager = DatabaseManager(self.config)
        self.candle_manager = CandleDataManager(self.db_manager, self.config)
        self.time_manager = TimeManager(self.config)

        # Collection state
        self.total_candles_collected = 0
        self.errors_count = 0
        self.consecutive_errors = 0
        self.start_time = None

        # Rate limiting
        self.last_request_time = 0
        self.requests_this_minute = 0
        self.minute_start_time = time.time()

        self.logger.info("FuturesCollector initialized with database connection")

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            raise Exception(f"Failed to load config from {self.config_path}: {e}")

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO"))
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        log_file = log_config.get("file", "logs/data_collector.log")

        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Create logger
        logger = logging.getLogger("FuturesCollector")
        logger.setLevel(log_level)

        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # File handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(log_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter("%(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def _init_bybit_client(self):
        """Initialize Bybit API client using local configuration."""
        try:
            # Get API credentials from local config
            api_config = self.config.get("api", {}).get("bybit", {})
            api_key = api_config.get("api_key")
            api_secret = api_config.get("api_secret")
            testnet = api_config.get("testnet", False)

            if not api_key or not api_secret:
                raise ValueError("API key and secret must be configured in data_collector_config.yaml")

            # Initialize client with direct credentials
            self.bybit_client = get_bybit_client(api_key=api_key, api_secret=api_secret, testnet=testnet)
            self.logger.info("Bybit client initialized successfully from local config")
        except Exception as e:
            self.logger.error(f"Failed to initialize Bybit client: {e}")
            raise

    def _rate_limit_check(self):
        """
        Check and enforce rate limits.
        Bybit allows 120 requests per minute, we use conservative 100.
        """
        current_time = time.time()

        # Reset counter every minute
        if current_time - self.minute_start_time >= 60:
            self.requests_this_minute = 0
            self.minute_start_time = current_time

        # Check if we've hit the rate limit
        rate_config = self.config.get("exchange", {}).get("rate_limit", {})
        max_requests = rate_config.get("requests_per_minute", 100)

        if self.requests_this_minute >= max_requests:
            sleep_time = 60 - (current_time - self.minute_start_time)
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached. Sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                self.requests_this_minute = 0
                self.minute_start_time = time.time()

        # Delay between requests
        min_delay = 1.0 / rate_config.get("requests_per_second", 2)
        time_since_last = current_time - self.last_request_time
        if time_since_last < min_delay:
            sleep_time = min_delay - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.requests_this_minute += 1

    def _convert_datetime_to_timestamp(self, dt_str: str) -> int:
        """
        Convert datetime string to millisecond timestamp using TimeManager.

        Args:
            dt_str: Datetime string in format "YYYY-MM-DD HH:MM:SS"

        Returns:
            UTC timestamp in milliseconds
        """
        dt = self.time_manager.parse_datetime_string(dt_str)
        return self.time_manager.datetime_to_timestamp(dt)

    def _validate_candle_data(self, candle: List[Any]) -> bool:
        """
        Validate single candle data.

        Args:
            candle: List containing [timestamp, open, high, low, close, volume, turnover]

        Returns:
            True if data is valid, False otherwise
        """
        try:
            if len(candle) != 7:
                return False

            # Check timestamp
            timestamp = int(candle[0])
            if timestamp <= 0:
                return False

            # Check OHLCV values
            open_price = float(candle[1])
            high_price = float(candle[2])
            low_price = float(candle[3])
            close_price = float(candle[4])
            volume = float(candle[5])

            # Basic price validation
            if any(price <= 0 for price in [open_price, high_price, low_price, close_price]):
                return False

            # High >= Low check
            if high_price < low_price:
                return False

            # Volume should be non-negative
            if volume < 0:
                return False

            return True

        except (ValueError, TypeError, IndexError):
            return False

    def _fetch_candles_batch(self, symbol: str, start_time: int, end_time: int) -> List[List[Any]]:
        """
        Fetch a batch of candles from Bybit API.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            List of candle data
        """
        self._rate_limit_check()

        exchange_config = self.config.get("exchange", {})
        max_candles = exchange_config.get("max_candles_per_request", 1000)

        try:
            response = self.bybit_client.get_kline(
                category=exchange_config.get("category", "linear"),
                symbol=symbol,
                interval=self.config["collection"]["interval"],
                start=start_time,
                end=end_time,
                limit=max_candles,
            )

            if response.get("retCode", 99999) == 0:
                result = response.get("result", {})
                candles = result.get("list", [])

                # Validate data if enabled
                if self.config.get("collection", {}).get("validate_data", True):
                    valid_candles = []
                    for candle in candles:
                        if self._validate_candle_data(candle):
                            valid_candles.append(candle)
                        else:
                            self.logger.warning(f"Invalid candle data for {symbol}: {candle}")
                    candles = valid_candles

                self.logger.debug(f"Fetched {len(candles)} candles for {symbol}")
                return candles

            else:
                error_msg = response.get("retMsg", "Unknown error")
                error_code = response.get("retCode", "Unknown code")
                raise Exception(f"API error: {error_msg} (code: {error_code})")

        except Exception as e:
            self.logger.error(f"Failed to fetch candles for {symbol}: {e}")
            raise

    def _calculate_smart_chunks(self, start_timestamp: int, end_timestamp: int) -> List[Tuple[int, int]]:
        """
        Calculate smart chunks for data collection to ensure full period coverage.

        Each chunk is sized to fetch maximum 1000 candles (API limit), with proper
        overlaps to prevent gaps and ensure complete temporal coverage.

        Args:
            start_timestamp: Start timestamp in milliseconds
            end_timestamp: End timestamp in milliseconds

        Returns:
            List of (chunk_start_timestamp, chunk_end_timestamp) tuples
        """
        # API limit is 1000 candles per request
        max_candles_per_chunk = self.config.get("exchange", {}).get("max_candles_per_request", 1000)

        # For 1-minute candles: 1000 candles = 1000 minutes = 16.67 hours
        chunk_duration_ms = (max_candles_per_chunk - 1) * 60 * 1000  # -1 for safety margin

        # Calculate total period in minutes for logging
        total_period_ms = end_timestamp - start_timestamp
        total_minutes = total_period_ms / (60 * 1000)
        expected_candles = int(total_minutes) + 1

        chunks = []
        current_start = start_timestamp
        chunk_number = 1

        while current_start < end_timestamp:
            # Calculate chunk end, but don't exceed the target end_timestamp
            chunk_end = min(current_start + chunk_duration_ms, end_timestamp)

            chunks.append((current_start, chunk_end))

            # Log chunk details for debugging
            chunk_start_dt = datetime.datetime.fromtimestamp(current_start / 1000, tz=datetime.timezone.utc)
            chunk_end_dt = datetime.datetime.fromtimestamp(chunk_end / 1000, tz=datetime.timezone.utc)
            chunk_minutes = (chunk_end - current_start) / (60 * 1000)

            self.logger.debug(
                f"Chunk {chunk_number}: {chunk_start_dt.strftime('%Y-%m-%d %H:%M')} to "
                f"{chunk_end_dt.strftime('%Y-%m-%d %H:%M')} (~{int(chunk_minutes)} candles)"
            )

            # Move to next chunk start (add 1 minute to prevent overlap)
            current_start = chunk_end + (60 * 1000)
            chunk_number += 1

        self.logger.info(f"Created {len(chunks)} smart chunks for {expected_candles} expected candles")
        return chunks

    def _filter_candles_by_timerange(
        self, candles: List[List[Any]], start_timestamp: int, end_timestamp: int
    ) -> List[List[Any]]:
        """
        Filter candles to only include those within the specified time range.

        Args:
            candles: List of candle data [timestamp, open, high, low, close, volume, turnover]
            start_timestamp: Start timestamp in milliseconds (inclusive)
            end_timestamp: End timestamp in milliseconds (inclusive)

        Returns:
            Filtered list of candles within the time range
        """
        if not candles:
            return []

        filtered_candles = []
        for candle in candles:
            try:
                candle_timestamp = int(candle[0])  # First element is timestamp (open time)

                # Include candle if its open time is within our range
                if start_timestamp <= candle_timestamp <= end_timestamp:
                    filtered_candles.append(candle)

            except (ValueError, IndexError) as e:
                self.logger.warning(f"Invalid candle timestamp format: {candle}")
                continue

        # Sort candles by timestamp to ensure chronological order
        filtered_candles.sort(key=lambda x: int(x[0]))

        if filtered_candles:
            first_candle_time = datetime.datetime.fromtimestamp(
                int(filtered_candles[0][0]) / 1000, tz=datetime.timezone.utc
            )
            last_candle_time = datetime.datetime.fromtimestamp(
                int(filtered_candles[-1][0]) / 1000, tz=datetime.timezone.utc
            )

            self.logger.debug(
                f"Filtered {len(filtered_candles)}/{len(candles)} candles: "
                f"{first_candle_time.strftime('%H:%M')} to {last_candle_time.strftime('%H:%M')}"
            )

        return filtered_candles

    def _calculate_time_ranges(self, start_date: str, end_date: str) -> List[Tuple[int, int]]:
        """
        Calculate time ranges for data collection using smart chunking.

        Args:
            start_date: Start date string
            end_date: End date string

        Returns:
            List of (start_timestamp, end_timestamp) tuples
        """
        start_timestamp = self._convert_datetime_to_timestamp(start_date)
        end_timestamp = self._convert_datetime_to_timestamp(end_date)

        # Use new smart chunking logic
        return self._calculate_smart_chunks(start_timestamp, end_timestamp)

    def collect_symbol_data(self, symbol: str, start_date: str, end_date: str) -> int:
        """
        Collect historical data for a specific symbol with precise time filtering.

        Args:
            symbol: Trading pair symbol
            start_date: Start date string
            end_date: End date string

        Returns:
            Number of candles collected
        """
        self.logger.info(f"Starting data collection for {symbol} from {start_date} to {end_date}")

        # Get target time range for filtering
        target_start_timestamp = self._convert_datetime_to_timestamp(start_date)
        target_end_timestamp = self._convert_datetime_to_timestamp(end_date)

        # Calculate smart chunks
        time_ranges = self._calculate_time_ranges(start_date, end_date)
        total_ranges = len(time_ranges)
        candles_collected = 0
        all_collected_candles = []  # Collect all candles for final filtering

        # Create progress bar for chunks
        pbar = tqdm(
            time_ranges,
            desc=f"Collecting {symbol}",
            unit="chunk",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

        for i, (chunk_start, chunk_end) in enumerate(pbar, 1):
            try:
                # Convert timestamps back to readable format for logging
                start_dt = datetime.datetime.fromtimestamp(chunk_start / 1000, tz=datetime.timezone.utc)
                end_dt = datetime.datetime.fromtimestamp(chunk_end / 1000, tz=datetime.timezone.utc)

                # Log chunk collection
                self.logger.debug(
                    f"Collecting {symbol} chunk {i}/{total_ranges}: "
                    f"{start_dt.strftime('%Y-%m-%d %H:%M')} to {end_dt.strftime('%Y-%m-%d %H:%M')}"
                )

                # Fetch candles for this chunk
                chunk_candles = self._fetch_candles_batch(symbol, chunk_start, chunk_end)

                if chunk_candles:
                    # Add to collection for final processing
                    all_collected_candles.extend(chunk_candles)

                    # Update progress bar
                    pbar.set_postfix(
                        {"raw_candles": f"{len(all_collected_candles):,}", "chunk_size": f"{len(chunk_candles)}"}
                    )

                    self.logger.debug(f"Fetched {len(chunk_candles)} candles from chunk {i}")

                # Reset consecutive errors on success
                self.consecutive_errors = 0

                # Small delay between chunks
                batch_delay = self.config.get("exchange", {}).get("rate_limit", {}).get("batch_delay", 0.5)
                time.sleep(batch_delay)

            except Exception as e:
                self.errors_count += 1
                self.consecutive_errors += 1

                max_consecutive = self.config.get("monitoring", {}).get("max_consecutive_errors", 5)
                if self.consecutive_errors >= max_consecutive:
                    self.logger.error(f"Too many consecutive errors ({self.consecutive_errors}). Stopping.")
                    raise

                retry_delay = self.config.get("exchange", {}).get("retry_delay", 1)
                self.logger.warning(f"Error in chunk {i}: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

                # Retry the same chunk
                continue

        # Close progress bar
        pbar.close()

        # Now filter all collected candles by exact target time range
        if all_collected_candles:
            self.logger.info(f"Filtering {len(all_collected_candles)} raw candles to exact time range")

            # Apply precise time filtering
            filtered_candles = self._filter_candles_by_timerange(
                all_collected_candles, target_start_timestamp, target_end_timestamp
            )

            if filtered_candles:
                # Remove duplicates based on timestamp (in case of chunk overlaps)
                seen_timestamps = set()
                unique_candles = []
                for candle in filtered_candles:
                    timestamp = int(candle[0])
                    if timestamp not in seen_timestamps:
                        seen_timestamps.add(timestamp)
                        unique_candles.append(candle)

                # Save filtered and deduplicated candles to database
                if unique_candles:
                    candles_collected = self.candle_manager.insert_candles_batch(
                        unique_candles, symbol, self.config["collection"]["interval"]
                    )
                    self.total_candles_collected += candles_collected

                    # Log filtering results
                    self.logger.info(
                        f"Final results for {symbol}: "
                        f"{len(all_collected_candles)} raw → "
                        f"{len(filtered_candles)} filtered → "
                        f"{len(unique_candles)} unique → "
                        f"{candles_collected} saved to DB"
                    )

                    if len(unique_candles) > 0:
                        first_candle_time = datetime.datetime.fromtimestamp(
                            int(unique_candles[0][0]) / 1000, tz=datetime.timezone.utc
                        )
                        last_candle_time = datetime.datetime.fromtimestamp(
                            int(unique_candles[-1][0]) / 1000, tz=datetime.timezone.utc
                        )
                        self.logger.info(
                            f"Time range saved: {first_candle_time.strftime('%Y-%m-%d %H:%M')} to "
                            f"{last_candle_time.strftime('%Y-%m-%d %H:%M')}"
                        )
                else:
                    self.logger.warning(f"No unique candles found for {symbol} after deduplication")
            else:
                self.logger.warning(f"No candles found for {symbol} in target time range")
        else:
            self.logger.warning(f"No candles fetched for {symbol}")

        self.logger.info(f"Completed {symbol}: {candles_collected} candles collected")
        return candles_collected

    def _collect_symbol_smart(self, symbol: str, missing_periods: List[Tuple[int, int]]) -> int:
        """
        Collect data for a symbol using smart collection (only missing periods).
        Now uses the improved chunking and filtering logic.

        Args:
            symbol: Trading pair symbol
            missing_periods: List of (start_timestamp, end_timestamp) tuples to collect

        Returns:
            Number of candles collected
        """
        if not missing_periods:
            self.logger.info(f"✅ {symbol}: All data already exists - skipping")
            return 0

        total_candles_collected = 0

        self.logger.info(f"📊 {symbol}: Collecting {len(missing_periods)} missing periods using improved logic")

        # Process each missing period using the improved chunking logic
        for period_num, (period_start, period_end) in enumerate(missing_periods, 1):
            try:
                # Convert timestamps to datetime strings for the improved collect_symbol_data method
                start_dt = datetime.datetime.fromtimestamp(period_start / 1000, tz=datetime.timezone.utc)
                end_dt = datetime.datetime.fromtimestamp(period_end / 1000, tz=datetime.timezone.utc)

                start_date_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_date_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

                self.logger.info(
                    f"Processing missing period {period_num}/{len(missing_periods)}: "
                    f"{start_date_str} to {end_date_str}"
                )

                # Use the improved collect_symbol_data method for this period
                # But ensure we use the original target time range, not the detected missing period
                # This prevents extending beyond the user-requested range
                period_candles = self.collect_symbol_data(symbol, start_date_str, end_date_str)
                total_candles_collected += period_candles

                self.logger.info(f"Completed period {period_num}: {period_candles} candles collected")

            except Exception as e:
                self.logger.error(f"Error processing missing period {period_num}: {e}")
                self.errors_count += 1
                # Continue with next period instead of stopping
                continue

        self.logger.info(f"Completed {symbol} smart collection: {total_candles_collected} candles collected")
        return total_candles_collected

    def collect_all_symbols(self) -> Dict[str, int]:
        """
        Collect data for all configured symbols using smart collection logic.

        Returns:
            Dictionary with symbol -> candles_collected mapping
        """
        if not self.bybit_client:
            self._init_bybit_client()

        self.start_time = time.time()
        collection_config = self.config.get("collection", {})

        symbols = collection_config.get("symbols", [])
        smart_collection = collection_config.get("smart_collection", True)
        force_overwrite = collection_config.get("force_overwrite", False)
        mode = collection_config.get("mode", "smart_gaps")

        if not symbols:
            raise ValueError("No symbols configured for collection")

        # Parse and validate collection period using TimeManager
        try:
            start_timestamp, end_timestamp, start_dt, end_dt = self.time_manager.get_collection_period()
        except ValueError as e:
            self.logger.error(f"Invalid collection period configuration: {e}")
            raise

        # Validate large collections and show summary
        validation = self.time_manager.validate_large_collection(start_dt, end_dt)
        summary = self.time_manager.format_period_summary(start_dt, end_dt, symbols, validation)

        # Log the collection summary
        for line in summary.split("\n"):
            self.logger.info(line)

        # Show warnings if any
        if validation["warnings"]:
            for warning in validation["warnings"]:
                self.logger.warning(warning)

        results = {}

        # Determine collection mode based on configuration
        if force_overwrite:
            self.logger.info("Mode: FORCE OVERWRITE - All data will be rewritten")
            collection_mode = "force_overwrite"
        elif mode == "full_range":
            self.logger.info("Mode: FULL RANGE - Collect entire specified period (fill all gaps)")
            collection_mode = "full_range"
        elif smart_collection:
            self.logger.info("Mode: SMART COLLECTION - Only missing periods will be collected")
            collection_mode = "smart"
        else:
            self.logger.info("Mode: STANDARD COLLECTION - Duplicate filtering enabled")
            collection_mode = "standard"

        # Create collection plan
        if collection_mode in ["smart", "full_range"]:
            self.logger.info("Analyzing existing data and creating collection plan...")

            if collection_mode == "full_range":
                # For full_range mode, always include the entire period
                collection_plan = {}
                for symbol in symbols:
                    # Get missing periods but ensure we cover the full range
                    missing_periods = self.candle_manager.find_missing_periods(
                        symbol, start_timestamp, end_timestamp, collection_config.get("interval", "1")
                    )
                    # If no missing periods found, it means data might exist but not cover full range
                    # Force include the entire range to ensure complete coverage
                    if not missing_periods:
                        missing_periods = [(start_timestamp, end_timestamp)]
                    collection_plan[symbol] = missing_periods
            else:
                # Standard smart collection
                collection_plan = self.candle_manager.get_collection_plan(
                    symbols, start_timestamp, end_timestamp, collection_config.get("interval", "1")
                )

            # Log collection plan summary
            total_missing_periods = sum(len(periods) for periods in collection_plan.values())
            if total_missing_periods == 0 and collection_mode == "smart":
                self.logger.info("✅ All data already exists - nothing to collect!")
                return {symbol: 0 for symbol in symbols}
            else:
                self.logger.info(f"📋 Collection plan: {total_missing_periods} missing periods to collect")
                for symbol, periods in collection_plan.items():
                    if periods:
                        period_details = []
                        for start_ts, end_ts in periods:
                            start_dt = self.time_manager.timestamp_to_datetime(start_ts)
                            end_dt = self.time_manager.timestamp_to_datetime(end_ts)
                            expected_candles = self.time_manager.calculate_expected_candles(start_ts, end_ts)
                            period_details.append(
                                f"{start_dt.strftime('%m-%d %H:%M')} to {end_dt.strftime('%m-%d %H:%M')} ({expected_candles:,} candles)"
                            )
                        self.logger.debug(f"  {symbol}: {len(periods)} periods - {'; '.join(period_details)}")

        for i, symbol in enumerate(symbols, 1):
            try:
                self.logger.info(f"Processing symbol {i}/{len(symbols)}: {symbol}")

                if collection_mode in ["smart", "full_range"]:
                    # Collect using smart/full_range collection plan
                    periods = collection_plan.get(symbol, [])
                    if periods:
                        self.logger.info(f"📊 {symbol}: Collecting {len(periods)} missing periods")
                        candles_count = self._collect_symbol_smart(symbol, periods)
                    else:
                        self.logger.info(f"✅ {symbol}: No missing periods found")
                        candles_count = 0
                else:
                    # Use standard collection (with or without force overwrite)
                    candles_count = self.collect_symbol_data(
                        symbol,
                        self.time_manager.timestamp_to_datetime(start_timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                        self.time_manager.timestamp_to_datetime(end_timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                    )

                results[symbol] = candles_count

                # Log total progress
                elapsed_time = time.time() - self.start_time
                avg_time_per_symbol = elapsed_time / i
                remaining_symbols = len(symbols) - i
                estimated_remaining = avg_time_per_symbol * remaining_symbols

                self.logger.info(
                    f"Completed {symbol}: {candles_count} candles. "
                    f"Remaining: {remaining_symbols} symbols, "
                    f"~{estimated_remaining/60:.1f} minutes"
                )

            except Exception as e:
                self.logger.error(f"Failed to collect data for {symbol}: {e}")
                results[symbol] = 0

                if self.config.get("monitoring", {}).get("stop_on_critical_error", True):
                    raise

        # Final summary with detailed statistics
        total_time = time.time() - self.start_time
        total_candles = sum(results.values())
        expected_candles_per_symbol = validation["estimated_candles"]
        expected_total = expected_candles_per_symbol * len(symbols)

        self.logger.info("=" * 80)
        self.logger.info("COLLECTION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Mode: {collection_mode.upper()}")
        self.logger.info(f"Period: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')} UTC")
        self.logger.info(f"Duration: {validation['period_days']:.2f} days")
        self.logger.info(f"Total symbols: {len(symbols)}")
        self.logger.info(f"Total candles collected: {total_candles:,}")
        self.logger.info(f"Expected candles: {expected_total:,}")

        # Collection efficiency
        if expected_total > 0:
            completion_rate = (
                (total_candles / expected_total) * 100 if collection_mode != "smart" else "N/A (smart mode)"
            )
            if isinstance(completion_rate, float):
                self.logger.info(f"Collection completion: {completion_rate:.1f}%")

        self.logger.info(f"Total time: {total_time/60:.1f} minutes")
        if total_time > 0:
            self.logger.info(f"Average speed: {total_candles/(total_time/60):.0f} candles/minute")
        self.logger.info(f"Total errors: {self.errors_count}")

        # Per-symbol breakdown
        self.logger.info("")
        self.logger.info("Per-symbol results:")
        for symbol, count in results.items():
            if collection_mode != "smart":
                symbol_completion = (
                    (count / expected_candles_per_symbol * 100) if expected_candles_per_symbol > 0 else 0
                )
                self.logger.info(f"  {symbol}: {count:,} candles ({symbol_completion:.1f}% of expected)")
            else:
                self.logger.info(f"  {symbol}: {count:,} candles")

        # Data quality validation (if enabled)
        validate_continuity = collection_config.get("validate_continuity", False)
        if validate_continuity and total_candles > 0:
            self.logger.info("")
            self.logger.info("Data continuity validation:")
            for symbol in symbols:
                if results[symbol] > 0:
                    try:
                        existing_range = self.candle_manager.get_existing_data_range(symbol)
                        if existing_range:
                            start_ts, end_ts = existing_range
                            actual_period_minutes = (end_ts - start_ts) / (1000 * 60)
                            actual_candles = self.candle_manager.count_candles(symbol)
                            continuity = (
                                (actual_candles / actual_period_minutes) * 100 if actual_period_minutes > 0 else 0
                            )
                            self.logger.info(
                                f"  {symbol}: {continuity:.1f}% data continuity ({actual_candles:,} candles)"
                            )
                    except Exception as e:
                        self.logger.warning(f"  {symbol}: Could not validate continuity - {e}")

        # Storage and performance summary
        actual_storage_mb = (total_candles * 200) / (1024 * 1024)  # ~200 bytes per candle
        self.logger.info("")
        self.logger.info(f"Estimated storage used: {actual_storage_mb:.2f} MB")
        if validation["estimated_size_mb"] > 0:
            storage_efficiency = (actual_storage_mb / (validation["estimated_size_mb"] * len(symbols))) * 100
            self.logger.info(f"Storage efficiency: {storage_efficiency:.1f}% of estimated")

        # Close database connections
        self.db_manager.close()

        return results


def main():
    """Main function for standalone execution."""
    try:
        collector = FuturesCollector()

        # Check if test mode is enabled
        if collector.config.get("advanced", {}).get("test_mode", False):
            print("Running in TEST MODE")
            test_days = collector.config.get("advanced", {}).get("test_days", 1)

            # Override config for testing
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=test_days)

            collector.config["collection"]["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S")
            collector.config["collection"]["end_date"] = end_date.strftime("%Y-%m-%d %H:%M:%S")
            collector.config["collection"]["symbols"] = ["BTCUSDT"]  # Only one symbol for testing

            print(
                f"Test range: {collector.config['collection']['start_date']} to {collector.config['collection']['end_date']}"
            )

        # Start collection
        results = collector.collect_all_symbols()

        print("\nCollection completed successfully!")
        print(f"Total candles: {sum(results.values()):,}")

        return True

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        return False


if __name__ == "__main__":
    import sys

    success = main()
    sys.exit(0 if success else 1)
