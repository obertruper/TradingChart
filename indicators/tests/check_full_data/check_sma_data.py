#!/usr/bin/env python3
"""
SMA Mathematical Data Validation Script

This script performs comprehensive mathematical validation of SMA (Simple Moving Average)
indicator data stored in the database. Unlike status check scripts that only verify
data presence, this validator recalculates SMA values from source candle data and
compares them with stored values to detect calculation errors, data corruption, and gaps.

Features:
- Mathematical correctness verification (recalculate and compare)
- Data completeness checking (no gaps in expected data)
- Warm-up period validation (first N-1 candles should be NULL)
- Support for all 3 timeframes (1m, 15m, 1h)
- Support for all configured symbols and periods
- Detailed error reporting with exact timestamps
- Batch processing for memory efficiency

Usage:
    # Full validation (all symbols, timeframes, periods)
    python3 check_sma_data.py

    # Single symbol
    python3 check_sma_data.py --symbol BTCUSDT

    # Single timeframe
    python3 check_sma_data.py --timeframe 1h

    # Last 30 days only
    python3 check_sma_data.py --days 30

    # Specific period
    python3 check_sma_data.py --period 50

    # Verbose output
    python3 check_sma_data.py --verbose
"""

import sys
import os
import argparse
import warnings
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
from decimal import Decimal

import psycopg2
import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm

# Suppress pandas SQLAlchemy warning
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def setup_logging(symbols: Optional[List[str]] = None,
                  timeframes: Optional[List[str]] = None,
                  periods: Optional[List[int]] = None,
                  days: Optional[int] = None) -> logging.Logger:
    """
    Настройка логирования для SMA validation с именем файла по параметрам.

    Args:
        symbols: Список символов для валидации
        timeframes: Список таймфреймов
        periods: Список SMA периодов
        days: Количество дней для проверки

    Returns:
        Настроенный logger
    """
    # Создаем директорию для логов
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Генерируем имя файла на основе параметров
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Определяем части имени файла
    symbol_part = ""
    if symbols and len(symbols) == 1:
        symbol_part = f"_{symbols[0]}"
    elif symbols and len(symbols) > 1:
        symbol_part = "_all-symbols"

    tf_part = ""
    if timeframes and len(timeframes) == 1:
        tf_part = f"_{timeframes[0]}"
    elif timeframes and len(timeframes) > 1:
        tf_part = "_all-tf"

    # Если оба пустые или multiple, используем "all"
    if not symbol_part and not tf_part:
        filename_base = "sma_validation_all"
    elif symbol_part and tf_part:
        filename_base = f"sma_validation{symbol_part}{tf_part}"
    else:
        filename_base = f"sma_validation{symbol_part}{tf_part}"

    log_filename = os.path.join(log_dir, f'{filename_base}_{timestamp}.log')

    # Настройка форматирования (стандарт проекта)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Файловый обработчик
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Создаем logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Очищаем существующие handlers (на случай повторного вызова)
    logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Логируем подтверждение настройки
    logger.info(f"📝 SMA Validation: Логирование настроено. Лог-файл: {log_filename}")

    return logger


class SMAValidator:
    """
    Comprehensive SMA data validator with mathematical correctness checking.
    """

    # Tolerance for floating point comparison (8 decimal places)
    TOLERANCE = Decimal('0.00000001')

    # Batch size for processing (days at a time)
    BATCH_DAYS = 30

    # Timeframe to minutes mapping
    TIMEFRAME_MINUTES = {
        '1m': 1,
        '15m': 15,
        '1h': 60
    }

    # Known data gaps (periods where source data is unavailable)
    # Format: {'symbol': [(start_date, end_date, reason), ...]}
    KNOWN_DATA_GAPS = {
        'BTCUSDT': [
            (
                datetime(2020, 3, 25, 10, 0, tzinfo=timezone.utc),
                datetime(2020, 4, 2, 19, 0, tzinfo=timezone.utc),
                'No 1m candle data available for aggregation (before initial data collection)'
            )
        ]
    }

    def __init__(self, config_path: str = None):
        """
        Initialize validator with configuration.

        Args:
            config_path: Path to indicators_config.yaml
        """
        if config_path is None:
            # Default path relative to this script
            indicators_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(indicators_dir, 'indicators_config.yaml')

        self.config_path = config_path
        self.config = self._load_config()
        self.conn = None
        self.logger = None  # Will be initialized in validate_all()
        self.errors = []
        self.warnings = []
        self.stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'total_records': 0,
            'error_records': 0,
            'total_rows_loaded': 0,
            'total_memory_mb': 0.0
        }

    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"❌ Error loading config from {self.config_path}: {e}")
            sys.exit(1)

    def connect(self):
        """Establish database connection."""
        try:
            db_config = self.config['database']
            self.conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            print(f"✅ Connected to database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_table_name(self, timeframe: str) -> str:
        """
        Get indicators table name based on timeframe.

        Args:
            timeframe: '1m', '15m', or '1h'

        Returns:
            Indicators table name

        Note:
            Candles are always fetched from candles_bybit_futures_1m
            and aggregated if needed
        """
        indicators_table = f'indicators_bybit_futures_{timeframe}'
        return indicators_table

    def fetch_data(
        self,
        symbol: str,
        timeframe: str,
        period: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        verbose: bool = False
    ) -> pd.DataFrame:
        """
        Fetch candle data and stored SMA values for validation.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            timeframe: '1m', '15m', or '1h'
            period: SMA period (e.g., 10, 30, 50, 100, 200)
            start_date: Start date for data (None = from beginning)
            end_date: End date for data (None = until now)
            verbose: Show detailed loading messages

        Returns:
            DataFrame with columns: timestamp, close, sma_stored
        """
        if verbose:
            date_range = ""
            if start_date:
                date_range = f" from {start_date.date()}"
            if end_date:
                date_range += f" to {end_date.date()}"
            msg = f"   📥 Loading data: {symbol} {timeframe} SMA_{period}{date_range}"
            print(msg)
            if self.logger:
                self.logger.info(f"📥 Загрузка данных: {symbol} {timeframe} SMA_{period}{date_range}")

        indicators_table = self.get_table_name(timeframe)
        sma_column = f'sma_{period}'
        minutes = self.TIMEFRAME_MINUTES[timeframe]

        # For aggregated timeframes, save original start_date before adjustment
        original_start_date = start_date

        # For aggregated timeframes, we need to load extra 1m candles BEFORE aggregation
        # to ensure the first aggregated candle has complete source data.
        #
        # Example: To create 15m candle at 10:30, we need 1m candles from 10:15-10:29.
        # If we filter 1m candles with timestamp >= 10:30, the aggregated candle is incomplete.
        #
        # Solution: Subtract one timeframe period from start_date for 1m candle loading,
        # then filter aggregated results back to original start_date.
        if timeframe != '1m' and start_date is not None:
            start_date = start_date - timedelta(minutes=minutes)

        # Build parameters list (now with adjusted start_date for aggregated timeframes)
        params = [symbol]

        # Add date parameters if specified
        if start_date:
            params.append(start_date)
        if end_date:
            params.append(end_date)

        try:
            # For 1m timeframe: direct query
            if timeframe == '1m':
                # Build date filter for 1m query (with c. prefix)
                date_filter = ""
                if start_date:
                    date_filter += " AND c.timestamp >= %s"
                if end_date:
                    date_filter += " AND c.timestamp <= %s"

                query = f"""
                    SELECT
                        c.timestamp,
                        c.close,
                        i.{sma_column} as sma_stored
                    FROM candles_bybit_futures_1m c
                    LEFT JOIN {indicators_table} i
                        ON c.timestamp = i.timestamp AND c.symbol = i.symbol
                    WHERE c.symbol = %s
                        {date_filter}
                    ORDER BY c.timestamp ASC
                """
            # For 15m and 1h: aggregate from 1m data
            else:
                # Build date filter for aggregation query (no prefix in CTE)
                # start_date is already adjusted to load extra 1m candles
                date_filter = ""
                if start_date:
                    date_filter += " AND timestamp >= %s"
                if end_date:
                    date_filter += " AND timestamp <= %s"

                # ВАЖНО: Timestamp = КОНЕЦ периода (не начало!)
                # Пример для 1h: timestamp 15:00 содержит данные 14:00-14:59
                # Пример для 15m: timestamp 15:15 содержит данные 15:00-15:14
                # FIX: 2025-11-10 - исправлен timestamp offset bug
                query = f"""
                    WITH aggregated_candles AS (
                        SELECT
                            date_trunc('hour', timestamp) + INTERVAL '{minutes} minutes' as period_end,
                            close,
                            symbol,
                            timestamp as original_timestamp
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                            {date_filter}
                    ),
                    last_in_period AS (
                        SELECT DISTINCT ON (period_end, symbol)
                            period_end as timestamp,
                            symbol,
                            close
                        FROM aggregated_candles
                        ORDER BY period_end, symbol, original_timestamp DESC
                    )
                    SELECT
                        c.timestamp,
                        c.close,
                        i.{sma_column} as sma_stored
                    FROM last_in_period c
                    LEFT JOIN {indicators_table} i
                        ON c.timestamp = i.timestamp AND c.symbol = i.symbol
                    ORDER BY c.timestamp ASC
                """

            df = pd.read_sql_query(query, self.conn, params=params)

            # Calculate and update statistics
            if not df.empty:
                memory_bytes = df.memory_usage(deep=True).sum()
                memory_mb = memory_bytes / (1024 * 1024)

                # Update global stats
                self.stats['total_rows_loaded'] += len(df)
                self.stats['total_memory_mb'] += memory_mb

                if verbose:
                    msg = f"      ✓ Loaded {len(df):,} rows, {memory_mb:.2f} MB"
                    print(msg)
                    if self.logger:
                        self.logger.info(f"   ✓ Загружено: {len(df):,} строк, {memory_mb:.2f} MB")

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            # For aggregated timeframes: filter back to original start_date
            # (we loaded extra 1m candles for complete aggregation)
            if timeframe != '1m' and original_start_date is not None:
                df = df[df.index >= original_start_date]

            # Convert close and sma_stored to Decimal for precise comparison
            df['close'] = df['close'].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)
            df['sma_stored'] = df['sma_stored'].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)

            return df
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            return pd.DataFrame()

    def calculate_sma(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculate SMA locally using pandas rolling mean.

        Args:
            df: DataFrame with 'close' column
            period: SMA period

        Returns:
            Series with calculated SMA values (Decimal type)
        """
        # Convert to float for pandas calculation, then back to Decimal
        close_float = df['close'].apply(lambda x: float(x) if x is not None else np.nan)
        sma_float = close_float.rolling(window=period, min_periods=period).mean()

        # Convert back to Decimal for precise comparison
        sma_decimal = sma_float.apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)

        return sma_decimal

    def is_in_known_gap(self, symbol: str, timestamp: datetime) -> Tuple[bool, str]:
        """
        Check if timestamp falls within a known data gap.

        Args:
            symbol: Trading pair symbol
            timestamp: Timestamp to check

        Returns:
            Tuple of (is_in_gap, reason)
        """
        if symbol not in self.KNOWN_DATA_GAPS:
            return False, ""

        for start_date, end_date, reason in self.KNOWN_DATA_GAPS[symbol]:
            # Ensure timezone-aware comparison
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            if start_date <= timestamp < end_date:
                return True, reason

        return False, ""

    def validate_period(
        self,
        symbol: str,
        timeframe: str,
        period: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        verbose: bool = False
    ) -> Dict:
        """
        Validate SMA data for a specific symbol, timeframe, and period.

        Args:
            symbol: Trading pair symbol
            timeframe: '1m', '15m', or '1h'
            period: SMA period
            start_date: Start date for validation (None = from beginning)
            end_date: End date for validation (None = until now)
            verbose: Print detailed progress

        Returns:
            Dictionary with validation results
        """
        result = {
            'symbol': symbol,
            'timeframe': timeframe,
            'period': period,
            'total_records': 0,
            'valid_records': 0,
            'null_records': 0,
            'mismatch_records': 0,
            'errors': [],
            'passed': True
        }

        if verbose:
            print(f"\n{'='*80}")
            print(f"  Validating: {symbol} - {timeframe} - SMA_{period}")
            print(f"{'='*80}")

        # Calculate lookback period for fetching extra data
        # Need (period * lookback_multiplier) extra candles for warm-up
        lookback_multiplier = 2
        fetch_start_date = start_date

        if start_date is not None:
            # Add lookback period to start_date
            minutes = self.TIMEFRAME_MINUTES[timeframe]
            lookback_candles = period * lookback_multiplier
            lookback_minutes = lookback_candles * minutes
            fetch_start_date = start_date - timedelta(minutes=lookback_minutes)

        # Fetch data (with lookback if start_date specified)
        df = self.fetch_data(symbol, timeframe, period, fetch_start_date, end_date, verbose)

        if df.empty:
            result['errors'].append(f"No data found for {symbol} {timeframe}")
            result['passed'] = False
            return result

        # Calculate SMA locally (on full dataset including lookback)
        df['sma_calculated'] = self.calculate_sma(df, period)

        # Filter to requested date range (remove lookback data from validation)
        if start_date is not None:
            df = df[df.index >= start_date]

        if df.empty:
            result['errors'].append(f"No data in requested date range")
            result['passed'] = False
            return result

        result['total_records'] = len(df)

        # Warm-up period check: only if validating from beginning of history
        # If start_date is specified, we're checking a subset, so skip warm-up validation
        warmup_period = period - 1
        if start_date is None:
            warmup_df = df.head(warmup_period)
            invalid_warmup = warmup_df[warmup_df['sma_stored'].notna()]

            if not invalid_warmup.empty:
                for timestamp in invalid_warmup.index:
                    error_msg = (
                        f"Warm-up period violation at {timestamp}: "
                        f"SMA should be NULL (within first {warmup_period} candles)"
                    )
                    result['errors'].append(error_msg)
                    result['passed'] = False

        # Validate all records (or after warm-up if full history)
        validation_df = df.iloc[warmup_period:] if start_date is None else df

        for timestamp, row in validation_df.iterrows():
            sma_stored = row['sma_stored']
            sma_calculated = row['sma_calculated']

            # Check if this timestamp is in a known data gap
            is_gap, gap_reason = self.is_in_known_gap(symbol, timestamp)

            # Case 1: Both are None (unexpected after warm-up)
            if pd.isna(sma_stored) and pd.isna(sma_calculated):
                if not is_gap:
                    result['null_records'] += 1
                    error_msg = f"Missing data at {timestamp}: Both stored and calculated SMA are NULL"
                    result['errors'].append(error_msg)
                    result['passed'] = False
                    result['mismatch_records'] += 1
                # else: skip - known gap, not an error

            # Case 2: Stored is None but should have value
            elif pd.isna(sma_stored) and pd.notna(sma_calculated):
                if not is_gap:
                    result['null_records'] += 1
                    error_msg = (
                        f"Missing stored value at {timestamp}: "
                        f"Expected {sma_calculated:.8f}, Found NULL"
                    )
                    result['errors'].append(error_msg)
                    result['passed'] = False
                    result['mismatch_records'] += 1
                # else: skip - known gap, expected behavior

            # Case 3: Has stored value but shouldn't
            elif pd.notna(sma_stored) and pd.isna(sma_calculated):
                if not is_gap:
                    error_msg = (
                        f"Unexpected stored value at {timestamp}: "
                        f"Found {sma_stored:.8f}, Should be NULL"
                    )
                    result['errors'].append(error_msg)
                    result['passed'] = False
                    result['mismatch_records'] += 1
                # else: skip - known gap, expected behavior

            # Case 4: Both have values - compare
            elif pd.notna(sma_stored) and pd.notna(sma_calculated):
                diff = abs(sma_stored - sma_calculated)

                if diff > self.TOLERANCE:
                    if not is_gap:
                        error_msg = (
                            f"Calculation mismatch at {timestamp}: "
                            f"Expected {sma_calculated:.8f}, "
                            f"Found {sma_stored:.8f}, "
                            f"Δ = {diff:.10f}"
                        )
                        result['errors'].append(error_msg)
                        result['passed'] = False
                        result['mismatch_records'] += 1
                    # else: skip - known gap, not an error
                else:
                    result['valid_records'] += 1

        # Update global stats
        self.stats['total_checks'] += 1
        self.stats['total_records'] += result['total_records']
        self.stats['error_records'] += result['mismatch_records']

        if result['passed']:
            self.stats['passed_checks'] += 1
        else:
            self.stats['failed_checks'] += 1

        return result

    def validate_all(
        self,
        symbols: Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
        periods: Optional[List[int]] = None,
        days: Optional[int] = None,
        verbose: bool = False
    ) -> List[Dict]:
        """
        Validate SMA data across multiple symbols, timeframes, and periods.

        Args:
            symbols: List of symbols to validate (None = all from config)
            timeframes: List of timeframes to validate (None = all from config)
            periods: List of periods to validate (None = all from config)
            days: Number of recent days to validate (None = all history)
            verbose: Print detailed progress

        Returns:
            List of validation results
        """
        # Use config defaults if not specified
        if symbols is None:
            symbols = self.config['symbols']
        if timeframes is None:
            timeframes = self.config['timeframes']
        if periods is None:
            periods = self.config['indicators']['sma']['periods']

        # Initialize logger with parameters
        self.logger = setup_logging(symbols, timeframes, periods, days)

        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days) if days else None

        results = []
        total_validations = len(symbols) * len(timeframes) * len(periods)

        # Log configuration
        self.logger.info(f"📋 Конфигурация загружена из {self.config_path}")
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("🔍 ЗАПУСК SMA VALIDATION - Проверка математической корректности SMA индикаторов")
        self.logger.info("=" * 80)
        self.logger.info("")
        self.logger.info(f"📅 Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"🎯 Символы: {', '.join(symbols)} ({len(symbols)} симв.)")
        self.logger.info(f"📊 Таймфреймы: {', '.join(timeframes)} ({len(timeframes)} тф.)")
        self.logger.info(f"💹 SMA Периоды: {periods}")
        if days:
            self.logger.info(f"📅 Диапазон дат: Последние {days} дней ({start_date.date()} - {end_date.date()})")
        else:
            self.logger.info(f"📅 Диапазон дат: Вся история")
        self.logger.info(f"🔢 Всего проверок: {total_validations} ({len(symbols)} × {len(timeframes)} × {len(periods)})")
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("")

        print(f"\n{'='*80}")
        print(f"  SMA MATHEMATICAL VALIDATION")
        print(f"{'='*80}")
        print(f"Symbols: {len(symbols)} | Timeframes: {len(timeframes)} | Periods: {len(periods)}")
        print(f"Total validations: {total_validations}")
        if days:
            print(f"Date range: Last {days} days ({start_date.date()} to {end_date.date()})")
        else:
            print(f"Date range: Full history")
        print(f"{'='*80}\n")

        # Progress bar
        with tqdm(total=total_validations, desc="Validation Progress", unit="check") as pbar:
            for symbol in symbols:
                for timeframe in timeframes:
                    for period in periods:
                        result = self.validate_period(
                            symbol, timeframe, period,
                            start_date, end_date, verbose
                        )
                        results.append(result)
                        pbar.update(1)

                        # Update progress bar description with current validation
                        status = "✅" if result['passed'] else "❌"
                        pbar.set_postfix_str(f"{status} {symbol} {timeframe} SMA_{period}")

        return results

    def print_report(self, results: List[Dict], max_errors: int = 10):
        """
        Print comprehensive validation report.

        Args:
            results: List of validation results from validate_all()
            max_errors: Maximum number of errors to display per check
        """
        print(f"\n{'='*80}")
        print(f"  VALIDATION REPORT")
        print(f"{'='*80}\n")

        # Display known data gaps information
        if self.KNOWN_DATA_GAPS:
            print(f"ℹ️  Known Data Gaps (excluded from validation):")
            for symbol, gaps in self.KNOWN_DATA_GAPS.items():
                for start_date, end_date, reason in gaps:
                    print(f"   {symbol}: {start_date.strftime('%Y-%m-%d %H:%M')} → {end_date.strftime('%Y-%m-%d %H:%M')}")
                    print(f"      Reason: {reason}")
            print()

        # Group results by status
        passed = [r for r in results if r['passed']]
        failed = [r for r in results if not r['passed']]

        if len(results) > 0:
            print(f"✅ Passed: {len(passed)}/{len(results)} ({len(passed)/len(results)*100:.1f}%)")
            print(f"❌ Failed: {len(failed)}/{len(results)} ({len(failed)/len(results)*100:.1f}%)\n")
        else:
            print(f"⚠️  No results to display\n")

        # Display failed validations
        if failed:
            print(f"{'='*80}")
            print(f"  FAILED VALIDATIONS")
            print(f"{'='*80}\n")

            # Log to file: Full details with ALL errors
            if self.logger:
                self.logger.info("")
                self.logger.info("=" * 80)
                self.logger.info("❌ ДЕТАЛЬНЫЙ СПИСОК ОШИБОК ВАЛИДАЦИИ")
                self.logger.info("=" * 80)
                self.logger.info("")

                # Log known data gaps
                if self.KNOWN_DATA_GAPS:
                    self.logger.info("ℹ️  Known Data Gaps (excluded from validation):")
                    for symbol, gaps in self.KNOWN_DATA_GAPS.items():
                        for start_date, end_date, reason in gaps:
                            self.logger.info(f"   {symbol}: {start_date.strftime('%Y-%m-%d %H:%M')} → {end_date.strftime('%Y-%m-%d %H:%M')}")
                            self.logger.info(f"      Reason: {reason}")
                    self.logger.info("")

            for result in failed:
                # Console output: limited errors
                print(f"❌ {result['symbol']} - {result['timeframe']} - SMA_{result['period']}")
                print(f"   Total Records: {result['total_records']}")
                print(f"   Valid: {result['valid_records']}")
                print(f"   Mismatches: {result['mismatch_records']}")
                print(f"   NULL Issues: {result['null_records']}")

                # Show first N errors in console
                error_count = len(result['errors'])
                displayed_errors = result['errors'][:max_errors]

                print(f"   Errors ({error_count} total, showing {len(displayed_errors)}):")
                for error in displayed_errors:
                    print(f"      └─ {error}")

                if error_count > max_errors:
                    print(f"      └─ ... and {error_count - max_errors} more errors")

                print()

                # Log to file: ALL errors without limits
                if self.logger:
                    self.logger.info(f"❌ {result['symbol']} - {result['timeframe']} - SMA_{result['period']}")
                    self.logger.info(f"   📊 Всего записей: {result['total_records']:,}")
                    self.logger.info(f"   ✅ Корректных: {result['valid_records']:,}")
                    self.logger.info(f"   ❌ Ошибок: {error_count}")
                    if result['mismatch_records'] > 0:
                        self.logger.info(f"      └─ Calculation mismatches: {result['mismatch_records']}")
                    if result['null_records'] > 0:
                        self.logger.info(f"      └─ Missing stored values: {result['null_records']}")

                    if error_count > 0:
                        accuracy = (result['valid_records'] / result['total_records'] * 100) if result['total_records'] > 0 else 0
                        self.logger.info(f"   📈 Accuracy: {accuracy:.2f}%")
                        self.logger.info("")
                        self.logger.info(f"   Полный список ошибок ({error_count} total):")

                        # Log ALL errors to file
                        for idx, error in enumerate(result['errors'], 1):
                            self.logger.info(f"      [{idx}/{error_count}] {error}")

                    self.logger.info("")

        # Summary statistics
        print(f"{'='*80}")
        print(f"  SUMMARY STATISTICS")
        print(f"{'='*80}")
        print(f"Total Checks: {self.stats['total_checks']}")
        if self.stats['total_checks'] > 0:
            print(f"Passed Checks: {self.stats['passed_checks']} ({self.stats['passed_checks']/self.stats['total_checks']*100:.1f}%)")
            print(f"Failed Checks: {self.stats['failed_checks']} ({self.stats['failed_checks']/self.stats['total_checks']*100:.1f}%)")
        else:
            print(f"Passed Checks: 0")
            print(f"Failed Checks: 0")
        print(f"Total Records Validated: {self.stats['total_records']:,}")
        print(f"Error Records: {self.stats['error_records']:,}")
        if self.stats['total_records'] > 0:
            print(f"Error Rate: {self.stats['error_records']/self.stats['total_records']*100:.4f}%")
        print(f"\n📊 Data Loading Statistics:")
        print(f"Total Rows Loaded: {self.stats['total_rows_loaded']:,}")
        print(f"Total Memory Used: {self.stats['total_memory_mb']:.2f} MB")
        print(f"{'='*80}\n")

        # Log summary statistics to file
        if self.logger:
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info("📊 ИТОГОВАЯ СТАТИСТИКА ВАЛИДАЦИИ")
            self.logger.info("=" * 80)
            self.logger.info("")
            self.logger.info("Проверки:")
            self.logger.info(f"   🔢 Всего проверок: {self.stats['total_checks']}")
            if self.stats['total_checks'] > 0:
                self.logger.info(f"   ✅ Успешных: {self.stats['passed_checks']} ({self.stats['passed_checks']/self.stats['total_checks']*100:.1f}%)")
                self.logger.info(f"   ❌ С ошибками: {self.stats['failed_checks']} ({self.stats['failed_checks']/self.stats['total_checks']*100:.1f}%)")
            self.logger.info("")
            self.logger.info("Записи:")
            self.logger.info(f"   📊 Всего записей: {self.stats['total_records']:,}")
            self.logger.info(f"   ✅ Корректных: {self.stats['total_records'] - self.stats['error_records']:,}")
            self.logger.info(f"   ❌ С ошибками: {self.stats['error_records']:,}")
            if self.stats['total_records'] > 0:
                self.logger.info(f"   📈 Error Rate: {self.stats['error_records']/self.stats['total_records']*100:.4f}%")
            self.logger.info("")
            self.logger.info("Загрузка данных:")
            self.logger.info(f"   📥 Всего строк загружено: {self.stats['total_rows_loaded']:,}")
            self.logger.info(f"   💾 Использовано памяти: {self.stats['total_memory_mb']:.2f} MB")
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info("✅ ВАЛИДАЦИЯ ЗАВЕРШЕНА")
            self.logger.info("=" * 80)


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='SMA Mathematical Data Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full validation
  python3 check_sma_data.py

  # Single symbol
  python3 check_sma_data.py --symbol BTCUSDT

  # Single timeframe
  python3 check_sma_data.py --timeframe 1h

  # Last 30 days
  python3 check_sma_data.py --days 30

  # Specific period
  python3 check_sma_data.py --period 50

  # Combination
  python3 check_sma_data.py --symbol BTCUSDT --timeframe 1h --days 7 --verbose
        """
    )

    parser.add_argument('--symbol', type=str, help='Single symbol to validate (e.g., BTCUSDT)')
    parser.add_argument('--timeframe', type=str, choices=['1m', '15m', '1h'],
                        help='Single timeframe to validate')
    parser.add_argument('--period', type=int, choices=[10, 30, 50, 100, 200],
                        help='Single SMA period to validate')
    parser.add_argument('--days', type=int, help='Number of recent days to validate (default: all history)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--max-errors', type=int, default=10,
                        help='Maximum errors to display per check (default: 10)')

    args = parser.parse_args()

    # Initialize validator
    validator = SMAValidator()

    try:
        # Connect to database
        validator.connect()

        # Prepare parameters
        symbols = [args.symbol] if args.symbol else None
        timeframes = [args.timeframe] if args.timeframe else None
        periods = [args.period] if args.period else None

        # Run validation
        start_time = datetime.now()
        results = validator.validate_all(
            symbols=symbols,
            timeframes=timeframes,
            periods=periods,
            days=args.days,
            verbose=args.verbose
        )
        end_time = datetime.now()

        # Print report
        validator.print_report(results, max_errors=args.max_errors)

        # Print execution time
        elapsed = end_time - start_time
        print(f"Total execution time: {elapsed}\n")

        # Exit code based on results
        sys.exit(0 if validator.stats['failed_checks'] == 0 else 1)

    except KeyboardInterrupt:
        print("\n\n❌ Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        validator.disconnect()


if __name__ == '__main__':
    main()
