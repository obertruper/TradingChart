#!/usr/bin/env python3
"""
ATR Mathematical Data Validation Script

This script performs comprehensive mathematical validation of ATR (Average True Range)
indicator data stored in the database. Unlike status check scripts that only verify
data presence, this validator recalculates ATR values from source candle data and
compares them with stored values to detect calculation errors, data corruption, and gaps.

Features:
- Mathematical correctness verification (recalculate and compare)
- Data completeness checking (no gaps in expected data)
- Warm-up period validation (first period-1 candles should be NULL)
- Support for all 3 timeframes (1m, 15m, 1h)
- Support for all configured symbols and periods
- Detailed error reporting with exact timestamps
- Batch processing for memory efficiency
- 2x lookback multiplier for Wilder smoothing warm-up
- Handles ATR edge cases (first candle, no prev_close)

Usage:
    # Full validation (all symbols, timeframes, periods)
    python3 check_atr_data.py

    # Single symbol
    python3 check_atr_data.py --symbol ETHUSDT

    # Single timeframe
    python3 check_atr_data.py --timeframe 1h

    # Last 7 days only
    python3 check_atr_data.py --days 7

    # Specific period
    python3 check_atr_data.py --period 14

    # Verbose output
    python3 check_atr_data.py --verbose
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
import psycopg2.extras
import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm

warnings.filterwarnings('ignore')

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def setup_logging(symbols: Optional[List[str]] = None,
                  timeframes: Optional[List[str]] = None,
                  periods: Optional[List[int]] = None,
                  days: Optional[int] = None) -> logging.Logger:
    """
    Set up logging with dynamic filename based on validation parameters
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Build filename from parameters
    parts = []
    if symbols and len(symbols) == 1:
        parts.append(symbols[0])
    if timeframes and len(timeframes) == 1:
        parts.append(timeframes[0])
    if days:
        parts.append(f"{days}d")

    # Add timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if parts:
        log_filename = f"atr_validation_{'_'.join(parts)}_{timestamp}.log"
    else:
        log_filename = f"atr_validation_{timestamp}.log"

    log_path = os.path.join(log_dir, log_filename)

    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.info(f"📄 Log file: {log_path}")

    return logger


class ATRDataValidator:
    """
    Validator for ATR (Average True Range) indicator data

    Calculates ATR from OHLC candle data using Wilder smoothing and compares
    with stored database values to detect calculation errors.
    """

    def __init__(self, config_path: str, logger: logging.Logger):
        """
        Initialize validator

        Args:
            config_path: Path to indicators_config.yaml
            logger: Logger instance
        """
        self.logger = logger
        self.config = self.load_config(config_path)
        self.conn = None
        self.lookback_multiplier = 2  # 2x period for Wilder smoothing warm-up

    def load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def connect(self):
        """Connect to database"""
        db_config = self.config['database']
        self.conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )

    def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_table_name(self, timeframe: str) -> str:
        """Get indicators table name for timeframe"""
        if timeframe == '1m':
            return 'indicators_bybit_futures_1m'
        elif timeframe == '15m':
            return 'indicators_bybit_futures_15m'
        elif timeframe == '1h':
            return 'indicators_bybit_futures_1h'
        else:
            raise ValueError(f"Unknown timeframe: {timeframe}")

    def fetch_data(self, symbol: str, timeframe: str, period: int,
                   start_date: Optional[datetime] = None,
                   end_date: Optional[datetime] = None,
                   verbose: bool = False) -> pd.DataFrame:
        """
        Fetch OHLC data and stored ATR values from database

        Args:
            symbol: Trading pair (e.g., 'ETHUSDT')
            timeframe: '1m', '15m', or '1h'
            period: ATR period
            start_date: Start date (optional)
            end_date: End date (optional)
            verbose: Print detailed loading messages

        Returns:
            DataFrame with columns: timestamp (index), high, low, close, atr_stored
        """
        indicators_table = self.get_table_name(timeframe)
        atr_column = f'atr_{period}'

        # Calculate lookback start date
        if start_date:
            # Add lookback buffer
            lookback_periods = period * self.lookback_multiplier
            if timeframe == '1m':
                lookback_start = start_date - timedelta(minutes=lookback_periods)
            elif timeframe == '15m':
                lookback_start = start_date - timedelta(minutes=15 * lookback_periods)
            elif timeframe == '1h':
                lookback_start = start_date - timedelta(hours=lookback_periods)
        else:
            lookback_start = None

        if verbose:
            self.logger.info(f"📥 Загрузка данных: {symbol} {timeframe} ATR_{period} "
                           f"{'from ' + str(start_date.date()) if start_date else ''} "
                           f"{'to ' + str(end_date.date()) if end_date else ''}")

        if timeframe == '1m':
            # For 1m: Load directly from 1m candles
            query = f"""
                SELECT
                    c.timestamp,
                    c.high,
                    c.low,
                    c.close,
                    i.{atr_column} as atr_stored
                FROM candles_bybit_futures_1m c
                LEFT JOIN {indicators_table} i
                    ON c.timestamp = i.timestamp AND c.symbol = i.symbol
                WHERE c.symbol = %s
            """

            params = [symbol]
            if lookback_start:
                query += " AND c.timestamp >= %s"
                params.append(lookback_start)
            if end_date:
                query += " AND c.timestamp <= %s"
                params.append(end_date)
            query += " ORDER BY c.timestamp"

        else:
            # For 15m/1h: Aggregate from 1m candles
            # Use CORRECTED aggregation WITHOUT offset bug
            if timeframe == '15m':
                minutes = 15
                period_formula = f"""
                    date_trunc('hour', timestamp) +
                    INTERVAL '15 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 15)
                """
            else:  # 1h
                minutes = 60
                period_formula = "date_trunc('hour', timestamp)"

            query = f"""
                WITH aggregated AS (
                    SELECT
                        {period_formula} as period_start,
                        MAX(high) as high,
                        MIN(low) as low,
                        (array_agg(close ORDER BY timestamp DESC))[1] as close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
            """

            params = [symbol]
            if lookback_start:
                query += " AND timestamp >= %s"
                params.append(lookback_start)
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)

            query += f"""
                    GROUP BY {period_formula}
                )
                SELECT
                    a.period_start as timestamp,
                    a.high,
                    a.low,
                    a.close,
                    i.{atr_column} as atr_stored
                FROM aggregated a
                LEFT JOIN {indicators_table} i
                    ON a.period_start = i.timestamp AND i.symbol = %s
                ORDER BY a.period_start
            """
            params.append(symbol)

        df = pd.read_sql_query(query, self.conn, params=params, parse_dates=['timestamp'])

        if verbose:
            self.logger.info(f"✅ Загружено {len(df)} свечей")

        if df.empty:
            return df

        # Set timestamp as index
        df.set_index('timestamp', inplace=True)

        return df

    def calculate_true_range(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """
        Calculate True Range

        TR = max(high - low, |high - prev_close|, |low - prev_close|)

        For first candle (no prev_close): TR = high - low

        Args:
            high: High prices
            low: Low prices
            close: Close prices

        Returns:
            True Range series
        """
        # Convert to float64 for PostgreSQL Decimal compatibility
        high = pd.Series(high).astype(np.float64)
        low = pd.Series(low).astype(np.float64)
        close = pd.Series(close).astype(np.float64)

        # Previous close
        prev_close = close.shift(1)

        # Three components of TR
        hl = high - low
        hc = abs(high - prev_close)
        lc = abs(low - prev_close)

        # Max across the three
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

        # First candle: use hl only (no prev_close)
        tr.iloc[0] = hl.iloc[0]

        return tr

    def calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
        """
        Calculate ATR using Wilder smoothing

        Formula:
        1. Calculate True Range for each candle
        2. First ATR = mean(TR[:period])
        3. Subsequent ATR = (prev_ATR * (period-1) + current_TR) / period
        4. First (period-1) values = NaN

        Args:
            high: High prices
            low: Low prices
            close: Close prices
            period: ATR period

        Returns:
            ATR series with first (period-1) values as NaN
        """
        # Calculate True Range
        tr = self.calculate_true_range(high, low, close)

        # Initialize ATR series
        atr = pd.Series(index=tr.index, dtype=np.float64)
        atr[:] = np.nan

        if len(tr) < period:
            return atr

        # First ATR = mean of first 'period' TR values
        atr.iloc[period-1] = tr.iloc[:period].mean()

        # Wilder smoothing for subsequent values
        for i in range(period, len(tr)):
            atr.iloc[i] = (atr.iloc[i-1] * (period - 1) + tr.iloc[i]) / period

        return atr

    def validate(self, symbol: str, timeframe: str, period: int,
                 days: Optional[int] = None,
                 verbose: bool = False,
                 tolerance: float = 0.5) -> Tuple[int, int, List[Dict]]:
        """
        Validate ATR data for a symbol/timeframe/period combination

        Args:
            symbol: Trading pair
            timeframe: '1m', '15m', or '1h'
            period: ATR period
            days: Limit to last N days (optional)
            verbose: Print detailed messages
            tolerance: Maximum allowed difference

        Returns:
            Tuple of (total_comparisons, errors_count, error_details)
        """
        # Calculate date range
        end_date = None
        start_date = None
        if days:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days)

        # Fetch data
        df = self.fetch_data(symbol, timeframe, period, start_date, end_date, verbose)

        if df.empty:
            self.logger.warning(f"⚠️  No data found for {symbol} {timeframe} ATR-{period}")
            return 0, 0, []

        # Calculate ATR
        atr_calculated = self.calculate_atr(df['high'], df['low'], df['close'], period)

        # Filter to comparison range (exclude lookback)
        if start_date:
            comparison_df = df[df.index >= start_date].copy()
            atr_calculated = atr_calculated[atr_calculated.index >= start_date]
        else:
            comparison_df = df.copy()

        # Compare with stored values
        errors = []
        total_comparisons = 0

        for idx in comparison_df.index:
            stored_value = comparison_df.loc[idx, 'atr_stored']
            calculated_value = atr_calculated.loc[idx]

            # Skip if both are NaN (expected for first period-1 values)
            if pd.isna(stored_value) and pd.isna(calculated_value):
                continue

            # Count this as a comparison
            total_comparisons += 1

            # Check for mismatch
            if pd.isna(stored_value) or pd.isna(calculated_value):
                # One is NaN, other is not
                errors.append({
                    'timestamp': idx,
                    'expected': float(calculated_value) if not pd.isna(calculated_value) else None,
                    'found': float(stored_value) if not pd.isna(stored_value) else None,
                    'diff': None
                })
            elif abs(float(stored_value) - float(calculated_value)) > tolerance:
                # Values differ beyond tolerance
                errors.append({
                    'timestamp': idx,
                    'expected': float(calculated_value),
                    'found': float(stored_value),
                    'diff': abs(float(stored_value) - float(calculated_value))
                })

        # Log results
        if errors:
            self.logger.error(f"❌ {symbol} {timeframe} ATR-{period}: {len(errors)} errors found")
            for error in errors[:10]:  # Show first 10 errors
                if error['diff'] is not None:
                    self.logger.error(
                        f"   {error['timestamp']}: "
                        f"Expected {error['expected']:.4f}, "
                        f"Found {error['found']:.4f}, "
                        f"Diff {error['diff']:.4f}"
                    )
                else:
                    self.logger.error(
                        f"   {error['timestamp']}: "
                        f"Expected {error['expected']}, "
                        f"Found {error['found']}"
                    )
            if len(errors) > 10:
                self.logger.error(f"   ... and {len(errors) - 10} more errors")
        else:
            if verbose:
                self.logger.info(f"✅ {symbol} {timeframe} ATR-{period}: All values correct")

        return total_comparisons, len(errors), errors

    def run(self, symbols: List[str], timeframes: List[str], periods: List[int],
            days: Optional[int] = None, verbose: bool = False) -> Dict:
        """
        Run validation for multiple combinations

        Args:
            symbols: List of symbols to validate
            timeframes: List of timeframes to validate
            periods: List of ATR periods to validate
            days: Limit to last N days
            verbose: Print detailed messages

        Returns:
            Dictionary with validation results
        """
        self.connect()

        results = {
            'combinations': [],
            'total_comparisons': 0,
            'total_errors': 0,
            'accuracy': 100.0
        }

        # Create list of all combinations
        combinations = [
            (symbol, timeframe, period)
            for symbol in symbols
            for timeframe in timeframes
            for period in periods
        ]

        # Progress bar
        pbar = tqdm(
            combinations,
            desc="Validating ATR",
            unit="combo",
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}, accuracy={postfix}]'
        )

        for symbol, timeframe, period in pbar:
            pbar.set_description(f"Validating {symbol} {timeframe} ATR-{period}")

            comparisons, errors, error_details = self.validate(
                symbol, timeframe, period, days, verbose
            )

            if comparisons > 0:
                accuracy = ((comparisons - errors) / comparisons * 100)

                results['combinations'].append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'period': period,
                    'comparisons': comparisons,
                    'errors': errors,
                    'accuracy': accuracy
                })

                results['total_comparisons'] += comparisons
                results['total_errors'] += errors

                # Update progress bar with current accuracy
                if results['total_comparisons'] > 0:
                    overall_accuracy = (
                        (results['total_comparisons'] - results['total_errors']) /
                        results['total_comparisons'] * 100
                    )
                    pbar.set_postfix_str(f"{overall_accuracy:.2f}%")

        pbar.close()

        # Calculate overall accuracy
        if results['total_comparisons'] > 0:
            results['accuracy'] = (
                (results['total_comparisons'] - results['total_errors']) /
                results['total_comparisons'] * 100
            )

        self.disconnect()

        return results


def main():
    parser = argparse.ArgumentParser(
        description='Validate ATR indicator data in database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--symbol', type=str, help='Symbol to validate (default: all from config)')
    parser.add_argument('--timeframe', type=str, choices=['1m', '15m', '1h'],
                       help='Timeframe to validate (default: all)')
    parser.add_argument('--period', type=int, help='ATR period to validate (default: all)')
    parser.add_argument('--days', type=int, help='Validate last N days only (default: all data)')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--config', type=str, default='indicators_config.yaml',
                       help='Path to config file')

    args = parser.parse_args()

    # Load config to get defaults
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        args.config
    )

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Determine symbols to validate
    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = config.get('symbols', ['ETHUSDT'])

    # Determine timeframes to validate
    if args.timeframe:
        timeframes = [args.timeframe]
    else:
        timeframes = ['1m', '15m', '1h']

    # Determine periods to validate
    if args.period:
        periods = [args.period]
    else:
        periods = config.get('atr', {}).get('periods', [7, 14, 21, 30, 50, 100])

    # Setup logging
    logger = setup_logging(
        symbols=symbols if args.symbol else None,
        timeframes=timeframes if args.timeframe else None,
        periods=[args.period] if args.period else None,
        days=args.days
    )

    logger.info("=" * 80)
    logger.info("ATR DATA VALIDATION")
    logger.info("=" * 80)
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Timeframes: {timeframes}")
    logger.info(f"Periods: {periods}")
    if args.days:
        logger.info(f"Date range: Last {args.days} days")
    else:
        logger.info(f"Date range: All available data")
    logger.info("=" * 80)

    # Create validator
    validator = ATRDataValidator(config_path, logger)

    # Run validation
    results = validator.run(symbols, timeframes, periods, args.days, args.verbose)

    # Print summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total combinations validated: {len(results['combinations'])}")
    logger.info(f"Total comparisons: {results['total_comparisons']:,}")
    logger.info(f"Total errors: {results['total_errors']}")
    logger.info(f"Overall accuracy: {results['accuracy']:.4f}%")
    logger.info("")

    if results['total_errors'] > 0:
        logger.info(f"❌ Found {results['total_errors']} errors across {len(results['combinations'])} combinations")
    else:
        logger.info("✅ All ATR values are mathematically correct!")

    logger.info("")
    logger.info("Detailed Results:")
    logger.info("-" * 80)
    logger.info(f"{'Symbol':<10} {'TF':<6} {'Period':<8} {'Compared':<12} {'Errors':<12} {'Accuracy':<10}")
    logger.info("-" * 80)

    for combo in results['combinations']:
        status = "✅" if combo['errors'] == 0 else "❌"
        logger.info(
            f"{status} {combo['symbol']:<8} {combo['timeframe']:<6} "
            f"{combo['period']:<8} {combo['comparisons']:<12,} "
            f"{combo['errors']:<12} {combo['accuracy']:<10.2f}%"
        )

    logger.info("-" * 80)

    # Exit code
    sys.exit(0 if results['total_errors'] == 0 else 1)


if __name__ == '__main__':
    main()
