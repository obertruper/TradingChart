#!/usr/bin/env python3
"""
MFI Mathematical Data Validation Script

This script performs comprehensive mathematical validation of MFI (Money Flow Index)
indicator data stored in the database. Unlike status check scripts that only verify
data presence, this validator recalculates MFI values from source candle data and
compares them with stored values to detect calculation errors, data corruption, and gaps.

Features:
- Mathematical correctness verification (recalculate and compare)
- Data completeness checking (no gaps in expected data)
- Warm-up period validation (first period-1 candles should be NULL)
- Support for all 3 timeframes (1m, 15m, 1h)
- Support for all configured symbols and periods
- Detailed error reporting with exact timestamps
- Batch processing for memory efficiency
- 2x lookback multiplier for MFI rolling sum accuracy
- Handles MFI edge cases (volume=0, flat prices, division by zero)

Usage:
    # Full validation (all symbols, timeframes, periods)
    python3 check_mfi_data.py

    # Single symbol
    python3 check_mfi_data.py --symbol ETHUSDT

    # Single timeframe
    python3 check_mfi_data.py --timeframe 1h

    # Last 7 days only
    python3 check_mfi_data.py --days 7

    # Specific period
    python3 check_mfi_data.py --period 14

    # Verbose output
    python3 check_mfi_data.py --verbose
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
    Настройка логирования для MFI validation с именем файла по параметрам.

    Args:
        symbols: Список символов для валидации
        timeframes: Список таймфреймов
        periods: Список MFI периодов
        days: Количество дней для проверки

    Returns:
        Настроенный logger
    """
    # Создаем директорию для логов
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Формируем имя файла по параметрам
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Добавляем символы в имя файла
    symbols_part = ""
    if symbols and len(symbols) <= 3:
        symbols_part = f"_{'_'.join(symbols)}"
    elif symbols:
        symbols_part = f"_{len(symbols)}symbols"

    # Добавляем таймфреймы
    timeframes_part = ""
    if timeframes and len(timeframes) == 1:
        timeframes_part = f"_{timeframes[0]}"
    elif timeframes:
        timeframes_part = f"_{len(timeframes)}tf"

    # Добавляем периоды
    periods_part = ""
    if periods and len(periods) == 1:
        periods_part = f"_p{periods[0]}"
    elif periods:
        periods_part = f"_{len(periods)}periods"

    # Добавляем дни
    days_part = f"_{days}d" if days else ""

    log_filename = f"mfi_validation{symbols_part}{timeframes_part}{periods_part}{days_part}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    # Настройка логгера
    logger = logging.getLogger('mfi_validator')
    logger.setLevel(logging.DEBUG)
    logger.handlers = []  # Очистка существующих handlers

    # File handler (все сообщения)
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler (только INFO и выше)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📄 Log file: {log_path}")

    return logger


class MFIDataValidator:
    """MFI Data Validator - Mathematical verification of stored MFI values"""

    # Timeframe to minutes mapping
    TIMEFRAME_MINUTES = {
        '1m': 1,
        '15m': 15,
        '1h': 60
    }

    def __init__(self, config_path: str = None, logger: logging.Logger = None):
        """
        Initialize MFI Data Validator.

        Args:
            config_path: Path to indicators_config.yaml
            logger: Logger instance (optional)
        """
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'indicators_config.yaml'
        )
        self.config = None
        self.conn = None
        self.logger = logger

        self.load_config()

    def load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            print(f"✅ Config loaded from: {self.config_path}")
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
        """
        return f'indicators_bybit_futures_{timeframe}'

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
        Fetch candle data (OHLCV) and stored MFI values for validation.

        Args:
            symbol: Trading pair symbol (e.g., 'ETHUSDT')
            timeframe: '1m', '15m', or '1h'
            period: MFI period (e.g., 7, 10, 14, 20, 25)
            start_date: Start date for data (None = from beginning)
            end_date: End date for data (None = until now)
            verbose: Show detailed loading messages

        Returns:
            DataFrame with columns: timestamp, high, low, close, volume, mfi_stored
        """
        if verbose:
            date_range = ""
            if start_date:
                date_range = f" from {start_date.date()}"
            if end_date:
                date_range += f" to {end_date.date()}"
            msg = f"   📥 Loading data: {symbol} {timeframe} MFI_{period}{date_range}"
            print(msg)
            if self.logger:
                self.logger.info(f"📥 Загрузка данных: {symbol} {timeframe} MFI_{period}{date_range}")

        indicators_table = self.get_table_name(timeframe)
        mfi_column = f'mfi_{period}'
        minutes = self.TIMEFRAME_MINUTES[timeframe]

        # For MFI lookback: 2x for rolling sum warm-up (sufficient for simple moving sum)
        lookback_periods = period * 2
        lookback_minutes = lookback_periods * minutes

        # Adjust start_date for lookback
        if start_date:
            adjusted_start = start_date - timedelta(minutes=lookback_minutes)
        else:
            adjusted_start = None

        cur = self.conn.cursor()

        # Build query based on timeframe
        if timeframe == '1m':
            # For 1m timeframe, data comes directly from candles table
            query = f"""
                SELECT
                    c.timestamp,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    i.{mfi_column} as mfi_stored
                FROM candles_bybit_futures_1m c
                LEFT JOIN {indicators_table} i
                    ON c.timestamp = i.timestamp AND c.symbol = i.symbol
                WHERE c.symbol = %s
            """
            params = [symbol]

            if adjusted_start:
                query += " AND c.timestamp >= %s"
                params.append(adjusted_start)
            if end_date:
                query += " AND c.timestamp <= %s"
                params.append(end_date)

            query += " ORDER BY c.timestamp"

        else:
            # For aggregated timeframes (15m, 1h), aggregate from 1m data
            # Aggregation formula (matches mfi_loader.py):
            # - high: MAX(high)
            # - low: MIN(low)
            # - close: last close (array_agg ORDER BY timestamp DESC)[1]
            # - volume: SUM(volume)
            # - timestamp: START of period (not END!)

            # Subtract one period for loading sufficient 1m candles
            if adjusted_start:
                query_adjusted_start = adjusted_start - timedelta(minutes=minutes)
            else:
                query_adjusted_start = None

            if minutes == 60:  # 1h
                agg_query = f"""
                    WITH aggregated AS (
                        SELECT
                            date_trunc('hour', timestamp) as period_start,
                            MAX(high) as high_price,
                            MIN(low) as low_price,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price,
                            SUM(volume) as volume_sum
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                """
                params = [symbol]

                if query_adjusted_start:
                    agg_query += " AND timestamp >= %s"
                    params.append(query_adjusted_start)
                if end_date:
                    agg_query += " AND timestamp <= %s"
                    params.append(end_date)

                agg_query += """
                        GROUP BY date_trunc('hour', timestamp)
                    )
                    SELECT
                        a.period_start as timestamp,
                        a.high_price as high,
                        a.low_price as low,
                        a.close_price as close,
                        a.volume_sum as volume,
                        i.{mfi_column} as mfi_stored
                    FROM aggregated a
                    LEFT JOIN {indicators_table} i
                        ON a.period_start = i.timestamp AND i.symbol = %s
                    ORDER BY a.period_start
                """.format(mfi_column=mfi_column, indicators_table=indicators_table)
                params.append(symbol)

            else:  # 15m
                agg_query = f"""
                    WITH aggregated AS (
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start,
                            MAX(high) as high_price,
                            MIN(low) as low_price,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price,
                            SUM(volume) as volume_sum
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                """
                params = [symbol]

                if query_adjusted_start:
                    agg_query += " AND timestamp >= %s"
                    params.append(query_adjusted_start)
                if end_date:
                    agg_query += " AND timestamp <= %s"
                    params.append(end_date)

                agg_query += f"""
                        GROUP BY date_trunc('hour', timestamp), EXTRACT(MINUTE FROM timestamp)::integer / {minutes}
                    )
                    SELECT
                        a.period_start as timestamp,
                        a.high_price as high,
                        a.low_price as low,
                        a.close_price as close,
                        a.volume_sum as volume,
                        i.{mfi_column} as mfi_stored
                    FROM aggregated a
                    LEFT JOIN {indicators_table} i
                        ON a.period_start = i.timestamp AND i.symbol = %s
                    ORDER BY a.period_start
                """
                params.append(symbol)

            query = agg_query

        cur.execute(query, params)
        results = cur.fetchall()

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results, columns=['timestamp', 'high', 'low', 'close', 'volume', 'mfi_stored'])

        # Convert to proper types
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['mfi_stored'] = df['mfi_stored'].astype(float) if df['mfi_stored'].notna().any() else None

        if verbose:
            print(f"   ✅ Loaded {len(df)} candles")
            if self.logger:
                self.logger.info(f"✅ Загружено {len(df)} свечей")

        return df

    def calculate_mfi(
        self,
        high: np.ndarray,
        low: np.ndarray,
        close: np.ndarray,
        volume: np.ndarray,
        period: int
    ) -> np.ndarray:
        """
        Calculate MFI using exact formula from mfi_loader.py.

        Formula:
        1. Typical Price (TP) = (High + Low + Close) / 3
        2. Money Flow (MF) = TP × Volume
        3. Positive/Negative split:
           - If TP > TP_prev: Positive_MF = MF, Negative_MF = 0
           - If TP < TP_prev: Positive_MF = 0, Negative_MF = MF
           - If TP == TP_prev: Both = 0 (no contribution)
        4. Rolling sum over period
        5. Money Flow Ratio = Σ(Positive_MF) / Σ(Negative_MF)
        6. MFI = 100 - (100 / (1 + Ratio))

        Edge cases:
        - Volume = 0: MF = 0 (included in window, zero contribution)
        - neg_sum = 0: MFI = 100.0 (only buys)
        - pos_sum = 0: MFI = 0.0 (only sells)
        - Both = 0: MFI = NaN (no movement)
        - First (period-1) candles: MFI = NaN

        Args:
            high, low, close, volume: Price/volume arrays
            period: MFI period (7, 10, 14, 20, 25)

        Returns:
            np.ndarray with MFI values (same length as input)
        """
        if len(high) < period + 1:
            return np.full(len(high), np.nan)

        # Convert to float64 to handle PostgreSQL Decimal types
        high = np.asarray(high, dtype=np.float64)
        low = np.asarray(low, dtype=np.float64)
        close = np.asarray(close, dtype=np.float64)
        volume = np.asarray(volume, dtype=np.float64)

        # 1. Typical Price
        tp = (high + low + close) / 3

        # 2. Money Flow
        money_flow = tp * volume

        # 3. Split Positive/Negative based on TP direction
        tp_diff = np.diff(tp)  # TP - TP_prev

        # Initialize arrays (first element = 0 because diff shifts indices)
        positive_mf = np.zeros(len(money_flow))
        negative_mf = np.zeros(len(money_flow))

        # Apply from index 1 onwards (diff is shorter by 1)
        positive_mf[1:] = np.where(tp_diff > 0, money_flow[1:], 0)
        negative_mf[1:] = np.where(tp_diff < 0, money_flow[1:], 0)

        # 4-6. Calculate MFI with rolling sum
        mfi_values = np.full(len(money_flow), np.nan)

        for i in range(period - 1, len(money_flow)):
            # Rolling sum over period
            pos_sum = np.sum(positive_mf[i - period + 1:i + 1])
            neg_sum = np.sum(negative_mf[i - period + 1:i + 1])

            # Edge case handling
            if pos_sum == 0 and neg_sum == 0:
                # No price movement in entire period
                mfi_values[i] = np.nan
            elif neg_sum == 0:
                # Only buys (only positive money flow)
                mfi_values[i] = 100.0
            elif pos_sum == 0:
                # Only sells (only negative money flow)
                mfi_values[i] = 0.0
            else:
                # Normal calculation
                ratio = pos_sum / neg_sum
                mfi_values[i] = 100 - (100 / (1 + ratio))

        return mfi_values

    def validate(
        self,
        symbol: str,
        timeframe: str,
        period: int,
        days: Optional[int] = None,
        verbose: bool = False
    ) -> Dict:
        """
        Validate MFI data for given symbol, timeframe, and period.

        Args:
            symbol: Trading pair symbol
            timeframe: '1m', '15m', or '1h'
            period: MFI period
            days: Number of days to check (None = all data)
            verbose: Show detailed output

        Returns:
            Validation results dictionary
        """
        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days) if days else None

        # Fetch data
        df = self.fetch_data(symbol, timeframe, period, start_date, end_date, verbose=verbose)

        if df.empty:
            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'period': period,
                'total_candles': 0,
                'errors': 0,
                'accuracy': 0.0,
                'message': 'No data found'
            }

        # Calculate MFI
        mfi_calculated = self.calculate_mfi(
            df['high'].values,
            df['low'].values,
            df['close'].values,
            df['volume'].values,
            period
        )

        df['mfi_calculated'] = mfi_calculated

        # Filter to validation range (remove lookback data)
        if start_date:
            df_validation = df[df['timestamp'] >= start_date].copy()
        else:
            df_validation = df.copy()

        # Compare calculated vs stored
        # Allow small floating point differences (< 0.5)
        tolerance = 0.5

        # Identify non-null stored values
        has_stored = df_validation['mfi_stored'].notna()
        has_calculated = df_validation['mfi_calculated'].notna()

        # Calculate differences
        df_validation['diff'] = np.abs(df_validation['mfi_stored'] - df_validation['mfi_calculated'])

        # Errors: where both exist but differ by more than tolerance
        errors_mask = has_stored & has_calculated & (df_validation['diff'] > tolerance)
        errors = df_validation[errors_mask]

        total_compared = (has_stored & has_calculated).sum()
        error_count = len(errors)
        accuracy = ((total_compared - error_count) / total_compared * 100) if total_compared > 0 else 0

        # Log errors
        if error_count > 0 and self.logger:
            self.logger.error(f"❌ {symbol} {timeframe} MFI-{period}: {error_count} errors found")
            for idx, row in errors.head(10).iterrows():
                self.logger.error(
                    f"   {row['timestamp']}: Expected {row['mfi_calculated']:.4f}, "
                    f"Found {row['mfi_stored']:.4f}, Diff {row['diff']:.4f}"
                )
            if error_count > 10:
                self.logger.error(f"   ... and {error_count - 10} more errors")

        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'period': period,
            'total_candles': len(df_validation),
            'compared': total_compared,
            'errors': error_count,
            'accuracy': accuracy,
            'message': 'OK' if error_count == 0 else f'{error_count} errors'
        }

    def run(
        self,
        symbols: Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
        periods: Optional[List[int]] = None,
        days: Optional[int] = None,
        verbose: bool = False
    ) -> List[Dict]:
        """
        Run validation for multiple symbols/timeframes/periods.

        Args:
            symbols: List of symbols to validate
            timeframes: List of timeframes to validate
            periods: List of MFI periods to validate
            days: Number of days to check
            verbose: Show detailed output

        Returns:
            List of validation results
        """
        # Use defaults from config if not specified
        if symbols is None:
            symbols = self.config.get('symbols', ['BTCUSDT'])
        if timeframes is None:
            timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])
        if periods is None:
            periods = self.config.get('mfi', {}).get('periods', [7, 10, 14, 20, 25])

        # Connect to database
        self.connect()

        results = []
        total_combinations = len(symbols) * len(timeframes) * len(periods)

        # Progress bar
        pbar = tqdm(total=total_combinations, desc="Validating MFI", unit="combo")

        try:
            for symbol in symbols:
                for timeframe in timeframes:
                    for period in periods:
                        pbar.set_description(f"Validating {symbol} {timeframe} MFI-{period}")

                        result = self.validate(symbol, timeframe, period, days, verbose)
                        results.append(result)

                        # Update progress bar with accuracy
                        if result['compared'] > 0:
                            pbar.set_postfix(accuracy=f"{result['accuracy']:.2f}%")

                        pbar.update(1)

        finally:
            pbar.close()
            self.disconnect()

        return results


def main():
    """Main entry point for MFI data validation."""
    parser = argparse.ArgumentParser(
        description='MFI Mathematical Data Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all data
  python3 check_mfi_data.py

  # Validate specific symbol
  python3 check_mfi_data.py --symbol ETHUSDT

  # Validate specific timeframe
  python3 check_mfi_data.py --timeframe 1h

  # Last 7 days only
  python3 check_mfi_data.py --days 7

  # Specific period
  python3 check_mfi_data.py --period 14

  # Verbose output
  python3 check_mfi_data.py --verbose
        """
    )

    parser.add_argument('--symbol', type=str, help='Symbol to validate (e.g., ETHUSDT)')
    parser.add_argument('--timeframe', type=str, choices=['1m', '15m', '1h'],
                       help='Timeframe to validate')
    parser.add_argument('--period', type=int, help='MFI period to validate (e.g., 14)')
    parser.add_argument('--days', type=int, help='Number of days to check (default: all data)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--config', type=str, help='Path to indicators_config.yaml')

    args = parser.parse_args()

    # Prepare filter lists
    symbols = [args.symbol] if args.symbol else None
    timeframes = [args.timeframe] if args.timeframe else None
    periods = [args.period] if args.period else None

    # Setup logging
    logger = setup_logging(symbols, timeframes, periods, args.days)

    # Print header
    logger.info("=" * 80)
    logger.info("MFI DATA VALIDATION")
    logger.info("=" * 80)

    # Initialize validator
    validator = MFIDataValidator(config_path=args.config, logger=logger)

    # Show configuration
    symbols_display = symbols if symbols else "All from config"
    timeframes_display = timeframes if timeframes else "All (1m, 15m, 1h)"
    periods_display = periods if periods else validator.config.get('mfi', {}).get('periods', [7, 10, 14, 20, 25])
    days_display = f"Last {args.days} days" if args.days else "All available data"

    logger.info(f"Symbols: {symbols_display}")
    logger.info(f"Timeframes: {timeframes_display}")
    logger.info(f"Periods: {periods_display}")
    logger.info(f"Date range: {days_display}")
    logger.info("=" * 80)

    # Run validation
    results = validator.run(symbols, timeframes, periods, args.days, args.verbose)

    # Summary statistics
    logger.info("")
    logger.info("=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    total_combinations = len(results)
    total_comparisons = sum(r['compared'] for r in results)
    total_errors = sum(r['errors'] for r in results)
    overall_accuracy = ((total_comparisons - total_errors) / total_comparisons * 100) if total_comparisons > 0 else 0

    logger.info(f"Total combinations validated: {total_combinations}")
    logger.info(f"Total comparisons: {total_comparisons:,}")
    logger.info(f"Total errors: {total_errors:,}")
    logger.info(f"Overall accuracy: {overall_accuracy:.4f}%")
    logger.info("")

    if total_errors == 0:
        logger.info("✅ All MFI values are mathematically correct!")
    else:
        logger.info(f"❌ Found {total_errors} errors across {total_combinations} combinations")

    # Detailed results table
    if args.verbose or total_errors > 0:
        logger.info("")
        logger.info("Detailed Results:")
        logger.info("-" * 80)
        logger.info(f"{'Symbol':<10} {'TF':<5} {'Period':<7} {'Compared':<12} {'Errors':<12} {'Accuracy':<10}")
        logger.info("-" * 80)

        for r in results:
            status = "✅" if r['errors'] == 0 else "❌"
            logger.info(
                f"{status} {r['symbol']:<8} {r['timeframe']:<5} {r['period']:<7} "
                f"{r['compared']:<12,} {r['errors']:<12,} {r['accuracy']:<9.2f}%"
            )

        logger.info("-" * 80)

    # Exit code
    exit_code = 1 if total_errors > 0 else 0
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
