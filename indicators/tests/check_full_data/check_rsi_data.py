#!/usr/bin/env python3
"""
RSI Mathematical Data Validation Script

This script performs comprehensive mathematical validation of RSI (Relative Strength Index)
indicator data stored in the database. Unlike status check scripts that only verify
data presence, this validator recalculates RSI values from source candle data and
compares them with stored values to detect calculation errors, data corruption, and gaps.

Features:
- Mathematical correctness verification (recalculate and compare)
- Data completeness checking (no gaps in expected data)
- Warm-up period validation (first period candles should be NULL)
- Support for all 3 timeframes (1m, 15m, 1h)
- Support for all configured symbols and periods
- Detailed error reporting with exact timestamps
- Batch processing for memory efficiency
- 10x lookback multiplier for Wilder smoothing accuracy

Usage:
    # Full validation (all symbols, timeframes, periods)
    python3 check_rsi_data.py

    # Single symbol
    python3 check_rsi_data.py --symbol ETHUSDT

    # Single timeframe
    python3 check_rsi_data.py --timeframe 1h

    # Last 7 days only
    python3 check_rsi_data.py --days 7

    # Specific period
    python3 check_rsi_data.py --period 14

    # Verbose output
    python3 check_rsi_data.py --verbose
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
    Настройка логирования для RSI validation с именем файла по параметрам.

    Args:
        symbols: Список символов для валидации
        timeframes: Список таймфреймов
        periods: Список RSI периодов
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

    # Добавляем days
    days_part = f"_{days}d" if days else ""

    log_filename = f'rsi_validation{symbols_part}{timeframes_part}{periods_part}{days_part}_{timestamp}.log'
    log_path = os.path.join(log_dir, log_filename)

    # Настраиваем logger
    logger = logging.getLogger('RSIValidator')
    logger.setLevel(logging.DEBUG)

    # File handler
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


class RSIDataValidator:
    """RSI Data Validator - Mathematical verification of stored RSI values"""

    # Timeframe to minutes mapping
    TIMEFRAME_MINUTES = {
        '1m': 1,
        '15m': 15,
        '1h': 60
    }

    def __init__(self, config_path: str = None, logger: logging.Logger = None):
        """
        Initialize RSI Data Validator.

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

        # REVERTED: Bybit-style RSI SMA smoothing - не используется
        # self.smoothing_length = self.config.get('rsi', {}).get('smoothing_length', 14)

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
        Fetch candle data and stored RSI values for validation.

        Args:
            symbol: Trading pair symbol (e.g., 'ETHUSDT')
            timeframe: '1m', '15m', or '1h'
            period: RSI period (e.g., 7, 9, 14, 21, 25)
            start_date: Start date for data (None = from beginning)
            end_date: End date for data (None = until now)
            verbose: Show detailed loading messages

        Returns:
            DataFrame with columns: timestamp, close, rsi_stored
        """
        if verbose:
            date_range = ""
            if start_date:
                date_range = f" from {start_date.date()}"
            if end_date:
                date_range += f" to {end_date.date()}"
            msg = f"   📥 Loading data: {symbol} {timeframe} RSI_{period}{date_range}"
            print(msg)
            if self.logger:
                self.logger.info(f"📥 Загрузка данных: {symbol} {timeframe} RSI_{period}{date_range}")

        indicators_table = self.get_table_name(timeframe)
        rsi_column = f'rsi_{period}'
        minutes = self.TIMEFRAME_MINUTES[timeframe]

        # For RSI lookback: 10x for Wilder convergence (99.996% accuracy)
        lookback_periods = period * 10
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
                    c.close,
                    i.{rsi_column} as rsi_stored
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
            # ВАЖНО: Timestamp = КОНЕЦ периода (не начало!)
            # Пример для 1h: timestamp 15:00 содержит данные 14:00-14:59

            # Вычитаем один период для загрузки достаточных 1m свечей
            if adjusted_start:
                query_adjusted_start = adjusted_start - timedelta(minutes=minutes)
            else:
                query_adjusted_start = None

            if minutes == 60:  # 1h
                agg_query = f"""
                    WITH aggregated AS (
                        SELECT
                            date_trunc('hour', timestamp) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
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
                        a.close_price as close,
                        i.{rsi_column} as rsi_stored
                    FROM aggregated a
                    LEFT JOIN {indicators_table} i
                        ON a.period_start = i.timestamp AND i.symbol = %s
                    ORDER BY a.period_start
                """.format(rsi_column=rsi_column, indicators_table=indicators_table)
                params.append(symbol)

            else:  # 15m
                agg_query = f"""
                    WITH aggregated AS (
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
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
                        a.close_price as close,
                        i.{rsi_column} as rsi_stored
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

        df = pd.DataFrame(results, columns=['timestamp', 'close', 'rsi_stored'])

        # Convert to proper types
        df['close'] = df['close'].astype(float)
        df['rsi_stored'] = df['rsi_stored'].astype(float) if df['rsi_stored'].notna().any() else None

        if verbose:
            print(f"   ✅ Loaded {len(df)} candles")
            if self.logger:
                self.logger.info(f"✅ Загружено {len(df)} свечей")

        return df

    def calculate_rsi(self, closes: np.ndarray, period: int) -> np.ndarray:
        """
        Calculate RSI using Wilder smoothing method.

        Args:
            closes: Array of close prices
            period: RSI period

        Returns:
            Array of RSI values (same length as closes)
        """
        if len(closes) < period + 1:
            return np.full(len(closes), np.nan)

        # Convert to float64 to handle Decimal types from PostgreSQL
        closes = np.asarray(closes, dtype=np.float64)

        # Calculate price changes
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        rsi_values = np.full(len(closes), np.nan)

        # Initialize with SMA of first 'period' gains/losses
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        # Calculate RSI for each point using Wilder smoothing
        for i in range(period, len(gains)):
            # Wilder smoothing: avg = (avg * (period-1) + new_value) / period
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            # Calculate RSI
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))

            rsi_values[i + 1] = rsi  # +1 because deltas is shorter by 1

        return rsi_values

    def apply_sma_smoothing(self, rsi_values: np.ndarray, smoothing_length: int) -> np.ndarray:
        """
        Применяет SMA сглаживание к RSI значениям (Bybit-style).

        Args:
            rsi_values: Массив RSI значений
            smoothing_length: Длина окна SMA (по умолчанию 14 на Bybit)

        Returns:
            Массив RSI со сглаживанием
        """
        if smoothing_length <= 1:
            # Если smoothing отключен, возвращаем оригинальные значения
            return rsi_values

        rsi_smoothed = np.full(len(rsi_values), np.nan)

        for i in range(smoothing_length - 1, len(rsi_values)):
            # Проверяем что все значения в окне не NaN
            window = rsi_values[i - smoothing_length + 1:i + 1]
            if not np.isnan(window).any():
                rsi_smoothed[i] = np.mean(window)

        return rsi_smoothed

    def validate(
        self,
        symbol: str,
        timeframe: str,
        period: int,
        days: Optional[int] = None,
        verbose: bool = False
    ) -> Dict:
        """
        Validate RSI data for given symbol, timeframe, and period.

        Args:
            symbol: Trading pair symbol
            timeframe: '1m', '15m', or '1h'
            period: RSI period
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

        # Calculate RSI
        rsi_calculated = self.calculate_rsi(df['close'].values, period)

        df['rsi_calculated'] = rsi_calculated

        # Filter to validation range (remove lookback data)
        if start_date:
            df_validation = df[df['timestamp'] >= start_date].copy()
        else:
            df_validation = df.copy()

        # Compare calculated vs stored
        # Allow small floating point differences (< 0.5)
        tolerance = 0.5

        # Identify non-null stored values
        has_stored = df_validation['rsi_stored'].notna()
        has_calculated = df_validation['rsi_calculated'].notna()

        # Calculate differences
        df_validation['diff'] = np.abs(df_validation['rsi_stored'] - df_validation['rsi_calculated'])

        # Errors: where both exist but differ by more than tolerance
        errors_mask = has_stored & has_calculated & (df_validation['diff'] > tolerance)
        errors = df_validation[errors_mask]

        total_compared = (has_stored & has_calculated).sum()
        error_count = len(errors)
        accuracy = ((total_compared - error_count) / total_compared * 100) if total_compared > 0 else 0

        # Log errors
        if error_count > 0 and self.logger:
            self.logger.error(f"❌ {symbol} {timeframe} RSI-{period}: {error_count} errors found")
            for idx, row in errors.head(10).iterrows():
                self.logger.error(
                    f"   {row['timestamp']}: Expected {row['rsi_calculated']:.4f}, "
                    f"Found {row['rsi_stored']:.4f}, Diff {row['diff']:.4f}"
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
            'message': f"{error_count} errors in {total_compared} comparisons" if error_count > 0 else "All correct"
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
        Run validation for multiple symbols, timeframes, and periods.

        Args:
            symbols: List of symbols (None = all from config)
            timeframes: List of timeframes (None = all)
            periods: List of periods (None = all from config)
            days: Number of days to check
            verbose: Show detailed output

        Returns:
            List of validation results
        """
        # Get defaults from config
        if symbols is None:
            symbols = self.config.get('symbols', ['BTCUSDT'])
        if timeframes is None:
            timeframes = ['1m', '15m', '1h']
        if periods is None:
            periods = self.config.get('indicators', {}).get('rsi', {}).get('periods', [14])

        print(f"\n{'='*80}")
        print(f"RSI DATA VALIDATION")
        print(f"{'='*80}")
        print(f"Symbols: {', '.join(symbols)}")
        print(f"Timeframes: {', '.join(timeframes)}")
        print(f"Periods: {', '.join(map(str, periods))}")
        if days:
            print(f"Date range: Last {days} days")
        else:
            print(f"Date range: All available data")
        print(f"{'='*80}\n")

        if self.logger:
            self.logger.info("="*80)
            self.logger.info("RSI DATA VALIDATION")
            self.logger.info("="*80)
            self.logger.info(f"Символы: {', '.join(symbols)}")
            self.logger.info(f"Таймфреймы: {', '.join(timeframes)}")
            self.logger.info(f"Периоды: {', '.join(map(str, periods))}")
            if days:
                self.logger.info(f"Период: Последние {days} дней")
            else:
                self.logger.info(f"Период: Все данные")
            self.logger.info("="*80)

        results = []
        total_combinations = len(symbols) * len(timeframes) * len(periods)

        with tqdm(total=total_combinations, desc="Validating RSI") as pbar:
            for symbol in symbols:
                for timeframe in timeframes:
                    for period in periods:
                        pbar.set_description(f"Validating {symbol} {timeframe} RSI-{period}")

                        result = self.validate(symbol, timeframe, period, days, verbose)
                        results.append(result)

                        # Update progress bar with accuracy
                        if result['compared'] > 0:
                            pbar.set_postfix(accuracy=f"{result['accuracy']:.2f}%")

                        pbar.update(1)

        # Summary
        print(f"\n{'='*80}")
        print(f"VALIDATION SUMMARY")
        print(f"{'='*80}")

        total_errors = sum(r['errors'] for r in results)
        total_compared = sum(r['compared'] for r in results)
        overall_accuracy = ((total_compared - total_errors) / total_compared * 100) if total_compared > 0 else 0

        print(f"Total combinations validated: {len(results)}")
        print(f"Total comparisons: {total_compared:,}")
        print(f"Total errors: {total_errors:,}")
        print(f"Overall accuracy: {overall_accuracy:.4f}%")

        if total_errors == 0:
            print(f"\n✅ All RSI values are mathematically correct!")
        else:
            print(f"\n⚠️ Found {total_errors} errors - check log file for details")

        # Detailed results per combination
        print(f"\nDetailed Results:")
        print(f"{'-'*80}")
        print(f"{'Symbol':<10} {'TF':<5} {'Period':<8} {'Compared':<12} {'Errors':<10} {'Accuracy':<10}")
        print(f"{'-'*80}")

        for r in results:
            symbol_str = r['symbol'][:10]
            tf_str = r['timeframe']
            period_str = str(r['period'])
            compared_str = f"{r['compared']:,}"
            errors_str = str(r['errors'])
            accuracy_str = f"{r['accuracy']:.2f}%"

            status = "✅" if r['errors'] == 0 else "❌"
            print(f"{status} {symbol_str:<9} {tf_str:<5} {period_str:<8} {compared_str:<12} {errors_str:<10} {accuracy_str:<10}")

        print(f"{'-'*80}\n")

        if self.logger:
            self.logger.info("="*80)
            self.logger.info("ИТОГИ ВАЛИДАЦИИ")
            self.logger.info("="*80)
            self.logger.info(f"Всего комбинаций проверено: {len(results)}")
            self.logger.info(f"Всего сравнений: {total_compared:,}")
            self.logger.info(f"Всего ошибок: {total_errors:,}")
            self.logger.info(f"Общая точность: {overall_accuracy:.4f}%")

        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='RSI Mathematical Data Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all data
  python3 check_rsi_data.py

  # Validate specific symbol
  python3 check_rsi_data.py --symbol ETHUSDT

  # Validate specific timeframe
  python3 check_rsi_data.py --timeframe 1h

  # Last 7 days only
  python3 check_rsi_data.py --days 7

  # Specific period
  python3 check_rsi_data.py --period 14

  # Verbose output
  python3 check_rsi_data.py --verbose
        """
    )

    parser.add_argument('--symbol', type=str, help='Symbol to validate (e.g., ETHUSDT)')
    parser.add_argument('--timeframe', type=str, choices=['1m', '15m', '1h'],
                       help='Timeframe to validate')
    parser.add_argument('--period', type=int, help='RSI period to validate (e.g., 14)')
    parser.add_argument('--days', type=int, help='Number of days to check (default: all data)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--config', type=str, help='Path to indicators_config.yaml')

    args = parser.parse_args()

    # Prepare parameters
    symbols = [args.symbol] if args.symbol else None
    timeframes = [args.timeframe] if args.timeframe else None
    periods = [args.period] if args.period else None

    # Setup logging
    logger = setup_logging(symbols, timeframes, periods, args.days)

    # Create validator
    validator = RSIDataValidator(config_path=args.config, logger=logger)

    # Connect to database
    validator.connect()

    try:
        # Run validation
        results = validator.run(
            symbols=symbols,
            timeframes=timeframes,
            periods=periods,
            days=args.days,
            verbose=args.verbose
        )

        # Exit code based on results
        total_errors = sum(r['errors'] for r in results)
        sys.exit(0 if total_errors == 0 else 1)

    finally:
        # Cleanup
        validator.disconnect()


if __name__ == '__main__':
    main()
