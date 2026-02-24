#!/usr/bin/env python3
"""
ADX (Average Directional Index) Indicator Loader

This script calculates ADX (Average Directional Index), +DI, and -DI indicators
and stores them in the PostgreSQL database for multiple timeframes.

ADX measures trend strength (0-100), while +DI/-DI show trend direction.
Uses Wilder's smoothing method with double smoothing for ADX calculation.

Author: Trading System
Date: 2025-10-17
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import warnings
from tqdm import tqdm
import pandas as pd
import numpy as np
from decimal import Decimal
import psycopg2.extras

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# ADX Configuration
PERIODS = [7, 10, 14, 20, 21, 25, 30, 50]  # 8 periods
LOOKBACK_MULTIPLIER = 4  # period × 4 for double smoothing
BATCH_DAYS = 1  # Process 1 day at a time
TIMEFRAMES = ['1m', '15m', '1h', '4h', '1d']

# Logging setup
def setup_logging(symbol: str) -> logging.Logger:
    """Setup logging configuration"""
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'adx_loader_{symbol}_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)


class ADXLoader:
    """
    ADX (Average Directional Index) Indicator Loader

    Calculates ADX, +DI, -DI for multiple periods and timeframes.

    Components:
    - ADX: Trend strength (0-100)
    - +DI: Bullish directional movement
    - -DI: Bearish directional movement

    Calculation steps:
    1. True Range (TR)
    2. +DM/-DM (Directional Movement)
    3. Wilder smoothing of TR, +DM, -DM
    4. Calculate +DI/-DI from smoothed values
    5. Calculate DX from +DI/-DI
    6. Wilder smoothing of DX → ADX
    """

    # Минуты на единицу таймфрейма (для расчёта natural boundary)
    TIMEFRAME_MINUTES = {
        '1m': 1, '15m': 15, '1h': 60, '4h': 240, '1d': 1440
    }

    def __init__(
        self,
        symbol: str = 'BTCUSDT',
        batch_days: int = BATCH_DAYS,
        lookback_multiplier: int = LOOKBACK_MULTIPLIER,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        force_reload: bool = False,
        check_nulls: bool = False
    ):
        """
        Initialize ADX Loader

        Args:
            symbol: Trading pair symbol
            batch_days: Number of days to process in each batch
            lookback_multiplier: Multiplier for lookback period (period × multiplier)
            start_date: Custom start date (optional)
            end_date: Custom end date (optional)
            force_reload: Force reload even if data exists (default: False)
            check_nulls: Check and fill NULL values in middle of data (default: False)
        """
        self.symbol = symbol
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.batch_days = batch_days
        self.lookback_multiplier = lookback_multiplier
        self.custom_start_date = start_date
        self.custom_end_date = end_date
        self.force_reload = force_reload
        self.check_nulls = check_nulls
        self.db = DatabaseConnection()
        self.logger = setup_logging(symbol)

        self.logger.info(f"ADXLoader initialized for {symbol}")
        self.logger.info(f"Periods: {PERIODS}")
        self.logger.info(f"Batch size: {batch_days} days")
        self.logger.info(f"Lookback multiplier: {lookback_multiplier}")
        if start_date:
            self.logger.info(f"Custom start date: {start_date.date()}")
        if end_date:
            self.logger.info(f"Custom end date: {end_date.date()}")
        if force_reload:
            self.logger.info("Force reload mode: ENABLED")
        if check_nulls:
            self.logger.info("Check nulls mode: ENABLED")

    def get_column_names(self, period: int) -> Dict[str, str]:
        """
        Get column names for a specific period

        Args:
            period: ADX period

        Returns:
            Dictionary with column names for adx, plus_di, minus_di
        """
        return {
            'adx': f'adx_{period}',
            'plus_di': f'adx_{period}_plus_di',
            'minus_di': f'adx_{period}_minus_di'
        }

    def ensure_columns_exist(self, timeframe: str, period: int):
        """
        Ensure ADX columns exist in the database

        Args:
            timeframe: Timeframe (1m, 15m, 1h)
            period: ADX period
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for col_name in columns.values():
                    # Check if column exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                            AND table_name = %s
                            AND column_name = %s
                        )
                    """, (table_name, col_name))

                    exists = cur.fetchone()[0]

                    if not exists:
                        # Create column
                        self.logger.info(f"Creating column {col_name} in {table_name}")
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col_name} DECIMAL(10,4)
                        """)
                        conn.commit()

    def get_last_processed_date(
        self,
        timeframe: str,
        period: int
    ) -> Optional[datetime]:
        """
        Get the last date where ADX data exists

        Args:
            timeframe: Timeframe (1m, 15m, 1h)
            period: ADX period

        Returns:
            Last processed datetime or None
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)
        adx_col = columns['adx']

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                    AND {adx_col} IS NOT NULL
                """, (self.symbol,))

                result = cur.fetchone()
                return result[0] if result and result[0] else None

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Get available data range from candles table

        Args:
            timeframe: Timeframe (1m, 15m, 1h)

        Returns:
            Tuple of (start_date, end_date)
        """
        # If custom dates are provided, use them
        if self.custom_start_date and self.custom_end_date:
            self.logger.info(f"Using custom date range: {self.custom_start_date.date()} to {self.custom_end_date.date()}")
            return self.custom_start_date, self.custom_end_date

        # Otherwise, query database for available data range
        # Always read from 1m base table
        table_name = 'candles_bybit_futures_1m'

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()
                if not result or not result[0]:
                    raise ValueError(f"No data found for {self.symbol}")

                return result[0], result[1]

    def get_null_timestamps_for_period(self, timeframe: str, period: int) -> set:
        """
        Находит timestamps где ADX/+DI/-DI IS NULL (исключая natural boundary).

        ADX использует двойное сглаживание Wilder:
        - Первое сглаживание TR/+DM/-DM: period записей
        - DX из +DI/-DI: сразу после первого сглаживания
        - Второе сглаживание DX → ADX: ещё period записей
        - Natural boundary = period * 2

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h, 4h, 1d)
            period: Период ADX

        Returns:
            set of timestamps с NULL значениями
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)
        adx_col = columns['adx']
        plus_di_col = columns['plus_di']
        minus_di_col = columns['minus_di']

        # Natural boundary: двойное сглаживание = period * 2
        minutes = self.TIMEFRAME_MINUTES[timeframe]
        boundary_periods = period * 2

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Минимальная дата данных
                cur.execute(f"""
                    SELECT MIN(timestamp) FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_date = cur.fetchone()[0]
                if not min_date:
                    return set()

                boundary = min_date + timedelta(minutes=boundary_periods * minutes)

                # Ищем NULL после boundary
                cur.execute(f"""
                    SELECT timestamp FROM {table_name}
                    WHERE symbol = %s AND timestamp >= %s
                      AND ({adx_col} IS NULL OR {plus_di_col} IS NULL OR {minus_di_col} IS NULL)
                """, (self.symbol, boundary))

                return {row[0] for row in cur.fetchall()}

    def calculate_true_range(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series
    ) -> pd.Series:
        """
        Calculate True Range

        TR = max(High - Low, |High - PrevClose|, |Low - PrevClose|)

        Args:
            high: High prices
            low: Low prices
            close: Close prices

        Returns:
            True Range series
        """
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        return tr

    def calculate_directional_movement(
        self,
        high: pd.Series,
        low: pd.Series
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Calculate +DM and -DM (Directional Movement)

        +DM = High - PrevHigh (if positive and > down_move)
        -DM = PrevLow - Low (if positive and > up_move)

        Args:
            high: High prices
            low: Low prices

        Returns:
            Tuple of (+DM, -DM) series
        """
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        plus_dm = pd.Series(0.0, index=high.index)
        minus_dm = pd.Series(0.0, index=high.index)

        # +DM when up_move > down_move and up_move > 0
        mask_plus = (up_move > down_move) & (up_move > 0)
        plus_dm[mask_plus] = up_move[mask_plus]

        # -DM when down_move > up_move and down_move > 0
        mask_minus = (down_move > up_move) & (down_move > 0)
        minus_dm[mask_minus] = down_move[mask_minus]

        return plus_dm, minus_dm

    def wilder_smoothing(
        self,
        series: pd.Series,
        period: int
    ) -> pd.Series:
        """
        Apply Wilder's smoothing method

        smoothed = (prev_smoothed × (period - 1) + current) / period

        For first value, use simple mean of first 'period' values

        Args:
            series: Data series to smooth
            period: Smoothing period

        Returns:
            Smoothed series
        """
        smoothed = pd.Series(np.nan, index=series.index)

        # First smoothed value = average of first 'period' values
        if len(series) >= period:
            smoothed.iloc[period - 1] = series.iloc[:period].mean()

            # Apply Wilder smoothing for subsequent values
            for i in range(period, len(series)):
                smoothed.iloc[i] = (
                    (smoothed.iloc[i - 1] * (period - 1) + series.iloc[i]) / period
                )

        return smoothed

    def calculate_adx(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int
    ) -> Dict[str, pd.Series]:
        """
        Calculate ADX, +DI, -DI

        Steps:
        1. Calculate TR, +DM, -DM
        2. Apply Wilder smoothing to TR, +DM, -DM
        3. Calculate +DI, -DI from smoothed values
        4. Calculate DX
        5. Apply Wilder smoothing to DX → ADX

        Args:
            high: High prices
            low: Low prices
            close: Close prices
            period: ADX period

        Returns:
            Dictionary with 'adx', 'plus_di', 'minus_di' series
        """
        # Convert to float for calculations
        high = high.astype(float)
        low = low.astype(float)
        close = close.astype(float)

        # Step 1: Calculate TR, +DM, -DM
        tr = self.calculate_true_range(high, low, close)
        plus_dm, minus_dm = self.calculate_directional_movement(high, low)

        # Step 2: Apply Wilder smoothing
        smoothed_tr = self.wilder_smoothing(tr, period)
        smoothed_plus_dm = self.wilder_smoothing(plus_dm, period)
        smoothed_minus_dm = self.wilder_smoothing(minus_dm, period)

        # Step 3: Calculate +DI, -DI
        plus_di = 100 * smoothed_plus_dm / smoothed_tr
        minus_di = 100 * smoothed_minus_dm / smoothed_tr

        # Step 4: Calculate DX
        di_sum = plus_di + minus_di
        di_diff = (plus_di - minus_di).abs()

        dx = pd.Series(np.nan, index=high.index)
        mask = di_sum != 0
        dx[mask] = 100 * di_diff[mask] / di_sum[mask]

        # Step 5: Apply Wilder smoothing to DX → ADX
        adx = self.wilder_smoothing(dx, period)

        return {
            'adx': adx,
            'plus_di': plus_di,
            'minus_di': minus_di
        }

    def aggregate_to_timeframe(
        self,
        df: pd.DataFrame,
        timeframe: str
    ) -> pd.DataFrame:
        """
        Aggregate 1m data to target timeframe

        Args:
            df: DataFrame with 1m candles
            timeframe: Target timeframe (15m, 1h)

        Returns:
            Aggregated DataFrame
        """
        if timeframe == '1m':
            return df

        # Map timeframe to pandas resample rule
        resample_rule = {
            '15m': '15min',
            '1h': '1h',
            '4h': '4h',
            '1d': '1D'
        }[timeframe]

        # Ensure timezone-aware datetime
        df = df.copy()
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')

        df.set_index('timestamp', inplace=True)

        # Aggregate
        agg_df = df.resample(resample_rule).agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()

        agg_df.reset_index(inplace=True)

        return agg_df

    def load_period(
        self,
        period: int,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        null_timestamps: Optional[set] = None
    ):
        """
        Load ADX data for a specific period and timeframe

        Args:
            period: ADX period
            timeframe: Timeframe (1m, 15m, 1h)
            start_date: Start date for processing
            end_date: End date for processing
            null_timestamps: Set of timestamps to fill (check_nulls mode)
        """
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"Loading ADX_{period} for {timeframe}")
        self.logger.info(f"Period: {start_date.date()} to {end_date.date()}")

        # Ensure columns exist
        self.ensure_columns_exist(timeframe, period)

        # Get last processed date (skip checkpoint if force reload or check_nulls)
        if not self.force_reload and null_timestamps is None:
            last_date = self.get_last_processed_date(timeframe, period)

            if last_date:
                # Start from beginning of day with last data (re-process partial day)
                start_date = max(start_date, last_date.replace(hour=0, minute=0, second=0, microsecond=0))
                self.logger.info(f"Resuming from {start_date.date()} (last: {last_date})")

            if start_date >= end_date:
                self.logger.info(f"ADX_{period} {timeframe} already up to date")
                return
        elif self.force_reload:
            self.logger.info(f"Force reload: Skipping checkpoint, will reload all data in range")

        # Calculate lookback period
        lookback_days = period * self.lookback_multiplier

        # Process in batches
        current_date = start_date
        total_days = (end_date - start_date).days

        columns = self.get_column_names(period)

        mode_label = " CHECK-NULLS" if null_timestamps is not None else ""
        with tqdm(total=total_days, desc=f"{self.symbol} {self.symbol_progress} ADX-{period} {timeframe.upper()}{mode_label}", unit="day",
                 ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            while current_date < end_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

                # Load data with lookback
                data_start = current_date - timedelta(days=lookback_days)

                # Get 1m candles from database
                with self.db.get_connection() as conn:
                    query = """
                        SELECT timestamp, high, low, close
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                        AND timestamp >= %s
                        AND timestamp < %s
                        ORDER BY timestamp
                    """

                    df = pd.read_sql_query(
                        query,
                        conn,
                        params=(self.symbol, data_start, batch_end)
                    )

                if df.empty:
                    self.logger.warning(f"No data for batch {current_date.date()}")
                    current_date = batch_end
                    pbar.update((batch_end - current_date).days + self.batch_days)
                    continue

                # Aggregate to target timeframe
                df_agg = self.aggregate_to_timeframe(df, timeframe)

                if len(df_agg) < period * 2:
                    self.logger.debug(f"Insufficient data for ADX_{period} calculation (have {len(df_agg)}, need {period * 2})")
                    current_date = batch_end
                    pbar.update((batch_end - current_date).days + self.batch_days)
                    continue

                # Calculate ADX
                result = self.calculate_adx(
                    df_agg['high'],
                    df_agg['low'],
                    df_agg['close'],
                    period
                )

                # Add results to dataframe
                df_agg['adx'] = result['adx']
                df_agg['plus_di'] = result['plus_di']
                df_agg['minus_di'] = result['minus_di']

                # Filter to target batch (exclude lookback)
                df_batch = df_agg[
                    (df_agg['timestamp'] >= current_date) &
                    (df_agg['timestamp'] < batch_end)
                ].copy()

                # check_nulls: дополнительная фильтрация — только NULL timestamps
                if null_timestamps is not None and not df_batch.empty:
                    df_batch = df_batch[df_batch['timestamp'].isin(null_timestamps)].copy()

                # Update database
                if not df_batch.empty:
                    self._update_database(df_batch, timeframe, columns)

                    # Update progress bar with last timestamp
                    last_ts = df_batch['timestamp'].max()
                    pbar.set_postfix({
                        'records': len(df_batch),
                        'last': last_ts.strftime('%Y-%m-%d %H:%M')
                    })

                current_date = batch_end
                pbar.update(self.batch_days)

    def _update_database(
        self,
        df: pd.DataFrame,
        timeframe: str,
        columns: Dict[str, str]
    ):
        """
        Update database with ADX values

        Args:
            df: DataFrame with ADX data
            timeframe: Timeframe (1m, 15m, 1h)
            columns: Column names mapping
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    # Convert numpy types to Python native types
                    adx_val = float(row['adx']) if pd.notna(row['adx']) else None
                    plus_di_val = float(row['plus_di']) if pd.notna(row['plus_di']) else None
                    minus_di_val = float(row['minus_di']) if pd.notna(row['minus_di']) else None

                    cur.execute(f"""
                        UPDATE {table_name}
                        SET {columns['adx']} = %s,
                            {columns['plus_di']} = %s,
                            {columns['minus_di']} = %s
                        WHERE symbol = %s
                        AND timestamp = %s
                    """, (
                        adx_val,
                        plus_di_val,
                        minus_di_val,
                        self.symbol,
                        row['timestamp']
                    ))

                conn.commit()

    # ══════════════════════════════════════════════════════════════════════════
    # Single-pass methods (OBV pattern — like ATR loader)
    # ══════════════════════════════════════════════════════════════════════════

    def load_all_candles(self, timeframe: str) -> pd.DataFrame:
        """
        Загрузка ВСЕХ свечей для single-pass расчёта ADX.
        SQL-агрегация из 1m свечей (OBV pattern как в ATR loader).

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h, 4h, 1d)

        Returns:
            DataFrame с колонками: timestamp, symbol, high, low, close
        """
        min_date, _ = self.get_data_range(timeframe)

        with self.db.get_connection() as conn:
            if timeframe == '1m':
                query = """
                    SELECT timestamp, symbol, high, low, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s AND timestamp >= %s
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, min_date))
            else:
                minutes = self.TIMEFRAME_MINUTES[timeframe]

                if minutes == 1440:  # 1d
                    query = """
                        WITH time_groups AS (
                            SELECT timestamp,
                                   DATE_TRUNC('day', timestamp) as period_start,
                                   high, low, close, symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s
                        )
                        SELECT period_start as timestamp, symbol,
                               MAX(high) as high, MIN(low) as low,
                               (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start ASC
                    """
                elif minutes == 240:  # 4h
                    query = """
                        WITH time_groups AS (
                            SELECT timestamp,
                                   DATE_TRUNC('day', timestamp) +
                                   INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4) as period_start,
                                   high, low, close, symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s
                        )
                        SELECT period_start as timestamp, symbol,
                               MAX(high) as high, MIN(low) as low,
                               (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start ASC
                    """
                elif minutes == 60:  # 1h
                    query = """
                        WITH time_groups AS (
                            SELECT timestamp,
                                   DATE_TRUNC('hour', timestamp) as period_start,
                                   high, low, close, symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s
                        )
                        SELECT period_start as timestamp, symbol,
                               MAX(high) as high, MIN(low) as low,
                               (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start ASC
                    """
                else:  # 15m and other sub-hourly
                    query = f"""
                        WITH time_groups AS (
                            SELECT timestamp,
                                   DATE_TRUNC('hour', timestamp) +
                                   INTERVAL '1 minute' * (FLOOR(EXTRACT(MINUTE FROM timestamp) / {minutes}) * {minutes}) as period_start,
                                   high, low, close, symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s
                        )
                        SELECT period_start as timestamp, symbol,
                               MAX(high) as high, MIN(low) as low,
                               (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start ASC
                    """

                df = pd.read_sql_query(query, conn, params=(self.symbol, min_date))

        self.logger.info(f"Загружено {len(df):,} свечей от {df['timestamp'].min()} до {df['timestamp'].max()}")
        return df

    def get_all_last_processed_dates(self, timeframe: str, periods: List[int]) -> Dict[int, Optional[datetime]]:
        """
        Получает последнюю дату с рассчитанным ADX для ВСЕХ периодов одним SQL запросом.

        Args:
            timeframe: Таймфрейм
            periods: Список периодов ADX

        Returns:
            Dict {period: last_date или None}
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        case_exprs = []
        for p in periods:
            case_exprs.append(
                f"MAX(CASE WHEN adx_{p} IS NOT NULL THEN timestamp END) AS last_{p}"
            )

        query = f"""
            SELECT {', '.join(case_exprs)}
            FROM {table_name}
            WHERE symbol = %s
        """

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.symbol,))
                row = cur.fetchone()
                if not row:
                    return {p: None for p in periods}
                return {p: row[i] for i, p in enumerate(periods)}

    def get_all_null_timestamps(self, timeframe: str, periods: List[int]) -> Dict[int, set]:
        """
        Получает NULL timestamps для ВСЕХ периодов одним подключением.
        Исключает естественные NULL в начале данных.
        Natural boundary для ADX = period × 2 (двойное сглаживание Wilder).

        Args:
            timeframe: Таймфрейм
            periods: Список периодов ADX

        Returns:
            Dict {period: set(timestamps)}
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.TIMEFRAME_MINUTES[timeframe]

        result = {p: set() for p in periods}

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MIN(timestamp) FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_date = cur.fetchone()[0]
                if not min_date:
                    return result

                for period in periods:
                    boundary = min_date + timedelta(minutes=period * 2 * minutes)
                    adx_col = f'adx_{period}'
                    plus_di_col = f'adx_{period}_plus_di'
                    minus_di_col = f'adx_{period}_minus_di'

                    cur.execute(f"""
                        SELECT timestamp FROM {table_name}
                        WHERE symbol = %s AND timestamp >= %s
                          AND ({adx_col} IS NULL OR {plus_di_col} IS NULL OR {minus_di_col} IS NULL)
                    """, (self.symbol, boundary))

                    result[period] = {row[0] for row in cur.fetchall()}

                return result

    def batch_update_all_adx(self, df: pd.DataFrame, table_name: str, periods: List[int]):
        """
        Пишет все ADX колонки (3 × len(periods)) одним UPDATE через execute_batch.
        Commit после каждого дня для безопасного восстановления.

        Args:
            df: DataFrame с колонками timestamp, symbol, adx_{p}, adx_{p}_plus_di, adx_{p}_minus_di
            table_name: Имя таблицы индикаторов
            periods: Список периодов ADX
        """
        if df.empty:
            self.logger.info("Нет данных для обновления ADX")
            return

        # Строим SET часть: adx_7=%s, adx_7_plus_di=%s, adx_7_minus_di=%s, ...
        set_parts = []
        for p in periods:
            set_parts.append(f"adx_{p} = %s")
            set_parts.append(f"adx_{p}_plus_di = %s")
            set_parts.append(f"adx_{p}_minus_di = %s")

        update_sql = f"""
            UPDATE {table_name}
            SET {', '.join(set_parts)}
            WHERE timestamp = %s AND symbol = %s
        """

        # Группируем по дням
        df_copy = df.copy()
        df_copy['_date'] = pd.to_datetime(df_copy['timestamp']).dt.date
        grouped = df_copy.groupby('_date')

        total_days = len(grouped)
        total_records = len(df_copy)

        self.logger.info(f"Начало обновления БД: {total_records:,} записей за {total_days} дней, {len(periods)} периодов × 3 колонки")

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                progress_desc = f"{self.symbol} {self.symbol_progress}ADX all periods {table_name.split('_')[-1].upper()}"
                pbar = tqdm(
                    total=total_days,
                    desc=progress_desc,
                    unit="d",
                    ncols=100,
                    bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
                )

                for date, day_data in grouped:
                    batch_data = []
                    for row in day_data.itertuples(index=False):
                        values = []
                        for p in periods:
                            adx_val = getattr(row, f'adx_{p}')
                            plus_di_val = getattr(row, f'adx_{p}_plus_di')
                            minus_di_val = getattr(row, f'adx_{p}_minus_di')
                            values.append(float(adx_val) if pd.notna(adx_val) else None)
                            values.append(float(plus_di_val) if pd.notna(plus_di_val) else None)
                            values.append(float(minus_di_val) if pd.notna(minus_di_val) else None)
                        values.append(row.timestamp)
                        values.append(row.symbol)
                        batch_data.append(tuple(values))

                    psycopg2.extras.execute_batch(cur, update_sql, batch_data, page_size=1000)

                    # Commit после каждого дня
                    conn.commit()
                    pbar.update(1)

                pbar.close()
                self.logger.info(f"✅ ADX all periods: {total_records:,} записей обновлено за {total_days} дней")

            except Exception as e:
                conn.rollback()
                pbar.close()
                self.logger.error(f"❌ Ошибка при обновлении ADX: {e}")
                raise
            finally:
                cur.close()

    def process_timeframe_single_pass(self, timeframe: str, periods: List[int]):
        """
        Single-pass расчёт и сохранение ADX для ВСЕХ периодов.

        Оптимизация (OBV pattern как в ATR):
        - 1 SQL SELECT (вместо 8)
        - 1 расчёт TR и DM (вместо 8)
        - 1 UPDATE на строку с 24 колонками (вместо 8 UPDATE по 3)
        - Commit после каждого дня

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h, 4h, 1d)
            periods: Список периодов ADX
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        self.logger.info(f"🚀 Single-pass ADX для {self.symbol} на таймфрейме {timeframe}")
        self.logger.info(f"📊 Периоды: {periods}")

        # Ensure columns exist for all periods
        for period in periods:
            self.ensure_columns_exist(timeframe, period)

        # Get data range
        min_date, max_date = self.get_data_range(timeframe)
        self.logger.info(f"📅 Диапазон данных: {min_date} - {max_date}")

        # ── Determine what to write ──
        write_timestamps = None  # None = use last_dates, set() = specific timestamps
        last_dates = None

        if self.force_reload:
            self.logger.info("🔄 Режим FORCE RELOAD: перезапись всех данных")
        elif self.check_nulls:
            all_nulls = self.get_all_null_timestamps(timeframe, periods)
            union_nulls = set()
            for p in periods:
                if all_nulls[p]:
                    self.logger.info(f"  🔍 ADX_{p}: {len(all_nulls[p])} NULL записей")
                    union_nulls |= all_nulls[p]
                else:
                    self.logger.info(f"  ✅ ADX_{p}: все данные актуальны")
            if not union_nulls:
                self.logger.info("✅ Все периоды ADX актуальны — нечего обновлять")
                return
            write_timestamps = union_nulls
            self.logger.info(f"🔍 Всего уникальных NULL timestamps: {len(write_timestamps)}")
        else:
            # Incremental
            last_dates = self.get_all_last_processed_dates(timeframe, periods)
            for p in periods:
                self.logger.info(f"  📅 ADX_{p} last_date: {last_dates[p]}")

            all_up_to_date = all(
                last_dates[p] is not None and last_dates[p] >= max_date
                for p in periods
            )
            if all_up_to_date:
                self.logger.info("✅ Все периоды ADX актуальны — нечего обновлять")
                return

        # ── Load all candles once ──
        print()
        print(f"🔄 [{self.symbol}] {self.symbol_progress}[{timeframe}] ADX single-pass: загрузка от начала истории")
        print("ℹ️  Wilder smoothing требует полную цепочку данных для точности")
        print()

        self.logger.info("Загрузка всех исторических свечей...")
        start_time = time.time()

        df = self.load_all_candles(timeframe)
        if df.empty:
            self.logger.warning(f"Нет данных для расчета ADX для {self.symbol}")
            return

        # Convert to float for calculations
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)

        # ── Calculate TR and DM once ──
        self.logger.info(f"Расчет TR и DM для {len(df):,} свечей...")
        tr = self.calculate_true_range(df['high'], df['low'], df['close'])
        plus_dm, minus_dm = self.calculate_directional_movement(df['high'], df['low'])

        # ── Calculate ADX for each period ──
        for period in periods:
            self.logger.info(f"Расчет ADX-{period}...")

            # Wilder smoothing of TR, +DM, -DM
            smoothed_tr = self.wilder_smoothing(tr, period)
            smoothed_plus_dm = self.wilder_smoothing(plus_dm, period)
            smoothed_minus_dm = self.wilder_smoothing(minus_dm, period)

            # +DI, -DI
            plus_di = 100 * smoothed_plus_dm / smoothed_tr
            minus_di = 100 * smoothed_minus_dm / smoothed_tr

            # DX
            di_sum = plus_di + minus_di
            di_diff = (plus_di - minus_di).abs()
            dx = pd.Series(np.nan, index=df.index)
            mask = di_sum != 0
            dx[mask] = 100 * di_diff[mask] / di_sum[mask]

            # Wilder smoothing of DX → ADX
            adx = self.wilder_smoothing(dx, period)

            df[f'adx_{period}'] = adx
            df[f'adx_{period}_plus_di'] = plus_di
            df[f'adx_{period}_minus_di'] = minus_di

        calc_time = time.time() - start_time
        if calc_time < 60:
            time_str = f"{calc_time:.1f} секунд"
        else:
            mins = int(calc_time // 60)
            secs = int(calc_time % 60)
            time_str = f"{mins} минут {secs} секунд"

        print(f"⏱️  Расчёт всех {len(periods)} периодов ADX завершён за {time_str}")
        print()
        self.logger.info(f"✓ Все ADX рассчитаны для {len(df):,} записей за {calc_time:.2f}s")

        # ── Filter data for writing ──
        if self.force_reload:
            df_to_update = df.copy()
            self.logger.info(f"Режим FORCE RELOAD: перезапись всех {len(df_to_update):,} записей")
        elif write_timestamps is not None:
            # check_nulls — only union NULL timestamps
            df_to_update = df[df['timestamp'].isin(write_timestamps)].copy()
            self.logger.info(f"Режим CHECK NULLS: запись {len(df_to_update):,} записей")
        else:
            # Incremental — from MIN(last_dates) of all periods
            valid_dates = [last_dates[p] for p in periods if last_dates[p] is not None]
            if valid_dates:
                earliest_last = min(valid_dates)
                df_to_update = df[df['timestamp'] > earliest_last].copy()
                self.logger.info(f"Incremental: {len(df_to_update):,} записей после {earliest_last}")
            else:
                # First load — all data
                df_to_update = df.copy()
                self.logger.info(f"Первичная загрузка: {len(df_to_update):,} записей")

        # ── Write all 24 columns in one UPDATE ──
        if not df_to_update.empty:
            self.batch_update_all_adx(df_to_update, table_name, periods)
        else:
            self.logger.info("✅ Нет новых данных для обновления")

        self.logger.info(f"🎉 Все периоды ADX для {timeframe} завершены!")

    # ══════════════════════════════════════════════════════════════════════════
    # Original methods (kept for backward compatibility and debugging)
    # ══════════════════════════════════════════════════════════════════════════

    def load_timeframe(
        self,
        timeframe: str,
        periods: List[int] = None
    ):
        """
        Load ADX for all periods in a specific timeframe.
        Uses single-pass calculation for full Wilder chain accuracy.

        Args:
            timeframe: Timeframe (1m, 15m, 1h, 4h, 1d)
            periods: List of periods to load (default: all PERIODS)
        """
        if periods is None:
            periods = PERIODS

        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"Processing timeframe: {timeframe}")
        self.logger.info(f"Periods: {periods}")

        self.process_timeframe_single_pass(timeframe, periods)

        self.logger.info(f"Completed timeframe {timeframe}")

    def load_all(
        self,
        timeframes: List[str] = None,
        periods: List[int] = None
    ):
        """
        Load ADX for all timeframes and periods

        Args:
            timeframes: List of timeframes (default: all TIMEFRAMES)
            periods: List of periods (default: all PERIODS)
        """
        if timeframes is None:
            timeframes = TIMEFRAMES

        if periods is None:
            periods = PERIODS

        self.logger.info("="*80)
        self.logger.info("ADX Loader - Starting")
        self.logger.info(f"Symbol: {self.symbol}")
        self.logger.info(f"Timeframes: {timeframes}")
        self.logger.info(f"Periods: {periods}")
        self.logger.info("="*80)

        for timeframe in timeframes:
            try:
                self.load_timeframe(timeframe, periods)
            except Exception as e:
                self.logger.error(f"Error loading timeframe {timeframe}: {e}", exc_info=True)
                continue

        self.logger.info("="*80)
        self.logger.info("ADX Loader - Completed")
        self.logger.info("="*80)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Load ADX indicators into database')
    parser.add_argument(
        '--symbol',
        type=str,
        default=None,
        help='One trading pair symbol (e.g., BTCUSDT)'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        default=None,
        help='Multiple trading pair symbols separated by comma (e.g., BTCUSDT,ETHUSDT)'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        choices=['1m', '15m', '1h', '4h', '1d', 'all'],
        default='all',
        help='Timeframe to process (default: all)'
    )
    parser.add_argument(
        '--period',
        type=int,
        choices=PERIODS,
        help='Specific period to process (default: all)'
    )
    parser.add_argument(
        '--batch-days',
        type=int,
        default=BATCH_DAYS,
        help=f'Batch size in days (default: {BATCH_DAYS})'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Custom start date (format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='Custom end date (format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Force reload even if data exists (ignores checkpoint)'
    )
    parser.add_argument(
        '--check-nulls',
        action='store_true',
        help='Check and fill NULL values in middle of data (excludes natural boundary)'
    )

    args = parser.parse_args()

    # Determine symbols to process
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        symbols = [args.symbol]
    else:
        import yaml
        config_path = Path(__file__).parent / 'indicators_config.yaml'
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    # Parse custom dates if provided
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    # Validate date range
    if (start_date and not end_date) or (end_date and not start_date):
        parser.error("Both --start-date and --end-date must be provided together")
    if start_date and end_date and start_date >= end_date:
        parser.error("--start-date must be before --end-date")

    # Prepare timeframes
    timeframes = TIMEFRAMES if args.timeframe == 'all' else [args.timeframe]

    # Prepare periods
    periods = PERIODS if args.period is None else [args.period]

    print(f"🎯 Processing symbols: {symbols}")

    # Засекаем общее время
    start_time = time.time()

    # Loop through all symbols
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        print(f"\n{'='*80}")
        print(f"📊 Starting processing for symbol: {symbol} [{idx}/{total_symbols}]")
        print(f"{'='*80}\n")

        try:
            # Create loader and run for current symbol
            loader = ADXLoader(
                symbol=symbol,
                batch_days=args.batch_days,
                start_date=start_date,
                end_date=end_date,
                force_reload=args.force_reload,
                check_nulls=args.check_nulls
            )
            loader.symbol_progress = f"[{idx}/{total_symbols}] "

            loader.load_all(timeframes=timeframes, periods=periods)

            print(f"\n✅ Symbol {symbol} processed\n")
        except KeyboardInterrupt:
            print("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Вычисляем общее время
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"\n🎉 All symbols processed: {symbols}")
    print(f"⏱️  Total time: {minutes}m {seconds}s")


if __name__ == '__main__':
    main()
