#!/usr/bin/env python3
"""
ADX Status Checker

This script checks the status of ADX indicators in the database.
Shows fill statistics, latest values with interpretation, and gap detection.

Author: Trading System
Date: 2025-10-17
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import warnings
import pandas as pd

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# ADX Configuration
PERIODS = [7, 10, 14, 20, 21, 25, 30, 50]
TIMEFRAMES = ['1m', '15m', '1h']


class ADXStatusChecker:
    """
    Check status of ADX indicators in database

    Features:
    - Fill statistics per period and timeframe
    - Latest values with trend interpretation
    - Gap detection for last 30 days
    - Trend strength analysis
    """

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Initialize ADX Status Checker

        Args:
            symbol: Trading pair symbol
        """
        self.symbol = symbol
        self.db = DatabaseConnection()

    def get_column_names(self, period: int) -> Dict[str, str]:
        """
        Get column names for a specific period

        Args:
            period: ADX period

        Returns:
            Dictionary with column names
        """
        return {
            'adx': f'adx_{period}',
            'plus_di': f'adx_{period}_plus_di',
            'minus_di': f'adx_{period}_minus_di'
        }

    def interpret_adx_strength(self, adx_value: float) -> str:
        """
        Interpret ADX strength value

        Args:
            adx_value: ADX value (0-100)

        Returns:
            Interpretation string
        """
        if adx_value < 25:
            return "Weak/Absent trend (sideways) 📊"
        elif adx_value < 50:
            return "Strong trend 📈"
        elif adx_value < 75:
            return "Very strong trend 🚀"
        else:
            return "Extremely strong trend (possible exhaustion) ⚠️"

    def interpret_trend_direction(
        self,
        plus_di: float,
        minus_di: float
    ) -> Tuple[str, str]:
        """
        Interpret trend direction from +DI/-DI

        Args:
            plus_di: +DI value
            minus_di: -DI value

        Returns:
            Tuple of (direction, strength description)
        """
        di_diff = abs(plus_di - minus_di)

        if plus_di > minus_di:
            direction = "Bullish (восходящий) 🟢"
        else:
            direction = "Bearish (нисходящий) 🔴"

        # Strength based on difference
        if di_diff < 5:
            strength = "Weak direction (слабое направление)"
        elif di_diff < 15:
            strength = "Moderate direction (умеренное направление)"
        elif di_diff < 30:
            strength = "Strong direction (сильное направление)"
        else:
            strength = "Very strong direction (очень сильное направление)"

        return direction, strength

    def check_fill_statistics(self, timeframe: str, period: int) -> Dict:
        """
        Check fill statistics for a specific period and timeframe

        Args:
            timeframe: Timeframe (1m, 15m, 1h)
            period: ADX period

        Returns:
            Dictionary with statistics
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Total records
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                total = cur.fetchone()[0]

                # Filled records
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE symbol = %s
                    AND {columns['adx']} IS NOT NULL
                """, (self.symbol,))
                filled = cur.fetchone()[0]

                # Date range
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                    AND {columns['adx']} IS NOT NULL
                """, (self.symbol,))
                date_range = cur.fetchone()

                fill_percent = (filled / total * 100) if total > 0 else 0

                return {
                    'total': total,
                    'filled': filled,
                    'fill_percent': fill_percent,
                    'start_date': date_range[0],
                    'end_date': date_range[1]
                }

    def get_latest_values(self, timeframe: str, period: int, limit: int = 10) -> pd.DataFrame:
        """
        Get latest ADX values

        Args:
            timeframe: Timeframe (1m, 15m, 1h)
            period: ADX period
            limit: Number of records to retrieve

        Returns:
            DataFrame with latest values
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)

        with self.db.get_connection() as conn:
            query = f"""
                SELECT
                    timestamp,
                    close,
                    {columns['adx']} as adx,
                    {columns['plus_di']} as plus_di,
                    {columns['minus_di']} as minus_di
                FROM {table_name}
                WHERE symbol = %s
                AND {columns['adx']} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT %s
            """

            df = pd.read_sql_query(query, conn, params=(self.symbol, limit))

            return df

    def find_gaps(self, timeframe: str, period: int, days: int = 30) -> List[Tuple]:
        """
        Find gaps in ADX data for the last N days

        Args:
            timeframe: Timeframe (1m, 15m, 1h)
            period: ADX period
            days: Number of days to check

        Returns:
            List of gap tuples (start, end)
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        columns = self.get_column_names(period)

        # Calculate expected interval in minutes
        interval_minutes = {
            '1m': 1,
            '15m': 15,
            '1h': 60
        }[timeframe]

        cutoff_date = datetime.now() - timedelta(days=days)

        with self.db.get_connection() as conn:
            query = f"""
                SELECT timestamp
                FROM {table_name}
                WHERE symbol = %s
                AND timestamp >= %s
                AND {columns['adx']} IS NOT NULL
                ORDER BY timestamp
            """

            df = pd.read_sql_query(query, conn, params=(self.symbol, cutoff_date))

            if df.empty:
                return []

            # Find gaps
            gaps = []
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['time_diff'] = df['timestamp'].diff()

            expected_diff = timedelta(minutes=interval_minutes)
            tolerance = timedelta(minutes=interval_minutes * 2)  # Allow some tolerance

            for i in range(1, len(df)):
                if df['time_diff'].iloc[i] > tolerance:
                    gap_start = df['timestamp'].iloc[i - 1]
                    gap_end = df['timestamp'].iloc[i]
                    gaps.append((gap_start, gap_end, df['time_diff'].iloc[i]))

            return gaps

    def print_summary(self):
        """Print comprehensive ADX status summary"""
        print("=" * 100)
        print(f"ADX Status Report - {self.symbol}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)

        for timeframe in TIMEFRAMES:
            print(f"\n{'='*100}")
            print(f"Timeframe: {timeframe}")
            print(f"{'='*100}")

            for period in PERIODS:
                print(f"\n{'-'*100}")
                print(f"ADX_{period}")
                print(f"{'-'*100}")

                try:
                    # Fill statistics
                    stats = self.check_fill_statistics(timeframe, period)

                    print(f"\n📊 Fill Statistics:")
                    print(f"  Total records:  {stats['total']:,}")
                    print(f"  Filled records: {stats['filled']:,}")
                    print(f"  Fill rate:      {stats['fill_percent']:.2f}%")

                    if stats['start_date'] and stats['end_date']:
                        print(f"  Date range:     {stats['start_date']} to {stats['end_date']}")

                    # Latest values
                    df_latest = self.get_latest_values(timeframe, period, limit=5)

                    if not df_latest.empty:
                        print(f"\n📈 Latest Values (top 5):")
                        print(f"\n{'Timestamp':<20} {'Close':>12} {'ADX':>8} {'+DI':>8} {'-DI':>8} {'Interpretation':<50}")
                        print("-" * 100)

                        for _, row in df_latest.iterrows():
                            adx_val = row['adx']
                            plus_di_val = row['plus_di']
                            minus_di_val = row['minus_di']

                            # Interpretation
                            strength = self.interpret_adx_strength(adx_val)
                            direction, dir_strength = self.interpret_trend_direction(
                                plus_di_val,
                                minus_di_val
                            )

                            interpretation = f"{strength} | {direction} | {dir_strength}"

                            print(
                                f"{row['timestamp']!s:<20} "
                                f"{row['close']:>12.2f} "
                                f"{adx_val:>8.2f} "
                                f"{plus_di_val:>8.2f} "
                                f"{minus_di_val:>8.2f} "
                                f"{interpretation:<50}"
                            )

                        # Show interpretation legend for first period only
                        if period == PERIODS[0] and timeframe == TIMEFRAMES[0]:
                            print(f"\n💡 Interpretation Guide:")
                            print(f"  ADX Strength:")
                            print(f"    0-25:   Weak/Absent trend (sideways movement)")
                            print(f"    25-50:  Strong trend (good for trend strategies)")
                            print(f"    50-75:  Very strong trend (excellent for trending)")
                            print(f"    75-100: Extremely strong trend (possible exhaustion)")
                            print(f"\n  Trend Direction:")
                            print(f"    +DI > -DI: Bullish trend (восходящий тренд)")
                            print(f"    -DI > +DI: Bearish trend (нисходящий тренд)")
                            print(f"    Difference shows direction strength")

                    # Gap detection
                    gaps = self.find_gaps(timeframe, period, days=30)

                    if gaps:
                        print(f"\n⚠️  Gaps detected (last 30 days): {len(gaps)}")
                        for gap_start, gap_end, gap_duration in gaps[:5]:  # Show first 5
                            print(f"  {gap_start} → {gap_end} (gap: {gap_duration})")
                    else:
                        print(f"\n✅ No gaps detected (last 30 days)")

                except Exception as e:
                    print(f"\n❌ Error checking ADX_{period}: {e}")

        print("\n" + "=" * 100)
        print("Report Complete")
        print("=" * 100)

    def print_comparison_values(self, timeframe: str = '1h'):
        """
        Print values suitable for comparison with TradingView/Bybit

        Args:
            timeframe: Timeframe to check (default: 1h for easier verification)
        """
        print("\n" + "=" * 100)
        print(f"ADX Values for TradingView/Bybit Comparison - {timeframe}")
        print("=" * 100)

        for period in [14, 20]:  # Show most common periods
            print(f"\n{'-'*100}")
            print(f"ADX_{period}")
            print(f"{'-'*100}")

            try:
                df = self.get_latest_values(timeframe, period, limit=3)

                if not df.empty:
                    print(f"\n{'Timestamp':<20} {'Close':>12} {'ADX':>8} {'+DI':>8} {'-DI':>8}")
                    print("-" * 60)

                    for _, row in df.iterrows():
                        print(
                            f"{row['timestamp']!s:<20} "
                            f"{row['close']:>12.2f} "
                            f"{row['adx']:>8.4f} "
                            f"{row['plus_di']:>8.4f} "
                            f"{row['minus_di']:>8.4f}"
                        )

                    print(f"\n✓ Compare these values with TradingView/Bybit chart at {timeframe} timeframe")
                    print(f"  ADX({period}) should match the main ADX line")
                    print(f"  +DI and -DI should match the directional indicators")

            except Exception as e:
                print(f"\n❌ Error: {e}")

        print("\n" + "=" * 100)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Check ADX indicator status')
    parser.add_argument(
        '--symbol',
        type=str,
        default='BTCUSDT',
        help='Trading pair symbol (default: BTCUSDT)'
    )
    parser.add_argument(
        '--comparison',
        action='store_true',
        help='Show values for TradingView/Bybit comparison'
    )

    args = parser.parse_args()

    checker = ADXStatusChecker(symbol=args.symbol)

    # Print full summary
    checker.print_summary()

    # Print comparison values if requested
    if args.comparison:
        checker.print_comparison_values()


if __name__ == '__main__':
    main()
