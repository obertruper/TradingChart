#!/usr/bin/env python3
"""
Verify ADX data reload for specific date range
Checks if the force-reload successfully filled the gap in ADX data

Author: Trading System
Date: 2025-10-20
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection


def verify_adx_data(
    symbol: str = 'BTCUSDT',
    start_date: str = '2023-02-25',
    end_date: str = '2023-02-27',
    timeframe: str = '1m'
):
    """
    Verify ADX data for specific date range

    Args:
        symbol: Trading pair symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        timeframe: Timeframe (1m, 15m, 1h)
    """
    db = DatabaseConnection()
    table_name = f'indicators_bybit_futures_{timeframe}'

    # ADX periods from config
    periods = [7, 10, 14, 20, 21, 25, 30, 50]

    print("=" * 100)
    print(f"ADX Data Verification Report")
    print(f"Symbol: {symbol} | Timeframe: {timeframe}")
    print(f"Date Range: {start_date} to {end_date}")
    print("=" * 100)
    print()

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Check total records per day
            print("1. RECORDS PER DAY")
            print("-" * 100)

            query_daily = f"""
                SELECT
                    DATE(timestamp) as date,
                    COUNT(*) as total_records
                FROM {table_name}
                WHERE symbol = %s
                AND timestamp >= %s::timestamptz
                AND timestamp < %s::timestamptz
                GROUP BY DATE(timestamp)
                ORDER BY date
            """

            cur.execute(query_daily, (symbol, start_date, end_date))
            daily_results = cur.fetchall()

            expected_1m = 1440  # 1440 minutes per day for 1m timeframe
            expected_15m = 96   # 96 periods per day for 15m
            expected_1h = 24    # 24 periods per day for 1h

            expected_records = {
                '1m': expected_1m,
                '15m': expected_15m,
                '1h': expected_1h
            }[timeframe]

            print(f"{'Date':<15} {'Records':<15} {'Expected':<15} {'Status':<15}")
            print("-" * 100)

            total_records = 0
            for row in daily_results:
                date = row['date']
                records = row['total_records']
                total_records += records
                status = "✓ OK" if records == expected_records else f"✗ Missing {expected_records - records}"
                print(f"{date!s:<15} {records:<15} {expected_records:<15} {status:<15}")

            print("-" * 100)
            print(f"{'TOTAL':<15} {total_records:<15}")
            print()

            # 2. Check ADX fill rate per period
            print("2. ADX FILL RATE PER PERIOD")
            print("-" * 100)

            print(f"{'Period':<10} {'ADX Filled':<15} {'Plus DI':<15} {'Minus DI':<15} {'Fill Rate %':<15} {'Status':<15}")
            print("-" * 100)

            all_periods_ok = True

            for period in periods:
                adx_col = f'adx_{period}'
                plus_di_col = f'adx_{period}_plus_di'
                minus_di_col = f'adx_{period}_minus_di'

                query_fill = f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT({adx_col}) as adx_filled,
                        COUNT({plus_di_col}) as plus_di_filled,
                        COUNT({minus_di_col}) as minus_di_filled,
                        ROUND(100.0 * COUNT({adx_col}) / COUNT(*), 2) as fill_rate
                    FROM {table_name}
                    WHERE symbol = %s
                    AND timestamp >= %s::timestamptz
                    AND timestamp < %s::timestamptz
                """

                cur.execute(query_fill, (symbol, start_date, end_date))
                result = cur.fetchone()

                total = result['total']
                adx_filled = result['adx_filled']
                plus_di = result['plus_di_filled']
                minus_di = result['minus_di_filled']
                fill_rate = result['fill_rate']

                # For 1m timeframe, we expect some records at the beginning to be NULL due to lookback
                # But for the gap fill (2023-02-25 to 2023-02-27), most should be filled
                expected_fill_rate = 95.0 if timeframe == '1m' else 98.0
                status = "✓ OK" if fill_rate >= expected_fill_rate else f"✗ Low ({fill_rate}%)"

                if fill_rate < expected_fill_rate:
                    all_periods_ok = False

                print(f"ADX_{period:<5} {adx_filled:<15} {plus_di:<15} {minus_di:<15} {fill_rate:<15.2f} {status:<15}")

            print("-" * 100)
            print()

            # 3. Check for NULL records (records that should be filled but aren't)
            print("3. NULL RECORDS ANALYSIS")
            print("-" * 100)

            for period in periods:
                adx_col = f'adx_{period}'

                query_nulls = f"""
                    SELECT
                        COUNT(*) as null_count,
                        MIN(timestamp) as first_null,
                        MAX(timestamp) as last_null
                    FROM {table_name}
                    WHERE symbol = %s
                    AND timestamp >= %s::timestamptz
                    AND timestamp < %s::timestamptz
                    AND {adx_col} IS NULL
                """

                cur.execute(query_nulls, (symbol, start_date, end_date))
                result = cur.fetchone()

                null_count = result['null_count']

                if null_count > 0:
                    first_null = result['first_null']
                    last_null = result['last_null']
                    print(f"ADX_{period}: {null_count} NULL records found")
                    print(f"  First NULL: {first_null}")
                    print(f"  Last NULL:  {last_null}")
                else:
                    print(f"ADX_{period}: ✓ No NULL records (all filled)")

            print()

            # 4. Validate data ranges (ADX should be 0-100)
            print("4. DATA VALIDATION (ADX range 0-100)")
            print("-" * 100)

            all_valid = True

            for period in periods:
                adx_col = f'adx_{period}'
                plus_di_col = f'adx_{period}_plus_di'
                minus_di_col = f'adx_{period}_minus_di'

                query_validation = f"""
                    SELECT
                        MIN({adx_col}) as min_adx,
                        MAX({adx_col}) as max_adx,
                        AVG({adx_col}) as avg_adx,
                        MIN({plus_di_col}) as min_plus_di,
                        MAX({plus_di_col}) as max_plus_di,
                        MIN({minus_di_col}) as min_minus_di,
                        MAX({minus_di_col}) as max_minus_di,
                        COUNT(CASE WHEN {adx_col} < 0 OR {adx_col} > 100 THEN 1 END) as invalid_adx,
                        COUNT(CASE WHEN {plus_di_col} < 0 THEN 1 END) as invalid_plus_di,
                        COUNT(CASE WHEN {minus_di_col} < 0 THEN 1 END) as invalid_minus_di
                    FROM {table_name}
                    WHERE symbol = %s
                    AND timestamp >= %s::timestamptz
                    AND timestamp < %s::timestamptz
                    AND {adx_col} IS NOT NULL
                """

                cur.execute(query_validation, (symbol, start_date, end_date))
                result = cur.fetchone()

                invalid_adx = result['invalid_adx'] or 0
                invalid_plus_di = result['invalid_plus_di'] or 0
                invalid_minus_di = result['invalid_minus_di'] or 0

                if invalid_adx > 0 or invalid_plus_di > 0 or invalid_minus_di > 0:
                    print(f"ADX_{period}: ✗ INVALID DATA FOUND")
                    if invalid_adx > 0:
                        print(f"  Invalid ADX values: {invalid_adx}")
                    if invalid_plus_di > 0:
                        print(f"  Invalid +DI values: {invalid_plus_di}")
                    if invalid_minus_di > 0:
                        print(f"  Invalid -DI values: {invalid_minus_di}")
                    all_valid = False
                else:
                    min_adx = result['min_adx']
                    max_adx = result['max_adx']
                    avg_adx = result['avg_adx']
                    print(f"ADX_{period}: ✓ Valid (min={min_adx:.2f}, max={max_adx:.2f}, avg={avg_adx:.2f})")

            print()

            # 5. Summary
            print("=" * 100)
            print("SUMMARY")
            print("=" * 100)

            if total_records == expected_records * len(daily_results):
                print(f"✓ Total records: {total_records} (expected {expected_records * len(daily_results)})")
            else:
                print(f"✗ Total records: {total_records} (expected {expected_records * len(daily_results)})")

            if all_periods_ok:
                print("✓ All ADX periods have good fill rates (>95%)")
            else:
                print("✗ Some ADX periods have low fill rates")

            if all_valid:
                print("✓ All data values are within valid ranges")
            else:
                print("✗ Some data values are invalid")

            print()

            # Overall status
            if total_records > 0 and all_periods_ok and all_valid:
                print("🎉 SUCCESS: ADX data reload completed successfully!")
                print(f"   The gap for {start_date} to {end_date} has been filled.")
            else:
                print("⚠️  WARNING: ADX data reload may have issues. Please review the report above.")

            print("=" * 100)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Verify ADX data reload')
    parser.add_argument(
        '--symbol',
        type=str,
        default='BTCUSDT',
        help='Trading pair symbol (default: BTCUSDT)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2023-02-25',
        help='Start date (YYYY-MM-DD, default: 2023-02-25)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='2023-02-27',
        help='End date (YYYY-MM-DD, default: 2023-02-27)'
    )
    parser.add_argument(
        '--timeframe',
        type=str,
        choices=['1m', '15m', '1h'],
        default='1m',
        help='Timeframe (default: 1m)'
    )

    args = parser.parse_args()

    verify_adx_data(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        timeframe=args.timeframe
    )


if __name__ == '__main__':
    main()
