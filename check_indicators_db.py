#!/usr/bin/env python3
"""
Check Indicators Status in Database

This script analyzes all loaded indicators in the database and displays
a comprehensive status report with a visual table showing which indicators
are available for each trading pair.

Usage:
    python3 check_indicators_db.py
    python3 check_indicators_db.py --verbose  # Show detailed statistics

Author: Trading System
Date: 2025-10-22
"""

import sys
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor


# Database configuration
DB_CONFIG = {
    'host': '82.25.115.144',
    'port': 5432,
    'database': 'trading_data',
    'user': 'trading_admin',
    'password': 'K9mX3pQ8vN5bR2tL7wY4zA6cE1dF0gH'
}


# Indicators to check with their representative column
INDICATORS_MAP = {
    'SMA': 'sma_10',
    'EMA': 'ema_12',
    'RSI': 'rsi_14',
    'VMA': 'vma_20',
    'ATR': 'atr_14',
    'MACD': 'macd_12_26_9_line',
    'ADX': 'adx_14',
    'Stoch': 'stoch_14_3_3_k',
    'VWAP': 'vwap_daily',
    'MFI': 'mfi_14',
    'OBV': 'obv',
    'BB': 'bollinger_bands_sma_20_2_0_middle',
    'WR': 'williamsr_14',
    'F&G_Alt': 'fear_and_greed_index_alternative',  # Fear & Greed Alternative.me
    'F&G_CMC': 'fear_and_greed_index_coinmarketcap',  # Fear & Greed CoinMarketCap
    'Long/Short': 'long_short_ratio'  # Long/Short Ratio
}


# Mapping of indicators to timeframes where they are available
# Indicators not listed here default to checking '1m' timeframe
INDICATOR_TIMEFRAMES = {
    'Long/Short': ['15m', '1h'],  # API doesn't support 1m, check 15m and 1h instead
}


def get_database_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)


def get_all_symbols(conn):
    """Get all trading symbols from database"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT symbol
            FROM indicators_bybit_futures_1m
            ORDER BY symbol
        """)
        return [row[0] for row in cur.fetchall()]


def check_indicator_status(conn, symbols, days_back=7):
    """
    Check which indicators are loaded for each symbol

    Args:
        conn: Database connection
        symbols: List of trading symbols
        days_back: Check data from last N days (default: 7)

    Returns:
        Dictionary with status for each symbol and indicator
    """
    results = {}

    with conn.cursor() as cur:
        for symbol in symbols:
            results[symbol] = {}

            for indicator_name, column_name in INDICATORS_MAP.items():
                # Determine which timeframes to check for this indicator
                timeframes = INDICATOR_TIMEFRAMES.get(indicator_name, ['1m'])

                # Check if data exists in ANY of the timeframes
                has_data = False
                for timeframe in timeframes:
                    try:
                        cur.execute(f"""
                            SELECT COUNT({column_name}) as count
                            FROM indicators_bybit_futures_{timeframe}
                            WHERE symbol = %s
                            AND timestamp >= NOW() - INTERVAL '{days_back} days'
                        """, (symbol,))

                        count = cur.fetchone()[0]
                        if count > 0:
                            has_data = True
                            break  # Found data, no need to check other timeframes
                    except Exception:
                        # Table or column might not exist for this timeframe, skip
                        continue

                results[symbol][indicator_name] = has_data

    return results


def get_indicator_details(conn):
    """Get detailed information about each indicator"""
    details = {}

    with conn.cursor() as cur:
        for indicator_name, column_name in INDICATORS_MAP.items():
            # Determine which timeframes to check for this indicator
            timeframes = INDICATOR_TIMEFRAMES.get(indicator_name, ['1m'])

            # Count symbols with this indicator across all relevant timeframes
            symbol_count = 0
            for timeframe in timeframes:
                try:
                    cur.execute(f"""
                        SELECT COUNT(DISTINCT symbol) as symbol_count
                        FROM indicators_bybit_futures_{timeframe}
                        WHERE {column_name} IS NOT NULL
                        AND timestamp >= NOW() - INTERVAL '7 days'
                    """)

                    count = cur.fetchone()[0]
                    # Use the maximum count across all timeframes
                    symbol_count = max(symbol_count, count)
                except Exception:
                    # Table or column might not exist for this timeframe, skip
                    continue

            details[indicator_name] = {
                'symbol_count': symbol_count,
                'column': column_name
            }

    return details


def print_status_table(results, symbols):
    """Print beautiful status table"""

    print("\n" + "="*100)
    print("📋 СВОДНАЯ ТАБЛИЦА ПО МОНЕТАМ")
    print("="*100)

    # Header
    headers = ['Монета'] + list(INDICATORS_MAP.keys())

    # Fixed widths: first column (symbol) = 10
    # Most indicators = 5, longer ones (F&G_Alt, F&G_CMC, Long/Short) = 10
    symbol_width = 10

    # Calculate width for each indicator column
    col_widths = [symbol_width]
    for h in headers[1:]:
        if len(h) > 5:
            col_widths.append(10)  # Wider for long names
        else:
            col_widths.append(5)   # Compact for short names

    # Print header
    header_parts = []
    for h, w in zip(headers, col_widths):
        if h == 'Монета':
            header_parts.append(f"{h:<{w}}")
        else:
            header_parts.append(f"{h:^{w}}")

    header_line = " | ".join(header_parts)
    print(f"\n| {header_line} |")

    # Print separator
    separator_parts = ["-" * w for w in col_widths]
    separator = "-+-".join(separator_parts)
    print(f"|-{separator}-|")

    # Print data rows
    for symbol in symbols:
        row_parts = [f"{symbol:<{col_widths[0]}}"]

        for idx, indicator_name in enumerate(INDICATORS_MAP.keys(), 1):
            status = results[symbol][indicator_name]
            # Use text symbols instead of emojis for consistent width
            symbol_char = "✓" if status else "✗"
            # Center symbol in appropriate width
            row_parts.append(f"{symbol_char:^{col_widths[idx]}}")

        row_line = " | ".join(row_parts)
        print(f"| {row_line} |")

    print("="*100)


def print_summary_statistics(results, details, symbols):
    """Print summary statistics"""

    print("\n" + "="*100)
    print("📊 СТАТИСТИКА ПО ИНДИКАТОРАМ")
    print("="*100)

    # Count indicators by status
    total_indicators = len(INDICATORS_MAP)

    fully_loaded = []  # All 10 symbols
    partially_loaded = []  # Some symbols
    not_loaded = []  # No symbols

    for indicator_name, detail in details.items():
        count = detail['symbol_count']

        if count == len(symbols):
            fully_loaded.append(indicator_name)
        elif count > 0:
            partially_loaded.append((indicator_name, count))
        else:
            not_loaded.append(indicator_name)

    print(f"\n✅ ПОЛНОСТЬЮ ЗАГРУЖЕНЫ (все {len(symbols)} монет): {len(fully_loaded)} индикаторов")
    for indicator in fully_loaded:
        print(f"   • {indicator}")

    print(f"\n⚠️  ЧАСТИЧНО ЗАГРУЖЕНЫ: {len(partially_loaded)} индикаторов")
    for indicator, count in partially_loaded:
        symbols_with_data = [s for s in symbols if results[s][indicator]]
        print(f"   • {indicator}: {count}/{len(symbols)} монет ({', '.join(symbols_with_data)})")

    if not_loaded:
        print(f"\n❌ НЕ ЗАГРУЖЕНЫ: {len(not_loaded)} индикаторов")
        for indicator in not_loaded:
            print(f"   • {indicator}")

    # Overall statistics
    print(f"\n📈 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего индикаторов: {total_indicators}")
    print(f"   Полностью загружено: {len(fully_loaded)}")
    print(f"   Частично загружено: {len(partially_loaded)}")
    print(f"   Не загружено: {len(not_loaded)}")
    print(f"   Торговых пар в БД: {len(symbols)}")

    # Calculate completion percentage
    total_possible = total_indicators * len(symbols)
    total_loaded = sum(1 for s in symbols for i in INDICATORS_MAP.keys() if results[s][i])
    completion_pct = (total_loaded / total_possible) * 100

    print(f"\n   Заполнение: {total_loaded}/{total_possible} ({completion_pct:.1f}%)")

    print("="*100)


def print_recommendations(results, details, symbols):
    """Print recommendations for next steps"""

    print("\n" + "="*100)
    print("🎯 РЕКОМЕНДАЦИИ ДЛЯ ДАЛЬНЕЙШЕЙ РАЗРАБОТКИ")
    print("="*100)

    # Find indicators that need completion
    needs_completion = []

    for indicator_name, detail in details.items():
        count = detail['symbol_count']

        if 0 < count < len(symbols):
            missing_symbols = [s for s in symbols if not results[s][indicator_name]]
            needs_completion.append({
                'indicator': indicator_name,
                'loaded': count,
                'total': len(symbols),
                'missing': missing_symbols
            })

    # Sort by priority (most loaded first)
    needs_completion.sort(key=lambda x: x['loaded'], reverse=True)

    if needs_completion:
        print("\n📋 Приоритет 1: Дозагрузить частично загруженные индикаторы")
        print("-" * 100)

        for i, item in enumerate(needs_completion, 1):
            print(f"\n{i}. {item['indicator']}")
            print(f"   Статус: {item['loaded']}/{item['total']} монет загружено")
            print(f"   Осталось загрузить для: {', '.join(item['missing'])}")

    # Find not loaded indicators
    not_loaded = [name for name, detail in details.items() if detail['symbol_count'] == 0]

    if not_loaded:
        print("\n\n📋 Приоритет 2: Загрузить новые индикаторы")
        print("-" * 100)

        for i, indicator in enumerate(not_loaded, 1):
            print(f"{i}. {indicator} - запустить загрузку для всех монет")

    print("\n" + "="*100)


def print_legend():
    """Print legend for table symbols"""

    print("\n" + "="*100)
    print("📖 ЛЕГЕНДА")
    print("="*100)
    print("""
✓ = Данные загружены
✗ = Данные НЕ загружены

Сокращения индикаторов:
  SMA         - Simple Moving Average (простая скользящая средняя)
  EMA         - Exponential Moving Average (экспоненциальная скользящая средняя)
  RSI         - Relative Strength Index (индекс относительной силы)
  VMA         - Volume Moving Average (скользящая средняя объема)
  ATR         - Average True Range (средний истинный диапазон)
  MACD        - Moving Average Convergence Divergence (схождение-расхождение скользящих средних)
  ADX         - Average Directional Index (индекс направленного движения)
  Stoch       - Stochastic Oscillator (стохастический осциллятор)
  VWAP        - Volume Weighted Average Price (средневзвешенная цена по объему)
  MFI         - Money Flow Index (индекс денежного потока)
  OBV         - On-Balance Volume (балансовый объем)
  BB          - Bollinger Bands (полосы Боллинджера)
  WR          - Williams %R (индикатор Уильямса %R)
  F&G_Alt     - Fear & Greed Index от Alternative.me
  F&G_CMC     - Fear & Greed Index от CoinMarketCap
  Long/Short  - Long/Short Ratio (соотношение лонгов и шортов)
    """)
    print("="*100)


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description='Check indicators status in database')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show verbose output')
    parser.add_argument('--days', type=int, default=7, help='Check data from last N days (default: 7)')
    args = parser.parse_args()

    print("\n" + "="*100)
    print("📊 ПРОВЕРКА СТАТУСА ИНДИКАТОРОВ В БД")
    print(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*100)

    # Connect to database
    print("\n⏳ Подключение к базе данных...")
    conn = get_database_connection()

    try:
        # Get all symbols
        print("⏳ Получение списка торговых пар...")
        symbols = get_all_symbols(conn)
        print(f"✅ Найдено {len(symbols)} торговых пар: {', '.join(symbols)}")

        # Check indicators status
        print(f"\n⏳ Проверка статуса индикаторов (последние {args.days} дней)...")
        results = check_indicator_status(conn, symbols, args.days)

        # Get detailed statistics
        print("⏳ Сбор детальной статистики...")
        details = get_indicator_details(conn)

        # Print results
        print_status_table(results, symbols)
        print_summary_statistics(results, details, symbols)
        print_recommendations(results, details, symbols)
        print_legend()

        print("\n✅ Проверка завершена успешно!\n")

    except Exception as e:
        print(f"\n❌ Ошибка при выполнении: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
