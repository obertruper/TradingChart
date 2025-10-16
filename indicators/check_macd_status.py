#!/usr/bin/env python3
"""
MACD Status Checker
===================
Проверка статуса MACD индикаторов в базе данных.

Показывает:
- Наличие колонок MACD для всех конфигураций
- Статистику заполнения данных
- Последние значения MACD
- Пропуски в данных (gaps)
"""

import sys
import os
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators.database import DatabaseConnection


# Конфигурации MACD для проверки
MACD_CONFIGS = [
    {'name': 'classic', 'fast': 12, 'slow': 26, 'signal': 9, 'description': 'Classic MACD - industry standard'},
    {'name': 'crypto', 'fast': 6, 'slow': 13, 'signal': 5, 'description': 'Optimized for crypto volatility'},
    {'name': 'aggressive', 'fast': 5, 'slow': 35, 'signal': 5, 'description': 'Aggressive short/long'},
    {'name': 'balanced', 'fast': 8, 'slow': 17, 'signal': 9, 'description': 'Fast but stable'},
    {'name': 'scalping', 'fast': 5, 'slow': 13, 'signal': 3, 'description': 'Ultra-fast scalping'},
    {'name': 'swing', 'fast': 10, 'slow': 21, 'signal': 9, 'description': 'Medium-term swing'},
    {'name': 'longterm', 'fast': 21, 'slow': 55, 'signal': 13, 'description': 'Long-term trend'},
    {'name': 'ultralong', 'fast': 50, 'slow': 200, 'signal': 9, 'description': 'Very slow trend'},
]


def check_macd_columns(symbol: str = 'BTCUSDT', timeframe: str = '1m'):
    """
    Проверяет наличие колонок MACD в таблице

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*80}")
    print(f"MACD Status for {symbol} ({timeframe})")
    print(f"{'='*80}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Получаем общее количество свечей
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE symbol = %s
        """, (symbol,))
        total_candles = cur.fetchone()[0]

        print(f"📊 Всего свечей в таблице: {total_candles:,}\n")

        # Проверяем каждую конфигурацию
        for config in MACD_CONFIGS:
            fast = config['fast']
            slow = config['slow']
            signal_period = config['signal']

            base_name = f"macd_{fast}_{slow}_{signal_period}"
            columns = {
                'line': f"{base_name}_line",
                'signal': f"{base_name}_signal",
                'histogram': f"{base_name}_histogram"
            }

            print(f"Configuration: {config['name']} ({fast}, {slow}, {signal_period})")
            print(f"Description: {config['description']}")

            # Проверяем существование колонок
            column_exists = {}
            for comp_name, col_name in columns.items():
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name = %s
                        AND column_name = %s
                    );
                """, (table_name, col_name))

                column_exists[comp_name] = cur.fetchone()[0]

            # Если колонки не существуют
            if not all(column_exists.values()):
                print(f"  ❌ Колонки не созданы в БД")
                missing = [name for name, exists in column_exists.items() if not exists]
                print(f"  Отсутствуют: {', '.join(missing)}")
                print()
                continue

            # Проверяем заполнение данных
            fill_stats = {}
            for comp_name, col_name in columns.items():
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE symbol = %s AND {col_name} IS NOT NULL
                """, (symbol,))

                filled = cur.fetchone()[0]
                fill_pct = (filled / total_candles * 100) if total_candles > 0 else 0
                fill_stats[comp_name] = {'filled': filled, 'percent': fill_pct}

            # Выводим статистику
            print(f"  ├─ Line:      {fill_stats['line']['filled']:,} / {total_candles:,} ({fill_stats['line']['percent']:.1f}%)")
            print(f"  ├─ Signal:    {fill_stats['signal']['filled']:,} / {total_candles:,} ({fill_stats['signal']['percent']:.1f}%)")
            print(f"  └─ Histogram: {fill_stats['histogram']['filled']:,} / {total_candles:,} ({fill_stats['histogram']['percent']:.1f}%)")

            # Получаем последние значения
            line_col = columns['line']
            signal_col = columns['signal']
            hist_col = columns['histogram']

            cur.execute(f"""
                SELECT timestamp, {line_col}, {signal_col}, {hist_col}
                FROM {table_name}
                WHERE symbol = %s
                  AND {line_col} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))

            result = cur.fetchone()
            if result:
                ts, line_val, signal_val, hist_val = result
                print(f"\n  Latest values ({ts}):")
                print(f"    Line:      {line_val:.8f}")
                print(f"    Signal:    {signal_val:.8f}")
                print(f"    Histogram: {hist_val:.8f}")

                # Определяем тренд
                if hist_val > 0:
                    trend = "🟢 Bullish (Histogram > 0)"
                elif hist_val < 0:
                    trend = "🔴 Bearish (Histogram < 0)"
                else:
                    trend = "⚪ Neutral (Histogram = 0)"

                print(f"    Trend: {trend}")

            print()

        cur.close()


def check_macd_gaps(symbol: str = 'BTCUSDT', timeframe: str = '1m', days: int = 30):
    """
    Проверяет пропуски в данных MACD за последние N дней

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        days: Количество дней для проверки
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*80}")
    print(f"Gap Detection for MACD (last {days} days)")
    print(f"{'='*80}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Дата начала проверки
        start_date = datetime.now() - timedelta(days=days)

        for config in MACD_CONFIGS:
            fast = config['fast']
            slow = config['slow']
            signal_period = config['signal']

            base_name = f"macd_{fast}_{slow}_{signal_period}"
            col_name = f"{base_name}_line"

            # Проверяем существование колонки
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = %s
                );
            """, (table_name, col_name))

            if not cur.fetchone()[0]:
                continue  # Колонка не существует, пропускаем

            # Ищем пропуски
            cur.execute(f"""
                WITH candle_data AS (
                    SELECT
                        timestamp,
                        {col_name},
                        LAG({col_name}) OVER (ORDER BY timestamp) as prev_value
                    FROM {table_name}
                    WHERE symbol = %s
                      AND timestamp >= %s
                    ORDER BY timestamp
                )
                SELECT
                    timestamp,
                    {col_name},
                    prev_value
                FROM candle_data
                WHERE {col_name} IS NULL AND prev_value IS NOT NULL
            """, (symbol, start_date))

            gaps = cur.fetchall()

            if gaps:
                print(f"⚠️  MACD {config['name']} ({fast}, {slow}, {signal_period}): {len(gaps)} gaps found")
                if len(gaps) <= 10:
                    for gap in gaps[:10]:
                        print(f"    {gap[0]}")
            else:
                print(f"✅ MACD {config['name']} ({fast}, {slow}, {signal_period}): No gaps")

        cur.close()


def show_macd_examples(symbol: str = 'BTCUSDT', timeframe: str = '1m', limit: int = 10):
    """
    Показывает примеры значений MACD для верификации

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        limit: Количество последних записей
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*80}")
    print(f"Latest MACD Values (Top {limit})")
    print(f"{'='*80}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Берём classic MACD для примера
        config = MACD_CONFIGS[0]  # classic
        fast = config['fast']
        slow = config['slow']
        signal_period = config['signal']

        base_name = f"macd_{fast}_{slow}_{signal_period}"
        line_col = f"{base_name}_line"
        signal_col = f"{base_name}_signal"
        hist_col = f"{base_name}_histogram"

        # Проверяем существование колонки
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = %s
            );
        """, (table_name, line_col))

        if not cur.fetchone()[0]:
            print(f"❌ Колонки MACD classic не созданы")
            return

        # Получаем последние значения с close ценой для верификации
        cur.execute(f"""
            SELECT
                i.timestamp,
                c.close,
                i.{line_col},
                i.{signal_col},
                i.{hist_col}
            FROM {table_name} i
            JOIN candles_bybit_futures_{timeframe} c
                ON i.timestamp = c.timestamp AND i.symbol = c.symbol
            WHERE i.symbol = %s
              AND i.{line_col} IS NOT NULL
            ORDER BY i.timestamp DESC
            LIMIT %s
        """, (symbol, limit))

        results = cur.fetchall()

        if not results:
            print("❌ Нет данных MACD")
            return

        print(f"Configuration: {config['name']} ({fast}, {slow}, {signal_period})")
        print(f"\n{'Timestamp':<20} {'Close':<12} {'MACD Line':<15} {'Signal':<15} {'Histogram':<15}")
        print('-' * 80)

        for row in results:
            ts, close, line, signal_val, hist = row
            print(f"{str(ts):<20} {close:<12.2f} {line:<15.8f} {signal_val:<15.8f} {hist:<15.8f}")

        print("\n💡 Hint: Вы можете сверить эти значения с Bybit для верификации корректности расчётов")

        cur.close()


def main():
    """Основная функция"""
    import argparse

    parser = argparse.ArgumentParser(description='Проверка статуса MACD индикаторов')
    parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Торговая пара')
    parser.add_argument('--timeframe', type=str, default='1m', help='Таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--gaps', action='store_true', help='Проверить пропуски в данных')
    parser.add_argument('--examples', action='store_true', help='Показать примеры значений')
    parser.add_argument('--days', type=int, default=30, help='Дней для проверки пропусков')
    parser.add_argument('--limit', type=int, default=10, help='Количество примеров')

    args = parser.parse_args()

    # Основная проверка статуса
    check_macd_columns(args.symbol, args.timeframe)

    # Проверка пропусков
    if args.gaps:
        check_macd_gaps(args.symbol, args.timeframe, args.days)

    # Примеры значений
    if args.examples:
        show_macd_examples(args.symbol, args.timeframe, args.limit)


if __name__ == '__main__':
    main()
