#!/usr/bin/env python3
"""
Bollinger Bands Status Checker
================================
Проверка статуса Bollinger Bands индикаторов в базе данных.

Показывает:
- Наличие колонок BB для всех конфигураций
- Статистику заполнения данных
- Последние значения BB
- Пропуски в данных (gaps)
- Примеры для верификации с Bybit/TradingView

Использование:
==============
# Основная проверка статуса
python indicators/check_bollinger_status.py

# Проверка конкретного таймфрейма
python indicators/check_bollinger_status.py --timeframe 1h

# С примерами значений для верификации
python indicators/check_bollinger_status.py --examples

# С проверкой пропусков (gaps)
python indicators/check_bollinger_status.py --gaps --days 30

Автор: Claude Code
Дата создания: 2025-10-16
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators.database import DatabaseConnection


# Конфигурации Bollinger Bands (должны совпадать с bollinger_bands_loader.py)
BOLLINGER_CONFIGS = [
    {'name': 'ultrafast', 'period': 3, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'scalping', 'period': 5, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'short', 'period': 10, 'std_dev': 1.5, 'base': 'sma'},
    {'name': 'intraday', 'period': 14, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'tight', 'period': 20, 'std_dev': 1.0, 'base': 'sma'},
    {'name': 'golden', 'period': 20, 'std_dev': 1.618, 'base': 'sma'},
    {'name': 'classic', 'period': 20, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'wide', 'period': 20, 'std_dev': 3.0, 'base': 'sma'},
    {'name': 'fibonacci', 'period': 21, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'fibonacci_medium', 'period': 34, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'fibonacci_long', 'period': 89, 'std_dev': 2.0, 'base': 'sma'},
    {'name': 'classic_ema', 'period': 20, 'std_dev': 2.0, 'base': 'ema'},
    {'name': 'golden_ema', 'period': 20, 'std_dev': 1.618, 'base': 'ema'},
]


def format_std_dev(std_dev: float) -> str:
    """Форматирует std_dev для имени колонки"""
    return str(std_dev).replace('.', '_')


def get_column_names(period: int, std_dev: float, base: str) -> Dict[str, str]:
    """Генерирует имена колонок для конфигурации BB"""
    std_str = format_std_dev(std_dev)
    prefix = f"bollinger_bands_{base}_{period}_{std_str}"

    return {
        'upper': f"{prefix}_upper",
        'middle': f"{prefix}_middle",
        'lower': f"{prefix}_lower",
        'percent_b': f"{prefix}_percent_b",
        'bandwidth': f"{prefix}_bandwidth",
        'squeeze': f"{prefix}_squeeze",
    }


def check_bollinger_columns(symbol: str = 'BTCUSDT', timeframe: str = '1m'):
    """
    Проверяет наличие колонок BB в таблице

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*100}")
    print(f"Bollinger Bands Status for {symbol} ({timeframe})")
    print(f"{'='*100}\n")

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
        for config in BOLLINGER_CONFIGS:
            period = config['period']
            std_dev = config['std_dev']
            base = config['base']
            name = config['name']

            columns = get_column_names(period, std_dev, base)

            print(f"Configuration: {name} ({period}, {std_dev}) {base.upper()}")

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
            print(f"  ├─ Upper:     {fill_stats['upper']['filled']:,} / {total_candles:,} ({fill_stats['upper']['percent']:.1f}%)")
            print(f"  ├─ Middle:    {fill_stats['middle']['filled']:,} / {total_candles:,} ({fill_stats['middle']['percent']:.1f}%)")
            print(f"  ├─ Lower:     {fill_stats['lower']['filled']:,} / {total_candles:,} ({fill_stats['lower']['percent']:.1f}%)")
            print(f"  ├─ %B:        {fill_stats['percent_b']['filled']:,} / {total_candles:,} ({fill_stats['percent_b']['percent']:.1f}%)")
            print(f"  ├─ Bandwidth: {fill_stats['bandwidth']['filled']:,} / {total_candles:,} ({fill_stats['bandwidth']['percent']:.1f}%)")
            print(f"  └─ Squeeze:   {fill_stats['squeeze']['filled']:,} / {total_candles:,} ({fill_stats['squeeze']['percent']:.1f}%)")

            # Получаем последние значения
            upper_col = columns['upper']
            middle_col = columns['middle']
            lower_col = columns['lower']
            percent_b_col = columns['percent_b']
            bandwidth_col = columns['bandwidth']
            squeeze_col = columns['squeeze']

            cur.execute(f"""
                SELECT timestamp, {upper_col}, {middle_col}, {lower_col},
                       {percent_b_col}, {bandwidth_col}, {squeeze_col}
                FROM {table_name}
                WHERE symbol = %s
                  AND {upper_col} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))

            result = cur.fetchone()
            if result:
                ts, upper, middle, lower, percent_b, bandwidth, squeeze = result
                print(f"\n  Latest values ({ts}):")
                print(f"    Upper:     {upper:.8f}")
                print(f"    Middle:    {middle:.8f}")
                print(f"    Lower:     {lower:.8f}")
                print(f"    %B:        {percent_b:.4f}" if percent_b else "    %B:        NULL")
                print(f"    Bandwidth: {bandwidth:.4f}%" if bandwidth else "    Bandwidth: NULL")
                print(f"    Squeeze:   {'🟢 YES' if squeeze else '🔴 NO'}" if squeeze is not None else "    Squeeze:   NULL")

                # Интерпретация
                if percent_b is not None:
                    if percent_b > 1.0:
                        position = "🔴 Выше верхней полосы (перекупленность)"
                    elif percent_b < 0.0:
                        position = "🟢 Ниже нижней полосы (перепроданность)"
                    elif percent_b > 0.8:
                        position = "⚠️  Близко к верхней полосе"
                    elif percent_b < 0.2:
                        position = "⚠️  Близко к нижней полосе"
                    else:
                        position = "⚪ Внутри полос (нормально)"

                    print(f"    Position: {position}")

            print()

        cur.close()


def check_bollinger_gaps(symbol: str = 'BTCUSDT', timeframe: str = '1m', days: int = 30):
    """
    Проверяет пропуски в данных BB за последние N дней

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        days: Количество дней для проверки
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*100}")
    print(f"Gap Detection for Bollinger Bands (last {days} days)")
    print(f"{'='*100}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Дата начала проверки
        start_date = datetime.now() - timedelta(days=days)

        for config in BOLLINGER_CONFIGS:
            period = config['period']
            std_dev = config['std_dev']
            base = config['base']
            name = config['name']

            columns = get_column_names(period, std_dev, base)
            upper_col = columns['upper']

            # Проверяем существование колонки
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = %s
                );
            """, (table_name, upper_col))

            if not cur.fetchone()[0]:
                continue  # Колонка не существует, пропускаем

            # Ищем пропуски
            cur.execute(f"""
                WITH bb_data AS (
                    SELECT
                        timestamp,
                        {upper_col},
                        LAG({upper_col}) OVER (ORDER BY timestamp) as prev_value
                    FROM {table_name}
                    WHERE symbol = %s
                      AND timestamp >= %s
                    ORDER BY timestamp
                )
                SELECT
                    timestamp,
                    {upper_col},
                    prev_value
                FROM bb_data
                WHERE {upper_col} IS NULL AND prev_value IS NOT NULL
            """, (symbol, start_date))

            gaps = cur.fetchall()

            if gaps:
                print(f"⚠️  BB {name} ({period}, {std_dev}) {base.upper()}: {len(gaps)} gaps found")
                if len(gaps) <= 10:
                    for gap in gaps[:10]:
                        print(f"    {gap[0]}")
            else:
                print(f"✅ BB {name} ({period}, {std_dev}) {base.upper()}: No gaps")

        cur.close()


def show_bollinger_examples(symbol: str = 'BTCUSDT', timeframe: str = '1m', limit: int = 10):
    """
    Показывает примеры значений BB для верификации

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        limit: Количество последних записей
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*100}")
    print(f"Latest Bollinger Bands Values (Top {limit})")
    print(f"{'='*100}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Берём classic BB для примера
        config = BOLLINGER_CONFIGS[6]  # classic
        period = config['period']
        std_dev = config['std_dev']
        base = config['base']
        name = config['name']

        columns = get_column_names(period, std_dev, base)
        upper_col = columns['upper']
        middle_col = columns['middle']
        lower_col = columns['lower']
        percent_b_col = columns['percent_b']
        bandwidth_col = columns['bandwidth']

        # Проверяем существование колонки
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = %s
            );
        """, (table_name, upper_col))

        if not cur.fetchone()[0]:
            print(f"❌ Колонки BB classic не созданы")
            return

        # Получаем последние значения с close ценой для верификации
        cur.execute(f"""
            SELECT
                i.timestamp,
                c.close,
                i.{upper_col},
                i.{middle_col},
                i.{lower_col},
                i.{percent_b_col},
                i.{bandwidth_col}
            FROM {table_name} i
            JOIN candles_bybit_futures_{timeframe} c
                ON i.timestamp = c.timestamp AND i.symbol = c.symbol
            WHERE i.symbol = %s
              AND i.{upper_col} IS NOT NULL
            ORDER BY i.timestamp DESC
            LIMIT %s
        """, (symbol, limit))

        results = cur.fetchall()

        if not results:
            print("❌ Нет данных BB")
            return

        print(f"Configuration: {name} ({period}, {std_dev}) {base.upper()}")
        print(f"\n{'Timestamp':<20} {'Close':<12} {'Upper':<12} {'Middle':<12} {'Lower':<12} {'%B':<10} {'Bandwidth':<10}")
        print('-' * 100)

        for row in results:
            ts, close, upper, middle, lower, percent_b, bandwidth = row
            percent_b_str = f"{percent_b:.4f}" if percent_b else "NULL"
            bandwidth_str = f"{bandwidth:.4f}" if bandwidth else "NULL"
            print(f"{str(ts):<20} {close:<12.2f} {upper:<12.2f} {middle:<12.2f} {lower:<12.2f} {percent_b_str:<10} {bandwidth_str:<10}")

        print(f"\n💡 Hint: Вы можете сверить эти значения с TradingView или Bybit для верификации корректности расчётов")
        print(f"   TradingView: https://www.tradingview.com/chart/?symbol=BYBIT:{symbol}")
        print(f"   Настройки BB: Period={period}, StdDev={std_dev}, Basis={'SMA' if base == 'sma' else 'EMA'}")

        cur.close()


def show_squeeze_events(symbol: str = 'BTCUSDT', timeframe: str = '1h', days: int = 30):
    """
    Показывает недавние события Squeeze (сжатия полос)

    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        days: Количество дней для анализа
    """
    table_name = f'indicators_bybit_futures_{timeframe}'

    print(f"\n{'='*100}")
    print(f"Recent Bollinger Squeeze Events (last {days} days)")
    print(f"{'='*100}\n")

    db = DatabaseConnection()

    with db.get_connection() as conn:
        cur = conn.cursor()

        start_date = datetime.now() - timedelta(days=days)

        # Берём classic BB для анализа
        config = BOLLINGER_CONFIGS[6]  # classic
        columns = get_column_names(config['period'], config['std_dev'], config['base'])
        squeeze_col = columns['squeeze']
        bandwidth_col = columns['bandwidth']
        upper_col = columns['upper']
        lower_col = columns['lower']

        # Проверяем существование
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = %s
            );
        """, (table_name, squeeze_col))

        if not cur.fetchone()[0]:
            print("❌ Squeeze колонка не создана")
            return

        # Ищем squeeze события
        cur.execute(f"""
            SELECT
                i.timestamp,
                c.close,
                i.{bandwidth_col},
                i.{upper_col},
                i.{lower_col}
            FROM {table_name} i
            JOIN candles_bybit_futures_{timeframe} c
                ON i.timestamp = c.timestamp AND i.symbol = c.symbol
            WHERE i.symbol = %s
              AND i.timestamp >= %s
              AND i.{squeeze_col} = TRUE
            ORDER BY i.timestamp DESC
            LIMIT 20
        """, (symbol, start_date))

        squeezes = cur.fetchall()

        if squeezes:
            print(f"🟢 Найдено {len(squeezes)} squeeze событий:\n")
            print(f"{'Timestamp':<20} {'Close':<12} {'Bandwidth':<12} {'Upper':<12} {'Lower':<12}")
            print('-' * 80)

            for sq in squeezes:
                ts, close, bandwidth, upper, lower = sq
                print(f"{str(ts):<20} {close:<12.2f} {bandwidth:<12.4f} {upper:<12.2f} {lower:<12.2f}")

            print(f"\n💡 Squeeze означает низкую волатильность. Обычно за сжатием следует резкое движение!")
        else:
            print("❌ Squeeze событий не найдено за последние {} дней".format(days))

        cur.close()


def main():
    """Основная функция"""
    import argparse

    parser = argparse.ArgumentParser(description='Проверка статуса Bollinger Bands индикаторов')
    parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Торговая пара')
    parser.add_argument('--timeframe', type=str, default='1m', help='Таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--gaps', action='store_true', help='Проверить пропуски в данных')
    parser.add_argument('--examples', action='store_true', help='Показать примеры значений')
    parser.add_argument('--squeeze', action='store_true', help='Показать squeeze события')
    parser.add_argument('--days', type=int, default=30, help='Дней для проверки пропусков/squeeze')
    parser.add_argument('--limit', type=int, default=10, help='Количество примеров')

    args = parser.parse_args()

    # Основная проверка статуса
    check_bollinger_columns(args.symbol, args.timeframe)

    # Проверка пропусков
    if args.gaps:
        check_bollinger_gaps(args.symbol, args.timeframe, args.days)

    # Примеры значений
    if args.examples:
        show_bollinger_examples(args.symbol, args.timeframe, args.limit)

    # Squeeze события
    if args.squeeze:
        show_squeeze_events(args.symbol, args.timeframe, args.days)


if __name__ == '__main__':
    main()
