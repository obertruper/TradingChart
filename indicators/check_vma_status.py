#!/usr/bin/env python3
"""
Проверка статуса VMA (Volume Moving Average) в базе данных
"""

import psycopg2
import yaml
from datetime import datetime, timedelta
from tabulate import tabulate
import os


def main():
    # Загружаем конфигурацию
    config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    db_config = config['database']

    # Подключаемся к БД
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
    )
    cur = conn.cursor()

    print("=" * 100)
    print("📊 СТАТУС VMA (VOLUME MOVING AVERAGE) В БАЗЕ ДАННЫХ")
    print("=" * 100)

    # Проверяем наличие колонок
    print("\n🔍 Проверка наличия колонок:")
    print("-" * 80)

    timeframes = ['1m', '15m', '1h']
    column_status = []

    for timeframe in timeframes:
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))

        table_exists = cur.fetchone()[0]

        if table_exists:
            # Проверяем наличие колонок VMA
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name LIKE 'vma_%%'
                ORDER BY column_name
            """, (table_name,))

            columns = [row[0] for row in cur.fetchall()]

            has_vma_10 = 'vma_10' in columns
            has_vma_20 = 'vma_20' in columns
            has_vma_50 = 'vma_50' in columns
            has_vma_100 = 'vma_100' in columns
            has_vma_200 = 'vma_200' in columns

            all_present = has_vma_10 and has_vma_20 and has_vma_50 and has_vma_100 and has_vma_200
            status = "✅ Готово" if all_present else "❌ Отсутствуют колонки"

            column_status.append([
                table_name,
                '✅' if has_vma_10 else '❌',
                '✅' if has_vma_20 else '❌',
                '✅' if has_vma_50 else '❌',
                '✅' if has_vma_100 else '❌',
                '✅' if has_vma_200 else '❌',
                status
            ])
        else:
            column_status.append([
                table_name,
                '❌', '❌', '❌', '❌', '❌',
                "❌ Таблица не существует"
            ])

    headers = ['Таблица', 'vma_10', 'vma_20', 'vma_50', 'vma_100', 'vma_200', 'Статус']
    print(tabulate(column_status, headers=headers, tablefmt='grid'))

    # Статистика по заполненности
    print("\n📈 Статистика заполненности для BTCUSDT:")
    print("-" * 80)

    stats_data = []

    for timeframe in timeframes:
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))

        if cur.fetchone()[0]:
            # Общая статистика
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(vma_10) as vma_10_filled,
                    COUNT(vma_20) as vma_20_filled,
                    COUNT(vma_50) as vma_50_filled,
                    COUNT(vma_100) as vma_100_filled,
                    COUNT(vma_200) as vma_200_filled,
                    MIN(timestamp) FILTER (WHERE vma_20 IS NOT NULL) as first_filled,
                    MAX(timestamp) FILTER (WHERE vma_20 IS NOT NULL) as last_filled,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record
                FROM {table_name}
                WHERE symbol = 'BTCUSDT'
            """)

            row = cur.fetchone()
            if row and row[0] > 0:
                total = row[0]
                vma_10_filled = row[1]
                vma_20_filled = row[2]
                vma_50_filled = row[3]
                vma_100_filled = row[4]
                vma_200_filled = row[5]
                first_filled = row[6].strftime('%Y-%m-%d %H:%M') if row[6] else 'N/A'
                last_filled = row[7].strftime('%Y-%m-%d %H:%M') if row[7] else 'N/A'
                first_record = row[8].strftime('%Y-%m-%d') if row[8] else 'N/A'
                last_record = row[9].strftime('%Y-%m-%d') if row[9] else 'N/A'

                # Проверяем минимальную заполненность
                min_filled = min(vma_10_filled, vma_20_filled, vma_50_filled, vma_100_filled, vma_200_filled)
                percent = (min_filled / total * 100) if total > 0 else 0

                status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"

                stats_data.append([
                    timeframe,
                    f"{min_filled:,}/{total:,}",
                    f"{percent:.1f}%",
                    first_filled,
                    last_filled,
                    f"{first_record} - {last_record}",
                    status
                ])
            else:
                stats_data.append([timeframe, "0/0", "0%", "N/A", "N/A", "N/A", "❌"])
        else:
            stats_data.append([timeframe, "N/A", "N/A", "N/A", "N/A", "N/A", "❌"])

    headers = ['Таймфрейм', 'Заполнено (мин)', '%', 'Первая запись VMA', 'Последняя запись VMA',
               'Период данных', 'Статус']
    print(tabulate(stats_data, headers=headers, tablefmt='grid'))

    # Детальная статистика по периодам
    print("\n📊 Детальная статистика по периодам (BTCUSDT):")
    print("-" * 80)

    for timeframe in timeframes:
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))

        if not cur.fetchone()[0]:
            continue

        print(f"\n⏰ Таймфрейм: {timeframe}")

        periods = [10, 20, 50, 100, 200]
        period_stats = []

        for period in periods:
            col_name = f'vma_{period}'

            cur.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT({col_name}) as filled,
                    MIN(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as first_date,
                    MAX(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as last_date
                FROM {table_name}
                WHERE symbol = 'BTCUSDT'
            """)

            result = cur.fetchone()
            if result and result[0] > 0:
                total, filled, first_date, last_date = result
                percent = (filled / total * 100) if total > 0 else 0
                status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"

                first_str = first_date.strftime('%Y-%m-%d %H:%M') if first_date else 'N/A'
                last_str = last_date.strftime('%Y-%m-%d %H:%M') if last_date else 'N/A'

                period_stats.append([
                    f"VMA_{period}",
                    f"{filled:,}/{total:,}",
                    f"{percent:.1f}%",
                    first_str,
                    last_str,
                    status
                ])

        if period_stats:
            headers = ['Период', 'Заполнено', '%', 'Первая дата', 'Последняя дата', 'Статус']
            print(tabulate(period_stats, headers=headers, tablefmt='grid'))

    # Проверяем последние значения
    print("\n🔄 ПОСЛЕДНИЕ ЗНАЧЕНИЯ VMA (таймфрейм 1h, BTCUSDT):")
    print("=" * 80)

    # Проверяем наличие таблицы и колонок
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'indicators_bybit_futures_1h'
        )
    """)

    if cur.fetchone()[0]:
        # Сначала проверим наличие данных volume в candles
        cur.execute("""
            SELECT timestamp, volume
            FROM candles_bybit_futures_1h
            WHERE symbol = 'BTCUSDT'
            ORDER BY timestamp DESC
            LIMIT 5
        """)

        candle_data = cur.fetchall()
        if candle_data:
            print("\n📦 Последние значения объема (candles_bybit_futures_1h):")
            headers = ['Timestamp', 'Volume']
            table_data = []
            for row in candle_data:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    f"{float(row[1]):.2f}" if row[1] else "N/A"
                ])
            print(tabulate(table_data, headers=headers, tablefmt='grid'))

        # Теперь VMA
        cur.execute("""
            SELECT
                i.timestamp,
                c.volume,
                i.vma_10,
                i.vma_20,
                i.vma_50,
                i.vma_100,
                i.vma_200
            FROM indicators_bybit_futures_1h i
            LEFT JOIN candles_bybit_futures_1h c
                ON i.timestamp = c.timestamp AND i.symbol = c.symbol
            WHERE i.symbol = 'BTCUSDT'
              AND i.vma_20 IS NOT NULL
            ORDER BY i.timestamp DESC
            LIMIT 10
        """)

        rows = cur.fetchall()
        if rows:
            headers = ['Timestamp', 'Volume', 'VMA_10', 'VMA_20', 'VMA_50', 'VMA_100', 'VMA_200']
            table_data = []
            for row in rows:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    f"{float(row[1]):.2f}" if row[1] else "N/A",
                    f"{float(row[2]):.2f}" if row[2] else "N/A",
                    f"{float(row[3]):.2f}" if row[3] else "N/A",
                    f"{float(row[4]):.2f}" if row[4] else "N/A",
                    f"{float(row[5]):.2f}" if row[5] else "N/A",
                    f"{float(row[6]):.2f}" if row[6] else "N/A"
                ])
            print("\n📊 Последние значения VMA:")
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            print("\n⚠️ Нет данных VMA")
    else:
        print("\n⚠️ Таблица indicators_bybit_futures_1h не существует")

    # Проверка пропусков
    print("\n🔍 ПРОВЕРКА ПРОПУСКОВ (последние 30 дней с данными):")
    print("=" * 80)

    cur.execute("""
        SELECT
            date_trunc('day', timestamp) as day,
            COUNT(DISTINCT vma_20) as unique_values,
            MIN(vma_20) as min_value,
            MAX(vma_20) as max_value
        FROM indicators_bybit_futures_1m
        WHERE symbol = 'BTCUSDT'
          AND timestamp >= NOW() - INTERVAL '30 days'
        GROUP BY day
        ORDER BY day DESC
    """)

    gaps = []
    for row in cur.fetchall():
        if row[2] is None:  # min_value is NULL означает отсутствие данных
            gaps.append(row[0].strftime('%Y-%m-%d'))

    if gaps:
        print(f"❌ Обнаружены дни без данных VMA: {', '.join(gaps[:10])}")
        if len(gaps) > 10:
            print(f"   ... и еще {len(gaps) - 10} дней")
    else:
        print("✅ Пропусков не обнаружено")

    cur.close()
    conn.close()

    print("\n" + "=" * 100)
    print("📊 Проверка завершена")
    print("=" * 100)
    print("\n💡 Для загрузки VMA:")
    print("   python indicators/vma_loader.py")
    print("\n📄 Документация:")
    print("   indicators/INDICATORS_REFERENCE.md - Полный справочник по индикаторам")


if __name__ == "__main__":
    main()
