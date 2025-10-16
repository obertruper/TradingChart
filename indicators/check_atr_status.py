#!/usr/bin/env python3
"""
Проверка статуса ATR (Average True Range) в базе данных
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
    print("📊 СТАТУС ATR (AVERAGE TRUE RANGE) В БАЗЕ ДАННЫХ")
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
            # Проверяем наличие колонок ATR
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name LIKE 'atr_%%'
                ORDER BY column_name
            """, (table_name,))

            columns = [row[0] for row in cur.fetchall()]

            has_atr_7 = 'atr_7' in columns
            has_atr_14 = 'atr_14' in columns
            has_atr_21 = 'atr_21' in columns
            has_atr_30 = 'atr_30' in columns
            has_atr_50 = 'atr_50' in columns
            has_atr_100 = 'atr_100' in columns

            all_present = has_atr_7 and has_atr_14 and has_atr_21 and has_atr_30 and has_atr_50 and has_atr_100
            status = "✅ Готово" if all_present else "❌ Отсутствуют колонки"

            column_status.append([
                table_name,
                '✅' if has_atr_7 else '❌',
                '✅' if has_atr_14 else '❌',
                '✅' if has_atr_21 else '❌',
                '✅' if has_atr_30 else '❌',
                '✅' if has_atr_50 else '❌',
                '✅' if has_atr_100 else '❌',
                status
            ])
        else:
            column_status.append([
                table_name,
                '❌', '❌', '❌', '❌', '❌', '❌',
                "❌ Таблица не существует"
            ])

    headers = ['Таблица', 'atr_7', 'atr_14', 'atr_21', 'atr_30', 'atr_50', 'atr_100', 'Статус']
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
                    COUNT(atr_7) as atr_7_filled,
                    COUNT(atr_14) as atr_14_filled,
                    COUNT(atr_21) as atr_21_filled,
                    COUNT(atr_30) as atr_30_filled,
                    COUNT(atr_50) as atr_50_filled,
                    COUNT(atr_100) as atr_100_filled,
                    MIN(timestamp) FILTER (WHERE atr_14 IS NOT NULL) as first_filled,
                    MAX(timestamp) FILTER (WHERE atr_14 IS NOT NULL) as last_filled,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record
                FROM {table_name}
                WHERE symbol = 'BTCUSDT'
            """)

            row = cur.fetchone()
            if row and row[0] > 0:
                total = row[0]
                atr_7_filled = row[1]
                atr_14_filled = row[2]
                atr_21_filled = row[3]
                atr_30_filled = row[4]
                atr_50_filled = row[5]
                atr_100_filled = row[6]
                first_filled = row[7].strftime('%Y-%m-%d %H:%M') if row[7] else 'N/A'
                last_filled = row[8].strftime('%Y-%m-%d %H:%M') if row[8] else 'N/A'
                first_record = row[9].strftime('%Y-%m-%d') if row[9] else 'N/A'
                last_record = row[10].strftime('%Y-%m-%d') if row[10] else 'N/A'

                # Проверяем минимальную заполненность
                min_filled = min(atr_7_filled, atr_14_filled, atr_21_filled, atr_30_filled, atr_50_filled, atr_100_filled)
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

    headers = ['Таймфрейм', 'Заполнено (мин)', '%', 'Первая запись ATR', 'Последняя запись ATR',
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

        periods = [7, 14, 21, 30, 50, 100]
        period_stats = []

        for period in periods:
            col_name = f'atr_{period}'

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
                    f"ATR_{period}",
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
    print("\n🔄 ПОСЛЕДНИЕ ЗНАЧЕНИЯ ATR (таймфрейм 1h, BTCUSDT):")
    print("=" * 80)

    # Проверяем наличие таблицы и колонок
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'indicators_bybit_futures_1h'
        )
    """)

    if cur.fetchone()[0]:
        # Сначала проверим наличие данных в candles
        cur.execute("""
            SELECT timestamp, high, low, close
            FROM candles_bybit_futures_1h
            WHERE symbol = 'BTCUSDT'
            ORDER BY timestamp DESC
            LIMIT 5
        """)

        candle_data = cur.fetchall()
        if candle_data:
            print("\n📦 Последние значения свечей (candles_bybit_futures_1h):")
            headers = ['Timestamp', 'High', 'Low', 'Close']
            table_data = []
            for row in candle_data:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    f"{float(row[1]):.2f}" if row[1] else "N/A",
                    f"{float(row[2]):.2f}" if row[2] else "N/A",
                    f"{float(row[3]):.2f}" if row[3] else "N/A"
                ])
            print(tabulate(table_data, headers=headers, tablefmt='grid'))

        # Теперь ATR
        cur.execute("""
            SELECT
                i.timestamp,
                c.high,
                c.low,
                c.close,
                i.atr_7,
                i.atr_14,
                i.atr_21,
                i.atr_30,
                i.atr_50,
                i.atr_100
            FROM indicators_bybit_futures_1h i
            LEFT JOIN candles_bybit_futures_1h c
                ON i.timestamp = c.timestamp AND i.symbol = c.symbol
            WHERE i.symbol = 'BTCUSDT'
              AND i.atr_14 IS NOT NULL
            ORDER BY i.timestamp DESC
            LIMIT 10
        """)

        rows = cur.fetchall()
        if rows:
            headers = ['Timestamp', 'High', 'Low', 'Close', 'ATR_7', 'ATR_14', 'ATR_21', 'ATR_30', 'ATR_50', 'ATR_100']
            table_data = []
            for row in rows:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    f"{float(row[1]):.2f}" if row[1] else "N/A",
                    f"{float(row[2]):.2f}" if row[2] else "N/A",
                    f"{float(row[3]):.2f}" if row[3] else "N/A",
                    f"{float(row[4]):.2f}" if row[4] else "N/A",
                    f"{float(row[5]):.2f}" if row[5] else "N/A",
                    f"{float(row[6]):.2f}" if row[6] else "N/A",
                    f"{float(row[7]):.2f}" if row[7] else "N/A",
                    f"{float(row[8]):.2f}" if row[8] else "N/A",
                    f"{float(row[9]):.2f}" if row[9] else "N/A"
                ])
            print("\n📊 Последние значения ATR:")
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            print("\n⚠️ Нет данных ATR")
    else:
        print("\n⚠️ Таблица indicators_bybit_futures_1h не существует")

    # Проверка пропусков
    print("\n🔍 ПРОВЕРКА ПРОПУСКОВ (последние 30 дней с данными):")
    print("=" * 80)

    cur.execute("""
        SELECT
            date_trunc('day', timestamp) as day,
            COUNT(DISTINCT atr_14) as unique_values,
            MIN(atr_14) as min_value,
            MAX(atr_14) as max_value
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
        print(f"❌ Обнаружены дни без данных ATR: {', '.join(gaps[:10])}")
        if len(gaps) > 10:
            print(f"   ... и еще {len(gaps) - 10} дней")
    else:
        print("✅ Пропусков не обнаружено")

    cur.close()
    conn.close()

    print("\n" + "=" * 100)
    print("📊 Проверка завершена")
    print("=" * 100)
    print("\n💡 Для загрузки ATR:")
    print("   python indicators/atr_loader.py")
    print("\n📄 Документация:")
    print("   indicators/INDICATORS_REFERENCE.md - Полный справочник по индикаторам")


if __name__ == "__main__":
    main()
