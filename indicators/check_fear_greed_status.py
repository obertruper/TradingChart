#!/usr/bin/env python3
"""
Проверка статуса Fear & Greed Index в базе данных
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

    print("="*100)
    print("📊 СТАТУС FEAR & GREED INDEX В БАЗЕ ДАННЫХ")
    print("="*100)

    # Проверяем наличие колонок
    print("\n🔍 Проверка наличия колонок:")
    print("-"*80)

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
            # Проверяем наличие колонок
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name IN ('fear_and_greed_index_alternative', 'fear_and_greed_index_classification_alternative')
                ORDER BY column_name
            """, (table_name,))

            columns = [row[0] for row in cur.fetchall()]

            has_index = 'fear_and_greed_index_alternative' in columns
            has_class = 'fear_and_greed_index_classification_alternative' in columns

            status = "✅ Готово" if (has_index and has_class) else "❌ Отсутствуют колонки"
            column_status.append([table_name, has_index, has_class, status])
        else:
            column_status.append([table_name, False, False, "❌ Таблица не существует"])

    headers = ['Таблица', 'fear_and_greed_index_alternative', 'fear_and_greed_index_classification_alternative', 'Статус']
    print(tabulate(column_status, headers=headers, tablefmt='grid'))

    # Статистика по заполненности
    print("\n📈 Статистика заполненности для BTCUSDT:")
    print("-"*80)

    stats_data = []

    for timeframe in timeframes:
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Проверяем существование таблицы и колонок
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = %s
                AND column_name = 'fear_and_greed_index_alternative'
            )
        """, (table_name,))

        if cur.fetchone()[0]:
            # Общая статистика
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(fear_and_greed_index_alternative) as filled_records,
                    MIN(timestamp) FILTER (WHERE fear_and_greed_index_alternative IS NOT NULL) as first_filled,
                    MAX(timestamp) FILTER (WHERE fear_and_greed_index_alternative IS NOT NULL) as last_filled,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record
                FROM {table_name}
                WHERE symbol = 'BTCUSDT'
            """)

            row = cur.fetchone()
            if row and row[0] > 0:
                total = row[0]
                filled = row[1]
                percent = (filled / total * 100) if total > 0 else 0

                first_filled = row[2].strftime('%Y-%m-%d %H:%M') if row[2] else 'N/A'
                last_filled = row[3].strftime('%Y-%m-%d %H:%M') if row[3] else 'N/A'
                first_record = row[4].strftime('%Y-%m-%d') if row[4] else 'N/A'
                last_record = row[5].strftime('%Y-%m-%d') if row[5] else 'N/A'

                status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"

                stats_data.append([
                    timeframe,
                    f"{filled:,}/{total:,}",
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

    headers = ['Таймфрейм', 'Заполнено', '%', 'Первая запись F&G', 'Последняя запись F&G',
               'Период данных', 'Статус']
    print(tabulate(stats_data, headers=headers, tablefmt='grid'))

    # Проверяем последние значения
    print("\n🔄 ПОСЛЕДНИЕ ЗНАЧЕНИЯ FEAR & GREED INDEX (таймфрейм 1h):")
    print("="*80)

    # Проверяем наличие колонки
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_name = 'indicators_bybit_futures_1h'
            AND column_name = 'fear_and_greed_index_alternative'
        )
    """)

    if cur.fetchone()[0]:
        cur.execute("""
            SELECT
                timestamp,
                fear_and_greed_index_alternative,
                fear_and_greed_index_classification_alternative
            FROM indicators_bybit_futures_1h
            WHERE symbol = 'BTCUSDT'
              AND fear_and_greed_index_alternative IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 10
        """)

        rows = cur.fetchall()
        if rows:
            headers = ['Timestamp', 'F&G Index', 'Classification']
            table_data = []
            for row in rows:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    row[1] if row[1] is not None else "N/A",
                    row[2] if row[2] else "N/A"
                ])
            print("\n" + tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            print("\n⚠️ Нет данных Fear & Greed Index")
    else:
        print("\n⚠️ Колонка fear_and_greed_index_alternative не существует в таблице indicators_bybit_futures_1h")

    # Проверка консистентности между таймфреймами
    print("\n🔍 ПРОВЕРКА КОНСИСТЕНТНОСТИ (последние 3 дня):")
    print("="*80)

    # Получаем последнюю дату с данными
    cur.execute("""
        SELECT MAX(date_trunc('day', timestamp))
        FROM indicators_bybit_futures_1m
        WHERE symbol = 'BTCUSDT'
          AND fear_and_greed_index_alternative IS NOT NULL
    """)

    last_date_result = cur.fetchone()

    if last_date_result and last_date_result[0]:
        last_date = last_date_result[0]

        consistency_data = []

        for i in range(3):
            check_date = last_date - timedelta(days=i)
            date_str = check_date.strftime('%Y-%m-%d')

            values = {}
            for timeframe in timeframes:
                table_name = f'indicators_bybit_futures_{timeframe}'

                # Получаем уникальные значения за день
                cur.execute(f"""
                    SELECT DISTINCT fear_and_greed_index_alternative, fear_and_greed_index_classification_alternative
                    FROM {table_name}
                    WHERE symbol = 'BTCUSDT'
                      AND date_trunc('day', timestamp) = %s
                      AND fear_and_greed_index_alternative IS NOT NULL
                """, (check_date,))

                results = cur.fetchall()
                if results:
                    if len(results) == 1:
                        values[timeframe] = f"{results[0][0]} ({results[0][1]})"
                    else:
                        values[timeframe] = f"❌ Несколько значений: {results}"
                else:
                    values[timeframe] = "N/A"

            # Проверяем консистентность
            unique_values = set([v for v in values.values() if v != "N/A"])
            status = "✅" if len(unique_values) == 1 else "❌" if len(unique_values) > 1 else "⚠️"

            consistency_data.append([
                date_str,
                values.get('1m', 'N/A'),
                values.get('15m', 'N/A'),
                values.get('1h', 'N/A'),
                status
            ])

        headers = ['Дата', '1m', '15m', '1h', 'Статус']
        print(tabulate(consistency_data, headers=headers, tablefmt='grid'))
    else:
        print("⚠️ Нет данных для проверки консистентности")

    # Проверяем пропуски
    print("\n🔍 ПРОВЕРКА ПРОПУСКОВ (последние 30 дней с данными):")
    print("="*80)

    cur.execute("""
        SELECT
            date_trunc('day', timestamp) as day,
            COUNT(DISTINCT fear_and_greed_index_alternative) as unique_values,
            MIN(fear_and_greed_index_alternative) as min_value,
            MAX(fear_and_greed_index_alternative) as max_value
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
        print(f"❌ Обнаружены дни без данных Fear & Greed: {', '.join(gaps[:10])}")
        if len(gaps) > 10:
            print(f"   ... и еще {len(gaps) - 10} дней")
    else:
        print("✅ Пропусков не обнаружено")

    cur.close()
    conn.close()

    print("\n" + "="*100)
    print("📊 Проверка завершена")
    print("="*100)


if __name__ == "__main__":
    main()