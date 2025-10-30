#!/usr/bin/env python3
"""
Проверка статуса RSI индикаторов в базе данных
"""

import psycopg2
import yaml
from datetime import datetime
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

    print("="*80)
    print("📊 СТАТУС RSI ИНДИКАТОРОВ В БАЗЕ ДАННЫХ")
    print("="*80)

    # Получаем список таблиц
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'indicators_bybit_futures_%'
        ORDER BY table_name
    """)

    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        timeframe = table.replace('indicators_bybit_futures_', '')
        print(f"\n🕐 Таймфрейм: {timeframe}")
        print("-"*60)

        # Получаем RSI колонки
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name LIKE 'rsi_%%'
            ORDER BY column_name
        """, (table,))

        rsi_columns = [row[0] for row in cur.fetchall()]

        if not rsi_columns:
            print("  ❌ RSI колонки не найдены")
            continue

        print(f"  📈 RSI периоды: {', '.join([col.replace('rsi_', '') for col in rsi_columns])}")

        # Проверяем данные для BTCUSDT
        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                MIN(timestamp) as first_date,
                MAX(timestamp) as last_date,
                {', '.join([f'COUNT({col}) as {col}_count' for col in rsi_columns])}
            FROM {table}
            WHERE symbol = 'BTCUSDT'
        """)

        result = cur.fetchone()
        if result and result[0] > 0:
            print(f"\n  📊 Символ: BTCUSDT")
            print(f"    Всего записей: {result[0]:,}")
            print(f"    Период: {result[1]} - {result[2]}")
            print(f"\n    Заполненность RSI:")

            for i, col in enumerate(rsi_columns, start=3):
                count = result[i]
                percent = (count / result[0]) * 100 if result[0] > 0 else 0
                status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"
                print(f"      {rsi_columns[i-3]}: {count:,}/{result[0]:,} ({percent:.1f}%) {status}")

            # Показываем последние значения RSI_14
            if 'rsi_14' in rsi_columns:
                cur.execute(f"""
                    SELECT timestamp, rsi_14, rsi_7, rsi_25
                    FROM {table}
                    WHERE symbol = 'BTCUSDT' AND rsi_14 IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 5
                """)

                last_values = cur.fetchall()
                if last_values:
                    print(f"\n    Последние значения RSI:")
                    print(f"    {'Timestamp':<20} | {'RSI_14':>8} | {'RSI_7':>8} | {'RSI_25':>8} | Состояние")
                    print(f"    {'-'*75}")

                    for row in last_values:
                        timestamp, rsi_14, rsi_7, rsi_25 = row

                        # Определяем состояние рынка
                        if rsi_14:
                            if rsi_14 > 70:
                                state = "🔴 Перекупленность"
                            elif rsi_14 < 30:
                                state = "🟢 Перепроданность"
                            elif 45 <= rsi_14 <= 55:
                                state = "⚪ Нейтральная зона"
                            elif rsi_14 > 55:
                                state = "🟡 Бычий импульс"
                            else:
                                state = "🔵 Медвежий импульс"
                        else:
                            state = "➖ Нет данных"

                        print(f"    {str(timestamp)[:19]:<20} | {float(rsi_14) if rsi_14 else 0:>8.2f} | {float(rsi_7) if rsi_7 else 0:>8.2f} | {float(rsi_25) if rsi_25 else 0:>8.2f} | {state}")
        else:
            print(f"  ⚠️ Нет данных для BTCUSDT")

    print("\n" + "="*80)
    print("📈 СВОДКА ПО RSI")
    print("="*80)

    # Общая статистика
    cur.execute("""
        SELECT
            '1m' as tf,
            COUNT(*) FILTER (WHERE rsi_14 IS NOT NULL) as rsi_14_count,
            MIN(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as first_rsi,
            MAX(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as last_rsi
        FROM indicators_bybit_futures_1m
        WHERE symbol = 'BTCUSDT'
        UNION ALL
        SELECT
            '15m' as tf,
            COUNT(*) FILTER (WHERE rsi_14 IS NOT NULL) as rsi_14_count,
            MIN(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as first_rsi,
            MAX(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as last_rsi
        FROM indicators_bybit_futures_15m
        WHERE symbol = 'BTCUSDT'
        UNION ALL
        SELECT
            '1h' as tf,
            COUNT(*) FILTER (WHERE rsi_14 IS NOT NULL) as rsi_14_count,
            MIN(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as first_rsi,
            MAX(CASE WHEN rsi_14 IS NOT NULL THEN timestamp END) as last_rsi
        FROM indicators_bybit_futures_1h
        WHERE symbol = 'BTCUSDT'
    """)

    results = cur.fetchall()

    print("\nRSI_14 покрытие по таймфреймам:")
    print(f"{'Таймфрейм':<12} | {'Записей с RSI':>15} | {'Первая дата':>20} | {'Последняя дата':>20}")
    print("-"*80)

    for row in results:
        tf, count, first_date, last_date = row
        if count and count > 0:
            print(f"{tf:<12} | {count:>15,} | {str(first_date)[:19]:>20} | {str(last_date)[:19]:>20}")
        else:
            print(f"{tf:<12} | {'Нет данных':>15} | {'-':>20} | {'-':>20}")

    conn.close()

    print("\n💡 Команды для загрузки RSI:")
    print("   python indicators/rsi_loader.py --timeframe 1m")
    print("   python indicators/rsi_loader.py --timeframe 15m")
    print("   python indicators/rsi_loader.py --timeframe 1h")

if __name__ == "__main__":
    main()