#!/usr/bin/env python3
"""
Исправление консистентности NULL/NaN в таблице индикаторов
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from indicators.database import DatabaseConnection


def fix_null_nan_consistency():
    """
    Заменяет все NULL на 'NaN'::numeric для консистентности
    """
    db = DatabaseConnection()

    print("=" * 60)
    print("ИСПРАВЛЕНИЕ NULL/NaN КОНСИСТЕНТНОСТИ")
    print("=" * 60)

    # Получаем список всех SMA колонок
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'indicators_bybit_futures_1m'
        AND column_name LIKE 'sma_%'
        ORDER BY column_name;
    """

    columns = db.execute_query(query)
    if not columns:
        print("❌ SMA колонки не найдены")
        return

    sma_columns = [col['column_name'] for col in columns]
    print(f"\n📊 Найдено SMA колонок: {len(sma_columns)}")
    for col in sma_columns:
        print(f"   - {col}")

    # Для каждой колонки проверяем и исправляем NULL
    print("\n🔄 Обработка колонок:")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            total_updated = 0

            for col in sma_columns:
                try:
                    # Проверяем количество NULL
                    check_query = f"""
                        SELECT COUNT(*) as null_count
                        FROM indicators_bybit_futures_1m
                        WHERE {col} IS NULL
                        AND symbol = 'BTCUSDT';
                    """
                    cur.execute(check_query)
                    null_count = cur.fetchone()[0]

                    if null_count > 0:
                        # Заменяем NULL на NaN
                        update_query = f"""
                            UPDATE indicators_bybit_futures_1m
                            SET {col} = 'NaN'::numeric
                            WHERE {col} IS NULL
                            AND symbol = 'BTCUSDT';
                        """
                        cur.execute(update_query)
                        rows_affected = cur.rowcount
                        total_updated += rows_affected
                        conn.commit()
                        print(f"   ✅ {col}: исправлено {rows_affected:,} NULL → NaN")
                    else:
                        print(f"   ✓ {col}: нет NULL значений")

                except Exception as e:
                    conn.rollback()
                    print(f"   ❌ {col}: ошибка - {e}")

            print(f"\n📈 Всего исправлено записей: {total_updated:,}")

    # Проверяем результат
    print("\n🔍 Проверка результата:")

    check_query = """
        SELECT
            col_name,
            null_count,
            nan_count
        FROM (
            SELECT 'sma_10' as col_name,
                   COUNT(*) FILTER (WHERE sma_10 IS NULL) as null_count,
                   COUNT(*) FILTER (WHERE sma_10 = 'NaN'::numeric) as nan_count
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT 'sma_20',
                   COUNT(*) FILTER (WHERE sma_20 IS NULL),
                   COUNT(*) FILTER (WHERE sma_20 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT 'sma_30',
                   COUNT(*) FILTER (WHERE sma_30 IS NULL),
                   COUNT(*) FILTER (WHERE sma_30 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT 'sma_50',
                   COUNT(*) FILTER (WHERE sma_50 IS NULL),
                   COUNT(*) FILTER (WHERE sma_50 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT 'sma_100',
                   COUNT(*) FILTER (WHERE sma_100 IS NULL),
                   COUNT(*) FILTER (WHERE sma_100 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT 'sma_200',
                   COUNT(*) FILTER (WHERE sma_200 IS NULL),
                   COUNT(*) FILTER (WHERE sma_200 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m WHERE symbol = 'BTCUSDT'
        ) as stats
        ORDER BY col_name;
    """

    results = db.execute_query(check_query)
    if results:
        print(f"{'Колонка':<10} {'NULL':<10} {'NaN':<10}")
        print("-" * 30)
        for row in results:
            print(f"{row['col_name']:<10} {row['null_count']:<10} {row['nan_count']:<10}")

    print("\n" + "=" * 60)
    print("✅ ЗАВЕРШЕНО")
    print("=" * 60)


if __name__ == "__main__":
    fix_null_nan_consistency()