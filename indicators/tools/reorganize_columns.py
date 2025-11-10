#!/usr/bin/env python3
"""
Утилита для реорганизации колонок SMA в правильном порядке
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from indicators.database import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def reorganize_sma_columns():
    """
    Реорганизует колонки SMA в правильном порядке (по возрастанию периодов)
    PostgreSQL не поддерживает изменение порядка колонок напрямую,
    поэтому создаем новую таблицу с правильным порядком
    """
    db = DatabaseConnection()

    print("=" * 60)
    print("РЕОРГАНИЗАЦИЯ КОЛОНОК SMA")
    print("=" * 60)

    # Получаем текущую структуру
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'indicators_bybit_futures_1m'
        ORDER BY ordinal_position;
    """

    columns = db.execute_query(query)
    if not columns:
        print("❌ Таблица не найдена")
        return

    # Разделяем колонки на группы
    primary_cols = []
    sma_cols = []
    other_cols = []

    for col in columns:
        col_name = col['column_name']
        if col_name in ['timestamp', 'symbol']:
            primary_cols.append(col_name)
        elif col_name.startswith('sma_'):
            try:
                period = int(col_name.split('_')[1])
                sma_cols.append((period, col_name))
            except:
                other_cols.append(col_name)
        else:
            other_cols.append(col_name)

    # Сортируем SMA колонки
    sma_cols.sort()

    print("\n📊 Текущие SMA колонки:")
    for period, col_name in sma_cols:
        print(f"   - {col_name}")

    # Формируем новый порядок
    new_order = primary_cols + [col for _, col in sma_cols] + other_cols

    print("\n✨ Новый порядок колонок:")
    for col in new_order:
        print(f"   - {col}")

    # Создаем временную таблицу с правильным порядком
    print("\n🔄 Создаю временную таблицу с правильным порядком...")

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # Создаем временную таблицу
                create_temp = f"""
                    CREATE TABLE indicators_bybit_futures_1m_temp AS
                    SELECT {', '.join(new_order)}
                    FROM indicators_bybit_futures_1m;
                """
                cur.execute(create_temp)

                # Удаляем старую таблицу
                cur.execute("DROP TABLE indicators_bybit_futures_1m;")

                # Переименовываем временную в основную
                cur.execute("ALTER TABLE indicators_bybit_futures_1m_temp RENAME TO indicators_bybit_futures_1m;")

                # Восстанавливаем первичный ключ
                cur.execute("""
                    ALTER TABLE indicators_bybit_futures_1m
                    ADD PRIMARY KEY (timestamp, symbol);
                """)

                # Создаем индексы
                cur.execute("""
                    CREATE INDEX idx_indicators_symbol_timestamp
                    ON indicators_bybit_futures_1m(symbol, timestamp);
                """)

                conn.commit()
                print("✅ Колонки успешно реорганизованы!")

            except Exception as e:
                conn.rollback()
                print(f"❌ Ошибка: {e}")

    print("\n" + "=" * 60)
    print("ЗАВЕРШЕНО")
    print("=" * 60)


def fix_null_consistency():
    """
    Заменяет NULL на NaN для консистентности
    """
    db = DatabaseConnection()

    print("\n" + "=" * 60)
    print("ИСПРАВЛЕНИЕ NULL/NaN КОНСИСТЕНТНОСТИ")
    print("=" * 60)

    # Получаем список SMA колонок
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'indicators_bybit_futures_1m'
        AND column_name LIKE 'sma_%';
    """

    columns = db.execute_query(query)
    if not columns:
        print("❌ SMA колонки не найдены")
        return

    sma_columns = [col['column_name'] for col in columns]
    print(f"\n📊 Найдено SMA колонок: {len(sma_columns)}")

    # Для каждой колонки заменяем NULL на 'NaN'
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            for col in sma_columns:
                try:
                    # PostgreSQL хранит NaN как специальное значение для numeric типов
                    update_query = f"""
                        UPDATE indicators_bybit_futures_1m
                        SET {col} = 'NaN'::numeric
                        WHERE {col} IS NULL
                        AND symbol = 'BTCUSDT';
                    """
                    cur.execute(update_query)
                    rows_affected = cur.rowcount
                    if rows_affected > 0:
                        print(f"   ✅ {col}: обновлено {rows_affected} записей")
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print(f"   ❌ Ошибка для {col}: {e}")

    print("\n✅ Консистентность NULL/NaN исправлена")
    print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Утилиты для реорганизации таблицы индикаторов')
    parser.add_argument('--reorganize', action='store_true',
                       help='Реорганизовать колонки в правильном порядке')
    parser.add_argument('--fix-nulls', action='store_true',
                       help='Исправить NULL/NaN консистентность')

    args = parser.parse_args()

    if args.reorganize:
        reorganize_sma_columns()

    if args.fix_nulls:
        fix_null_consistency()

    if not args.reorganize and not args.fix_nulls:
        print("Используйте --reorganize или --fix-nulls")
        print("Пример: python reorganize_columns.py --fix-nulls")