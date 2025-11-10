#!/usr/bin/env python3
"""
Управление колонками SMA в таблице индикаторов
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from indicators.database import DatabaseConnection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ColumnManager:
    def __init__(self):
        self.db = DatabaseConnection()

    def create_ordered_view(self):
        """
        Создает VIEW с правильным порядком колонок
        """
        print("=" * 60)
        print("СОЗДАНИЕ VIEW С УПОРЯДОЧЕННЫМИ КОЛОНКАМИ")
        print("=" * 60)

        # Получаем текущие колонки
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'indicators_bybit_futures_1m'
            ORDER BY ordinal_position;
        """

        columns = self.db.execute_query(query)
        if not columns:
            print("❌ Таблица не найдена")
            return

        # Разделяем колонки
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

        # Формируем список колонок
        ordered_columns = primary_cols + [col for _, col in sma_cols] + other_cols

        print("\n📊 Порядок колонок в VIEW:")
        for col in ordered_columns:
            print(f"   - {col}")

        # Создаем VIEW
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Удаляем старый VIEW если есть
                    cur.execute("DROP VIEW IF EXISTS indicators_sma_ordered;")

                    # Создаем новый VIEW
                    create_view = f"""
                        CREATE VIEW indicators_sma_ordered AS
                        SELECT {', '.join(ordered_columns)}
                        FROM indicators_bybit_futures_1m
                        ORDER BY timestamp DESC;
                    """
                    cur.execute(create_view)
                    conn.commit()

                    print("\n✅ VIEW 'indicators_sma_ordered' создан успешно!")
                    print("   Используйте: SELECT * FROM indicators_sma_ordered WHERE symbol = 'BTCUSDT' LIMIT 100;")

                except Exception as e:
                    conn.rollback()
                    print(f"❌ Ошибка: {e}")

    def add_sma_column_ordered(self, period: int):
        """
        Добавляет новую SMA колонку и пытается расположить её в правильном месте
        """
        print(f"\n🔨 Добавление колонки sma_{period}")

        # Проверяем существование
        check_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'indicators_bybit_futures_1m'
            AND column_name = %s;
        """

        exists = self.db.execute_query(check_query, (f'sma_{period}',))
        if exists:
            print(f"   ℹ️ Колонка sma_{period} уже существует")
            return

        # Создаем колонку
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    alter_query = f"""
                        ALTER TABLE indicators_bybit_futures_1m
                        ADD COLUMN sma_{period} DECIMAL(20,8);
                    """
                    cur.execute(alter_query)

                    # Устанавливаем значение по умолчанию 'NaN' для новой колонки
                    cur.execute(f"""
                        ALTER TABLE indicators_bybit_futures_1m
                        ALTER COLUMN sma_{period} SET DEFAULT 'NaN'::numeric;
                    """)

                    conn.commit()
                    print(f"   ✅ Колонка sma_{period} создана")

                    # Обновляем VIEW
                    self.create_ordered_view()

                except Exception as e:
                    conn.rollback()
                    print(f"   ❌ Ошибка: {e}")

    def show_table_info(self):
        """
        Показывает информацию о таблице
        """
        print("\n" + "=" * 60)
        print("ИНФОРМАЦИЯ О ТАБЛИЦЕ")
        print("=" * 60)

        # Структура таблицы
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = 'indicators_bybit_futures_1m'
            ORDER BY ordinal_position;
        """

        columns = self.db.execute_query(query)
        if columns:
            print("\n📊 Структура таблицы:")
            print(f"{'Колонка':<20} {'Тип':<15} {'NULL':<6} {'По умолчанию':<20}")
            print("-" * 70)
            for col in columns:
                default = col['column_default'] or ''
                if 'NaN' in str(default):
                    default = 'NaN'
                elif default:
                    default = default[:20]

                print(f"{col['column_name']:<20} {col['data_type']:<15} {col['is_nullable']:<6} {default:<20}")

        # Статистика по данным
        stats_query = """
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as symbols,
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM indicators_bybit_futures_1m;
        """

        stats = self.db.execute_query(stats_query)
        if stats and stats[0]['total_rows']:
            print(f"\n📈 Статистика:")
            print(f"   Всего записей: {stats[0]['total_rows']:,}")
            print(f"   Символов: {stats[0]['symbols']}")
            print(f"   Период: {stats[0]['min_ts']} - {stats[0]['max_ts']}")

        # Проверка NULL vs NaN для SMA колонок
        null_check = """
            SELECT
                'sma_10' as col,
                COUNT(*) FILTER (WHERE sma_10 IS NULL) as nulls,
                COUNT(*) FILTER (WHERE sma_10 = 'NaN'::numeric) as nans
            FROM indicators_bybit_futures_1m
            WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT
                'sma_20',
                COUNT(*) FILTER (WHERE sma_20 IS NULL),
                COUNT(*) FILTER (WHERE sma_20 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m
            WHERE symbol = 'BTCUSDT'
            UNION ALL
            SELECT
                'sma_30',
                COUNT(*) FILTER (WHERE sma_30 IS NULL),
                COUNT(*) FILTER (WHERE sma_30 = 'NaN'::numeric)
            FROM indicators_bybit_futures_1m
            WHERE symbol = 'BTCUSDT';
        """

        null_stats = self.db.execute_query(null_check)
        if null_stats:
            print(f"\n🔍 NULL vs NaN для BTCUSDT:")
            print(f"{'Колонка':<10} {'NULL':<10} {'NaN':<10}")
            print("-" * 30)
            for stat in null_stats:
                print(f"{stat['col']:<10} {stat['nulls']:<10} {stat['nans']:<10}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Управление колонками SMA')
    parser.add_argument('--create-view', action='store_true',
                       help='Создать VIEW с упорядоченными колонками')
    parser.add_argument('--add-column', type=int,
                       help='Добавить новую SMA колонку (укажите период)')
    parser.add_argument('--info', action='store_true',
                       help='Показать информацию о таблице')

    args = parser.parse_args()

    manager = ColumnManager()

    if args.create_view:
        manager.create_ordered_view()

    if args.add_column:
        manager.add_sma_column_ordered(args.add_column)

    if args.info:
        manager.show_table_info()

    if not any([args.create_view, args.add_column, args.info]):
        print("Используйте --help для справки")
        print("\nПримеры:")
        print("  python manage_columns.py --info")
        print("  python manage_columns.py --create-view")
        print("  python manage_columns.py --add-column 75")


if __name__ == "__main__":
    main()