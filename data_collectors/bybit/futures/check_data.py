#!/usr/bin/env python3
"""
Скрипт для быстрой проверки последних загруженных данных в PostgreSQL.
Показывает статистику по всем символам и возможные пробелы в данных.
"""

import os
import sys
import datetime
import pytz
import psycopg2
from tabulate import tabulate
import argparse
import yaml
from typing import List, Tuple, Optional

# Добавляем путь к корневой директории проекта
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Импортируем наш database manager
from data_collectors.bybit.futures.database import DatabaseManager


def format_timestamp(timestamp_ms: int) -> str:
    """Форматирование timestamp в читаемый вид."""
    if timestamp_ms:
        dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    return "N/A"


def calculate_gaps(db_manager: DatabaseManager, symbol: str, hours: int = 24) -> List[Tuple[str, str, int]]:
    """
    Находит пробелы в данных за последние N часов.
    Возвращает список пробелов: (начало, конец, минут пропущено)
    """
    gaps = []

    # Получаем данные за последние N часов
    end_time = datetime.datetime.now(pytz.UTC)
    start_time = end_time - datetime.timedelta(hours=hours)

    query = """
        SELECT open_time FROM candles_bybit_futures_1m 
        WHERE symbol = %s AND open_time >= %s AND open_time <= %s
        ORDER BY open_time
    """

    with db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (
                    symbol,
                    int(start_time.timestamp() * 1000),
                    int(end_time.timestamp() * 1000),
                ),
            )
            rows = cursor.fetchall()

            if len(rows) > 1:
                for i in range(1, len(rows)):
                    prev_time = rows[i - 1][0]
                    curr_time = rows[i][0]
                    diff_minutes = (curr_time - prev_time) / (1000 * 60)

                    # Если разница больше 1 минуты - есть пробел
                    if diff_minutes > 1:
                        gap_start = format_timestamp(prev_time + 60000)
                        gap_end = format_timestamp(curr_time - 60000)
                        gaps.append((gap_start, gap_end, int(diff_minutes - 1)))

    return gaps


def check_data_status(verbose: bool = False, hours: int = 24):
    """Проверка статуса загруженных данных."""

    # Загружаем конфигурацию
    config_path = os.path.join(os.path.dirname(__file__), "../../monitor_config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Подключаемся к БД
    db_manager = DatabaseManager(config)

    try:
        # Получаем статистику по всем символам
        query = """
            SELECT 
                symbol,
                COUNT(*) as total_candles,
                MIN(open_time) as first_candle,
                MAX(open_time) as last_candle,
                MAX(open_time) - MIN(open_time) as time_range_ms
            FROM candles_bybit_futures_1m
            GROUP BY symbol
            ORDER BY symbol
        """

        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

            if not rows:
                print("❌ Нет данных в базе данных")
                return

            # Подготавливаем данные для таблицы
            table_data = []
            current_time = datetime.datetime.now(pytz.UTC)

            for row in rows:
                symbol, total, first_ms, last_ms, range_ms = row

                # Форматируем даты
                first_date = format_timestamp(first_ms)
                last_date = format_timestamp(last_ms)

                # Вычисляем покрытие (сколько % времени покрыто данными)
                expected_candles = range_ms / (1000 * 60) if range_ms else 0
                coverage = (total / expected_candles * 100) if expected_candles > 0 else 0

                # Вычисляем задержку от текущего времени
                last_dt = datetime.datetime.fromtimestamp(last_ms / 1000, tz=pytz.UTC)
                delay = current_time - last_dt
                delay_str = f"{int(delay.total_seconds() / 60)} мин"

                # Проверяем наличие пробелов
                gaps = calculate_gaps(db_manager, symbol, hours)
                gaps_str = f"{len(gaps)} пробелов" if gaps else "✅ Нет"

                table_data.append(
                    [
                        symbol,
                        f"{total:,}",
                        first_date.split()[0],  # Только дата
                        last_date.split()[1],  # Только время
                        f"{coverage:.1f}%",
                        delay_str,
                        gaps_str,
                    ]
                )

            # Выводим красивую таблицу
            headers = [
                "Символ",
                "Свечей",
                "От",
                "До (время)",
                "Покрытие",
                "Задержка",
                f"Пробелы ({hours}ч)",
            ]
            print("\n📊 СТАТУС ДАННЫХ В POSTGRESQL")
            print("=" * 100)
            print(tabulate(table_data, headers=headers, tablefmt="grid"))

            # Дополнительная информация
            print(f"\n⏰ Текущее время: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            # Показываем детали пробелов если verbose
            if verbose:
                print("\n📍 ДЕТАЛИ ПРОБЕЛОВ В ДАННЫХ:")
                print("-" * 100)
                for row in rows:
                    symbol = row[0]
                    gaps = calculate_gaps(db_manager, symbol, hours)
                    if gaps:
                        print(f"\n{symbol}:")
                        for gap_start, gap_end, minutes in gaps:
                            print(f"  • {gap_start} → {gap_end} ({minutes} минут)")

            # Рекомендации
            print("\n💡 РЕКОМЕНДАЦИИ:")
            for row in rows:
                symbol, total, first_ms, last_ms, _ = row
                last_dt = datetime.datetime.fromtimestamp(last_ms / 1000, tz=pytz.UTC)
                delay_minutes = (current_time - last_dt).total_seconds() / 60

                if delay_minutes > 5:
                    print(
                        f"  ⚠️  {symbol}: Данные устарели на {int(delay_minutes)} минут - запустите monitor.py"
                    )

                gaps = calculate_gaps(db_manager, symbol, 24)
                if len(gaps) > 5:
                    print(f"  ⚠️  {symbol}: Обнаружено {len(gaps)} пробелов - требуется заполнение")

            # Общая статистика БД
            query = "SELECT pg_database_size(current_database())"
            with conn.cursor() as cursor:
                cursor.execute(query)
                db_size = cursor.fetchone()[0]
                print(f"\n💾 Размер базы данных: {db_size / 1024 / 1024:.1f} MB")

    except Exception as e:
        print(f"❌ Ошибка при проверке данных: {e}")


def main():
    parser = argparse.ArgumentParser(description="Проверка статуса загруженных данных")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Показать детальную информацию о пробелах",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Проверять пробелы за последние N часов (по умолчанию 24)",
    )

    args = parser.parse_args()

    check_data_status(verbose=args.verbose, hours=args.hours)


if __name__ == "__main__":
    main()
