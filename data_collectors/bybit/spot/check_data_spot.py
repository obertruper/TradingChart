#!/usr/bin/env python3
"""
Скрипт для быстрой проверки последних загруженных данных SPOT в PostgreSQL.
Показывает статистику по всем символам и возможные пробелы в данных.
"""

import os
import sys
import datetime
import pytz
import psycopg2
from tabulate import tabulate
import argparse
from typing import List, Tuple, Optional
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Добавляем путь к корневой директории проекта
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


def get_db_connection():
    """Создание подключения к БД."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "82.25.115.144"),
        port=int(os.getenv("DB_PORT", 5432)),
        database=os.getenv("DB_NAME", "trading_data"),
        user=os.getenv("DB_WRITER_USER", "trading_writer"),
        password=os.getenv("DB_WRITER_PASSWORD", ""),
    )


def format_timestamp(timestamp: datetime.datetime) -> str:
    """Форматирование timestamp в читаемый вид."""
    if timestamp:
        return timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
    return "N/A"


def calculate_gaps(conn, symbol: str, hours: int = 24) -> List[Tuple[str, str, int]]:
    """
    Находит пробелы в данных за последние N часов.
    Возвращает список пробелов: (начало, конец, минут пропущено)
    """
    gaps = []

    # Получаем данные за последние N часов
    end_time = datetime.datetime.now(pytz.UTC)
    start_time = end_time - datetime.timedelta(hours=hours)

    query = """
        SELECT timestamp FROM candles_bybit_spot_1m
        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """

    with conn.cursor() as cursor:
        cursor.execute(query, (symbol, start_time, end_time))
        rows = cursor.fetchall()

        if len(rows) > 1:
            for i in range(1, len(rows)):
                prev_time = rows[i - 1][0]
                curr_time = rows[i][0]
                diff_minutes = (curr_time - prev_time).total_seconds() / 60

                # Если разница больше 1 минуты - есть пробел
                if diff_minutes > 1:
                    gap_start = prev_time + datetime.timedelta(minutes=1)
                    gap_end = curr_time - datetime.timedelta(minutes=1)
                    gaps.append((
                        format_timestamp(gap_start),
                        format_timestamp(gap_end),
                        int(diff_minutes - 1)
                    ))

    return gaps


def check_data_status(verbose: bool = False, hours: int = 24):
    """Проверка статуса загруженных данных."""

    try:
        conn = get_db_connection()

        # Получаем статистику по всем символам из БД
        query = """
            SELECT
                symbol,
                COUNT(*) as total_candles,
                MIN(timestamp) as first_candle,
                MAX(timestamp) as last_candle
            FROM candles_bybit_spot_1m
            GROUP BY symbol
            ORDER BY symbol
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        if not rows:
            print("❌ Нет данных в базе данных")
            return

        # Подготавливаем данные для таблицы
        table_data = []
        current_time = datetime.datetime.now(pytz.UTC)
        total_candles_all = 0
        symbols_with_issues = []

        for row in rows:
            symbol, total, first_ts, last_ts = row
            total_candles_all += total

            # Форматируем даты
            first_date = first_ts.strftime("%Y-%m-%d") if first_ts else "N/A"
            last_date = last_ts.strftime("%Y-%m-%d %H:%M") if last_ts else "N/A"

            # Вычисляем ожидаемое количество свечей
            if first_ts and last_ts:
                time_range = last_ts - first_ts
                expected_candles = time_range.total_seconds() / 60 + 1
                coverage = (total / expected_candles * 100) if expected_candles > 0 else 0
            else:
                coverage = 0

            # Вычисляем задержку от текущего времени
            if last_ts:
                # Убедимся, что last_ts имеет timezone
                if last_ts.tzinfo is None:
                    last_ts = pytz.UTC.localize(last_ts)
                delay = current_time - last_ts
                delay_minutes = int(delay.total_seconds() / 60)
                if delay_minutes < 60:
                    delay_str = f"{delay_minutes} мин"
                elif delay_minutes < 1440:
                    delay_str = f"{delay_minutes // 60} ч {delay_minutes % 60} мин"
                else:
                    delay_str = f"{delay_minutes // 1440} дн"
            else:
                delay_str = "N/A"
                delay_minutes = 999999

            # Проверяем наличие пробелов
            gaps = calculate_gaps(conn, symbol, hours)
            gaps_str = f"{len(gaps)} пробелов" if gaps else "✅ Нет"

            # Статус
            if delay_minutes <= 2:
                status = "✅"
            elif delay_minutes <= 10:
                status = "⚠️"
            else:
                status = "❌"
                symbols_with_issues.append(symbol)

            table_data.append(
                [
                    symbol,
                    f"{total:,}",
                    first_date,
                    last_date,
                    f"{coverage:.2f}%",
                    delay_str,
                    gaps_str,
                    status,
                ]
            )

        # Выводим красивую таблицу
        headers = [
            "Символ",
            "Свечей",
            "От",
            "До",
            "Покрытие",
            "Задержка",
            f"Пробелы ({hours}ч)",
            "Статус",
        ]
        print("\n📊 СТАТУС ДАННЫХ SPOT В POSTGRESQL")
        print("=" * 110)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Общая статистика
        print(f"\n📈 ОБЩАЯ СТАТИСТИКА:")
        print(f"   • Всего символов: {len(rows)}")
        print(f"   • Всего свечей: {total_candles_all:,}")
        print(f"   • Текущее время: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        # Показываем детали пробелов если verbose
        if verbose:
            print("\n📍 ДЕТАЛИ ПРОБЕЛОВ В ДАННЫХ:")
            print("-" * 110)
            has_gaps = False
            for row in rows:
                symbol = row[0]
                gaps = calculate_gaps(conn, symbol, hours)
                if gaps:
                    has_gaps = True
                    print(f"\n{symbol}:")
                    for gap_start, gap_end, minutes in gaps:
                        print(f"  • {gap_start} → {gap_end} ({minutes} минут)")
            if not has_gaps:
                print("   Пробелов не обнаружено")

        # Рекомендации
        if symbols_with_issues:
            print("\n💡 РЕКОМЕНДАЦИИ:")
            for symbol in symbols_with_issues:
                print(f"   ⚠️  {symbol}: Данные устарели - запустите monitor_spot.py")

        # Размер базы данных
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_database_size(current_database())")
            db_size = cursor.fetchone()[0]

            # Размер таблицы
            cursor.execute("""
                SELECT pg_total_relation_size('candles_bybit_spot_1m')
            """)
            table_size = cursor.fetchone()[0]

        print(f"\n💾 РАЗМЕР ХРАНИЛИЩА:")
        print(f"   • База данных: {db_size / 1024 / 1024 / 1024:.2f} GB")
        print(f"   • Таблица spot: {table_size / 1024 / 1024 / 1024:.2f} GB")

        conn.close()

    except Exception as e:
        print(f"❌ Ошибка при проверке данных: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="Проверка статуса загруженных данных spot")
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
