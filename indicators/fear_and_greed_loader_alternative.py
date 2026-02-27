#!/usr/bin/env python3
"""
Fear & Greed Index Loader
=========================
Загрузчик индекса страха и жадности для BTCUSDT.
Загружает данные с API и обновляет существующие записи в БД.

Использование:
    python fear_and_greed_loader.py
"""

import sys
import os
# Добавляем корень проекта в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional, Tuple
import requests
import yaml
from tqdm import tqdm
import psycopg2
from indicators.database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'fear_greed_{timestamp}.log')

    # Настройка форматирования
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Файловый обработчик
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Настройка логгера
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📝 Fear & Greed Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger

logger = setup_logging()


class FearAndGreedLoader:
    """
    Загрузчик индекса страха и жадности для BTCUSDT
    """

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара (только BTCUSDT поддерживается)
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.config = self.load_config()

        # Проверяем что работаем только с BTCUSDT
        if symbol != 'BTCUSDT':
            raise ValueError("Fear & Greed Index доступен только для BTCUSDT")

        # Конфигурация Fear & Greed
        self.fng_config = self.config.get('indicators', {}).get('fear_and_greed', {})
        self.api_url = self.fng_config.get('api_url', 'https://api.alternative.me/fng/?limit=0')
        self.batch_days = self.fng_config.get('batch_days', 1)
        self.retry_count = self.fng_config.get('retry_on_error', 3)
        self.timeframes = self.fng_config.get('timeframes', ['1m', '15m', '1h'])

        # Данные с API (загружаются один раз)
        self.api_data = None

        # Флаги режимов (устанавливаются из main())
        self.force_reload = False
        self.check_nulls = False

    def load_config(self) -> dict:
        """Загружает конфигурацию из файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"📋 Конфигурация загружена из {config_path}")
        return config

    def create_columns(self) -> bool:
        """
        Создает колонки для Fear & Greed Index во всех таблицах indicators

        Returns:
            True если колонки созданы или уже существуют, False при ошибке
        """
        logger.info("🔨 Проверка и создание колонок Fear & Greed...")

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for timeframe in self.timeframes:
                    table_name = f'indicators_bybit_futures_{timeframe}'

                    # Проверяем существование таблицы
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = %s
                        )
                    """, (table_name,))

                    if not cur.fetchone()[0]:
                        logger.error(f"❌ Таблица {table_name} не существует!")
                        return False

                    # Проверяем и создаем колонки
                    for column_name, column_type in [
                        ('fear_and_greed_index_alternative', 'SMALLINT'),
                        ('fear_and_greed_index_classification_alternative', 'VARCHAR(20)')
                    ]:
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.columns
                                WHERE table_name = %s AND column_name = %s
                            )
                        """, (table_name, column_name))

                        if not cur.fetchone()[0]:
                            logger.info(f"  ➕ Создаю колонку {column_name} в таблице {table_name}")
                            cur.execute(f"""
                                ALTER TABLE {table_name}
                                ADD COLUMN {column_name} {column_type}
                            """)
                        else:
                            logger.info(f"  ✅ Колонка {column_name} уже существует в {table_name}")

                conn.commit()
                logger.info("✅ Все колонки готовы")
                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при создании колонок: {e}")
                conn.rollback()
                return False

    def get_api_data(self) -> Optional[Dict]:
        """
        Получает данные с API Fear & Greed Index

        Returns:
            Словарь с данными или None при ошибке
        """
        if self.api_data is not None:
            return self.api_data

        logger.info(f"📡 Загрузка данных с API: {self.api_url}")

        for attempt in range(self.retry_count):
            try:
                response = requests.get(self.api_url, timeout=30)
                response.raise_for_status()

                data = response.json()

                if 'data' not in data:
                    logger.error(f"❌ Неверный формат ответа API: {data}")
                    return None

                # Преобразуем в удобный формат {date: {value, classification}}
                self.api_data = {}
                for item in data['data']:
                    # Преобразуем timestamp в дату UTC
                    timestamp = int(item['timestamp'])
                    date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()

                    self.api_data[date] = {
                        'value': int(item['value']),
                        'classification': item['value_classification']
                    }

                logger.info(f"✅ Загружено {len(self.api_data)} дней данных")
                logger.info(f"📅 Период: {min(self.api_data.keys())} - {max(self.api_data.keys())}")

                return self.api_data

            except requests.RequestException as e:
                logger.warning(f"⚠️ Попытка {attempt + 1}/{self.retry_count} не удалась: {e}")
                if attempt == self.retry_count - 1:
                    logger.error(f"❌ Не удалось загрузить данные с API после {self.retry_count} попыток")
                    return None

        return None

    def get_last_filled_date(self, timeframe: str) -> Optional[datetime.date]:
        """
        Получает последнюю дату где Fear & Greed данные заполнены.
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT DATE(MAX(timestamp))
                FROM {table_name}
                WHERE symbol = %s
                  AND fear_and_greed_index_alternative IS NOT NULL
            """, (self.symbol,))
            result = cur.fetchone()
            return result[0] if result and result[0] else None

    def get_null_dates(self, timeframe: str) -> Dict[datetime.date, int]:
        """
        Получает даты где есть NULL записи для Fear & Greed

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)

        Returns:
            Словарь {дата: количество NULL записей}
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute(f"""
                SELECT DATE(timestamp) as day, COUNT(*) as null_count
                FROM {table_name}
                WHERE symbol = %s
                  AND fear_and_greed_index_alternative IS NULL
                GROUP BY DATE(timestamp)
                ORDER BY day
            """, (self.symbol,))

            results = cur.fetchall()
            return {row[0]: row[1] for row in results}

    def get_checkpoint(self, timeframe: str) -> Optional[datetime]:
        """
        Получает последнюю обработанную дату для таймфрейма
        DEPRECATED: Используется только для обратной совместимости

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)

        Returns:
            Последняя дата с данными или None
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM {table_name}
                WHERE symbol = %s
                  AND fear_and_greed_index_alternative IS NOT NULL
            """, (self.symbol,))

            result = cur.fetchone()
            if result and result[0]:
                return result[0]

            return None

    def get_start_date(self) -> Optional[datetime]:
        """
        Получает минимальную дату из БД для BTCUSDT

        Returns:
            Минимальная дата или None
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT MIN(timestamp)
                FROM indicators_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))

            result = cur.fetchone()
            if result and result[0]:
                return result[0]

            logger.error(f"❌ Нет данных для {self.symbol} в БД")
            return None

    def update_batch(self, timeframe: str, date: datetime.date,
                    value: int, classification: str, only_null: bool = True) -> int:
        """
        Обновляет записи за один день для указанного таймфрейма

        Args:
            timeframe: Таймфрейм
            date: Дата (день)
            value: Значение индекса (0-100)
            classification: Классификация
            only_null: Если True - обновлять только NULL записи (оптимизация)

        Returns:
            Количество обновленных записей
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Границы дня в UTC
        start_time = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_time = start_time + timedelta(days=1)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Базовый запрос
                query = f"""
                    UPDATE {table_name}
                    SET fear_and_greed_index_alternative = %s,
                        fear_and_greed_index_classification_alternative = %s
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp < %s
                """

                # Добавляем условие IS NULL если нужна оптимизация
                if only_null:
                    query += "      AND fear_and_greed_index_alternative IS NULL"

                cur.execute(query, (value, classification, self.symbol, start_time, end_time))

                updated_count = cur.rowcount
                conn.commit()

                return updated_count

            except Exception as e:
                logger.error(f"❌ Ошибка при обновлении {table_name} за {date}: {e}")
                conn.rollback()
                raise

    def validate_day_consistency(self, date: datetime.date) -> bool:
        """
        Проверяет консистентность данных между таймфреймами для одного дня

        Args:
            date: Дата для проверки

        Returns:
            True если данные консистентны
        """
        values = {}

        # Границы дня в UTC
        start_time = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_time = start_time + timedelta(days=1)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            for timeframe in self.timeframes:
                table_name = f'indicators_bybit_futures_{timeframe}'

                # Получаем уникальные значения за день
                cur.execute(f"""
                    SELECT DISTINCT fear_and_greed_index_alternative, fear_and_greed_index_classification_alternative
                    FROM {table_name}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp < %s
                      AND fear_and_greed_index_alternative IS NOT NULL
                """, (self.symbol, start_time, end_time))

                results = cur.fetchall()

                if len(results) > 1:
                    logger.error(f"❌ Несколько разных значений в {table_name} за {date}: {results}")
                    return False

                if results:
                    values[timeframe] = results[0]

        # Проверяем что все таймфреймы имеют одинаковое значение
        unique_values = set(values.values())
        if len(unique_values) > 1:
            logger.error(f"❌ Разные значения между таймфреймами за {date}: {values}")
            return False

        return True

    def process_timeframe(self, timeframe: str, start_date: datetime, end_date: datetime) -> bool:
        """
        Обрабатывает один таймфрейм.

        Режимы:
          - force_reload: обновляет все записи
          - check_nulls: сканирует NULL и заполняет пропуски
          - default: инкрементально от последней заполненной даты

        Args:
            timeframe: Таймфрейм
            start_date: Начальная дата
            end_date: Конечная дата

        Returns:
            True при успехе
        """
        logger.info(f"\n📊 Обработка таймфрейма: {timeframe}")

        if self.force_reload or self.check_nulls:
            # Режимы force-reload и check-nulls: сканируем NULL
            null_dates = self.get_null_dates(timeframe)

            dates_to_process = []
            for date in null_dates.keys():
                if date in self.api_data:
                    dates_to_process.append(date)
            dates_to_process.sort()

            total_null_dates = len(null_dates)
            total_with_api = len(dates_to_process)
            total_null_records = sum(null_dates.values())

            mode_name = "force-reload" if self.force_reload else "check-nulls"
            logger.info(f"🔍 Режим --{mode_name}: сканирование NULL записей")
            logger.info(f"   • Дней с NULL: {total_null_dates}")
            logger.info(f"   • Дней с API данными: {total_with_api}")
            logger.info(f"   • Всего NULL записей: {total_null_records:,}")
        else:
            # Режим по умолчанию: инкрементально от последней заполненной даты
            last_filled = self.get_last_filled_date(timeframe)

            if last_filled:
                logger.info(f"📅 Последняя заполненная дата: {last_filled}")
                dates_to_process = sorted([d for d in self.api_data.keys() if d > last_filled])
            else:
                logger.info(f"📅 Данных нет - обрабатываем все API даты")
                dates_to_process = sorted(self.api_data.keys())

            logger.info(f"   📝 Новых дней для обработки: {len(dates_to_process)}")

        if not dates_to_process:
            logger.info(f"✅ Таймфрейм {timeframe} актуален")
            return True

        logger.info(f"📅 Период обработки: {dates_to_process[0]} - {dates_to_process[-1]}")

        updated_total = 0

        with tqdm(total=len(dates_to_process), desc=f"Загрузка {timeframe}") as pbar:
            for current_date in dates_to_process:
                fng_data = self.api_data[current_date]

                try:
                    updated = self.update_batch(
                        timeframe,
                        current_date,
                        fng_data['value'],
                        fng_data['classification'],
                        only_null=not self.force_reload
                    )

                    updated_total += updated

                    pbar.set_description(
                        f"{timeframe}: {current_date} "
                        f"(FNG={fng_data['value']}, записано={updated})"
                    )

                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке {current_date}: {e}")
                    return False

                pbar.update(1)

        logger.info(f"✅ Таймфрейм {timeframe} обработан:")
        logger.info(f"   • Записано: {updated_total:,} записей")
        return True

    def run(self):
        """
        Основной метод запуска загрузчика
        """
        logger.info("="*60)
        logger.info("🚀 Запуск Fear & Greed Index Loader")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"⏱️ Таймфреймы: {', '.join(self.timeframes)}")
        logger.info("="*60)

        # Шаг 1: Создаем колонки если нужно
        if not self.create_columns():
            logger.error("❌ Не удалось создать колонки")
            return

        # Шаг 2: Получаем данные с API
        if not self.get_api_data():
            logger.error("❌ Не удалось получить данные с API")
            return

        # Шаг 3: Определяем период обработки
        start_date = self.get_start_date()
        if not start_date:
            logger.error("❌ Не удалось определить начальную дату")
            return

        end_date = datetime.now(timezone.utc)

        logger.info(f"\n📅 Общий период обработки:")
        logger.info(f"   Начало: {start_date}")
        logger.info(f"   Конец: {end_date}")

        # Шаг 4: Обрабатываем каждый таймфрейм последовательно
        for timeframe in self.timeframes:
            if not self.process_timeframe(timeframe, start_date, end_date):
                logger.error(f"❌ Ошибка при обработке таймфрейма {timeframe}")
                logger.error("⛔ Загрузка прервана")
                return

        # Шаг 5: Финальная валидация
        logger.info("\n🔍 Проверка консистентности данных...")

        # Проверяем последние 7 дней
        check_date = end_date.date()
        inconsistent_days = []

        for i in range(7):
            if check_date in self.api_data:
                if not self.validate_day_consistency(check_date):
                    inconsistent_days.append(check_date)
            check_date -= timedelta(days=1)

        if inconsistent_days:
            logger.warning(f"⚠️ Обнаружены несогласованные данные за дни: {inconsistent_days}")
        else:
            logger.info("✅ Данные консистентны между таймфреймами")

        logger.info("\n" + "="*60)
        logger.info("🎉 Fear & Greed Index успешно загружен!")
        logger.info("="*60)


def main():
    """Точка входа"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Fear & Greed Index Loader (Alternative.me API)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python fear_and_greed_loader_alternative.py                    # Инкрементальная загрузка (новые данные)
  python fear_and_greed_loader_alternative.py --check-nulls      # Проверить и заполнить NULL в середине данных
  python fear_and_greed_loader_alternative.py --force-reload     # Перезаписать все данные
  python fear_and_greed_loader_alternative.py --timeframe 1h     # Только 1h таймфрейм
        """
    )

    parser.add_argument('--force-reload', action='store_true',
                       help='Перезаписать все данные (игнорировать оптимизацию)')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Проверить и заполнить NULL значения в середине данных')
    parser.add_argument('--timeframe', type=str,
                       help='Обработать только указанный таймфрейм (1m, 15m, 1h)')

    args = parser.parse_args()

    # Подтверждение --force-reload
    if args.force_reload and os.environ.get('FORCE_RELOAD_CONFIRMED') != '1':
        print("\n⚠️  ВНИМАНИЕ: --force-reload полностью перезапишет все данные Fear & Greed в таблице!")
        print("⏱️  Полная перезапись может занять значительное время.")
        response = input("\nПродолжить? (y/n): ").strip().lower()
        if response != 'y':
            print("❌ Отменено пользователем.")
            sys.exit(0)

    loader = FearAndGreedLoader(symbol='BTCUSDT')
    loader.force_reload = args.force_reload
    loader.check_nulls = args.check_nulls

    # Если указан конкретный таймфрейм
    if args.timeframe:
        loader.timeframes = [args.timeframe]

    if args.force_reload:
        logger.info("🔄 Режим force-reload: будут перезаписаны ВСЕ данные")
    elif args.check_nulls:
        logger.info("🔍 Режим check-nulls: поиск и заполнение NULL значений")

    loader.run()


if __name__ == "__main__":
    main()