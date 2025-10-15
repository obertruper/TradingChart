#!/usr/bin/env python3
"""
Скрипт для заполнения пропущенных дней в Fear & Greed Index
методом интерполяции между соседними днями
"""

import sys
import psycopg2
import yaml
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FearGreedGapFiller:
    """Класс для заполнения пропущенных дней Fear & Greed Index"""

    def __init__(self, config_path: str = 'indicators_config.yaml'):
        """
        Инициализация

        Args:
            config_path: Путь к файлу конфигурации
        """
        # Загружаем конфигурацию
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.db_config = config['database']
        self.symbol = 'BTCUSDT'
        self.timeframes = ['1m', '15m', '1h']

    def connect_db(self):
        """Подключение к базе данных"""
        conn = psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
        # Отключаем автокоммит для управления транзакциями вручную
        conn.autocommit = False
        return conn

    def get_classification(self, value: int) -> str:
        """
        Получить классификацию для значения индекса

        Args:
            value: Значение индекса (0-100)

        Returns:
            Классификация: 'Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed'
        """
        if value >= 0 and value <= 25:
            return 'Extreme Fear'
        elif value > 25 and value <= 45:
            return 'Fear'
        elif value > 45 and value <= 55:
            return 'Neutral'
        elif value > 55 and value <= 75:
            return 'Greed'
        else:
            return 'Extreme Greed'

    def find_missing_days(self, conn) -> list:
        """
        Найти все дни с пропущенными данными F&G

        Args:
            conn: Соединение с БД

        Returns:
            Список пропущенных дней
        """
        cur = conn.cursor()

        # Находим диапазон дат с данными F&G
        cur.execute("""
            SELECT
                MIN(DATE(timestamp)) as first_date,
                MAX(DATE(timestamp)) as last_date
            FROM indicators_bybit_futures_1m
            WHERE symbol = %s
              AND fear_and_greed_index_alternative IS NOT NULL
        """, (self.symbol,))

        result = cur.fetchone()
        if not result or not result[0]:
            logger.warning("Нет данных Fear & Greed в БД")
            return []

        first_date, last_date = result

        # Находим все дни без данных F&G в этом периоде
        cur.execute("""
            WITH all_days AS (
                SELECT DISTINCT DATE(timestamp) as day
                FROM indicators_bybit_futures_1m
                WHERE symbol = %s
                  AND DATE(timestamp) >= %s
                  AND DATE(timestamp) <= %s
            ),
            days_with_fg AS (
                SELECT DISTINCT DATE(timestamp) as day
                FROM indicators_bybit_futures_1m
                WHERE symbol = %s
                  AND fear_and_greed_index_alternative IS NOT NULL
                  AND DATE(timestamp) >= %s
                  AND DATE(timestamp) <= %s
            )
            SELECT ad.day
            FROM all_days ad
            LEFT JOIN days_with_fg dfg ON ad.day = dfg.day
            WHERE dfg.day IS NULL
            ORDER BY ad.day
        """, (self.symbol, first_date, last_date, self.symbol, first_date, last_date))

        missing_days = [row[0] for row in cur.fetchall()]
        cur.close()

        # Коммитим транзакцию чтения
        conn.commit()

        return missing_days

    def get_neighbor_values(self, conn, date) -> Optional[Tuple[int, int]]:
        """
        Получить значения F&G за предыдущий и следующий день

        Args:
            conn: Соединение с БД
            date: Дата для проверки

        Returns:
            Кортеж (prev_value, next_value) или None если интерполяция невозможна
        """
        cur = conn.cursor()

        prev_day = date - timedelta(days=1)
        next_day = date + timedelta(days=1)

        # Получаем значения за предыдущий и следующий день
        cur.execute("""
            SELECT
                (SELECT fear_and_greed_index_alternative
                 FROM indicators_bybit_futures_1m
                 WHERE symbol = %s
                   AND DATE(timestamp) = %s
                   AND fear_and_greed_index_alternative IS NOT NULL
                 LIMIT 1) as prev_value,
                (SELECT fear_and_greed_index_alternative
                 FROM indicators_bybit_futures_1m
                 WHERE symbol = %s
                   AND DATE(timestamp) = %s
                   AND fear_and_greed_index_alternative IS NOT NULL
                 LIMIT 1) as next_value
        """, (self.symbol, prev_day, self.symbol, next_day))

        prev_value, next_value = cur.fetchone()
        cur.close()

        # Коммитим транзакцию чтения
        conn.commit()

        if prev_value is not None and next_value is not None:
            return (prev_value, next_value)
        return None

    def fill_day(self, conn, date, value: int, classification: str) -> Dict[str, int]:
        """
        Заполнить данные F&G для одного дня

        Args:
            conn: Соединение с БД
            date: Дата для заполнения
            value: Значение индекса
            classification: Классификация

        Returns:
            Словарь с количеством обновленных записей по таймфреймам
        """
        cur = conn.cursor()
        updated_counts = {}

        try:

            # Обновляем данные для каждого таймфрейма
            for timeframe in self.timeframes:
                table_name = f'indicators_bybit_futures_{timeframe}'

                cur.execute(f"""
                    UPDATE {table_name}
                    SET
                        fear_and_greed_index_alternative = %s,
                        fear_and_greed_index_classification_alternative = %s
                    WHERE symbol = %s
                      AND DATE(timestamp) = %s
                      AND fear_and_greed_index_alternative IS NULL
                """, (value, classification, self.symbol, date))

                updated_counts[timeframe] = cur.rowcount
                logger.info(f"  {timeframe}: обновлено {cur.rowcount} записей")

            # Проверяем корректность обновления
            total_updated = sum(updated_counts.values())
            if total_updated == 0:
                logger.warning(f"Не найдено записей для обновления за {date}")
                conn.rollback()
                return updated_counts

            # Коммитим транзакцию
            conn.commit()
            logger.info(f"✅ Успешно заполнен день {date} (всего {total_updated} записей)")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Ошибка при заполнении {date}: {e}")
            raise
        finally:
            cur.close()

        return updated_counts

    def run(self, specific_date: Optional[str] = None):
        """
        Основной метод выполнения

        Args:
            specific_date: Конкретная дата для заполнения (YYYY-MM-DD) или None для всех
        """
        logger.info("="*60)
        logger.info("🚀 Запуск заполнения пропущенных дней Fear & Greed Index")
        logger.info("="*60)

        conn = self.connect_db()

        try:
            if specific_date:
                # Заполняем конкретную дату
                date = datetime.strptime(specific_date, '%Y-%m-%d').date()
                missing_days = [date]
                logger.info(f"📅 Режим заполнения конкретной даты: {date}")
            else:
                # Находим все пропущенные дни
                missing_days = self.find_missing_days(conn)

                if not missing_days:
                    logger.info("✅ Нет пропущенных дней!")
                    return

                logger.info(f"📊 Найдено {len(missing_days)} пропущенных дней")

            # Обрабатываем каждый пропущенный день
            filled_days = 0
            skipped_days = 0

            for date in missing_days:
                logger.info(f"\n📅 Обработка {date}:")

                # Получаем значения соседних дней
                neighbor_values = self.get_neighbor_values(conn, date)

                if neighbor_values:
                    prev_value, next_value = neighbor_values
                    # Вычисляем среднее значение
                    avg_value = round((prev_value + next_value) / 2)
                    classification = self.get_classification(avg_value)

                    logger.info(f"  Интерполяция: ({prev_value} + {next_value}) / 2 = {avg_value} ({classification})")

                    # Заполняем день
                    updated_counts = self.fill_day(conn, date, avg_value, classification)

                    if sum(updated_counts.values()) > 0:
                        filled_days += 1
                    else:
                        skipped_days += 1
                else:
                    logger.warning(f"  ⚠️ Невозможно интерполировать (нет данных за соседние дни)")
                    skipped_days += 1

            # Итоговая статистика
            logger.info("\n" + "="*60)
            logger.info("📊 ИТОГИ:")
            logger.info(f"  ✅ Заполнено дней: {filled_days}")
            if skipped_days > 0:
                logger.info(f"  ⚠️ Пропущено дней: {skipped_days}")
            logger.info("="*60)

        finally:
            conn.close()


def main():
    """Основная функция"""
    filler = FearGreedGapFiller()

    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        # Заполнить конкретную дату
        specific_date = sys.argv[1]
        filler.run(specific_date)
    else:
        # Заполнить все пропущенные дни
        filler.run()


if __name__ == "__main__":
    main()