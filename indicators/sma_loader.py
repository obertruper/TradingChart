#!/usr/bin/env python3
"""
SMA (Simple Moving Average) Indicator Loader
=============================================
Универсальный загрузчик SMA индикаторов с поддержкой:
- Динамического создания колонок
- Пакетной обработки больших объемов данных
- Множественных символов
- Инкрементальных обновлений
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional
import logging
from tqdm import tqdm
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.database import DatabaseConnection

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SMALoader:
    """Универсальный загрузчик SMA индикаторов"""

    def __init__(self, symbol: str = 'BTCUSDT', batch_days: int = 30):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара (BTCUSDT, ETHUSDT и т.д.)
            batch_days: Количество дней для обработки за один батч
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.batch_days = batch_days
        self.source_table = 'candles_bybit_futures_1m'
        self.target_table = 'indicators_bybit_futures_1m'

    def check_and_create_columns(self, sma_periods: List[int]) -> Tuple[set, List[int]]:
        """
        Проверяет существование колонок SMA и создает отсутствующие

        Args:
            sma_periods: Список периодов SMA для проверки/создания

        Returns:
            Tuple (существующие периоды, новые созданные периоды)
        """
        # Получаем список существующих колонок
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'indicators_bybit_futures_1m'
            AND column_name LIKE 'sma_%';
        """

        existing_columns = self.db.execute_query(query)
        existing_sma = set()

        if existing_columns:
            for row in existing_columns:
                col_name = row['column_name']
                try:
                    period = int(col_name.split('_')[1])
                    existing_sma.add(period)
                except (IndexError, ValueError):
                    continue

        logger.info(f"📊 Существующие SMA периоды: {sorted(existing_sma)}")

        # Определяем какие колонки нужно создать
        columns_to_create = []
        for period in sma_periods:
            if period not in existing_sma:
                columns_to_create.append(period)

        # Создаем новые колонки
        if columns_to_create:
            logger.info(f"🔨 Создаю новые колонки для периодов: {columns_to_create}")

            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    for period in columns_to_create:
                        try:
                            alter_query = f"""
                                ALTER TABLE {self.target_table}
                                ADD COLUMN IF NOT EXISTS sma_{period} DECIMAL(20,8);
                            """
                            cur.execute(alter_query)
                            conn.commit()
                            logger.info(f"   ✅ Создана колонка sma_{period}")
                        except Exception as e:
                            logger.error(f"   ❌ Ошибка при создании sma_{period}: {e}")
                            conn.rollback()
        else:
            logger.info("✅ Все необходимые колонки уже существуют")

        return existing_sma, columns_to_create

    def get_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Получить диапазон дат из таблицы свечей для текущего символа

        Returns:
            Tuple (минимальная дата, максимальная дата)
        """
        query = f"""
            SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
            FROM {self.source_table}
            WHERE symbol = %s;
        """
        result = self.db.execute_query(query, (self.symbol,))
        if result and result[0]['min_ts']:
            return result[0]['min_ts'], result[0]['max_ts']
        return None, None

    def get_last_indicator_timestamp(self, sma_periods: List[int]) -> Optional[datetime]:
        """
        Получить последний timestamp с данными индикаторов

        Args:
            sma_periods: Список периодов для проверки

        Returns:
            Последний timestamp или None
        """
        # Проверяем последнюю дату для всех периодов
        conditions = []
        for period in sma_periods:
            conditions.append(f"sma_{period} IS NOT NULL")

        if not conditions:
            return None

        where_clause = " OR ".join(conditions)

        query = f"""
            SELECT MAX(timestamp) as last_ts
            FROM {self.target_table}
            WHERE symbol = %s
            AND ({where_clause});
        """

        result = self.db.execute_query(query, (self.symbol,))
        return result[0]['last_ts'] if result and result[0]['last_ts'] else None

    def calculate_sma_batch(self, df: pd.DataFrame, sma_periods: List[int]) -> pd.DataFrame:
        """
        Рассчитать SMA для батча данных

        Args:
            df: DataFrame со свечами (должен содержать колонку 'close')
            sma_periods: Список периодов SMA для расчета

        Returns:
            DataFrame с рассчитанными SMA
        """
        result_df = pd.DataFrame(index=df.index)
        result_df['symbol'] = self.symbol

        # Рассчитываем SMA для каждого периода
        for period in sma_periods:
            col_name = f'sma_{period}'
            result_df[col_name] = df['close'].rolling(
                window=period,
                min_periods=period
            ).mean().round(8)

        # Удаляем строки где все SMA = NaN
        sma_columns = [f'sma_{p}' for p in sma_periods]
        result_df = result_df.dropna(subset=sma_columns, how='all')

        return result_df

    def save_to_database(self, df: pd.DataFrame, sma_periods: List[int]):
        """
        Сохранить рассчитанные индикаторы в БД

        Args:
            df: DataFrame с индикаторами
            sma_periods: Список периодов SMA для сохранения
        """
        if df.empty:
            return 0

        # Подготавливаем записи для вставки
        records = []
        sma_columns = [f'sma_{p}' for p in sma_periods]

        for timestamp, row in df.iterrows():
            record = [timestamp, row['symbol']]
            for col in sma_columns:
                record.append(row.get(col, None))
            records.append(tuple(record))

        # Формируем SQL запрос
        columns = ['timestamp', 'symbol'] + sma_columns
        placeholders = ['%s'] * len(columns)

        insert_query = f"""
            INSERT INTO {self.target_table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (timestamp, symbol)
            DO UPDATE SET
        """

        # Добавляем UPDATE часть для каждой SMA колонки
        update_parts = []
        for col in sma_columns:
            update_parts.append(f"{col} = COALESCE(EXCLUDED.{col}, {self.target_table}.{col})")

        insert_query += ', '.join(update_parts) + ';'

        # Сохраняем батчами
        batch_size = 1000
        total_saved = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            rows_affected = self.db.execute_many(insert_query, batch)
            total_saved += len(batch)

        return total_saved

    def process_date_range(self, start_date: datetime, end_date: datetime,
                          sma_periods: List[int], show_progress: bool = True):
        """
        Обработать диапазон дат

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            sma_periods: Список периодов SMA
            show_progress: Показывать прогресс-бар
        """
        current_date = start_date
        total_records = 0

        # Определяем максимальный период для lookback
        max_period = max(sma_periods)

        # Создаем прогресс-бар если нужно
        if show_progress:
            total_days = (end_date - start_date).days
            pbar = tqdm(total=total_days, desc="Обработка", unit="день")

        while current_date < end_date:
            batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

            # Загружаем свечи с запасом для расчета SMA
            lookback_start = current_date - timedelta(minutes=max_period)

            query = f"""
                SELECT timestamp, close
                FROM {self.source_table}
                WHERE symbol = %s
                AND timestamp >= %s
                AND timestamp < %s
                ORDER BY timestamp;
            """

            with self.db.get_connection() as conn:
                df = pd.read_sql_query(query, conn,
                                      params=(self.symbol, lookback_start, batch_end))

            if not df.empty:
                df.set_index('timestamp', inplace=True)

                # Рассчитываем SMA
                sma_df = self.calculate_sma_batch(df, sma_periods)

                # Сохраняем только записи из текущего батча (без lookback)
                sma_df = sma_df[sma_df.index >= current_date]

                if not sma_df.empty:
                    saved = self.save_to_database(sma_df, sma_periods)
                    total_records += saved
                    logger.info(f"   Обработано {current_date.date()} - {batch_end.date()}: "
                              f"{saved:,} записей")

            # Обновляем прогресс
            if show_progress:
                days_processed = (batch_end - current_date).days
                pbar.update(days_processed)

            current_date = batch_end

        if show_progress:
            pbar.close()

        logger.info(f"✅ Всего обработано: {total_records:,} записей")

    def run(self, sma_periods: List[int] = None):
        """
        Главная функция запуска

        Args:
            sma_periods: Список периодов SMA (по умолчанию [10, 20, 50, 100, 200])
        """
        if sma_periods is None:
            sma_periods = [10, 20, 50, 100, 200]

        logger.info("=" * 60)
        logger.info(f"🚀 SMA LOADER для {self.symbol}")
        logger.info(f"📊 Периоды: {sma_periods}")
        logger.info("=" * 60)

        # Проверяем существование таблицы
        if not self.db.check_table_exists(self.target_table):
            logger.error(f"❌ Таблица {self.target_table} не существует!")
            logger.error("Создайте таблицу с помощью SQL команды")
            return

        # Получаем диапазон дат свечей
        min_date, max_date = self.get_date_range()
        if not min_date:
            logger.error(f"❌ Нет данных для {self.symbol} в таблице свечей")
            return

        logger.info(f"📅 Доступные данные: {min_date} - {max_date}")

        # Проверяем и создаем колонки
        existing_sma, new_columns = self.check_and_create_columns(sma_periods)

        # Определяем с какой даты начинать
        last_timestamp = self.get_last_indicator_timestamp(sma_periods)

        if last_timestamp:
            logger.info(f"📈 Найдены данные до {last_timestamp}")
            start_date = last_timestamp + timedelta(minutes=1)

            if start_date >= max_date:
                logger.info("✅ Данные уже актуальны!")
                return

            logger.info(f"🔄 Обновление с {start_date}")
        else:
            logger.info("📊 Таблица пустая, начинаю полную загрузку")
            start_date = min_date

        # Обрабатываем данные
        self.process_date_range(start_date, max_date, sma_periods)

        logger.info("\n" + "=" * 60)
        logger.info("✅ ЗАГРУЗКА ЗАВЕРШЕНА")
        logger.info("=" * 60)


def main():
    """Точка входа с поддержкой аргументов командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='SMA Indicator Loader')
    parser.add_argument('--symbol', type=str, default='BTCUSDT',
                       help='Торговая пара (по умолчанию BTCUSDT)')
    parser.add_argument('--periods', type=str, default='10,20,50,100,200',
                       help='Периоды SMA через запятую')
    parser.add_argument('--batch-days', type=int, default=30,
                       help='Размер батча в днях (по умолчанию 30)')

    args = parser.parse_args()

    # Парсим периоды
    sma_periods = [int(p.strip()) for p in args.periods.split(',')]

    # Создаем и запускаем загрузчик
    loader = SMALoader(symbol=args.symbol, batch_days=args.batch_days)
    loader.run(sma_periods)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Прервано пользователем")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise