#!/usr/bin/env python3
"""
Universal SMA Loader with Multi-Timeframe Support
==================================================
Универсальный загрузчик SMA индикаторов с поддержкой:
- Любых таймфреймов (1m, 5m, 15m, 30m, 1h, 4h, 1d и т.д.)
- Автоматической агрегации из 1m данных
- Динамического создания таблиц
- Инкрементальных обновлений
- Параллельной обработки символов
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional, Any
import logging
from tqdm import tqdm
import sys
import os
import warnings
import yaml
import argparse
import time

# Игнорируем предупреждение pandas о psycopg2
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование в консоль и файл"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'sma_multi_tf_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename, encoding='utf-8')
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"📝 Логирование настроено. Лог-файл: {log_filename}")

    return logger

logger = setup_logging()


class SMALoader:
    """Загрузчик SMA для разных таймфреймов"""

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.config = self.load_config()
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.force_reload = False  # Флаг принудительного пересчета (устанавливается из main())

        # Динамический мапинг таймфреймов на минуты (после загрузки конфига)
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """
        Динамически парсит таймфреймы из конфигурации
        Поддерживает форматы: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w

        Returns:
            dict: Мапинг таймфрейма на количество минут
        """
        timeframe_map = {}

        # Получаем таймфреймы из конфига или используем стандартные
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        for tf in timeframes:
            # Парсим число и единицу измерения
            import re
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)

                # Конвертируем в минуты
                if unit == 'm':
                    minutes = number
                elif unit == 'h':
                    minutes = number * 60
                elif unit == 'd':
                    minutes = number * 1440
                elif unit == 'w':
                    minutes = number * 10080  # 7 * 24 * 60
                else:
                    logger.warning(f"⚠️ Неизвестный формат таймфрейма: {tf}")
                    continue

                timeframe_map[tf] = minutes
                logger.debug(f"📊 Таймфрейм {tf} = {minutes} минут")
            else:
                logger.warning(f"⚠️ Неправильный формат таймфрейма: {tf} (используйте формат как 1m, 5m, 1h, 1d)")

        if not timeframe_map:
            # Если ничего не распарсили, используем минимальный набор
            logger.warning("⚠️ Не удалось распарсить таймфреймы, использую стандартные")
            timeframe_map = {'1m': 1, '15m': 15, '1h': 60}

        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из indicators_config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"📋 Конфигурация загружена из {config_path}")
                    return config
            except Exception as e:
                logger.error(f"❌ Не удалось загрузить indicators_config.yaml: {e}")
                raise
        else:
            logger.error("❌ indicators_config.yaml не найден")
            raise FileNotFoundError("indicators_config.yaml not found")

    def create_indicators_table(self, timeframe: str) -> bool:
        """
        Создает таблицу для индикаторов если её нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)

        Returns:
            True если таблица создана или уже существует
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Проверяем существование таблицы
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    );
                """, (table_name,))

                exists = cur.fetchone()[0]

                if not exists:
                    logger.info(f"🔨 Создаю таблицу {table_name}")

                    # Создаем таблицу
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        timestamp TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        -- SMA колонки будут добавлены динамически
                        PRIMARY KEY (timestamp, symbol)
                    );

                    -- Создаем индексы
                    CREATE INDEX idx_{timeframe}_symbol_timestamp
                    ON {table_name} (symbol, timestamp);

                    CREATE INDEX idx_{timeframe}_timestamp
                    ON {table_name} (timestamp);
                    """

                    cur.execute(create_query)
                    conn.commit()
                    logger.info(f"✅ Таблица {table_name} создана")
                else:
                    logger.info(f"ℹ️ Таблица {table_name} уже существует")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при создании таблицы: {e}")
                conn.rollback()
                return False
            finally:
                cur.close()

    def create_sma_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для SMA периодов

        Args:
            timeframe: Таймфрейм
            periods: Список периодов SMA
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for period in periods:
                    column_name = f'sma_{period}'

                    # Проверяем существование колонки
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                            AND table_name = %s
                            AND column_name = %s
                        );
                    """, (table_name, column_name))

                    exists = cur.fetchone()[0]

                    if not exists:
                        logger.info(f"  📊 Создаю колонку {column_name} в таблице {table_name}")
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {column_name} DECIMAL(20,8);
                        """)

                conn.commit()
                logger.info(f"✅ Колонки SMA созданы для {table_name}")

            except Exception as e:
                logger.error(f"❌ Ошибка при создании колонок: {e}")
                conn.rollback()
            finally:
                cur.close()

    def get_null_timestamps(self, timeframe: str, periods: List[int]) -> Tuple[Optional[datetime], Optional[datetime], int]:
        """
        Находит диапазон timestamps где есть NULL значения для любого SMA периода,
        ИСКЛЮЧАЯ неизбежные NULL в начале данных (где нет достаточной истории для расчёта)

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов SMA

        Returns:
            Tuple (min_timestamp, max_timestamp, count) или (None, None, 0) если NULL нет
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]
        max_period = max(periods)

        # Формируем условие WHERE для проверки NULL в любом SMA столбце
        null_conditions = ' OR '.join([f'sma_{p} IS NULL' for p in periods])

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Сначала находим минимальную дату данных для этого символа
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_data_date = cur.fetchone()[0]

                if min_data_date is None:
                    return None, None, 0

                # Рассчитываем границу "неизбежных NULL" - первые max_period записей
                # где SMA_200 не может быть рассчитан из-за отсутствия истории
                unavoidable_null_boundary = min_data_date + timedelta(minutes=max_period * minutes)

                # Находим диапазон и количество NULL ПОСЛЕ границы неизбежных NULL
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
                    FROM {table_name}
                    WHERE symbol = %s
                      AND ({null_conditions})
                      AND timestamp >= %s
                """, (self.symbol, unavoidable_null_boundary))

                result = cur.fetchone()

                if result and result[0] is not None:
                    return result[0], result[1], result[2]
                else:
                    return None, None, 0

            finally:
                cur.close()

    def get_null_timestamp_list(self, timeframe: str, periods: List[int]) -> List[datetime]:
        """
        Возвращает список конкретных timestamps где есть NULL значения,
        ИСКЛЮЧАЯ неизбежные NULL в начале данных

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов SMA

        Returns:
            List[datetime] - список timestamps с NULL
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]
        max_period = max(periods)

        null_conditions = ' OR '.join([f'sma_{p} IS NULL' for p in periods])

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Находим минимальную дату данных для этого символа
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_data_date = cur.fetchone()[0]

                if min_data_date is None:
                    return []

                # Граница "неизбежных NULL"
                unavoidable_null_boundary = min_data_date + timedelta(minutes=max_period * minutes)

                # Получаем список конкретных timestamps с NULL
                cur.execute(f"""
                    SELECT timestamp
                    FROM {table_name}
                    WHERE symbol = %s
                      AND ({null_conditions})
                      AND timestamp >= %s
                    ORDER BY timestamp
                """, (self.symbol, unavoidable_null_boundary))

                return [row[0] for row in cur.fetchall()]

            finally:
                cur.close()

    def fill_null_values(self, timeframe: str, periods: List[int]) -> int:
        """
        Заполняет NULL значения SMA для указанного таймфрейма

        Алгоритм:
        1. Получить список конкретных timestamps где есть NULL
        2. Загрузить свечи с lookback для расчёта SMA
        3. Рассчитать SMA для всего диапазона
        4. Обновить ТОЛЬКО записи из списка NULL timestamps

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов SMA

        Returns:
            Количество обновлённых записей
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Получаем список конкретных timestamps с NULL
        null_timestamps = self.get_null_timestamp_list(timeframe, periods)

        if not null_timestamps:
            logger.info(f"✅ [{self.symbol}] {timeframe}: Нет NULL значений, пропускаем")
            return 0

        min_null = min(null_timestamps)
        max_null = max(null_timestamps)
        null_count = len(null_timestamps)

        logger.info(f"🔍 [{self.symbol}] {timeframe}: Найдено {null_count:,} записей с NULL")
        logger.info(f"   Диапазон: {min_null} - {max_null}")

        # Определяем lookback для загрузки данных
        max_period = max(periods)
        minutes = self.timeframe_minutes[timeframe]
        lookback = timedelta(minutes=max_period * minutes)

        # Загружаем свечи с lookback
        data_start = min_null - lookback
        df = self.aggregate_candles(data_start, max_null + timedelta(minutes=minutes), timeframe)

        if df.empty:
            logger.warning(f"⚠️ [{self.symbol}] {timeframe}: Нет данных для расчёта")
            return 0

        # Рассчитываем SMA
        for period in periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period, min_periods=period).mean()

        # Фильтруем ТОЛЬКО записи с NULL timestamps (не по диапазону!)
        null_timestamps_set = set(null_timestamps)
        df_to_update = df[df['timestamp'].isin(null_timestamps_set)].copy()

        # Удаляем строки где все SMA = NaN
        sma_columns = [f'sma_{p}' for p in periods]
        df_to_update = df_to_update.dropna(subset=sma_columns, how='all')

        if df_to_update.empty:
            logger.info(f"⚠️ [{self.symbol}] {timeframe}: Нет данных для обновления после расчёта")
            return 0

        # Обновляем только записи с NULL через UPDATE
        updated_count = 0

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Формируем SET clause для всех SMA колонок
                set_clauses = []
                for period in periods:
                    set_clauses.append(f'sma_{period} = %s')
                set_clause = ', '.join(set_clauses)

                # Формируем условие WHERE с проверкой NULL
                null_conditions = ' OR '.join([f'sma_{p} IS NULL' for p in periods])

                update_query = f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE timestamp = %s AND symbol = %s AND ({null_conditions})
                """

                # Подготавливаем данные
                update_data = []
                for _, row in df_to_update.iterrows():
                    values = [float(row[f'sma_{p}']) if pd.notna(row[f'sma_{p}']) else None for p in periods]
                    values.extend([row['timestamp'], self.symbol])
                    update_data.append(tuple(values))

                # Выполняем batch update с прогресс-баром
                progress_desc = f"{self.symbol} {self.symbol_progress} SMA {timeframe.upper()}"
                batch_size = 1000

                with tqdm(total=len(update_data), desc=progress_desc, unit="rec",
                         ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt}') as pbar:
                    for i in range(0, len(update_data), batch_size):
                        batch = update_data[i:i+batch_size]
                        psycopg2.extras.execute_batch(cur, update_query, batch, page_size=100)
                        pbar.update(len(batch))

                conn.commit()
                updated_count = len(update_data)

                logger.info(f"✅ [{self.symbol}] {timeframe}: Обновлено {updated_count:,} записей")

            except Exception as e:
                logger.error(f"❌ Ошибка при обновлении NULL: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

        return updated_count

    def clear_sma_columns(self, timeframe: str, periods: List[int]) -> bool:
        """
        Обнуляет (устанавливает NULL) все SMA столбцы для указанного таймфрейма и символа

        Args:
            timeframe: Таймфрейм для очистки (1m, 15m, 1h)
            periods: Список периодов SMA для очистки

        Returns:
            True если очистка успешна, False в случае ошибки
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Формируем SET clause для всех SMA колонок
                set_clauses = [f'sma_{period} = NULL' for period in periods]
                set_clause = ', '.join(set_clauses)

                # Выполняем UPDATE запрос
                query = f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE symbol = %s
                """

                cur.execute(query, (self.symbol,))
                rows_affected = cur.rowcount

                conn.commit()
                logger.info(f"🗑️  Обнулено {rows_affected:,} записей для SMA столбцов в {table_name} (символ: {self.symbol})")
                logger.info(f"   Очищены столбцы: {', '.join([f'sma_{p}' for p in periods])}")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при очистке SMA столбцов: {e}")
                conn.rollback()
                return False
            finally:
                cur.close()

    def aggregate_candles(self, start_date: datetime, end_date: datetime,
                         timeframe: str) -> pd.DataFrame:
        """
        Агрегирует 1m свечи в нужный таймфрейм

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            timeframe: Целевой таймфрейм

        Returns:
            DataFrame с агрегированными свечами
        """
        minutes = self.timeframe_minutes[timeframe]

        with self.db.get_connection() as conn:
            # Для 1m просто читаем данные
            if timeframe == '1m':
                query = """
                    SELECT timestamp, symbol, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                # Для остальных таймфреймов агрегируем из 1m свечей
                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                # Пример для 1h: timestamp 14:00 содержит данные 14:00:00-14:59:59
                # Пример для 15m: timestamp 14:15 содержит данные 14:15:00-14:29:59
                # Пример для 4h: timestamp 04:00 содержит данные 04:00:00-07:59:59
                # Пример для 1d: timestamp 00:00 содержит данные 00:00:00-23:59:59
                if minutes == 1440:  # 1d timeframe
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                date_trunc('day', timestamp) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start, symbol)
                                period_start as timestamp,
                                symbol,
                                close
                            FROM time_groups
                            ORDER BY period_start, symbol, original_timestamp DESC
                        )
                        SELECT * FROM last_in_period
                        ORDER BY timestamp
                    """
                elif minutes == 240:  # 4h timeframe
                    # Фиксированные интервалы: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                date_trunc('day', timestamp) +
                                INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::INTEGER / 4) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start, symbol)
                                period_start as timestamp,
                                symbol,
                                close
                            FROM time_groups
                            ORDER BY period_start, symbol, original_timestamp DESC
                        )
                        SELECT * FROM last_in_period
                        ORDER BY timestamp
                    """
                elif minutes == 60:  # 1h timeframe
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                date_trunc('hour', timestamp) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start, symbol)
                                period_start as timestamp,
                                symbol,
                                close
                            FROM time_groups
                            ORDER BY period_start, symbol, original_timestamp DESC
                        )
                        SELECT * FROM last_in_period
                        ORDER BY timestamp
                    """
                else:  # 15m and other sub-hourly timeframes
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                date_trunc('hour', timestamp) +
                                INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {minutes}) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start, symbol)
                                period_start as timestamp,
                                symbol,
                                close
                            FROM time_groups
                            ORDER BY period_start, symbol, original_timestamp DESC
                        )
                        SELECT * FROM last_in_period
                        ORDER BY timestamp
                    """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            if df.empty:
                logger.warning(f"⚠️ Нет данных для {self.symbol} в период {start_date} - {end_date}")
                return pd.DataFrame()

            # Убеждаемся, что timestamp - это datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            return df

    def calculate_and_save_sma(self, timeframe: str, periods: List[int],
                               batch_days: int = 30):
        """
        Рассчитывает и сохраняет SMA для указанного таймфрейма

        Args:
            timeframe: Таймфрейм
            periods: Список периодов SMA
            batch_days: Размер батча в днях
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Получаем диапазон дат из исходных данных
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Находим диапазон дат в 1m данных
                cur.execute("""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()
                if not result or result[0] is None:
                    logger.error(f"❌ Нет данных для {self.symbol}")
                    return

                min_date, max_date = result
                logger.info(f"📅 Диапазон данных: {min_date} - {max_date}")

                # Проверяем последнюю загруженную дату для КАЖДОГО периода SMA (оптимизированно)
                logger.info(f"\n📊 Анализ заполнения SMA периодов:")

                # Один запрос для всех SMA периодов
                sma_columns = [f'sma_{p}' for p in periods]
                sma_max_queries = ', '.join([f'MAX({col}) as max_{col}' for col in sma_columns])

                cur.execute(f"""
                    SELECT {sma_max_queries}
                    FROM (
                        SELECT timestamp, {', '.join(sma_columns)}
                        FROM {table_name}
                        WHERE symbol = %s
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    ) t
                """, (self.symbol,))

                result_row = cur.fetchone()

                sma_status = {}
                earliest_date = max_date  # Начинаем с максимальной даты

                # Теперь проверяем каждый период
                for idx, period in enumerate(tqdm(periods, desc="   Проверка периодов SMA", unit="период",
                                                  leave=False, bar_format='{desc}: {n}/{total} [{elapsed}]')):
                    column_name = f'sma_{period}'

                    # Получаем последнюю дату для этого SMA
                    cur.execute(f"""
                        SELECT MAX(timestamp)
                        FROM {table_name}
                        WHERE symbol = %s AND {column_name} IS NOT NULL
                    """, (self.symbol,))

                    last_sma_date = cur.fetchone()[0]

                    if last_sma_date:
                        gap_days = (max_date - last_sma_date).days
                        gap_hours = (max_date - last_sma_date).total_seconds() / 3600

                        if gap_hours < 24:
                            logger.info(f"   • SMA_{period}: актуален (отстает на {gap_hours:.1f} часов)")
                        else:
                            logger.info(f"   • SMA_{period}: последняя {last_sma_date.strftime('%Y-%m-%d %H:%M')} (отстает на {gap_days} дней)")

                        sma_status[period] = last_sma_date

                        # Находим самую раннюю дату из всех SMA
                        if last_sma_date < earliest_date:
                            earliest_date = last_sma_date
                    else:
                        # Этот SMA совсем пустой
                        min_start = min_date + timedelta(minutes=period * self.timeframe_minutes[timeframe])
                        sma_status[period] = None
                        logger.info(f"   • SMA_{period}: ПУСТО (нужна полная загрузка)")

                        if min_start < earliest_date:
                            earliest_date = min_start

                # Используем самую раннюю дату для начала загрузки
                if earliest_date < max_date:
                    start_date = earliest_date
                    logger.info(f"\n📍 Начинаю загрузку с {start_date} (самый отстающий период)")
                else:
                    start_date = max_date
                    logger.info(f"\n✅ Все SMA актуальны!")

                # Подсчитываем общее количество батчей для прогресса
                # Учитываем не только дни, но и часы/минуты для точного подсчета
                total_time_diff = max_date - start_date
                total_days = total_time_diff.days
                total_hours = total_time_diff.total_seconds() / 3600

                # Более точный расчет батчей
                if total_days == 0:
                    # Если меньше дня, все равно будет 1 батч
                    total_batches = 1
                else:
                    # Рассчитываем количество батчей
                    total_batches = (total_days + batch_days - 1) // batch_days  # Округление вверх

                # Показываем статистику загрузки
                logger.info(f"\n📊 План загрузки:")

                if total_hours < 1:
                    logger.info(f"   • Период: менее часа (последние минуты)")
                elif total_hours < 24:
                    logger.info(f"   • Период: {total_hours:.1f} часов ({start_date.strftime('%H:%M')} → {max_date.strftime('%H:%M')})")
                else:
                    logger.info(f"   • Период: {total_days} дней ({start_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')})")

                logger.info(f"   • Батчей: {total_batches} по {batch_days} дней")
                logger.info(f"   • Таймфрейм: {timeframe}")
                logger.info(f"   • SMA периоды: {periods}")

                # Оцениваем количество записей
                if timeframe == '1m':
                    est_records = int(total_hours * 60) if total_days == 0 else total_days * 1440
                else:
                    minutes = self.timeframe_minutes[timeframe]
                    if total_days == 0:
                        est_records = int((total_hours * 60) / minutes)
                    else:
                        est_records = (total_days * 1440) // minutes

                if est_records > 0:
                    logger.info(f"   • Ожидается записей: ~{est_records:,}")
                    if total_batches <= 1:
                        logger.info(f"   • Примерное время: менее минуты")
                    else:
                        logger.info(f"   • Примерное время: ~{(total_batches * 1.5)/60:.1f} минут")
                else:
                    logger.info(f"   • Данные полностью актуальны!")

                # Обработка батчами с улучшенным прогресс-баром
                current_date = start_date
                total_records = 0
                processed_batches = 0

                # Создаем прогресс-бар с общим количеством батчей
                sma_list = ','.join([str(p) for p in periods])
                progress_desc = f"{self.symbol} {self.symbol_progress} SMA[{sma_list}] {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} SMA[{sma_list}] {timeframe.upper()}"
                with tqdm(total=total_batches,
                         desc=progress_desc,
                         unit="batch",
                         ncols=100,
                         bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

                    while current_date <= max_date:
                        batch_end = min(current_date + timedelta(days=batch_days), max_date)

                        # Загружаем данные с запасом для расчета SMA
                        # Для SMA lookback = 1x period оптимален (100% точность)
                        # В отличие от EMA, где нужен 5x для покрытия 99% весов
                        max_period = max(periods)
                        lookback = timedelta(minutes=max_period * self.timeframe_minutes[timeframe])

                        # Для агрегированных таймфреймов вычитаем один период таймфрейма
                        # Чтобы загрузить достаточно 1m свечей для агрегации
                        if timeframe != '1m':
                            minutes_tf = self.timeframe_minutes[timeframe]
                            data_start = current_date - lookback - timedelta(minutes=minutes_tf)
                        else:
                            data_start = current_date - lookback

                        # Агрегируем свечи
                        df = self.aggregate_candles(data_start, batch_end, timeframe)

                        if df.empty:
                            logger.warning(f"⚠️ Нет данных для батча {current_date} - {batch_end}")
                            current_date = batch_end
                            pbar.update(1)
                            continue

                        # Рассчитываем SMA
                        for period in periods:
                            df[f'sma_{period}'] = df['close'].rolling(window=period, min_periods=period).mean()

                        # Фильтруем только нужный диапазон (убираем lookback данные)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # Удаляем строки где все SMA = NaN
                        sma_columns = [f'sma_{p}' for p in periods]
                        df_to_save = df_to_save.dropna(subset=sma_columns, how='all')

                        if not df_to_save.empty:
                            # Сохраняем в БД
                            self.save_to_db(df_to_save, table_name, periods)
                            total_records += len(df_to_save)

                        processed_batches += 1
                        pbar.update(1)

                        # Обновляем детальную информацию
                        if not df_to_save.empty:
                            progress_pct = (processed_batches / total_batches) * 100
                            pbar.set_postfix({
                                'записей': f'{total_records:,}',
                                'последняя': df_to_save['timestamp'].max().strftime('%Y-%m-%d %H:%M'),
                                'прогресс': f'{progress_pct:.1f}%'
                            })

                        current_date = batch_end

                        # Защита от бесконечного цикла для актуальных данных
                        if current_date >= max_date:
                            break

                logger.info(f"✅ Загружено {total_records} записей для {timeframe}")

            except Exception as e:
                logger.error(f"❌ Ошибка при расчете SMA: {e}")
                raise
            finally:
                cur.close()

    def save_to_db(self, df: pd.DataFrame, table_name: str, periods: List[int]):
        """
        Сохраняет данные в БД

        Args:
            df: DataFrame с данными
            table_name: Имя таблицы
            periods: Список периодов SMA
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Подготавливаем данные
                sma_columns = [f'sma_{p}' for p in periods]
                columns = ['timestamp', 'symbol'] + sma_columns

                # Формируем данные для вставки
                records = []
                for _, row in df.iterrows():
                    record = [row['timestamp'], self.symbol]
                    for col in sma_columns:
                        value = row[col] if pd.notna(row[col]) else None
                        record.append(value)
                    records.append(tuple(record))

                # Формируем запрос
                placeholders = ','.join(['%s'] * len(columns))
                update_set = ','.join([f"{col} = EXCLUDED.{col}" for col in sma_columns])

                insert_query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT (timestamp, symbol) DO UPDATE SET
                    {update_set};
                """

                # Выполняем вставку батчами
                psycopg2.extras.execute_batch(cur, insert_query, records, page_size=1000)
                conn.commit()

            except Exception as e:
                logger.error(f"❌ Ошибка при сохранении в БД: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def process_timeframe(self, timeframe: str):
        """
        Обрабатывает один таймфрейм

        Args:
            timeframe: Таймфрейм для обработки
        """
        logger.info(f"\n{'='*50}")
        logger.info(f"⏰ Обработка таймфрейма: {timeframe}")
        logger.info(f"{'='*50}")

        # Получаем периоды из конфига
        sma_config = self.config.get('indicators', {}).get('sma', {})
        if not sma_config.get('enabled', False):
            logger.info(f"⏭️ SMA отключен в конфигурации")
            return

        periods = sma_config.get('periods', [])
        if not periods:
            logger.warning(f"⚠️ Не указаны периоды SMA")
            return

        logger.info(f"📊 Периоды SMA: {periods}")

        # Создаем таблицу если нужно
        if not self.create_indicators_table(timeframe):
            logger.error(f"❌ Не удалось создать таблицу для {timeframe}")
            return

        # Создаем колонки для SMA
        self.create_sma_columns(timeframe, periods)

        # Обнуляем существующие данные если включен флаг force-reload
        if self.force_reload:
            logger.info(f"\n🔄 Включен режим force-reload - обнуление существующих SMA данных")
            if not self.clear_sma_columns(timeframe, periods):
                logger.error(f"❌ Не удалось обнулить SMA столбцы для {timeframe}")
                return

        # Рассчитываем и сохраняем SMA
        self.calculate_and_save_sma(timeframe, periods)

    def run(self, timeframes: Optional[List[str]] = None):
        """
        Запускает обработку для всех таймфреймов

        Args:
            timeframes: Список таймфреймов или None для использования из конфига
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"🚀 Запуск обработки для таймфреймов: {timeframes}")
        logger.info(f"📈 Символ: {self.symbol}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe)

        logger.info(f"\n✅ Обработка завершена для всех таймфреймов")


def main():
    """Основная функция с поддержкой обратной совместимости"""
    parser = argparse.ArgumentParser(description='Universal SMA Loader')
    parser.add_argument('--symbol', type=str, default=None,
                      help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                      help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframes', type=str, default=None,
                      help='Таймфреймы через запятую (1m,15m,1h) или пусто для всех из config.yaml')
    parser.add_argument('--timeframe', type=str, default=None,
                      help='Один таймфрейм (для обратной совместимости)')
    parser.add_argument('--batch-days', type=int, default=30,
                      help='Размер батча в днях (по умолчанию 30)')
    parser.add_argument('--force-reload', action='store_true',
                      help='Обнулить все SMA столбцы перед загрузкой (принудительный полный пересчет)')
    parser.add_argument('--check-nulls', action='store_true',
                      help='Проверить и заполнить NULL значения в середине данных')

    args = parser.parse_args()

    # Определяем символы для обработки
    if args.symbols:
        # Если указаны конкретные символы через аргумент --symbols
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        # Если указан один символ через аргумент --symbol
        symbols = [args.symbol]
    else:
        # Читаем символы из config.yaml
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    # Обратная совместимость: если указан --timeframe, используем его
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = [tf.strip() for tf in args.timeframes.split(',')]
    else:
        timeframes = None  # Будут использованы из config.yaml

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        try:
            # Создаем и запускаем загрузчик для текущего символа
            loader = SMALoader(symbol=symbol)
            # Устанавливаем информацию о прогрессе символов
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            # Передаем флаг принудительного пересчета
            loader.force_reload = args.force_reload

            if args.check_nulls:
                # Режим проверки и заполнения NULL значений
                sma_config = loader.config.get('indicators', {}).get('sma', {})
                periods = sma_config.get('periods', [10, 30, 50, 100, 200])

                # Определяем таймфреймы для проверки
                tfs_to_check = timeframes if timeframes else loader.config.get('timeframes', ['1m', '15m', '1h'])

                logger.info(f"🔍 Режим --check-nulls: проверка NULL значений")
                for tf in tfs_to_check:
                    loader.fill_null_values(tf, periods)
            else:
                # Обычный режим: загрузка новых данных
                if timeframes and len(timeframes) == 1:
                    loader.process_timeframe(timeframes[0])
                else:
                    loader.run(timeframes=timeframes)

            logger.info(f"\n✅ Символ {symbol} обработан\n")
        except KeyboardInterrupt:
            logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Вычисляем общее время обработки
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()