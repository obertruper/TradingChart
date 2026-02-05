#!/usr/bin/env python3
"""
EMA Loader - основной загрузчик EMA индикаторов
С батчевой обработкой и системой checkpoint для продолжения загрузки

Использование:
    python ema_loader.py                     # Загрузка всех таймфреймов из config.yaml
    python ema_loader.py --timeframe 1m      # Загрузка конкретного таймфрейма
    python ema_loader.py --batch-days 7      # Размер батча 7 дней
    python ema_loader.py --start-date 2024-01-01  # Начать с конкретной даты
"""

import os
import sys
import re
import logging
import argparse
import yaml
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Tuple
from tqdm import tqdm
import time

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'ema_{timestamp}.log')

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

    logger.info(f"📝 EMA Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger

logger = setup_logging()

class EMALoader:
    """
    Загрузчик EMA индикаторов с батчевой обработкой и checkpoint системой
    """

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
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """
        Парсит таймфреймы из конфигурации
        Поддерживает форматы: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w

        Returns:
            dict: Мапинг таймфрейма на количество минут
        """
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        for tf in timeframes:
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)

                if unit == 'm':
                    minutes = number
                elif unit == 'h':
                    minutes = number * 60
                elif unit == 'd':
                    minutes = number * 1440
                elif unit == 'w':
                    minutes = number * 10080
                else:
                    continue

                timeframe_map[tf] = minutes

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

    def create_ema_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для EMA периодов в существующей таблице

        Args:
            timeframe: Таймфрейм
            periods: Список периодов EMA
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем существование таблицы
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table_name,))

            if not cur.fetchone()[0]:
                logger.error(f"❌ Таблица {table_name} не существует")
                return False

            # Проверяем существующие колонки
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name LIKE 'ema_%%'
            """, (table_name,))

            existing = {row[0] for row in cur.fetchall()}

            # Создаем недостающие колонки
            created = []
            for period in periods:
                col_name = f'ema_{period}'
                if col_name not in existing:
                    try:
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(20,8)
                        """)
                        created.append(col_name)
                        logger.info(f"  📊 Создана колонка {col_name} в таблице {table_name}")
                    except Exception as e:
                        logger.error(f"  ❌ Ошибка создания колонки {col_name}: {e}")

            conn.commit()

            if created:
                logger.info(f"✅ Созданы колонки EMA для {table_name}: {created}")
            else:
                logger.info(f"ℹ️ Все колонки EMA уже существуют в {table_name}")

            return True

    def clear_ema_columns(self, timeframe: str, periods: List[int]) -> bool:
        """
        Обнуляет (устанавливает NULL) все EMA столбцы для указанного таймфрейма и символа

        Args:
            timeframe: Таймфрейм для очистки (1m, 15m, 1h)
            periods: Список периодов EMA для очистки

        Returns:
            True если очистка успешна, False в случае ошибки
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Формируем SET clause для всех EMA колонок
                set_clauses = [f'ema_{period} = NULL' for period in periods]
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
                logger.info(f"🗑️  Обнулено {rows_affected:,} записей для EMA столбцов в {table_name} (символ: {self.symbol})")
                logger.info(f"   Очищены столбцы: {', '.join([f'ema_{p}' for p in periods])}")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при очистке EMA столбцов: {e}")
                conn.rollback()
                return False
            finally:
                cur.close()

    def get_min_date_for_symbol(self, symbol: str) -> datetime:
        """
        Получает минимальную дату доступных данных для символа

        Args:
            symbol: Торговая пара

        Returns:
            Минимальная дата или текущая дата если данных нет
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT MIN(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (symbol,))
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            return datetime.now(timezone.utc)

    def get_last_ema_checkpoint(self, timeframe: str, period: int) -> Tuple[Optional[datetime], Optional[float]]:
        """
        Получает последнее сохраненное значение EMA для продолжения

        Args:
            timeframe: Таймфрейм
            period: Период EMA

        Returns:
            (timestamp, ema_value) или (None, None)
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        column_name = f'ema_{period}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем последнее значение
            try:
                cur.execute(f"""
                    SELECT timestamp, {column_name}
                    FROM {table_name}
                    WHERE symbol = %s
                    AND {column_name} IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.symbol,))

                result = cur.fetchone()
                if result:
                    return result[0], float(result[1])
            except Exception as e:
                logger.debug(f"Не удалось получить checkpoint для {column_name}: {e}")

            return None, None

    def get_null_timestamp_list(self, timeframe: str, periods: List[int]) -> List[datetime]:
        """
        Возвращает список конкретных timestamps где есть NULL значения EMA,
        ИСКЛЮЧАЯ неизбежные NULL в начале данных (где нет достаточной истории для расчёта)

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов EMA

        Returns:
            List[datetime] - список timestamps с NULL
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]
        max_period = max(periods)

        null_conditions = ' OR '.join([f'ema_{p} IS NULL' for p in periods])

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

                # Граница "неизбежных NULL" - первые max_period записей
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
        Заполняет NULL значения EMA для указанного таймфрейма.

        ВАЖНО: Использует ПОЛНЫЙ пересчёт с начала данных для 100% точности,
        так как EMA - кумулятивный индикатор, где каждое значение зависит от предыдущего.

        Алгоритм:
        1. Получить список конкретных timestamps где есть NULL
        2. Загрузить ВСЕ свечи с начала данных
        3. Рассчитать EMA с нуля для всего диапазона
        4. Записать ТОЛЬКО записи из списка NULL timestamps

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов EMA

        Returns:
            Количество обновлённых записей
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]

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
        logger.info(f"   ⚠️ Полный пересчёт EMA с начала данных (100% точность)")

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Получаем минимальную дату данных
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_data_date = cur.fetchone()[0]

                if min_data_date is None:
                    logger.warning(f"⚠️ [{self.symbol}] {timeframe}: Нет данных в таблице")
                    return 0

                logger.info(f"   📅 Загрузка данных с {min_data_date}")

                # Загружаем ВСЕ свечи с начала данных
                if timeframe == '1m':
                    query = """
                        SELECT timestamp, close
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                        AND timestamp >= %s
                        AND timestamp <= %s
                        ORDER BY timestamp
                    """
                    cur.execute(query, (self.symbol, min_data_date, max_null))
                else:
                    # Для агрегированных таймфреймов
                    if minutes == 1440:  # 1d timeframe
                        query = f"""
                            WITH candle_data AS (
                                SELECT
                                    date_trunc('day', timestamp) as period_start,
                                    close,
                                    timestamp as original_timestamp
                                FROM candles_bybit_futures_1m
                                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                            ),
                            last_in_period AS (
                                SELECT DISTINCT ON (period_start)
                                    period_start as timestamp,
                                    close as price
                                FROM candle_data
                                ORDER BY period_start, original_timestamp DESC
                            )
                            SELECT timestamp, price
                            FROM last_in_period
                            ORDER BY timestamp
                        """
                    elif minutes == 240:  # 4h timeframe (fixed intervals: 00, 04, 08, 12, 16, 20 UTC)
                        query = f"""
                            WITH candle_data AS (
                                SELECT
                                    date_trunc('day', timestamp) +
                                    INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::INTEGER / 4) as period_start,
                                    close,
                                    timestamp as original_timestamp
                                FROM candles_bybit_futures_1m
                                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                            ),
                            last_in_period AS (
                                SELECT DISTINCT ON (period_start)
                                    period_start as timestamp,
                                    close as price
                                FROM candle_data
                                ORDER BY period_start, original_timestamp DESC
                            )
                            SELECT timestamp, price
                            FROM last_in_period
                            ORDER BY timestamp
                        """
                    elif minutes == 60:  # 1h
                        query = f"""
                            WITH candle_data AS (
                                SELECT
                                    date_trunc('hour', timestamp) as period_start,
                                    close,
                                    timestamp as original_timestamp
                                FROM candles_bybit_futures_1m
                                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                            ),
                            last_in_period AS (
                                SELECT DISTINCT ON (period_start)
                                    period_start as timestamp,
                                    close as price
                                FROM candle_data
                                ORDER BY period_start, original_timestamp DESC
                            )
                            SELECT timestamp, price
                            FROM last_in_period
                            ORDER BY timestamp
                        """
                    else:  # 15m and other sub-hourly
                        query = f"""
                            WITH candle_data AS (
                                SELECT
                                    date_trunc('hour', timestamp) +
                                    INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {minutes}) as period_start,
                                    close,
                                    timestamp as original_timestamp
                                FROM candles_bybit_futures_1m
                                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                            ),
                            last_in_period AS (
                                SELECT DISTINCT ON (period_start)
                                    period_start as timestamp,
                                    close as price
                                FROM candle_data
                                ORDER BY period_start, original_timestamp DESC
                            )
                            SELECT timestamp, price
                            FROM last_in_period
                            ORDER BY timestamp
                        """
                    cur.execute(query, (self.symbol, min_data_date, max_null))

                rows = cur.fetchall()

                if not rows:
                    logger.warning(f"⚠️ [{self.symbol}] {timeframe}: Нет данных для расчёта")
                    return 0

                logger.info(f"   📊 Загружено {len(rows):,} свечей для расчёта")

                # Создаем DataFrame
                df = pd.DataFrame(rows, columns=['timestamp', 'price'])
                df['price'] = df['price'].astype(float)

                # Рассчитываем EMA с нуля
                for period in periods:
                    col_name = f'ema_{period}'
                    df[col_name] = df['price'].ewm(span=period, adjust=False, min_periods=period).mean()

                # Фильтруем ТОЛЬКО записи с NULL timestamps
                null_timestamps_set = set(null_timestamps)
                df_to_update = df[df['timestamp'].isin(null_timestamps_set)].copy()

                # Удаляем строки где все EMA = NaN
                ema_columns = [f'ema_{p}' for p in periods]
                df_to_update = df_to_update.dropna(subset=ema_columns, how='all')

                if df_to_update.empty:
                    logger.info(f"⚠️ [{self.symbol}] {timeframe}: Нет данных для обновления после расчёта")
                    return 0

                # Обновляем записи в БД
                set_clauses = ', '.join([f'ema_{p} = %s' for p in periods])
                update_query = f"""
                    UPDATE {table_name}
                    SET {set_clauses}
                    WHERE timestamp = %s AND symbol = %s
                """

                update_data = []
                for _, row in df_to_update.iterrows():
                    values = [float(row[f'ema_{p}']) if pd.notna(row[f'ema_{p}']) else None for p in periods]
                    values.extend([row['timestamp'], self.symbol])
                    update_data.append(tuple(values))

                # Batch update с прогресс-баром
                progress_desc = f"{self.symbol} {self.symbol_progress} EMA {timeframe.upper()}"
                batch_size = 1000

                with tqdm(total=len(update_data), desc=progress_desc, unit="rec",
                         ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt}') as pbar:
                    for i in range(0, len(update_data), batch_size):
                        batch = update_data[i:i+batch_size]
                        psycopg2.extras.execute_batch(cur, update_query, batch, page_size=100)
                        pbar.update(len(batch))

                conn.commit()
                logger.info(f"✅ [{self.symbol}] {timeframe}: Обновлено {len(update_data):,} записей")
                return len(update_data)

            finally:
                cur.close()

    def calculate_ema_batch(self, df: pd.DataFrame, periods: List[int],
                           initial_emas: Dict[int, float]) -> pd.DataFrame:
        """
        Рассчитывает EMA для батча данных

        Args:
            df: DataFrame с ценами
            periods: Список периодов EMA
            initial_emas: Начальные значения EMA для каждого периода

        Returns:
            DataFrame с рассчитанными EMA
        """
        for period in periods:
            column_name = f'ema_{period}'
            alpha = 2.0 / (period + 1)

            if period in initial_emas and initial_emas[period] is not None:
                # Есть начальное значение - используем рекурсивную формулу
                ema_values = []
                prev_ema = initial_emas[period]

                for price in df['price']:
                    if pd.notna(price):
                        new_ema = float(price) * alpha + prev_ema * (1 - alpha)
                        ema_values.append(new_ema)
                        prev_ema = new_ema
                    else:
                        ema_values.append(None)

                df[column_name] = ema_values
            else:
                # Нет начального значения - используем pandas.ewm
                df[column_name] = df['price'].ewm(span=period, adjust=False, min_periods=period).mean()

        return df

    def process_batch(self, timeframe: str, periods: List[int],
                     overlap_start: datetime, batch_start: datetime,
                     batch_end: datetime) -> None:
        """
        Обрабатывает один батч данных используя FULL RECALCULATION подход с lookback

        ВАЖНО: Для корректного расчета EMA используется full recalculation на истории
        с достаточным lookback периодом, а не checkpoint-based incremental calculation.

        Почему full recalculation:
        - EMA — экспоненциальная скользящая средняя, учитывающая ВСЮ историю
        - Checkpoint-based подход для агрегированных таймфреймов (15m, 1h)
          математически некорректен и дает 100% ошибок
        - Full recalculation гарантирует точность через pandas.ewm()

        Алгоритм:
        1. Загружаем данные с lookback (overlap_start до batch_end)
        2. Рассчитываем EMA на ВСЕЙ загруженной истории
        3. Фильтруем и сохраняем только новые данные (batch_start до batch_end)

        Args:
            timeframe: Таймфрейм
            periods: Периоды EMA
            overlap_start: Начало данных включая lookback для warm-up
            batch_start: Начало новых данных для сохранения
            batch_end: Конец батча

        Returns:
            None (данные сохраняются в БД напрямую)
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Для агрегированных таймфреймов нужно загружать 1m свечи РАНЬШЕ overlap_start
            # Пример: для 1h свечи в 14:00 нужны 1m свечи от 13:00 до 13:59
            # Это критично для корректной агрегации!
            if timeframe != '1m':
                minutes = self.timeframe_minutes[timeframe]
                # Вычитаем один период таймфрейма для загрузки достаточных 1m свечей
                adjusted_overlap_start = overlap_start - timedelta(minutes=minutes)
            else:
                adjusted_overlap_start = overlap_start

            # Загружаем данные
            if timeframe == '1m':
                # Для 1m берем close из свечей
                query = """
                    SELECT timestamp, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                    AND timestamp >= %s
                    AND timestamp <= %s
                    ORDER BY timestamp
                """
                cur.execute(query, (self.symbol, adjusted_overlap_start, batch_end))
            else:
                # Для других таймфреймов - агрегация из 1m свечей
                # Используем adjusted_overlap_start для загрузки достаточных 1m свечей
                #
                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                # Пример для 1h: timestamp 14:00 содержит данные 14:00:00-14:59:59
                # Пример для 15m: timestamp 14:15 содержит данные 14:15:00-14:29:59
                # Пример для 4h: timestamp 04:00 содержит данные 04:00:00-07:59:59
                # Пример для 1d: timestamp 00:00 содержит данные 00:00:00-23:59:59
                if minutes == 1440:  # 1d timeframe
                    query = f"""
                        WITH candle_data AS (
                            SELECT
                                date_trunc('day', timestamp) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start)
                                period_start as timestamp,
                                close as price
                            FROM candle_data
                            ORDER BY period_start, original_timestamp DESC
                        )
                        SELECT timestamp, price
                        FROM last_in_period
                        ORDER BY timestamp
                    """
                elif minutes == 240:  # 4h timeframe (fixed intervals: 00, 04, 08, 12, 16, 20 UTC)
                    query = f"""
                        WITH candle_data AS (
                            SELECT
                                date_trunc('day', timestamp) +
                                INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::INTEGER / 4) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start)
                                period_start as timestamp,
                                close as price
                            FROM candle_data
                            ORDER BY period_start, original_timestamp DESC
                        )
                        SELECT timestamp, price
                        FROM last_in_period
                        ORDER BY timestamp
                    """
                elif minutes == 60:  # 1h timeframe
                    query = f"""
                        WITH candle_data AS (
                            SELECT
                                date_trunc('hour', timestamp) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start)
                                period_start as timestamp,
                                close as price
                            FROM candle_data
                            ORDER BY period_start, original_timestamp DESC
                        )
                        SELECT timestamp, price
                        FROM last_in_period
                        ORDER BY timestamp
                    """
                else:  # 15m and other sub-hourly timeframes
                    query = f"""
                        WITH candle_data AS (
                            SELECT
                                date_trunc('hour', timestamp) +
                                INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {minutes}) as period_start,
                                close,
                                symbol,
                                timestamp as original_timestamp
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                        ),
                        last_in_period AS (
                            SELECT DISTINCT ON (period_start)
                                period_start as timestamp,
                                close as price
                            FROM candle_data
                            ORDER BY period_start, original_timestamp DESC
                        )
                        SELECT timestamp, price
                        FROM last_in_period
                        ORDER BY timestamp
                    """
                cur.execute(query, (self.symbol, adjusted_overlap_start, batch_end))

            rows = cur.fetchall()

            if not rows:
                logger.warning(f"Нет данных для батча {overlap_start} - {batch_end}")
                return

            # Создаем DataFrame со ВСЕМИ данными (включая overlap)
            df = pd.DataFrame(rows, columns=['timestamp', 'price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['price'] = df['price'].astype(float)
            df.set_index('timestamp', inplace=True)

            # Рассчитываем EMA для ВСЕГО диапазона используя pandas.ewm
            for period in periods:
                col_name = f'ema_{period}'
                df[col_name] = df['price'].ewm(span=period, adjust=False, min_periods=period).mean()

            # Фильтруем только НОВЫЕ данные для сохранения (исключаем overlap)
            df_to_save = df[df.index >= batch_start].copy()
            df_to_save.reset_index(inplace=True)

            if df_to_save.empty:
                logger.warning(f"Нет новых данных для сохранения в батче {batch_start} - {batch_end}")
                return

            # Сохраняем в БД
            table_name = f'indicators_bybit_futures_{timeframe}'

            # Подготавливаем данные для batch update (только новые данные)
            updates = []
            for _, row in df_to_save.iterrows():
                update_values = {'timestamp': row['timestamp'], 'symbol': self.symbol}
                for period in periods:
                    col_name = f'ema_{period}'
                    if col_name in df_to_save.columns and pd.notna(row[col_name]):
                        update_values[col_name] = float(row[col_name])

                if len(update_values) > 2:  # Есть хотя бы одно значение EMA
                    updates.append(update_values)

            # Выполняем batch update
            if updates:
                for update in updates:
                    # Формируем динамический UPDATE запрос
                    ema_columns = [k for k in update.keys() if k.startswith('ema_')]
                    if ema_columns:
                        set_clause = ', '.join([f"{col} = %s" for col in ema_columns])
                        values = [update[col] for col in ema_columns]
                        values.extend([update['timestamp'], update['symbol']])

                        update_query = f"""
                            UPDATE {table_name}
                            SET {set_clause}
                            WHERE timestamp = %s AND symbol = %s
                        """

                        try:
                            cur.execute(update_query, values)
                        except Exception as e:
                            logger.error(f"Ошибка обновления: {e}")

            conn.commit()
            logger.debug(f"✅ Сохранено {len(df_to_save)} записей для батча {batch_start} - {batch_end}")

    def calculate_and_save_ema(self, timeframe: str, periods: List[int],
                               batch_days: int = 7,
                               start_date: Optional[datetime] = None):
        """
        Рассчитывает и сохраняет EMA для указанного таймфрейма с батчевой обработкой

        Args:
            timeframe: Таймфрейм
            periods: Список периодов EMA
            batch_days: Размер батча в днях
            start_date: Начальная дата (если None, продолжаем с последней или начинаем сначала)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Обработка EMA для таймфрейма {timeframe}")
        logger.info(f"💹 Периоды EMA: {periods}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        logger.info(f"{'='*60}")

        # Создаем колонки если нужно
        if not self.create_ema_columns(timeframe, periods):
            return

        # Обнуляем существующие данные если включен флаг force-reload
        if self.force_reload:
            logger.info(f"\n🔄 Включен режим force-reload - обнуление существующих EMA данных")
            if not self.clear_ema_columns(timeframe, periods):
                logger.error(f"❌ Не удалось обнулить EMA столбцы для {timeframe}")
                return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем checkpoint для каждого периода
            logger.info("\n🔍 Проверка существующих данных (checkpoint):")
            checkpoints = {}
            latest_checkpoint = None

            for period in periods:
                last_timestamp, last_ema = self.get_last_ema_checkpoint(timeframe, period)
                checkpoints[period] = {
                    'last_timestamp': last_timestamp,
                    'last_ema': last_ema
                }

                if last_timestamp:
                    logger.info(f"  ✅ EMA_{period}: продолжение с {last_timestamp} (последнее значение: {last_ema:.2f})")
                    if not latest_checkpoint or last_timestamp > latest_checkpoint:
                        latest_checkpoint = last_timestamp
                else:
                    logger.info(f"  📝 EMA_{period}: нет данных (будет рассчитан с начала)")

            # Определяем начальную точку
            if start_date:
                current_date = start_date
                logger.info(f"\n📅 Используем указанную дату начала: {current_date}")
            elif latest_checkpoint:
                current_date = latest_checkpoint
                logger.info(f"\n♻️ Продолжаем с checkpoint: {current_date}")
            else:
                # Начинаем с самого начала
                cur.execute("""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    current_date = result[0]
                    logger.info(f"\n🚀 Начинаем с самого начала: {current_date}")
                else:
                    logger.error("❌ Нет данных для обработки")
                    return

            # Определяем размер lookback для точного расчета EMA
            # Lookback multiplier = 5 покрывает ~99% весов EMA для идеальной точности
            # (2x = 86%, 3x = 95%, 4x = 98%, 5x = 99% весов экспоненциальной функции)
            # Это обеспечивает расхождение < 0.01 пункта при валидации
            lookback_multiplier = 5
            overlap_periods = max(periods) * lookback_multiplier if periods else 1000
            overlap_minutes = overlap_periods * self.timeframe_minutes[timeframe]

            # Получаем конечную дату
            cur.execute("""
                SELECT MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            max_date = cur.fetchone()[0]

            if not max_date or current_date >= max_date:
                logger.info("✅ Данные уже актуальны")
                return

            # Рассчитываем количество батчей
            total_days = (max_date - current_date).days
            if total_days <= 0:
                total_days = 1
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"\n📊 План загрузки:")
            logger.info(f"   • Период: {total_days} дней ({current_date.strftime('%Y-%m-%d %H:%M')} → {max_date.strftime('%Y-%m-%d %H:%M')})")
            logger.info(f"   • Батчей: {total_batches}")
            logger.info(f"   • Таймфрейм: {timeframe}")
            logger.info(f"   • EMA периоды: {periods}")

            # Обработка батчами с прогресс-баром
            logger.info(f"\n🚀 Начинаю обработку...")

            periods_str = ','.join(map(str, periods))
            progress_desc = f"{self.symbol} {self.symbol_progress} EMA[{periods_str}] {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} EMA[{periods_str}] {timeframe.upper()}"
            with tqdm(total=total_batches,
                     desc=progress_desc,
                     unit='batch',
                     ncols=100,
                     bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

                batch_count = 0
                total_records = 0

                while current_date < max_date:
                    batch_end = min(current_date + timedelta(days=batch_days), max_date)

                    # Определяем overlap_start для ВСЕХ батчей (включая первый)
                    # Используем lookback для правильного warm-up периода EMA
                    # Если недостаточно исторических данных, берем MIN(timestamp)
                    min_available_date = self.get_min_date_for_symbol(self.symbol)
                    overlap_start = max(current_date - timedelta(minutes=overlap_minutes),
                                      min_available_date)

                    # Обрабатываем батч с overlap
                    try:
                        self.process_batch(
                            timeframe, periods,
                            overlap_start, current_date, batch_end
                        )

                        # Считаем записи в батче
                        if timeframe == '1m':
                            batch_records = batch_days * 1440  # Примерная оценка
                        else:
                            batch_records = batch_days * (1440 // self.timeframe_minutes[timeframe])

                        total_records += batch_records

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча: {e}")
                        logger.info(f"   Батч: {current_date} - {batch_end}")
                        # Продолжаем со следующего батча
                        pass

                    batch_count += 1
                    pbar.update(1)
                    pbar.set_postfix({
                        'текущая_дата': batch_end.strftime('%Y-%m-%d'),
                        'записей': f'~{total_records:,}'
                    })

                    current_date = batch_end  # Без разрыва между батчами

                    # Checkpoint каждые 10 батчей
                    if batch_count % 10 == 0:
                        logger.debug(f"💾 Checkpoint: обработано {batch_count} батчей, последняя дата: {batch_end}")

            logger.info(f"\n✅ Загрузка EMA для {timeframe} завершена!")
            logger.info(f"   • Обработано батчей: {batch_count}")
            logger.info(f"   • Примерно записей: {total_records:,}")

    def process_timeframe(self, timeframe: str, batch_days: int = 7,
                         start_date: Optional[datetime] = None):
        """
        Обрабатывает один таймфрейм

        Args:
            timeframe: Таймфрейм для обработки
            batch_days: Размер батча в днях
            start_date: Начальная дата (опционально)
        """
        # Получаем периоды из конфига
        ema_config = self.config.get('indicators', {}).get('ema', {})
        if not ema_config.get('enabled', False):
            logger.info(f"⏭️ EMA отключен в конфигурации")
            return

        periods = ema_config.get('periods', [])
        if not periods:
            logger.warning(f"⚠️ Не указаны периоды EMA")
            return

        # Рассчитываем и сохраняем EMA
        self.calculate_and_save_ema(timeframe, periods, batch_days, start_date)

    def run(self, timeframes: Optional[List[str]] = None,
            batch_days: int = 7,
            start_date: Optional[datetime] = None):
        """
        Запускает обработку для всех таймфреймов

        Args:
            timeframes: Список таймфреймов или None для использования из конфига
            batch_days: Размер батча в днях
            start_date: Начальная дата (опционально)
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Запуск EMA Loader")
        logger.info(f"📊 Таймфреймы: {timeframes}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        if start_date:
            logger.info(f"📅 Начальная дата: {start_date}")
        else:
            logger.info(f"♻️ Режим: продолжение с последнего checkpoint")
        logger.info(f"{'='*60}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe, batch_days, start_date)

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Обработка всех таймфреймов завершена!")
        logger.info(f"{'='*60}")

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='EMA Indicator Loader - основной загрузчик EMA индикаторов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python ema_loader.py                          # Загрузка всех таймфреймов из indicators_config.yaml
  python ema_loader.py --timeframe 1m           # Загрузка только 1m таймфрейма
  python ema_loader.py --timeframes 1m,15m,1h   # Загрузка нескольких таймфреймов
  python ema_loader.py --batch-days 3           # Использовать батчи по 3 дня
  python ema_loader.py --start-date 2024-01-01  # Начать с конкретной даты
  python ema_loader.py --symbol ETHUSDT         # Загрузка для другого символа
        """
    )

    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str,
                       help='Один таймфрейм для обработки')
    parser.add_argument('--timeframes', type=str,
                       help='Несколько таймфреймов через запятую (например: 1m,15m,1h)')
    parser.add_argument('--batch-days', type=int, default=1,
                       help='Размер батча в днях (по умолчанию: 1)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD (если не указана, продолжает с checkpoint)')
    parser.add_argument('--force-reload', action='store_true',
                       help='Обнулить все EMA столбцы перед загрузкой (принудительный полный пересчет)')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Проверить и заполнить NULL значения в середине данных (полный пересчёт EMA с начала)')

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

    # Определяем таймфреймы
    timeframes = None
    if args.timeframes:
        timeframes = args.timeframes.split(',')
    elif args.timeframe:
        timeframes = [args.timeframe]

    # Парсим дату если указана
    start_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"❌ Неверный формат даты: {args.start_date}. Используйте YYYY-MM-DD")
            sys.exit(1)

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Режим --check-nulls
    if args.check_nulls:
        logger.info(f"🔍 Режим --check-nulls: проверка NULL значений (полный пересчёт EMA)")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        # Создаем загрузчик и запускаем для текущего символа
        try:
            loader = EMALoader(symbol=symbol)
            loader.force_reload = args.force_reload
            loader.symbol_progress = f"[{idx}/{total_symbols}]"

            if args.check_nulls:
                # Режим проверки и заполнения NULL
                # Получаем периоды из конфига
                ema_config = loader.config.get('indicators', {}).get('ema', {})
                periods = ema_config.get('periods', [9, 12, 21, 26, 50, 100, 200])

                # Определяем таймфреймы для проверки
                check_timeframes = timeframes if timeframes else loader.config.get('timeframes', ['1m', '15m', '1h'])

                for tf in check_timeframes:
                    if tf in loader.timeframe_minutes:
                        loader.fill_null_values(tf, periods)
            else:
                # Обычный режим загрузки
                loader.run(timeframes, args.batch_days, start_date)

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