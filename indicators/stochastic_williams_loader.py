#!/usr/bin/env python3
"""
Stochastic Oscillator & Williams %R Loader with Multi-Timeframe Support
=======================================================================
Загрузчик Stochastic и Williams %R индикаторов с поддержкой:
- Множественных конфигураций Stochastic (scalping, intraday, fast, classic, swing, fibonacci_swing, position, fibonacci_long)
- Множественных периодов Williams %R (6, 10, 14, 20, 30)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h)
- Инкрементальных обновлений
- Multi-symbol support

Stochastic Oscillator:
- %K = (Close - Low_N) / (High_N - Low_N) × 100
- %K_smooth = SMA(%K, k_smooth)
- %D = SMA(%K_smooth, d_period)

Williams %R:
- %R = -((High_N - Close) / (High_N - Low_N)) × 100
- Диапазон: от -100 до 0
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional
import logging
from tqdm import tqdm
import sys
import os
import warnings
import yaml
import argparse
import time

# Игнорируем предупреждения pandas
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование в консоль и файл"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'stochastic_williams_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

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


class StochasticLoader:
    """Загрузчик Stochastic Oscillator для разных таймфреймов"""

    def __init__(self, symbol: str = 'BTCUSDT', force_reload: bool = False, check_nulls: bool = False):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
            force_reload: Принудительный пересчет всех данных с начала истории
            check_nulls: Проверить и заполнить NULL значения в середине данных
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.force_reload = force_reload
        self.check_nulls = check_nulls
        self.symbol_progress = ""  # Для отображения прогресса при multi-symbol
        self.config = self.load_config()

        # Динамический мапинг таймфреймов на минуты
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

            if not match:
                logger.warning(f"Неизвестный таймфрейм: {tf}, пропускаем")
                continue

            value = int(match.group(1))
            unit = match.group(2)

            # Конвертируем в минуты
            if unit == 'm':
                minutes = value
            elif unit == 'h':
                minutes = value * 60
            elif unit == 'd':
                minutes = value * 60 * 24
            elif unit == 'w':
                minutes = value * 60 * 24 * 7
            else:
                logger.warning(f"Неподдерживаемая единица времени: {unit}")
                continue

            timeframe_map[tf] = minutes

        logger.info(f"Распарсенные таймфреймы: {timeframe_map}")
        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из YAML файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config

    def get_table_name(self, timeframe: str) -> str:
        """Получить имя таблицы индикаторов для таймфрейма"""
        return f"indicators_bybit_futures_{timeframe}"

    def calculate_stochastic(self, df: pd.DataFrame, k_period: int,
                            k_smooth: int, d_period: int) -> Tuple[pd.Series, pd.Series]:
        """
        Рассчитывает Stochastic Oscillator (%K и %D)

        Формула:
        1. %K_raw = (Close - Low_N) / (High_N - Low_N) × 100
        2. %K = SMA(%K_raw, k_smooth) если k_smooth > 1, иначе %K = %K_raw
        3. %D = SMA(%K, d_period)

        Args:
            df: DataFrame с колонками high, low, close
            k_period: Период для расчёта High/Low диапазона
            k_smooth: Период сглаживания %K (1 = без сглаживания)
            d_period: Период сглаживания %D (сигнальная линия)

        Returns:
            Tuple[k_values, d_values]: %K и %D значения
        """
        # Рассчитываем rolling high и low
        rolling_high = df['high'].rolling(window=k_period).max()
        rolling_low = df['low'].rolling(window=k_period).min()

        # %K raw (fast stochastic)
        # Избегаем деления на ноль
        denominator = rolling_high - rolling_low
        k_raw = np.where(
            denominator != 0,
            ((df['close'] - rolling_low) / denominator) * 100,
            np.nan
        )
        k_raw = pd.Series(k_raw, index=df.index)

        # %K smoothed (slow stochastic)
        if k_smooth > 1:
            k = k_raw.rolling(window=k_smooth).mean()
        else:
            k = k_raw

        # %D (signal line)
        d = k.rolling(window=d_period).mean()

        return k, d

    def ensure_stochastic_columns(self, timeframe: str, configs: List[Dict]):
        """
        Создает колонки для Stochastic конфигураций если их нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            configs: Список конфигураций Stochastic
        """
        table_name = self.get_table_name(timeframe)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for config in configs:
                    k_period = config['k_period']
                    k_smooth = config['k_smooth']
                    d_period = config['d_period']

                    # Формируем имена колонок (только цифры)
                    base_name = f"stoch_{k_period}_{k_smooth}_{d_period}"
                    columns = [
                        f"{base_name}_k",
                        f"{base_name}_d"
                    ]

                    for col_name in columns:
                        # Проверяем существование колонки
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = 'public'
                                AND table_name = %s
                                AND column_name = %s
                            );
                        """, (table_name, col_name))

                        exists = cur.fetchone()[0]

                        if not exists:
                            logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                            cur.execute(f"""
                                ALTER TABLE {table_name}
                                ADD COLUMN {col_name} DECIMAL(10,4);
                            """)
                            logger.info(f"✅ Колонка {col_name} создана")

                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при создании колонок: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Получает диапазон доступных данных в базовой таблице свечей
        Всегда читает из candles_bybit_futures_1m

        Returns:
            (min_date, max_date)
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute("""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()

                if not result or result[0] is None:
                    raise ValueError(f"Нет данных для {self.symbol} в candles_bybit_futures_1m")

                return result[0], result[1]
            finally:
                cur.close()

    def get_last_stochastic_date(self, timeframe: str, config: Dict) -> Optional[datetime]:
        """
        Получает последнюю дату с рассчитанным Stochastic для конфигурации

        Args:
            timeframe: Таймфрейм
            config: Конфигурация Stochastic

        Returns:
            Последняя дата или None
        """
        table_name = self.get_table_name(timeframe)

        k_period = config['k_period']
        k_smooth = config['k_smooth']
        d_period = config['d_period']
        col_name = f"stoch_{k_period}_{k_smooth}_{d_period}_k"

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s AND {col_name} IS NOT NULL
                """, (self.symbol,))

                result = cur.fetchone()
                return result[0] if result[0] else None
            finally:
                cur.close()

    def get_null_timestamps_for_config(self, timeframe: str, config: Dict) -> set:
        """
        Находит timestamps с NULL значениями для конкретной конфигурации Stochastic.
        Исключает естественные NULL в начале данных (недостаточно lookback).

        Args:
            timeframe: Таймфрейм
            config: Конфигурация Stochastic

        Returns:
            set: Множество timestamps с NULL
        """
        table_name = self.get_table_name(timeframe)
        k_period = config['k_period']
        k_smooth = config['k_smooth']
        d_period = config['d_period']
        base_name = f"stoch_{k_period}_{k_smooth}_{d_period}"
        col_k = f"{base_name}_k"
        col_d = f"{base_name}_d"

        # Natural boundary: k_period + k_smooth + d_period - 2 periods
        max_lookback = k_period + k_smooth + d_period - 2
        minutes = self.timeframe_minutes[timeframe]

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"""
                    SELECT MIN(timestamp) FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_date = cur.fetchone()[0]
                if not min_date:
                    return set()

                boundary = min_date + timedelta(minutes=max_lookback * minutes)

                cur.execute(f"""
                    SELECT timestamp FROM {table_name}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND ({col_k} IS NULL OR {col_d} IS NULL)
                """, (self.symbol, boundary))

                return {row[0] for row in cur.fetchall()}
            finally:
                cur.close()

    def get_last_complete_period(self, current_time: datetime, timeframe: str) -> datetime:
        """
        Получает timestamp последнего ЗАВЕРШЕННОГО периода

        Args:
            current_time: Текущее время (timezone-aware)
            timeframe: Таймфрейм

        Returns:
            Timestamp последнего завершенного периода
        """
        minutes = self.timeframe_minutes[timeframe]

        if timeframe == '1m':
            return current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)

        elif timeframe == '15m':
            minute = (current_time.minute // 15) * 15
            result = current_time.replace(minute=minute, second=0, microsecond=0)

            if current_time.minute % 15 == 0 and current_time.second == 0:
                result -= timedelta(minutes=15)

            return result

        elif timeframe == '1h':
            result = current_time.replace(minute=0, second=0, microsecond=0)

            if current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=1)

            return result

        elif timeframe == '4h':
            hour_block = (current_time.hour // 4) * 4
            result = current_time.replace(hour=hour_block, minute=0, second=0, microsecond=0)
            if current_time.hour % 4 == 0 and current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=4)
            return result

        elif timeframe == '1d':
            return (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        else:
            total_minutes = int(current_time.timestamp() / 60)
            period_start_minutes = (total_minutes // minutes) * minutes
            result = datetime.fromtimestamp(period_start_minutes * 60)

            if total_minutes % minutes == 0:
                result -= timedelta(minutes=minutes)

            return result

    def aggregate_candles(self, start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """
        Агрегирует минутные свечи в нужный таймфрейм или читает напрямую

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            timeframe: Целевой таймфрейм

        Returns:
            DataFrame с колонками: timestamp, symbol, high, low, close
        """
        with self.db.get_connection() as conn:
            if timeframe == '1m':
                # Для минутных данных читаем напрямую
                query = """
                    SELECT timestamp, symbol, high, low, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                # Для старших таймфреймов агрегируем из минутных
                minutes = self.timeframe_minutes[timeframe]

                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                if timeframe == '1d':
                    query = """
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                date_trunc('day', timestamp) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """
                elif timeframe == '4h':
                    query = """
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                date_trunc('day', timestamp) +
                                INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """
                else:
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                DATE_TRUNC('hour', timestamp) +
                                INTERVAL '1 minute' * (FLOOR(EXTRACT(MINUTE FROM timestamp) / {minutes}) * {minutes}) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """

                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            return df

    def save_stochastic_to_db(self, df: pd.DataFrame, table_name: str, config: Dict):
        """
        Сохраняет Stochastic компоненты в базу данных

        Args:
            df: DataFrame с Stochastic компонентами
            table_name: Имя таблицы
            config: Конфигурация Stochastic
        """
        k_period = config['k_period']
        k_smooth = config['k_smooth']
        d_period = config['d_period']

        base_name = f"stoch_{k_period}_{k_smooth}_{d_period}"
        col_k = f"{base_name}_k"
        col_d = f"{base_name}_d"

        # Фильтруем только строки с не-NULL значениями
        df_to_save = df[df[col_k].notna()].copy()

        if len(df_to_save) == 0:
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                update_query = f"""
                    UPDATE {table_name}
                    SET {col_k} = %s,
                        {col_d} = %s
                    WHERE timestamp = %s AND symbol = %s
                """

                # Подготавливаем данные для batch update
                data = [
                    (
                        float(row[col_k]) if pd.notna(row[col_k]) else None,
                        float(row[col_d]) if pd.notna(row[col_d]) else None,
                        row['timestamp'],
                        row['symbol']
                    )
                    for _, row in df_to_save.iterrows()
                ]

                # Выполняем batch update
                psycopg2.extras.execute_batch(cur, update_query, data, page_size=1000)
                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при сохранении Stochastic {config['name']}: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def calculate_and_save_stochastic(self, timeframe: str, configs: List[Dict], batch_days: int = 1):
        """
        Последовательно рассчитывает и сохраняет Stochastic для каждой конфигурации

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            configs: Список конфигураций Stochastic
            batch_days: Размер батча в днях
        """
        table_name = self.get_table_name(timeframe)

        logger.info(f"🚀 Начало расчета Stochastic для {self.symbol} на таймфрейме {timeframe}")
        logger.info(f"📊 Конфигурации: {[c['name'] for c in configs]}")
        logger.info(f"📦 Размер батча: {batch_days} дней")

        # Получаем диапазон доступных данных
        min_date, max_date = self.get_data_range(timeframe)
        logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Определяем последний завершенный период
        last_complete_period = self.get_last_complete_period(max_date, timeframe)

        # Ограничиваем max_date последним завершенным периодом
        if max_date > last_complete_period:
            logger.info(f"⏸️  Ограничение max_date до последнего завершенного периода: {last_complete_period}")
            max_date = last_complete_period

        # Получаем lookback multiplier из конфига
        lookback_multiplier = self.config['indicators']['stochastic'].get('lookback_multiplier', 2)

        # Последовательная обработка каждой конфигурации
        for config in configs:
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 Обработка конфигурации: {config['name']} ({config['k_period']}, {config['k_smooth']}, {config['d_period']})")
            logger.info(f"{'='*80}")

            null_timestamps = None  # None = записываем всё, set() = только NULL timestamps

            # Если force_reload - всегда начинаем с начала
            if self.force_reload:
                start_date = min_date
                logger.info(f"🔄 Stochastic {config['name']}: FORCE RELOAD - пересчет с начала истории")
            elif self.check_nulls:
                null_ts = self.get_null_timestamps_for_config(timeframe, config)
                if not null_ts:
                    logger.info(f"  ✅ Stochastic {config['name']}: все данные актуальны — пропускаем")
                    continue
                null_timestamps = null_ts
                logger.info(f"🔍 Stochastic {config['name']}: найдено {len(null_ts)} NULL записей")
                start_date = min(null_ts).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                # Находим последнюю дату с данными для этой конфигурации
                last_date = self.get_last_stochastic_date(timeframe, config)

                if last_date:
                    start_date = last_date + timedelta(days=1)
                    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    logger.info(f"📅 Последняя дата Stochastic {config['name']}: {last_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")
                else:
                    start_date = min_date
                    logger.info(f"🆕 Stochastic {config['name']} пуст, начинаем с начала: {start_date}")

            # Если уже все обработано
            if start_date > max_date:
                logger.info(f"✅ Stochastic {config['name']} уже актуален (до {max_date})")
                continue

            # Рассчитываем количество дней для обработки
            total_days = (max_date.date() - start_date.date()).days + 1
            logger.info(f"📆 Всего дней для обработки: {total_days}")

            # Обрабатываем батчами
            current_date = start_date
            processed_days = 0
            total_records = 0

            # Lookback для корректного расчета
            max_period = max(config['k_period'], config['k_smooth'], config['d_period'])
            lookback_periods = max_period * lookback_multiplier
            lookback_minutes = lookback_periods * self.timeframe_minutes[timeframe]
            lookback_delta = timedelta(minutes=lookback_minutes)

            logger.info(f"🔙 Lookback период: {lookback_minutes} минут ({lookback_periods} периодов × {self.timeframe_minutes[timeframe]} мин)")

            # Формируем описание конфигурации для прогресс-бара
            config_names = [f"{c['k_period']}_{c['k_smooth']}_{c['d_period']}" for c in configs]
            config_list_str = ','.join(config_names)

            with tqdm(total=total_days,
                     desc=f"{self.symbol} {self.symbol_progress} STOCH[{config_list_str}] {timeframe.upper()}",
                     unit="day",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}') as pbar:
                while current_date <= max_date:
                    # Определяем конец батча
                    batch_end = min(
                        current_date + timedelta(days=batch_days) - timedelta(seconds=1),
                        max_date
                    )

                    # Добавляем lookback к началу для корректного расчета
                    data_start = current_date - lookback_delta

                    try:
                        # Загружаем данные с lookback
                        df = self.aggregate_candles(data_start, batch_end, timeframe)

                        if len(df) == 0:
                            current_date += timedelta(days=batch_days)
                            processed_days += batch_days
                            pbar.update(min(batch_days, total_days - processed_days + batch_days))
                            continue

                        # Рассчитываем Stochastic
                        k_values, d_values = self.calculate_stochastic(
                            df,
                            config['k_period'],
                            config['k_smooth'],
                            config['d_period']
                        )

                        # Добавляем к DataFrame
                        base_name = f"stoch_{config['k_period']}_{config['k_smooth']}_{config['d_period']}"
                        df[f'{base_name}_k'] = k_values
                        df[f'{base_name}_d'] = d_values

                        # Фильтруем только целевой диапазон (без lookback)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # В режиме check_nulls записываем только NULL timestamps
                        if null_timestamps is not None and not df_to_save.empty:
                            df_to_save = df_to_save[df_to_save['timestamp'].isin(null_timestamps)]

                        if df_to_save.empty:
                            current_date += timedelta(days=batch_days)
                            processed_days += batch_days
                            pbar.update(min(batch_days, total_days - processed_days + batch_days))
                            continue

                        # Сохраняем с retry логикой
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                self.save_stochastic_to_db(df_to_save, table_name, config)
                                break
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    wait_time = 2 ** attempt
                                    time.sleep(wait_time)
                                else:
                                    logger.error(f"❌ Все {max_retries} попытки не удались для батча {current_date.date()}")
                                    raise

                        # Обновляем прогресс
                        days_in_batch = min(batch_days, (max_date.date() - current_date.date()).days + 1)
                        processed_days += days_in_batch
                        total_records += len(df_to_save)
                        pbar.update(days_in_batch)

                        # Обновляем информацию в progress bar
                        if not df_to_save.empty:
                            latest_timestamp = df_to_save['timestamp'].max()
                            pbar.set_postfix({
                                'всего': f'{total_records:,}',
                                'последняя': latest_timestamp.strftime('%Y-%m-%d %H:%M')
                            })

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча {current_date.date()}: {e}")
                        raise

                    # Переходим к следующему батчу
                    current_date += timedelta(days=batch_days)

            logger.info(f"✅ Stochastic {config['name']} завершен: {total_records:,} записей обработано за {processed_days} дней")

        logger.info(f"\n{'='*80}")
        logger.info(f"🎉 Все конфигурации Stochastic для {timeframe} завершены!")
        logger.info(f"{'='*80}")

    def run(self, timeframe: str = None, batch_days: int = None):
        """
        Запуск загрузки Stochastic

        Args:
            timeframe: Конкретный таймфрейм или None для всех
            batch_days: Размер батча в днях
        """
        # Получаем параметры из конфига
        stochastic_config = self.config['indicators']['stochastic']
        configs = stochastic_config['configurations']

        if batch_days is None:
            batch_days = stochastic_config.get('batch_days', 1)

        # Определяем таймфреймы для обработки
        if timeframe:
            timeframes = [timeframe]
        else:
            timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        logger.info(f"🚀 Запуск Stochastic Loader для {self.symbol}")
        logger.info(f"⏰ Таймфреймы: {timeframes}")
        logger.info(f"📊 Конфигурации: {[c['name'] for c in configs]}")
        logger.info(f"📦 Batch size: {batch_days} дней")

        for tf in timeframes:
            logger.info(f"\n{'#'*80}")
            logger.info(f"⏰ Таймфрейм: {tf}")
            logger.info(f"{'#'*80}")

            # Создаем колонки если нужно
            self.ensure_stochastic_columns(tf, configs)

            # Рассчитываем и сохраняем Stochastic
            self.calculate_and_save_stochastic(tf, configs, batch_days)

        logger.info(f"\n{'#'*80}")
        logger.info(f"🎉 Загрузка Stochastic завершена для всех таймфреймов!")
        logger.info(f"{'#'*80}")


class WilliamsRLoader:
    """Загрузчик Williams %R для разных таймфреймов"""

    def __init__(self, symbol: str = 'BTCUSDT', force_reload: bool = False, check_nulls: bool = False):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
            force_reload: Принудительный пересчет всех данных с начала истории
            check_nulls: Проверить и заполнить NULL значения в середине данных
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.force_reload = force_reload
        self.check_nulls = check_nulls
        self.symbol_progress = ""  # Для отображения прогресса при multi-symbol
        self.config = self.load_config()

        # Динамический мапинг таймфреймов на минуты
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """Динамически парсит таймфреймы из конфигурации"""
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        for tf in timeframes:
            import re
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())

            if not match:
                logger.warning(f"Неизвестный таймфрейм: {tf}, пропускаем")
                continue

            value = int(match.group(1))
            unit = match.group(2)

            if unit == 'm':
                minutes = value
            elif unit == 'h':
                minutes = value * 60
            elif unit == 'd':
                minutes = value * 60 * 24
            elif unit == 'w':
                minutes = value * 60 * 24 * 7
            else:
                logger.warning(f"Неподдерживаемая единица времени: {unit}")
                continue

            timeframe_map[tf] = minutes

        logger.info(f"Распарсенные таймфреймы: {timeframe_map}")
        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из YAML файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config

    def get_table_name(self, timeframe: str) -> str:
        """Получить имя таблицы индикаторов для таймфрейма"""
        return f"indicators_bybit_futures_{timeframe}"

    def calculate_williams_r(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Рассчитывает Williams %R

        Формула:
        %R = -((High_N - Close) / (High_N - Low_N)) × 100

        Диапазон: от -100 до 0
        - Overbought: > -20
        - Oversold: < -80

        Args:
            df: DataFrame с колонками high, low, close
            period: Период для расчёта High/Low диапазона

        Returns:
            Series с Williams %R значениями
        """
        # Рассчитываем rolling high и low
        rolling_high = df['high'].rolling(window=period).max()
        rolling_low = df['low'].rolling(window=period).min()

        # Williams %R = -((High_N - Close) / (High_N - Low_N)) × 100
        # Избегаем деления на ноль
        denominator = rolling_high - rolling_low
        wr = np.where(
            denominator != 0,
            -((rolling_high - df['close']) / denominator) * 100,
            np.nan
        )

        return pd.Series(wr, index=df.index)

    def ensure_williams_r_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для Williams %R периодов если их нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов Williams %R
        """
        table_name = self.get_table_name(timeframe)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for period in periods:
                    col_name = f"williamsr_{period}"

                    # Проверяем существование колонки
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                            AND table_name = %s
                            AND column_name = %s
                        );
                    """, (table_name, col_name))

                    exists = cur.fetchone()[0]

                    if not exists:
                        logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col_name} DECIMAL(10,4);
                        """)
                        logger.info(f"✅ Колонка {col_name} создана")

                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при создании колонок Williams %R: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """Получает диапазон доступных данных"""
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute("""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()

                if not result or result[0] is None:
                    raise ValueError(f"Нет данных для {self.symbol} в candles_bybit_futures_1m")

                return result[0], result[1]
            finally:
                cur.close()

    def get_last_williams_r_date(self, timeframe: str, period: int) -> Optional[datetime]:
        """Получает последнюю дату с рассчитанным Williams %R"""
        table_name = self.get_table_name(timeframe)
        col_name = f"williamsr_{period}"

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s AND {col_name} IS NOT NULL
                """, (self.symbol,))

                result = cur.fetchone()
                return result[0] if result[0] else None
            finally:
                cur.close()

    def get_null_timestamps_for_period(self, timeframe: str, period: int) -> set:
        """
        Находит timestamps с NULL значениями для конкретного периода Williams %R.
        Исключает естественные NULL в начале данных (недостаточно lookback).

        Args:
            timeframe: Таймфрейм
            period: Период Williams %R

        Returns:
            set: Множество timestamps с NULL
        """
        table_name = self.get_table_name(timeframe)
        col_name = f"williamsr_{period}"
        minutes = self.timeframe_minutes[timeframe]

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"""
                    SELECT MIN(timestamp) FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))
                min_date = cur.fetchone()[0]
                if not min_date:
                    return set()

                boundary = min_date + timedelta(minutes=period * minutes)

                cur.execute(f"""
                    SELECT timestamp FROM {table_name}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND {col_name} IS NULL
                """, (self.symbol, boundary))

                return {row[0] for row in cur.fetchall()}
            finally:
                cur.close()

    def get_last_complete_period(self, current_time: datetime, timeframe: str) -> datetime:
        """Получает timestamp последнего ЗАВЕРШЕННОГО периода"""
        minutes = self.timeframe_minutes[timeframe]

        if timeframe == '1m':
            return current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)

        elif timeframe == '15m':
            minute = (current_time.minute // 15) * 15
            result = current_time.replace(minute=minute, second=0, microsecond=0)

            if current_time.minute % 15 == 0 and current_time.second == 0:
                result -= timedelta(minutes=15)

            return result

        elif timeframe == '1h':
            result = current_time.replace(minute=0, second=0, microsecond=0)

            if current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=1)

            return result

        elif timeframe == '4h':
            hour_block = (current_time.hour // 4) * 4
            result = current_time.replace(hour=hour_block, minute=0, second=0, microsecond=0)
            if current_time.hour % 4 == 0 and current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=4)
            return result

        elif timeframe == '1d':
            return (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        else:
            total_minutes = int(current_time.timestamp() / 60)
            period_start_minutes = (total_minutes // minutes) * minutes
            result = datetime.fromtimestamp(period_start_minutes * 60)

            if total_minutes % minutes == 0:
                result -= timedelta(minutes=minutes)

            return result

    def aggregate_candles(self, start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """Агрегирует минутные свечи в нужный таймфрейм"""
        with self.db.get_connection() as conn:
            if timeframe == '1m':
                query = """
                    SELECT timestamp, symbol, high, low, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                minutes = self.timeframe_minutes[timeframe]

                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                if timeframe == '1d':
                    query = """
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                date_trunc('day', timestamp) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """
                elif timeframe == '4h':
                    query = """
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                date_trunc('day', timestamp) +
                                INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """
                else:
                    query = f"""
                        WITH time_groups AS (
                            SELECT
                                timestamp,
                                DATE_TRUNC('hour', timestamp) +
                                INTERVAL '1 minute' * (FLOOR(EXTRACT(MINUTE FROM timestamp) / {minutes}) * {minutes}) as period_start,
                                high, low, close,
                                symbol
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                              AND timestamp >= %s
                              AND timestamp <= %s
                        )
                        SELECT
                            period_start as timestamp,
                            symbol,
                            MAX(high) as high,
                            MIN(low) as low,
                            (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                        FROM time_groups
                        GROUP BY period_start, symbol
                        ORDER BY period_start
                    """

                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            return df

    def save_williams_r_to_db(self, df: pd.DataFrame, table_name: str, period: int):
        """Сохраняет Williams %R в базу данных"""
        col_name = f"williamsr_{period}"

        # Фильтруем только строки с не-NULL значениями
        df_to_save = df[df[col_name].notna()].copy()

        if len(df_to_save) == 0:
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                update_query = f"""
                    UPDATE {table_name}
                    SET {col_name} = %s
                    WHERE timestamp = %s AND symbol = %s
                """

                # Подготавливаем данные для batch update
                data = [
                    (
                        float(row[col_name]) if pd.notna(row[col_name]) else None,
                        row['timestamp'],
                        row['symbol']
                    )
                    for _, row in df_to_save.iterrows()
                ]

                # Выполняем batch update
                psycopg2.extras.execute_batch(cur, update_query, data, page_size=1000)
                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при сохранении Williams %R period={period}: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def calculate_and_save_williams_r(self, timeframe: str, periods: List[int], batch_days: int = 1):
        """
        Последовательно рассчитывает и сохраняет Williams %R для каждого периода

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов Williams %R
            batch_days: Размер батча в днях
        """
        table_name = self.get_table_name(timeframe)

        logger.info(f"🚀 Начало расчета Williams %R для {self.symbol} на таймфрейме {timeframe}")
        logger.info(f"📊 Периоды: {periods}")
        logger.info(f"📦 Размер батча: {batch_days} дней")

        # Получаем диапазон доступных данных
        min_date, max_date = self.get_data_range(timeframe)
        logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Определяем последний завершенный период
        last_complete_period = self.get_last_complete_period(max_date, timeframe)

        if max_date > last_complete_period:
            logger.info(f"⏸️  Ограничение max_date до последнего завершенного периода: {last_complete_period}")
            max_date = last_complete_period

        # Получаем lookback multiplier из конфига
        lookback_multiplier = self.config['indicators']['williams_r'].get('lookback_multiplier', 2)

        # Последовательная обработка каждого периода
        for period in periods:
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 Обработка Williams %R period={period}")
            logger.info(f"{'='*80}")

            null_timestamps = None  # None = записываем всё, set() = только NULL timestamps

            # Если force_reload - всегда начинаем с начала
            if self.force_reload:
                start_date = min_date
                logger.info(f"🔄 Williams %R period={period}: FORCE RELOAD - пересчет с начала истории")
            elif self.check_nulls:
                null_ts = self.get_null_timestamps_for_period(timeframe, period)
                if not null_ts:
                    logger.info(f"  ✅ Williams %R period={period}: все данные актуальны — пропускаем")
                    continue
                null_timestamps = null_ts
                logger.info(f"🔍 Williams %R period={period}: найдено {len(null_ts)} NULL записей")
                start_date = min(null_ts).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                # Находим последнюю дату с данными для этого периода
                last_date = self.get_last_williams_r_date(timeframe, period)

                if last_date:
                    start_date = last_date + timedelta(days=1)
                    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    logger.info(f"📅 Последняя дата Williams %R period={period}: {last_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")
                else:
                    start_date = min_date
                    logger.info(f"🆕 Williams %R period={period} пуст, начинаем с начала: {start_date}")

            # Если уже все обработано
            if start_date > max_date:
                logger.info(f"✅ Williams %R period={period} уже актуален (до {max_date})")
                continue

            # Рассчитываем количество дней для обработки
            total_days = (max_date.date() - start_date.date()).days + 1
            logger.info(f"📆 Всего дней для обработки: {total_days}")

            # Обрабатываем батчами
            current_date = start_date
            processed_days = 0
            total_records = 0

            # Lookback для корректного расчета
            lookback_periods = period * lookback_multiplier
            lookback_minutes = lookback_periods * self.timeframe_minutes[timeframe]
            lookback_delta = timedelta(minutes=lookback_minutes)

            logger.info(f"🔙 Lookback период: {lookback_minutes} минут ({lookback_periods} периодов × {self.timeframe_minutes[timeframe]} мин)")

            # Формируем список периодов для прогресс-бара
            periods_str = ','.join(map(str, periods))

            with tqdm(total=total_days,
                     desc=f"{self.symbol} {self.symbol_progress} WILLIAMS[{periods_str}] {timeframe.upper()}",
                     unit="day",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}') as pbar:
                while current_date <= max_date:
                    batch_end = min(
                        current_date + timedelta(days=batch_days) - timedelta(seconds=1),
                        max_date
                    )

                    data_start = current_date - lookback_delta

                    try:
                        # Загружаем данные с lookback
                        df = self.aggregate_candles(data_start, batch_end, timeframe)

                        if len(df) == 0:
                            current_date += timedelta(days=batch_days)
                            processed_days += batch_days
                            pbar.update(min(batch_days, total_days - processed_days + batch_days))
                            continue

                        # Рассчитываем Williams %R
                        wr_values = self.calculate_williams_r(df, period)

                        # Добавляем к DataFrame
                        df[f'williamsr_{period}'] = wr_values

                        # Фильтруем только целевой диапазон (без lookback)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # В режиме check_nulls записываем только NULL timestamps
                        if null_timestamps is not None and not df_to_save.empty:
                            df_to_save = df_to_save[df_to_save['timestamp'].isin(null_timestamps)]

                        if df_to_save.empty:
                            current_date += timedelta(days=batch_days)
                            processed_days += batch_days
                            pbar.update(min(batch_days, total_days - processed_days + batch_days))
                            continue

                        # Сохраняем с retry логикой
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                self.save_williams_r_to_db(df_to_save, table_name, period)
                                break
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    wait_time = 2 ** attempt
                                    time.sleep(wait_time)
                                else:
                                    logger.error(f"❌ Все {max_retries} попытки не удались для батча {current_date.date()}")
                                    raise

                        # Обновляем прогресс
                        days_in_batch = min(batch_days, (max_date.date() - current_date.date()).days + 1)
                        processed_days += days_in_batch
                        total_records += len(df_to_save)
                        pbar.update(days_in_batch)

                        # Обновляем информацию в progress bar
                        if not df_to_save.empty:
                            latest_timestamp = df_to_save['timestamp'].max()
                            pbar.set_postfix({
                                'всего': f'{total_records:,}',
                                'последняя': latest_timestamp.strftime('%Y-%m-%d %H:%M')
                            })

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча {current_date.date()}: {e}")
                        raise

                    current_date += timedelta(days=batch_days)

            logger.info(f"✅ Williams %R period={period} завершен: {total_records:,} записей обработано за {processed_days} дней")

        logger.info(f"\n{'='*80}")
        logger.info(f"🎉 Все периоды Williams %R для {timeframe} завершены!")
        logger.info(f"{'='*80}")

    def run(self, timeframe: str = None, batch_days: int = None):
        """
        Запуск загрузки Williams %R

        Args:
            timeframe: Конкретный таймфрейм или None для всех
            batch_days: Размер батча в днях
        """
        # Получаем параметры из конфига
        williams_config = self.config['indicators']['williams_r']
        periods = williams_config['periods']

        if batch_days is None:
            batch_days = williams_config.get('batch_days', 1)

        # Определяем таймфреймы для обработки
        if timeframe:
            timeframes = [timeframe]
        else:
            timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        logger.info(f"🚀 Запуск Williams %R Loader для {self.symbol}")
        logger.info(f"⏰ Таймфреймы: {timeframes}")
        logger.info(f"📊 Периоды: {periods}")
        logger.info(f"📦 Batch size: {batch_days} дней")

        for tf in timeframes:
            logger.info(f"\n{'#'*80}")
            logger.info(f"⏰ Таймфрейм: {tf}")
            logger.info(f"{'#'*80}")

            # Создаем колонки если нужно
            self.ensure_williams_r_columns(tf, periods)

            # Рассчитываем и сохраняем Williams %R
            self.calculate_and_save_williams_r(tf, periods, batch_days)

        logger.info(f"\n{'#'*80}")
        logger.info(f"🎉 Загрузка Williams %R завершена для всех таймфреймов!")
        logger.info(f"{'#'*80}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Stochastic & Williams %R Loader для разных таймфреймов')
    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str, help='Конкретный таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--batch-days', type=int, help='Размер батча в днях')
    parser.add_argument('--indicator', type=str, choices=['stochastic', 'williams', 'both'], default='both',
                       help='Какой индикатор загружать: stochastic, williams, или both (по умолчанию both)')
    parser.add_argument('--force-reload', action='store_true',
                       help='Принудительный пересчет ВСЕХ данных с начала истории')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Проверить и заполнить NULL значения в середине данных')

    args = parser.parse_args()

    # Определяем символы для обработки
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        symbols = [args.symbol]
    else:
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    logger.info(f"🎯 Обработка символов: {symbols}")
    logger.info(f"📊 Индикаторы: {args.indicator}")
    if args.force_reload:
        logger.info(f"🔄 Режим FORCE RELOAD: все данные будут пересчитаны с начала истории")
    if args.check_nulls:
        logger.info(f"🔍 Режим CHECK NULLS: поиск и заполнение NULL значений в середине данных")

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        try:
            # Загружаем Stochastic
            if args.indicator in ['stochastic', 'both']:
                logger.info(f"\n{'#'*80}")
                logger.info(f"📊 Загрузка Stochastic Oscillator для {symbol}")
                logger.info(f"{'#'*80}\n")

                stoch_loader = StochasticLoader(symbol=symbol, force_reload=args.force_reload, check_nulls=args.check_nulls)
                stoch_loader.symbol_progress = f"[{idx}/{total_symbols}]"
                stoch_loader.run(timeframe=args.timeframe, batch_days=args.batch_days)

            # Загружаем Williams %R
            if args.indicator in ['williams', 'both']:
                logger.info(f"\n{'#'*80}")
                logger.info(f"📊 Загрузка Williams %R для {symbol}")
                logger.info(f"{'#'*80}\n")

                williams_loader = WilliamsRLoader(symbol=symbol, force_reload=args.force_reload, check_nulls=args.check_nulls)
                williams_loader.symbol_progress = f"[{idx}/{total_symbols}]"
                williams_loader.run(timeframe=args.timeframe, batch_days=args.batch_days)

            logger.info(f"\n✅ Символ {symbol} обработан\n")
        except KeyboardInterrupt:
            logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")


if __name__ == "__main__":
    main()
