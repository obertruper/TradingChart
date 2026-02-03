#!/usr/bin/env python3
"""
RSI (Relative Strength Index) Loader with Single-Pass Batch Processing
========================================================================
Загрузчик RSI индикаторов с поддержкой:
- Автоматического определения пустых столбцов
- Раздельной загрузки для разных уровней заполненности
- Множественных периодов (7, 9, 14, 21, 25)
- Батчевой записи данных (расчет в одном проходе, запись батчами)
- Любых таймфреймов (1m, 15m, 1h и т.д.)
- Инкрементальных обновлений
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import yaml
import psycopg2.extras
from tqdm import tqdm
import argparse
import warnings
import time

warnings.filterwarnings('ignore')

from database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'rsi_{timestamp}.log')

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

    logger.info(f"📝 RSI Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger

logger = setup_logging()

class RSILoader:
    """
    Улучшенный загрузчик RSI индикаторов с автоматическим определением пустых столбцов
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
        self.timeframe_minutes = self._parse_timeframes()
        self.force_reload = False  # Флаг принудительного пересчета (устанавливается из main())
        # REVERTED: Bybit-style RSI SMA smoothing - не используется
        # self.smoothing_length = self.config.get('rsi', {}).get('smoothing_length', 14)

    def load_config(self):
        """Загружает конфигурацию из файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def _parse_timeframes(self) -> dict:
        """Парсит таймфреймы из конфигурации"""
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m'])

        for tf in timeframes:
            import re
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                if unit == 'm':
                    timeframe_map[tf] = number
                elif unit == 'h':
                    timeframe_map[tf] = number * 60
                elif unit == 'd':
                    timeframe_map[tf] = number * 1440
                elif unit == 'w':
                    timeframe_map[tf] = number * 10080

        return timeframe_map

    def create_rsi_columns(self, timeframe: str, periods: List[int]) -> bool:
        """Создает колонки для RSI если их нет"""
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

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

            # Получаем существующие колонки
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """, (table_name,))

            existing_columns = {row[0] for row in cur.fetchall()}

            # Добавляем недостающие колонки
            columns_added = []
            for period in periods:
                col_name = f'rsi_{period}'
                if col_name not in existing_columns:
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(10,4)
                    """)
                    columns_added.append(col_name)

            if columns_added:
                conn.commit()
                logger.info(f"✅ Добавлены колонки RSI: {columns_added} в таблицу {table_name}")
            else:
                logger.info(f"ℹ️ Все колонки RSI уже существуют в {table_name}")

            return True

    def analyze_rsi_periods(self, timeframe: str, periods: List[int]) -> Dict[str, List[int]]:
        """
        Анализирует заполненность RSI периодов и группирует их по статусу

        Returns:
            Словарь с группировкой периодов:
            - 'empty': пустые периоды (< 50% заполнения)
            - 'partial': частично заполненные (50-95%)
            - 'complete': почти полные (> 95%)
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        groups = {
            'empty': [],
            'partial': [],
            'complete': []
        }

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            logger.info(f"🔍 {self.symbol} {self.symbol_progress}. Анализ данных RSI {periods}:")

            # Строим динамический SQL для проверки всех периодов одним запросом
            select_parts = ["COUNT(*) as total"]
            for period in periods:
                col_name = f'rsi_{period}'
                select_parts.append(f"COUNT({col_name}) as filled_{period}")
                select_parts.append(f"MIN(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as first_{period}")
                select_parts.append(f"MAX(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as last_{period}")

            query = f"""
                SELECT {', '.join(select_parts)}
                FROM {table_name}
                WHERE symbol = %s
            """

            # Один запрос для всех периодов (вместо N запросов)
            cur.execute(query, (self.symbol,))
            result = cur.fetchone()

            if result:
                total = result[0]

                # Парсим результаты для каждого периода
                idx = 1
                for period in periods:
                    filled = result[idx]
                    # first_rsi = result[idx + 1]  # Не используется, но доступно при необходимости
                    last_rsi = result[idx + 2]
                    idx += 3

                    if total > 0:
                        fill_percent = (filled / total) * 100 if filled else 0

                        if fill_percent < 50:
                            groups['empty'].append(period)
                            logger.info(f"  ❌ RSI_{period}: {fill_percent:.1f}% заполнено (будет загружен с начала)")
                        elif fill_percent < 95:
                            groups['partial'].append(period)
                            logger.info(f"  ⚠️ RSI_{period}: {fill_percent:.1f}% заполнено (продолжение с {last_rsi.strftime('%Y-%m-%d %H:%M') if last_rsi else 'начала'})")
                        else:
                            groups['complete'].append(period)
                            logger.info(f"  ✅ RSI_{period}: {fill_percent:.1f}% заполнено (обновление с {last_rsi.strftime('%Y-%m-%d %H:%M') if last_rsi else 'конца'})")
                    else:
                        groups['empty'].append(period)
                        logger.info(f"  📝 RSI_{period}: нет данных в таблице (будет загружен с начала)")
            else:
                # Если нет результата - все периоды пустые
                for period in periods:
                    groups['empty'].append(period)
                    logger.info(f"  📝 RSI_{period}: нет данных (будет загружен с начала)")

        return groups


    def clear_rsi_columns(self, timeframe: str, periods: List[int]) -> bool:
        """
        Обнуляет (устанавливает NULL) все RSI столбцы для указанного таймфрейма и символа

        Args:
            timeframe: Таймфрейм для очистки (1m, 15m, 1h)
            periods: Список периодов RSI для очистки

        Returns:
            True если очистка успешна, False в случае ошибки
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Формируем SET clause для всех RSI колонок
                set_clauses = [f'rsi_{period} = NULL' for period in periods]
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
                logger.info(f"🗑️  Обнулено {rows_affected:,} записей для RSI столбцов в {table_name} (символ: {self.symbol})")
                logger.info(f"   Очищены столбцы: {', '.join([f'rsi_{p}' for p in periods])}")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при очистке RSI столбцов: {e}")
                conn.rollback()
                return False

    def get_null_timestamps(self, timeframe: str, periods: List[int]) -> set:
        """
        Получает timestamps где хотя бы один RSI период IS NULL.
        Используется для оптимизации - записываем только новые данные.

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов RSI для проверки

        Returns:
            Set of timestamps где RSI IS NULL
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Строим условие: rsi_7 IS NULL OR rsi_9 IS NULL OR ...
        null_conditions = ' OR '.join([f'rsi_{p} IS NULL' for p in periods])

        query = f"""
            SELECT timestamp
            FROM {table_name}
            WHERE symbol = %s
            AND ({null_conditions})
        """

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (self.symbol,))
            return {row[0] for row in cur.fetchall()}

    def get_null_timestamp_list(self, timeframe: str, periods: List[int]) -> List[datetime]:
        """
        Возвращает список конкретных timestamps где есть NULL значения RSI,
        ИСКЛЮЧАЯ неизбежные NULL в начале данных (где нет достаточной истории для расчёта)

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов RSI

        Returns:
            List[datetime] - список timestamps с NULL
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]
        max_period = max(periods)

        null_conditions = ' OR '.join([f'rsi_{p} IS NULL' for p in periods])

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
                # RSI требует period+1 значений для первого расчёта
                unavoidable_null_boundary = min_data_date + timedelta(minutes=(max_period + 1) * minutes)

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
        Заполняет NULL значения RSI для указанного таймфрейма.

        ВАЖНО: Использует ПОЛНЫЙ пересчёт с начала данных для 100% точности,
        так как RSI использует Wilder smoothing - кумулятивную формулу,
        где каждое значение зависит от предыдущего.

        Алгоритм:
        1. Получить список конкретных timestamps где есть NULL
        2. Загрузить ВСЕ свечи с начала данных
        3. Рассчитать RSI с нуля для всего диапазона
        4. Записать ТОЛЬКО записи из списка NULL timestamps

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов RSI

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
        logger.info(f"   ⚠️ Полный пересчёт RSI с начала данных (100% точность)")

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
                    if minutes == 60:  # 1h
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
                df = pd.DataFrame(rows, columns=['timestamp', 'close'])
                df['close'] = df['close'].astype(float)

                # Рассчитываем RSI с нуля для каждого периода
                closes = df['close'].values
                for period in periods:
                    col_name = f'rsi_{period}'
                    df[col_name] = self.calculate_rsi(closes, period)

                # Фильтруем ТОЛЬКО записи с NULL timestamps
                null_timestamps_set = set(null_timestamps)
                df_to_update = df[df['timestamp'].isin(null_timestamps_set)].copy()

                # Удаляем строки где все RSI = NaN
                rsi_columns = [f'rsi_{p}' for p in periods]
                df_to_update = df_to_update.dropna(subset=rsi_columns, how='all')

                if df_to_update.empty:
                    logger.info(f"⚠️ [{self.symbol}] {timeframe}: Нет данных для обновления после расчёта")
                    return 0

                # Обновляем записи в БД
                set_clauses = ', '.join([f'rsi_{p} = %s' for p in periods])
                update_query = f"""
                    UPDATE {table_name}
                    SET {set_clauses}
                    WHERE timestamp = %s AND symbol = %s
                """

                update_data = []
                for _, row in df_to_update.iterrows():
                    values = [float(row[f'rsi_{p}']) if pd.notna(row[f'rsi_{p}']) else None for p in periods]
                    values.extend([row['timestamp'], self.symbol])
                    update_data.append(tuple(values))

                # Batch update с прогресс-баром
                progress_desc = f"{self.symbol} {self.symbol_progress} RSI {timeframe.upper()}"
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

    def calculate_rsi(self, closes: np.ndarray, period: int) -> np.ndarray:
        """
        Calculate RSI using Wilder smoothing method (single-pass, like validator).

        Args:
            closes: Array of close prices
            period: RSI period

        Returns:
            Array of RSI values (same length as closes)
        """
        if len(closes) < period + 1:
            return np.full(len(closes), np.nan)

        # Convert to float64 to handle Decimal types from PostgreSQL
        closes = np.asarray(closes, dtype=np.float64)

        # Calculate price changes
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        rsi_values = np.full(len(closes), np.nan)

        # Initialize with SMA of first 'period' gains/losses
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        # Calculate RSI for each point using Wilder smoothing
        for i in range(period, len(gains)):
            # Wilder smoothing: avg = (avg * (period-1) + new_value) / period
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            # Calculate RSI
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))

            rsi_values[i + 1] = rsi  # +1 because deltas is shorter by 1

        return rsi_values

    def apply_sma_smoothing(self, rsi_values: np.ndarray, smoothing_length: int) -> np.ndarray:
        """
        Применяет SMA сглаживание к RSI значениям (Bybit-style)

        Args:
            rsi_values: Массив RSI значений
            smoothing_length: Длина окна SMA (по умолчанию 14 на Bybit)

        Returns:
            Массив RSI со сглаживанием
        """
        if smoothing_length <= 1:
            # Если smoothing отключен, возвращаем оригинальные значения
            return rsi_values

        rsi_smoothed = np.full(len(rsi_values), np.nan)

        for i in range(smoothing_length - 1, len(rsi_values)):
            # Проверяем что все значения в окне не NaN
            window = rsi_values[i - smoothing_length + 1:i + 1]
            if not np.isnan(window).any():
                rsi_smoothed[i] = np.mean(window)

        return rsi_smoothed

    def load_all_data(
        self,
        timeframe: str,
        max_period: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Load ALL candle data for RSI calculation (single-pass like validator).

        Args:
            timeframe: '1m', '15m', or '1h'
            max_period: Maximum RSI period (for lookback calculation)
            start_date: Start date (None = from beginning)
            end_date: End date (None = until now)

        Returns:
            DataFrame with columns: timestamp, close
        """
        minutes = self.timeframe_minutes[timeframe]

        # For RSI lookback: 10x for Wilder convergence (99.996% accuracy)
        lookback_periods = max_period * 10
        lookback_minutes = lookback_periods * minutes

        # Adjust start_date for lookback
        if start_date:
            adjusted_start = start_date - timedelta(minutes=lookback_minutes)
        else:
            adjusted_start = None

        if end_date is None:
            end_date = datetime.now(timezone.utc)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            if timeframe == '1m':
                # For 1m timeframe, data comes directly from candles table
                query = """
                    SELECT timestamp, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """
                params = [self.symbol]

                if adjusted_start:
                    query += " AND timestamp >= %s"
                    params.append(adjusted_start)
                if end_date:
                    query += " AND timestamp <= %s"
                    params.append(end_date)

                query += " ORDER BY timestamp"
                cur.execute(query, params)

            else:
                # For aggregated timeframes (15m, 1h), aggregate from 1m data
                # IMPORTANT: Timestamp = START of period (Bybit standard)

                # Subtract one period to load enough 1m candles
                if adjusted_start:
                    query_adjusted_start = adjusted_start - timedelta(minutes=minutes)
                else:
                    query_adjusted_start = None

                if minutes == 60:  # 1h
                    query = """
                        SELECT
                            date_trunc('hour', timestamp) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                    """
                    params = [self.symbol]

                    if query_adjusted_start:
                        query += " AND timestamp >= %s"
                        params.append(query_adjusted_start)
                    if end_date:
                        query += " AND timestamp <= %s"
                        params.append(end_date)

                    query += """
                        GROUP BY date_trunc('hour', timestamp)
                        ORDER BY period_start
                    """

                else:  # 15m
                    query = f"""
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {minutes}) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                    """
                    params = [self.symbol]

                    if query_adjusted_start:
                        query += " AND timestamp >= %s"
                        params.append(query_adjusted_start)
                    if end_date:
                        query += " AND timestamp <= %s"
                        params.append(end_date)

                    query += f"""
                        GROUP BY date_trunc('hour', timestamp),
                                 EXTRACT(MINUTE FROM timestamp)::INTEGER / {minutes}
                        ORDER BY period_start
                    """

                cur.execute(query, params)

            rows = cur.fetchall()

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=['timestamp', 'close'])

            logger.info(f"   📊 Загружено {len(df):,} свечей для {timeframe} (включая lookback)")
            if start_date:
                lookback_count = len(df[df['timestamp'] < start_date])
                data_count = len(df[df['timestamp'] >= start_date])
                logger.info(f"      • Lookback: {lookback_count:,} свечей")
                logger.info(f"      • Данные: {data_count:,} свечей")

            return df

    def process_batch(self, timeframe: str, periods: List[int],
                     start_date: datetime, end_date: datetime,
                     initial_states: Dict[int, Dict]) -> Dict[int, Dict]:
        """
        Обрабатывает батч данных и сохраняет RSI в БД

        Returns:
            Финальные состояния для следующего батча
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Для RSI всегда используем цену закрытия (close)
            price_column = 'close'  # RSI должен рассчитываться по close для всех таймфреймов

            # Загружаем данные
            if timeframe == '1m':
                cur.execute(f"""
                    SELECT timestamp, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                    AND timestamp > %s
                    AND timestamp <= %s
                    ORDER BY timestamp
                """, (self.symbol, start_date, end_date))
            else:
                # Для других таймфреймов агрегируем из 1m данных
                interval_minutes = self.timeframe_minutes[timeframe]

                # Вычитаем один период для загрузки достаточных 1m свечей
                adjusted_start = start_date - timedelta(minutes=interval_minutes)

                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                # Пример для 1h: timestamp 14:00 содержит данные 14:00:00 - 14:59:59
                # Пример для 15m: timestamp 14:00 содержит данные 14:00:00 - 14:14:59
                if interval_minutes == 60:  # 1h
                    cur.execute(f"""
                        SELECT
                            date_trunc('hour', timestamp) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                        AND timestamp >= %s
                        AND timestamp <= %s
                        GROUP BY date_trunc('hour', timestamp)
                        ORDER BY period_start
                    """, (self.symbol, adjusted_start, end_date))
                else:  # 15m
                    cur.execute(f"""
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as period_start,
                            (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                        AND timestamp >= %s
                        AND timestamp <= %s
                        GROUP BY date_trunc('hour', timestamp), EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}
                        ORDER BY period_start
                    """, (self.symbol, adjusted_start, end_date))

            data = cur.fetchall()
            if not data:
                return initial_states

            timestamps = [row[0] for row in data]
            closes = np.array([float(row[1]) for row in data])

            # Рассчитываем RSI для каждого периода
            rsi_results = {}
            final_states = {}

            for period in periods:
                # Используем initial_states (передается между батчами в памяти)
                initial_state = initial_states.get(period, {})
                initial_avg_gain = initial_state.get('avg_gain')
                initial_avg_loss = initial_state.get('avg_loss')

                # Если нет initial_state - загружаем буфер данных для инициализации
                if initial_avg_gain is None:
                    # Загружаем дополнительные данные для инициализации
                    # Lookback: 10x для Wilder convergence (99.996% точности)
                    lookback_periods = period * 10
                    buffer_start = start_date - timedelta(minutes=lookback_periods * self.timeframe_minutes.get(timeframe, 1))

                    if timeframe == '1m':
                        cur.execute(f"""
                            SELECT close
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                            AND timestamp > %s
                            AND timestamp <= %s
                            ORDER BY timestamp
                        """, (self.symbol, buffer_start, start_date))
                    else:
                        # Для других таймфреймов агрегируем из 1m данных
                        interval_minutes = self.timeframe_minutes[timeframe]

                        # Вычитаем один период для загрузки достаточных 1m свечей
                        adjusted_buffer_start = buffer_start - timedelta(minutes=interval_minutes)

                        # ВАЖНО: Используем period_start (начало периода, Bybit standard)
                        if interval_minutes == 60:  # 1h
                            cur.execute(f"""
                                SELECT
                                    (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                                FROM (
                                    SELECT
                                        date_trunc('hour', timestamp) as period_start,
                                        timestamp,
                                        close
                                    FROM candles_bybit_futures_1m
                                    WHERE symbol = %s
                                    AND timestamp >= %s
                                    AND timestamp <= %s
                                ) t
                                GROUP BY period_start
                                ORDER BY period_start
                            """, (self.symbol, adjusted_buffer_start, start_date))
                        else:  # 15m
                            cur.execute(f"""
                                SELECT
                                    (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                                FROM (
                                    SELECT
                                        date_trunc('hour', timestamp) +
                                        INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as period_start,
                                        timestamp,
                                        close
                                    FROM candles_bybit_futures_1m
                                    WHERE symbol = %s
                                    AND timestamp >= %s
                                    AND timestamp <= %s
                                ) t
                                GROUP BY period_start
                                ORDER BY period_start
                            """, (self.symbol, adjusted_buffer_start, start_date))

                    buffer_data = [float(row[0]) for row in cur.fetchall()]
                    if len(buffer_data) >= period:
                        # Объединяем буферные данные с основными
                        all_closes = np.concatenate([buffer_data, closes])
                        rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                            all_closes, period
                        )
                        # Берем только RSI для основных данных
                        rsi_values = rsi_values[len(buffer_data):]
                    else:
                        # Недостаточно данных для инициализации
                        rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                            closes, period
                        )
                else:
                    # Продолжаем с переданным состоянием
                    rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                        closes, period, initial_avg_gain, initial_avg_loss
                    )

                rsi_results[period] = rsi_values
                final_states[period] = {
                    'avg_gain': avg_gain,
                    'avg_loss': avg_loss
                }

            # Обновляем данные в БД
            for i, timestamp in enumerate(timestamps):
                updates = []
                params = []

                for period in periods:
                    if i < len(rsi_results[period]):
                        rsi_value = rsi_results[period][i]
                        if not np.isnan(rsi_value):
                            updates.append(f"rsi_{period} = %s")
                            params.append(float(rsi_value))

                if updates:
                    params.extend([self.symbol, timestamp])
                    update_query = f"""
                        UPDATE {table_name}
                        SET {', '.join(updates)}
                        WHERE symbol = %s AND timestamp = %s
                    """
                    cur.execute(update_query, params)

            conn.commit()
        return final_states

    def process_periods_group(self, timeframe: str, periods: List[int],
                             batch_days: int, start_date: Optional[datetime] = None,
                             from_beginning: bool = False):
        """
        Обрабатывает группу периодов RSI

        Args:
            timeframe: Таймфрейм
            periods: Список периодов для обработки
            batch_days: Размер батча в днях
            start_date: Начальная дата
            from_beginning: Начать с самого начала (для пустых периодов)
        """
        if not periods:
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Определяем начальную дату
            if start_date:
                current_date = start_date
            elif from_beginning:
                # Начинаем с самого начала - из таблицы 1m свечей
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    current_date = result[0]
                else:
                    logger.error("❌ Нет данных для обработки")
                    return
            else:
                # Для частичных периодов начинаем с начала (single-pass approach)
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    current_date = result[0]
                else:
                    logger.error("❌ Нет данных для обработки")
                    return

            # Получаем конечную дату
            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            max_date = cur.fetchone()[0]

            if not max_date or current_date >= max_date:
                logger.info("✅ Данные уже актуальны")
                return

            # Подсчитываем батчи
            total_days = (max_date - current_date).days
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"   • Период: {total_days} дней ({current_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')})")
            logger.info(f"   • Батчей: {total_batches}")

            # Инициализируем состояния (always start fresh for single-pass calculation)
            current_states = {period: {} for period in periods}

            action = 'Загрузка' if from_beginning else 'Обновление'
            periods_str = '/'.join(map(str, periods))
            progress_desc = f"{self.symbol} {self.symbol_progress} RSI[{periods_str}] {timeframe.upper()} - {action}" if self.symbol_progress else f"{self.symbol} RSI[{periods_str}] {timeframe.upper()} - {action}"
            with tqdm(total=total_batches, desc=progress_desc, unit="batch",
                     ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n}/{total} [{elapsed}<{remaining}]') as pbar:
                batch_num = 0

                while current_date < max_date:
                    batch_end = min(current_date + timedelta(days=batch_days), max_date)

                    try:
                        # Обрабатываем батч
                        current_states = self.process_batch(
                            timeframe, periods,
                            current_date, batch_end,
                            current_states
                        )

                        batch_num += 1
                        # Добавляем информацию о символе в progress bar
                        symbol_info = f"{self.symbol} {self.symbol_progress} " if self.symbol_progress else f"{self.symbol} "
                        pbar.set_description(
                            f"{symbol_info}Батч {batch_num}/{total_batches} "
                            f"(до {batch_end.strftime('%Y-%m-%d %H:%M')})"
                        )
                        pbar.update(1)

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча: {e}")
                        import traceback
                        traceback.print_exc()
                        break

                    current_date = batch_end

    def process_timeframe(self, timeframe: str, batch_days: int = 7,
                         start_date: Optional[datetime] = None):
        """
        Process RSI for specified timeframe using single-pass calculation (like validator).

        Args:
            timeframe: Timeframe ('1m', '15m', '1h')
            batch_days: Batch size in days (for database writes only)
            start_date: Start date (None = from beginning)
        """
        # Get periods from config
        periods = self.config.get('indicators', {}).get('rsi', {}).get('periods', [14])
        batch_days = self.config.get('indicators', {}).get('rsi', {}).get('batch_days', batch_days)

        logger.info(f"📊 Обработка RSI для таймфрейма {timeframe}")
        logger.info(f"📈 Периоды RSI: {periods}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча записи: {batch_days} дней")

        # Create columns if needed
        if not self.create_rsi_columns(timeframe, periods):
            return

        # Clear existing data if force-reload enabled
        if self.force_reload:
            logger.info(f"\n🔄 Включен режим force-reload - обнуление существующих RSI данных")
            if not self.clear_rsi_columns(timeframe, periods):
                logger.error(f"❌ Не удалось обнулить RSI столбцы для {timeframe}")
                return

        # Determine date range
        end_date = datetime.now(timezone.utc)

        if start_date is None:
            # Find earliest candle
            with self.db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    start_date = result[0]
                else:
                    logger.error(f"❌ Нет данных для {self.symbol}")
                    return

        logger.info(f"📅 Период данных: {start_date.date()} → {end_date.date()}")

        # STEP 1: Load ALL data at once (with lookback)
        logger.info(f"\n🔄 ШАГ 1/3: Загрузка всех данных...")
        max_period = max(periods)
        df = self.load_all_data(timeframe, max_period, start_date, end_date)

        if df.empty:
            logger.error(f"❌ Нет данных для загрузки")
            return

        # STEP 2: Calculate RSI for all periods (single-pass)
        logger.info(f"\n🔄 ШАГ 2/3: Расчет RSI для всех периодов...")
        rsi_results = {}

        for period in tqdm(periods, desc=f"{self.symbol} {timeframe} - Расчет RSI"):
            rsi_values = self.calculate_rsi(df['close'].values, period)
            rsi_results[period] = rsi_values

        # Filter to actual data range (remove lookback)
        df_write = df[df['timestamp'] >= start_date].copy()

        for period in periods:
            # Get RSI values for actual data range
            full_rsi = rsi_results[period]
            actual_rsi = full_rsi[len(full_rsi) - len(df_write):]
            df_write[f'rsi_{period}'] = actual_rsi

        logger.info(f"   ✅ Рассчитано RSI для {len(df_write):,} свечей")

        # STEP 3: Filter and write results to database
        logger.info(f"\n🔄 ШАГ 3/3: Запись результатов в БД...")

        # Оптимизация: если не force_reload, записываем только записи где RSI IS NULL
        total_records = len(df_write)
        if self.force_reload:
            # force_reload - записываем все данные
            logger.info(f"   🔄 Режим force-reload: записываем все {total_records:,} записей")
            records_to_write = df_write
        else:
            # Оптимизация: получаем timestamps где хотя бы один RSI период IS NULL
            null_timestamps = self.get_null_timestamps(timeframe, periods)

            if not null_timestamps:
                logger.info(f"   ✅ Все RSI данные актуальны для {timeframe} - пропускаем запись")
                logger.info(f"\n✅ Обработка RSI для {timeframe} завершена!")
                return

            # Фильтруем df_write - оставляем только записи с NULL timestamps
            records_to_write = df_write[df_write['timestamp'].isin(null_timestamps)]
            skipped_records = total_records - len(records_to_write)

            logger.info(f"   📊 Статистика оптимизации:")
            logger.info(f"      • Всего рассчитано: {total_records:,} записей")
            logger.info(f"      • Нужно записать: {len(records_to_write):,} записей (RSI IS NULL)")
            logger.info(f"      • Пропущено: {skipped_records:,} записей (уже заполнены)")

        if len(records_to_write) > 0:
            self.write_results_in_batches(timeframe, periods, records_to_write, batch_days)
        else:
            logger.info(f"   ✅ Нет записей для обновления")

        logger.info(f"\n✅ Обработка RSI для {timeframe} завершена!")

    def write_results_in_batches(
        self,
        timeframe: str,
        periods: List[int],
        df: pd.DataFrame,
        batch_days: int
    ):
        """
        Write RSI results to database in batches.

        Args:
            timeframe: Timeframe
            periods: RSI periods
            df: DataFrame with timestamp, close, and rsi_* columns
            batch_days: Batch size in days
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        minutes = self.timeframe_minutes[timeframe]

        # Split data into daily batches
        start_date = df['timestamp'].min()
        end_date = df['timestamp'].max()
        current_date = start_date

        total_days = (end_date - start_date).days + 1
        total_batches = (total_days + batch_days - 1) // batch_days  # Ceiling division

        # Формируем описание прогресс-бара с номером символа
        periods_str = '/'.join(map(str, periods))
        progress_desc = f"{self.symbol} {self.symbol_progress} RSI[{periods_str}] {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} RSI[{periods_str}] {timeframe.upper()}"

        with tqdm(total=total_batches, desc=progress_desc, unit="batch",
                 ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n}/{total} [{elapsed}<{remaining}]') as pbar:
            batch_num = 0

            while current_date <= end_date:
                batch_end = min(current_date + timedelta(days=batch_days), end_date + timedelta(days=1))

                # Filter batch data
                batch_df = df[(df['timestamp'] >= current_date) & (df['timestamp'] < batch_end)]

                if not batch_df.empty:
                    # Write to database
                    with self.db.get_connection() as conn:
                        cur = conn.cursor()

                        # Build UPDATE query for all periods at once
                        set_clauses = [f"rsi_{period} = data.rsi_{period}" for period in periods]

                        # Prepare data tuples
                        values = []
                        for _, row in batch_df.iterrows():
                            value_tuple = (self.symbol, row['timestamp']) + tuple(row[f'rsi_{period}'] for period in periods)
                            values.append(value_tuple)

                        if values:
                            # Create temporary table for batch update
                            value_columns = ', '.join([f'rsi_{p}' for p in periods])
                            placeholders = ', '.join(['%s'] * (2 + len(periods)))

                            update_query = f"""
                                UPDATE {table_name} t
                                SET {', '.join(set_clauses)}
                                FROM (VALUES {', '.join([f'({placeholders})' for _ in values])})
                                AS data(symbol, timestamp, {value_columns})
                                WHERE t.symbol = data.symbol::VARCHAR
                                AND t.timestamp = data.timestamp::TIMESTAMPTZ
                            """

                            # Flatten values list
                            flat_values = [item for value_tuple in values for item in value_tuple]

                            cur.execute(update_query, flat_values)
                            conn.commit()

                batch_num += 1
                pbar.update(1)

                current_date = batch_end

        logger.info(f"   ✅ Записано {len(df):,} записей в {table_name}")

    def run(self, timeframes: List[str] = None, batch_days: int = 7,
            start_date: Optional[datetime] = None):
        """
        Запускает обработку RSI для указанных таймфреймов

        Args:
            timeframes: Список таймфреймов (если None, берет из конфига)
            batch_days: Размер батча в днях
            start_date: Начальная дата
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"🚀 Запуск RSI Loader")
        logger.info(f"📊 Таймфреймы: {timeframes}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        if start_date:
            logger.info(f"📅 Начальная дата: {start_date}")
        else:
            logger.info(f"♻️ Режим: автоматическое определение пустых столбцов")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe, batch_days, start_date)

        logger.info(f"✅ Обработка всех таймфреймов завершена!")

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='RSI Indicator Loader - загрузчик с автоматическим определением пустых столбцов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python rsi_loader.py                          # Автоматическое определение и загрузка пустых столбцов
  python rsi_loader.py --timeframe 1m           # Загрузка только 1m таймфрейма
  python rsi_loader.py --timeframes 1m,15m,1h   # Загрузка нескольких таймфреймов
  python rsi_loader.py --batch-days 7           # Использовать батчи по 7 дней
  python rsi_loader.py --start-date 2024-01-01  # Принудительно начать с конкретной даты
  python rsi_loader.py --symbol ETHUSDT         # Загрузка для другого символа
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
    parser.add_argument('--batch-days', type=int, default=7,
                       help='Размер батча в днях (по умолчанию: 7)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD (если не указана, автоматическое определение)')
    parser.add_argument('--force-reload', action='store_true',
                       help='Обнулить все RSI столбцы перед загрузкой (принудительный полный пересчет)')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Проверить и заполнить NULL значения в середине данных (полный пересчёт RSI с начала)')

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
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    # Определяем таймфреймы
    timeframes = None
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = args.timeframes.split(',')

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
        logger.info(f"🔍 Режим --check-nulls: проверка NULL значений (полный пересчёт RSI)")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        # Добавляем разделитель между торговыми парами (но не перед первой)
        if idx > 1:
            logger.info("")
            logger.info("=" * 80)

        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")

        # Создаем загрузчик и запускаем для текущего символа
        try:
            loader = RSILoader(symbol=symbol)
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            loader.force_reload = args.force_reload

            if args.check_nulls:
                # Режим проверки и заполнения NULL
                # Получаем периоды из конфига
                rsi_config = loader.config.get('indicators', {}).get('rsi', {})
                periods = rsi_config.get('periods', [7, 9, 14, 21, 25])

                # Определяем таймфреймы для проверки
                check_timeframes = timeframes if timeframes else loader.config.get('timeframes', ['1m', '15m', '1h'])

                for tf in check_timeframes:
                    if tf in loader.timeframe_minutes:
                        loader.fill_null_values(tf, periods)
            else:
                # Обычный режим загрузки
                loader.run(timeframes, args.batch_days, start_date)

            logger.info(f"✅ Символ {symbol} обработан")
        except KeyboardInterrupt:
            logger.info("⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
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

    logger.info(f"🎉 Все символы обработаны: {symbols}")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")

if __name__ == "__main__":
    main()