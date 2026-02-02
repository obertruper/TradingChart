#!/usr/bin/env python3
"""
SuperTrend Loader with Multi-Timeframe Support
===============================================
Загрузчик индикатора SuperTrend с поддержкой:
- 5 конфигураций (period, multiplier)
- Wilder smoothing для ATR
- 3-х таймфреймов (1m, 15m, 1h)
- Инкрементальных обновлений (только NULL записи)
- Полной перезагрузки (--force-reload)

Конфигурации:
- (7, 1.5)  - Scalping: быстрые сигналы
- (10, 2.0) - Standard: классический баланс
- (10, 3.0) - Conservative: меньше сигналов, выше качество
- (14, 2.5) - Medium-term: позиционная торговля
- (20, 3.0) - Long-term: крупные тренды

Колонки (20 на таймфрейм):
- supertrend_p{period}_m{multiplier*10}: значение линии SuperTrend
- supertrend_p{period}_m{multiplier*10}_dir: направление (1=UPTREND, -1=DOWNTREND)
- supertrend_p{period}_m{multiplier*10}_upper: Final Upper Band
- supertrend_p{period}_m{multiplier*10}_lower: Final Lower Band

Автор: Trading System
Дата: 2026-01-30
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

    log_filename = os.path.join(log_dir, f'supertrend_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename, encoding='utf-8')
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Логирование настроено. Лог-файл: {log_filename}")

    return logger

logger = setup_logging()


class SuperTrendLoader:
    """Загрузчик SuperTrend индикатора для разных таймфреймов"""

    # Конфигурации SuperTrend: (period, multiplier)
    CONFIGURATIONS = [
        (7, 1.5),    # Scalping
        (10, 2.0),   # Standard
        (10, 3.0),   # Conservative
        (14, 2.5),   # Medium-term
        (20, 3.0),   # Long-term
    ]

    def __init__(self, symbol: str = 'BTCUSDT', force_reload: bool = False):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
            force_reload: Пересчитать все данные с начала
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.config = self.load_config()
        self.symbol_progress = ""
        self.force_reload = force_reload

        if force_reload:
            logger.info("Режим FORCE RELOAD: пересчет всех данных с начала")

        # Генерируем имена колонок
        self.COLUMNS = self._generate_column_names()

        # Парсим таймфреймы
        self.timeframe_minutes = self._parse_timeframes()

    def _generate_column_names(self) -> List[str]:
        """Генерирует имена всех колонок SuperTrend"""
        columns = []
        for period, multiplier in self.CONFIGURATIONS:
            base_name = self._get_column_base_name(period, multiplier)
            columns.extend([
                base_name,                    # Значение SuperTrend
                f"{base_name}_dir",           # Направление
                f"{base_name}_upper",         # Upper Band
                f"{base_name}_lower",         # Lower Band
            ])
        return columns

    def _get_column_base_name(self, period: int, multiplier: float) -> str:
        """Формирует базовое имя колонки: supertrend_p{period}_m{multiplier*10}"""
        mult_int = int(multiplier * 10)
        return f"supertrend_p{period}_m{mult_int}"

    def _parse_timeframes(self) -> dict:
        """Парсит таймфреймы из конфигурации"""
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        import re
        for tf in timeframes:
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                value = int(match.group(1))
                unit = match.group(2)

                if unit == 'm':
                    timeframe_map[tf] = value
                elif unit == 'h':
                    timeframe_map[tf] = value * 60
                elif unit == 'd':
                    timeframe_map[tf] = value * 1440
                elif unit == 'w':
                    timeframe_map[tf] = value * 10080
            else:
                raise ValueError(f"Неизвестный формат таймфрейма: {tf}")

        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из indicators_config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

        if not os.path.exists(config_path):
            logger.warning(f"Конфиг не найден: {config_path}. Используем дефолты.")
            return {
                'timeframes': ['1m', '15m', '1h'],
                'supertrend': {
                    'batch_days': 1
                }
            }

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config.get('indicators', {}).get('supertrend', {
            'timeframes': ['1m', '15m', '1h'],
            'batch_days': 1
        })

    def get_table_name(self, timeframe: str) -> str:
        """Возвращает имя таблицы индикаторов для таймфрейма"""
        return f"indicators_bybit_futures_{timeframe}"

    def ensure_columns(self, timeframe: str) -> bool:
        """
        Создает колонки SuperTrend в таблице индикаторов, если их нет.

        Returns:
            True если успешно
        """
        table_name = self.get_table_name(timeframe)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Получаем существующие колонки
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                """, (table_name,))
                existing_columns = {row[0] for row in cur.fetchall()}

                # Добавляем отсутствующие колонки
                columns_added = []
                for col in self.COLUMNS:
                    if col not in existing_columns:
                        # Определяем тип колонки
                        if col.endswith('_dir'):
                            col_type = 'SMALLINT'  # 1 или -1
                        else:
                            col_type = 'NUMERIC(20,8)'

                        cur.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} {col_type}')
                        columns_added.append(col)

                if columns_added:
                    conn.commit()
                    logger.info(f"Добавлено {len(columns_added)} колонок в {table_name}")
                else:
                    logger.info(f"Все SuperTrend колонки уже существуют в {table_name}")

                return True

            except Exception as e:
                logger.error(f"Ошибка при создании колонок: {e}")
                conn.rollback()
                return False

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Получает диапазон данных в таблице свечей.

        Returns:
            (min_date, max_date)
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT MIN(timestamp), MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            result = cur.fetchone()

            if result[0] is None:
                raise ValueError(f"Нет данных для символа {self.symbol}")

            return result[0], result[1]

    def get_last_complete_period(self, max_date: datetime, timeframe: str) -> datetime:
        """
        Возвращает timestamp последнего завершенного периода.
        """
        now_utc = datetime.now(timezone.utc)

        if max_date.tzinfo is None:
            max_date = max_date.replace(tzinfo=timezone.utc)

        if max_date > now_utc:
            max_date = now_utc

        minutes = self.timeframe_minutes.get(timeframe, 1)

        # Отнимаем один период для гарантии завершенности
        last_complete = max_date - timedelta(minutes=minutes)

        # Выравниваем по границе периода
        if timeframe == '1h':
            last_complete = last_complete.replace(minute=0, second=0, microsecond=0)
        elif timeframe == '15m':
            minute_aligned = (last_complete.minute // 15) * 15
            last_complete = last_complete.replace(minute=minute_aligned, second=0, microsecond=0)
        else:
            last_complete = last_complete.replace(second=0, microsecond=0)

        return last_complete

    def load_all_candles(self, timeframe: str, start_date: datetime = None) -> pd.DataFrame:
        """
        Загружает ВСЕ свечи для расчёта SuperTrend (паттерн ATR loader).

        Args:
            timeframe: Таймфрейм
            start_date: Начальная дата (если None, берется минимальная)

        Returns:
            DataFrame с колонками: timestamp, high, low, close
        """
        if start_date is None:
            start_date, _ = self.get_data_range(timeframe)

        with self.db.get_connection() as conn:
            if timeframe == '1m':
                query = """
                    SELECT timestamp, high, low, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s AND timestamp >= %s
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, start_date),
                    parse_dates=['timestamp']
                )
            else:
                # Агрегация из 1m данных
                minutes = self.timeframe_minutes[timeframe]
                query = f"""
                    WITH time_groups AS (
                        SELECT
                            timestamp,
                            DATE_TRUNC('hour', timestamp) +
                            INTERVAL '1 minute' * (FLOOR(EXTRACT(MINUTE FROM timestamp) / {minutes}) * {minutes}) as period_start,
                            high,
                            low,
                            close
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s AND timestamp >= %s
                    )
                    SELECT
                        period_start as timestamp,
                        MAX(high) as high,
                        MIN(low) as low,
                        (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                    FROM time_groups
                    GROUP BY period_start
                    ORDER BY period_start ASC
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, start_date),
                    parse_dates=['timestamp']
                )

        # Конвертируем Decimal в float64
        for col in ['high', 'low', 'close']:
            df[col] = df[col].astype(np.float64)

        logger.info(f"Загружено {len(df):,} свечей от {df['timestamp'].min()} до {df['timestamp'].max()}")
        return df

    def calculate_atr(self, df: pd.DataFrame, period: int) -> np.ndarray:
        """
        Рассчитывает ATR с Wilder smoothing на ВСЕХ данных.

        Args:
            df: DataFrame с колонками high, low, close
            period: Период ATR

        Returns:
            numpy array с ATR значениями
        """
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values

        n = len(df)
        tr = np.zeros(n)
        atr = np.full(n, np.nan)

        # True Range
        tr[0] = high[0] - low[0]
        for i in range(1, n):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i - 1])
            lc = abs(low[i] - close[i - 1])
            tr[i] = max(hl, hc, lc)

        # Первый ATR = SMA(TR)
        if n >= period:
            atr[period - 1] = np.mean(tr[:period])

            # Wilder smoothing
            for i in range(period, n):
                atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        return atr

    def calculate_supertrend(
        self,
        df: pd.DataFrame,
        period: int,
        multiplier: float,
        atr: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Рассчитывает SuperTrend на основе ATR.

        Args:
            df: DataFrame с high, low, close
            period: Период ATR
            multiplier: Множитель ATR
            atr: Предварительно рассчитанный ATR

        Returns:
            Tuple: (supertrend, direction, upper_band, lower_band)
        """
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values

        n = len(df)

        # Инициализируем массивы
        basic_upper = np.full(n, np.nan)
        basic_lower = np.full(n, np.nan)
        final_upper = np.full(n, np.nan)
        final_lower = np.full(n, np.nan)
        supertrend = np.full(n, np.nan)
        direction = np.zeros(n, dtype=np.int8)  # 1 = UPTREND, -1 = DOWNTREND

        # Рассчитываем basic bands
        for i in range(n):
            if not np.isnan(atr[i]):
                hl2 = (high[i] + low[i]) / 2
                basic_upper[i] = hl2 + multiplier * atr[i]
                basic_lower[i] = hl2 - multiplier * atr[i]

        # Находим первый валидный индекс (после warm-up ATR)
        first_valid = period - 1
        while first_valid < n and np.isnan(atr[first_valid]):
            first_valid += 1

        if first_valid >= n:
            logger.warning(f"Недостаточно данных для SuperTrend (period={period})")
            return supertrend, direction, final_upper, final_lower

        # Инициализируем первые значения
        final_upper[first_valid] = basic_upper[first_valid]
        final_lower[first_valid] = basic_lower[first_valid]

        # Начальное направление: если close > upper → downtrend, иначе uptrend
        if close[first_valid] <= final_upper[first_valid]:
            supertrend[first_valid] = final_upper[first_valid]
            direction[first_valid] = -1  # DOWNTREND
        else:
            supertrend[first_valid] = final_lower[first_valid]
            direction[first_valid] = 1   # UPTREND

        # Основной цикл расчёта
        for i in range(first_valid + 1, n):
            if np.isnan(basic_upper[i]):
                continue

            # Final Upper Band
            if basic_upper[i] < final_upper[i - 1] or close[i - 1] > final_upper[i - 1]:
                final_upper[i] = basic_upper[i]
            else:
                final_upper[i] = final_upper[i - 1]

            # Final Lower Band
            if basic_lower[i] > final_lower[i - 1] or close[i - 1] < final_lower[i - 1]:
                final_lower[i] = basic_lower[i]
            else:
                final_lower[i] = final_lower[i - 1]

            # SuperTrend и Direction
            if direction[i - 1] == 1:  # Был UPTREND
                if close[i] >= final_lower[i]:
                    supertrend[i] = final_lower[i]
                    direction[i] = 1  # Остаёмся в UPTREND
                else:
                    supertrend[i] = final_upper[i]
                    direction[i] = -1  # Переключаемся в DOWNTREND
            else:  # Был DOWNTREND
                if close[i] <= final_upper[i]:
                    supertrend[i] = final_upper[i]
                    direction[i] = -1  # Остаёмся в DOWNTREND
                else:
                    supertrend[i] = final_lower[i]
                    direction[i] = 1  # Переключаемся в UPTREND

        return supertrend, direction, final_upper, final_lower

    def get_null_timestamps(self, timeframe: str) -> set:
        """
        Получает timestamps где хотя бы одна SuperTrend колонка IS NULL.
        """
        table_name = self.get_table_name(timeframe)

        null_conditions = ' OR '.join([f'{col} IS NULL' for col in self.COLUMNS])

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

    def write_results_in_batches(
        self,
        timeframe: str,
        df: pd.DataFrame,
        batch_days: int
    ):
        """
        Записывает результаты SuperTrend в БД батчами.

        Args:
            timeframe: Таймфрейм
            df: DataFrame с результатами
            batch_days: Размер батча в днях
        """
        table_name = self.get_table_name(timeframe)

        start_date = df['timestamp'].min()
        end_date = df['timestamp'].max()
        current_date = start_date

        total_days = (end_date - start_date).days + 1
        total_batches = (total_days + batch_days - 1) // batch_days

        with tqdm(total=total_batches, desc=f"{self.symbol} {timeframe} - Запись в БД") as pbar:
            batch_num = 0

            while current_date <= end_date:
                batch_end = min(current_date + timedelta(days=batch_days), end_date + timedelta(days=1))

                batch_df = df[(df['timestamp'] >= current_date) & (df['timestamp'] < batch_end)]

                if not batch_df.empty:
                    with self.db.get_connection() as conn:
                        cur = conn.cursor()

                        # SET clause с приведением типов
                        set_clauses = []
                        for col in self.COLUMNS:
                            if col.endswith('_dir'):
                                set_clauses.append(f"{col} = data.{col}::SMALLINT")
                            else:
                                set_clauses.append(f"{col} = data.{col}::NUMERIC")

                        values = []
                        for _, row in batch_df.iterrows():
                            value_tuple = (self.symbol, row['timestamp'])
                            for col in self.COLUMNS:
                                val = row.get(col)
                                if pd.isna(val):
                                    value_tuple += (None,)
                                elif col.endswith('_dir'):
                                    value_tuple += (int(val),)
                                else:
                                    value_tuple += (float(val),)
                            values.append(value_tuple)

                        if values:
                            value_columns = ', '.join(self.COLUMNS)
                            placeholders = ', '.join(['%s'] * (2 + len(self.COLUMNS)))

                            update_query = f"""
                                UPDATE {table_name} t
                                SET {', '.join(set_clauses)}
                                FROM (VALUES {', '.join([f'({placeholders})' for _ in values])})
                                AS data(symbol, timestamp, {value_columns})
                                WHERE t.symbol = data.symbol::VARCHAR
                                AND t.timestamp = data.timestamp::TIMESTAMPTZ
                            """

                            flat_values = [item for value_tuple in values for item in value_tuple]

                            cur.execute(update_query, flat_values)
                            conn.commit()

                batch_num += 1
                pbar.set_description(f"{self.symbol} {timeframe} - Батч {batch_num}/{total_batches}")
                pbar.update(1)

                current_date = batch_end

        logger.info(f"Записано {len(df):,} записей в {table_name}")

    def process_timeframe(self, timeframe: str, batch_days: int = 1):
        """
        Обрабатывает один таймфрейм.

        Args:
            timeframe: Таймфрейм
            batch_days: Размер батча для записи в днях
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"SuperTrend для {self.symbol} - таймфрейм {timeframe}")
        logger.info(f"{'='*80}")

        # Шаг 1: Создаём колонки
        logger.info("ШАГ 1/4: Проверка/создание колонок в БД...")
        if not self.ensure_columns(timeframe):
            logger.error("Не удалось создать колонки")
            return

        # Шаг 2: Загружаем ВСЕ свечи
        logger.info("\nШАГ 2/4: Загрузка всех исторических свечей...")
        min_date, max_date = self.get_data_range(timeframe)
        last_complete = self.get_last_complete_period(max_date, timeframe)

        logger.info(f"   Диапазон данных: {min_date} - {max_date}")
        logger.info(f"   Последний завершённый период: {last_complete}")

        load_start = time.time()
        df = self.load_all_candles(timeframe, min_date)
        load_time = time.time() - load_start

        if df.empty:
            logger.warning(f"Нет данных для {self.symbol} на {timeframe}")
            return

        logger.info(f"   Загружено за {load_time:.1f} сек")

        # Ограничиваем последним завершённым периодом
        df = df[df['timestamp'] <= last_complete].copy()
        logger.info(f"   После фильтрации: {len(df):,} свечей")

        # Шаг 3: Расчёт SuperTrend для всех конфигураций
        logger.info("\nШАГ 3/4: Расчёт SuperTrend для всех конфигураций...")

        calc_start = time.time()

        with tqdm(total=len(self.CONFIGURATIONS), desc=f"{self.symbol} {timeframe} - Расчёт") as pbar:
            for period, multiplier in self.CONFIGURATIONS:
                base_name = self._get_column_base_name(period, multiplier)
                pbar.set_description(f"{self.symbol} {timeframe} - {base_name}")

                # Рассчитываем ATR
                atr = self.calculate_atr(df, period)

                # Рассчитываем SuperTrend
                supertrend, direction, upper, lower = self.calculate_supertrend(
                    df, period, multiplier, atr
                )

                # Добавляем в DataFrame
                df[base_name] = supertrend
                df[f"{base_name}_dir"] = direction
                df[f"{base_name}_upper"] = upper
                df[f"{base_name}_lower"] = lower

                pbar.update(1)

        calc_time = time.time() - calc_start
        logger.info(f"   Расчёт завершён за {calc_time:.1f} сек")

        # Шаг 4: Запись в БД
        logger.info("\nШАГ 4/4: Запись результатов в БД...")

        total_records = len(df)

        if self.force_reload:
            logger.info(f"   Режим force-reload: записываем все {total_records:,} записей")
            records_to_write = df
        else:
            null_timestamps = self.get_null_timestamps(timeframe)

            if not null_timestamps:
                logger.info(f"   Все SuperTrend данные актуальны - пропускаем запись")
                logger.info(f"\nОбработка {timeframe} завершена!")
                return

            records_to_write = df[df['timestamp'].isin(null_timestamps)]
            skipped_records = total_records - len(records_to_write)

            logger.info(f"   Статистика оптимизации:")
            logger.info(f"      Всего рассчитано: {total_records:,} записей")
            logger.info(f"      Нужно записать: {len(records_to_write):,} записей (NULL)")
            logger.info(f"      Пропущено: {skipped_records:,} записей (уже заполнены)")

        if len(records_to_write) > 0:
            self.write_results_in_batches(timeframe, records_to_write, batch_days)
        else:
            logger.info("   Нет записей для обновления")

        logger.info(f"\nОбработка {timeframe} завершена!")

    def run(self, timeframes: List[str] = None, batch_days: int = 1):
        """
        Запускает расчёт SuperTrend для указанных таймфреймов.

        Args:
            timeframes: Список таймфреймов (если None, берётся из конфига)
            batch_days: Размер батча для записи
        """
        if timeframes is None:
            timeframes = list(self.timeframe_minutes.keys())

        logger.info(f"\nЗапуск SuperTrend Loader для {self.symbol}")
        logger.info(f"Таймфреймы: {timeframes}")
        logger.info(f"Конфигурации: {self.CONFIGURATIONS}")
        logger.info(f"Размер батча: {batch_days} дней")

        for timeframe in timeframes:
            self.process_timeframe(timeframe, batch_days)

        logger.info(f"\nВсе таймфреймы обработаны для {self.symbol}")


def main():
    """Точка входа"""
    parser = argparse.ArgumentParser(description='SuperTrend Indicator Loader')
    parser.add_argument('--symbol', type=str, help='Торговая пара (если не указана, берутся все из конфига)')
    parser.add_argument('--timeframe', type=str, help='Конкретный таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--batch-days', type=int, default=1, help='Размер батча в днях (по умолчанию 1)')
    parser.add_argument('--force-reload', action='store_true', help='Полный пересчёт всех данных')

    args = parser.parse_args()

    # Загружаем конфиг
    config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = yaml.safe_load(f)
        symbols = full_config.get('symbols', ['BTCUSDT'])
    else:
        symbols = ['BTCUSDT']

    # Определяем символы для обработки
    if args.symbol:
        symbols = [args.symbol]

    # Определяем таймфреймы
    timeframes = None
    if args.timeframe:
        timeframes = [args.timeframe]

    logger.info("=" * 80)
    logger.info("SuperTrend Loader - Начало работы")
    logger.info("=" * 80)
    logger.info(f"Символы: {symbols}")
    logger.info(f"Таймфреймы: {timeframes or 'все из конфига'}")
    logger.info(f"Batch days: {args.batch_days}")
    logger.info(f"Force reload: {args.force_reload}")

    start_time = time.time()

    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"Обработка символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        try:
            loader = SuperTrendLoader(symbol=symbol, force_reload=args.force_reload)
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            loader.run(timeframes, args.batch_days)
            logger.info(f"\nСимвол {symbol} обработан")
        except KeyboardInterrupt:
            logger.info("\nПрервано пользователем")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)

    logger.info("\n" + "=" * 80)
    logger.info("SuperTrend Loader - Завершено")
    logger.info(f"Обработано символов: {total_symbols}")
    logger.info(f"Общее время: {minutes}m {seconds}s")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
