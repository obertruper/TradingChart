#!/usr/bin/env python3
"""
Historical Volatility (HV) Loader with Single-Pass Batch Processing
====================================================================
Загрузчик индикатора Historical Volatility с поддержкой:
- Автоматического определения пустых столбцов
- Множественных периодов (7, 14, 30, 60, 90)
- Производных метрик (HV Ratio, HV Percentile)
- Батчевой записи данных (расчет в одном проходе, запись батчами)
- Любых таймфреймов (1m, 15m, 1h и т.д.)
- Инкрементальных обновлений

Historical Volatility = StdDev(log returns) × √(periods_per_year) × 100%
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm
import argparse
import warnings
import time

warnings.filterwarnings('ignore')

from indicators.database import DatabaseConnection


# Константы аннуализации для разных таймфреймов
# Количество периодов в году для каждого таймфрейма
ANNUALIZATION_FACTORS = {
    '1m': np.sqrt(525600),    # 60 * 24 * 365 = 525600 минут в году, √ ≈ 725
    '15m': np.sqrt(35040),    # 4 * 24 * 365 = 35040 15-минуток в году, √ ≈ 187
    '1h': np.sqrt(8760),      # 24 * 365 = 8760 часов в году, √ ≈ 93.6
    '4h': np.sqrt(2190),      # 6 * 365 = 2190 4-часовок в году, √ ≈ 46.8
    '1d': np.sqrt(365),       # 365 дней в году, √ ≈ 19.1
}

# Периоды для percentile расчёта (в количестве периодов для каждого таймфрейма)
# 7 дней и 90 дней
PERCENTILE_PERIODS = {
    '1m': {'7d': 10080, '90d': 129600},    # 7*24*60, 90*24*60
    '15m': {'7d': 672, '90d': 8640},       # 7*24*4, 90*24*4
    '1h': {'7d': 168, '90d': 2160},        # 7*24, 90*24
    '4h': {'7d': 42, '90d': 540},          # 7*6, 90*6
    '1d': {'7d': 7, '90d': 90},            # 7, 90
}


def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'hv_{timestamp}.log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📝 HV Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger


logger = setup_logging()


class HVLoader:
    """
    Загрузчик Historical Volatility индикаторов
    """

    # Колонки HV для создания в БД
    HV_COLUMNS = [
        'hv_7', 'hv_14', 'hv_30', 'hv_60', 'hv_90',
        'hv_ratio_7_30', 'hv_percentile_7d', 'hv_percentile_90d'
    ]

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.config = self.load_config()
        self.symbol_progress = ""
        self.timeframe_minutes = self._parse_timeframes()
        self.force_reload = False

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

    def create_hv_columns(self, timeframe: str) -> bool:
        """Создает колонки для HV если их нет"""
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
            for col_name in self.HV_COLUMNS:
                if col_name not in existing_columns:
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(10,4)
                    """)
                    columns_added.append(col_name)

            if columns_added:
                conn.commit()
                logger.info(f"✅ Добавлены колонки HV: {columns_added} в таблицу {table_name}")
            else:
                logger.info(f"ℹ️ Все колонки HV уже существуют в {table_name}")

            return True

    def clear_hv_columns(self, timeframe: str) -> bool:
        """Обнуляет все HV столбцы для указанного таймфрейма и символа"""
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                set_clauses = [f'{col} = NULL' for col in self.HV_COLUMNS]
                set_clause = ', '.join(set_clauses)

                query = f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE symbol = %s
                """

                cur.execute(query, (self.symbol,))
                rows_affected = cur.rowcount

                conn.commit()
                logger.info(f"🗑️  Обнулено {rows_affected:,} записей для HV столбцов в {table_name}")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при очистке HV столбцов: {e}")
                conn.rollback()
                return False

    def get_null_timestamps(self, timeframe: str) -> set:
        """
        Получает timestamps где хотя бы одна HV колонка IS NULL.
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        null_conditions = ' OR '.join([f'{col} IS NULL' for col in self.HV_COLUMNS])

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

    def calculate_hv(self, closes: np.ndarray, period: int) -> np.ndarray:
        """
        Рассчитывает Historical Volatility для заданного периода.

        HV = StdDev(log returns, period) × annualization_factor × 100

        Args:
            closes: Массив цен закрытия
            period: Период для расчёта

        Returns:
            Массив HV значений (в %)
        """
        if len(closes) < period + 1:
            return np.full(len(closes), np.nan)

        # Конвертируем в float64
        closes = np.asarray(closes, dtype=np.float64)

        # Рассчитываем log returns
        log_returns = np.log(closes[1:] / closes[:-1])

        # Prepend NaN to align with closes
        log_returns = np.concatenate([[np.nan], log_returns])

        # Rolling standard deviation
        hv_values = np.full(len(closes), np.nan)

        for i in range(period, len(closes)):
            window = log_returns[i - period + 1:i + 1]
            if not np.any(np.isnan(window)):
                hv_values[i] = np.std(window, ddof=1)  # Sample std dev

        return hv_values

    def calculate_hv_ratio(self, hv_short: np.ndarray, hv_long: np.ndarray) -> np.ndarray:
        """
        Рассчитывает HV Ratio = HV_short / HV_long

        Args:
            hv_short: Массив HV для короткого периода
            hv_long: Массив HV для длинного периода

        Returns:
            Массив HV Ratio
        """
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = hv_short / hv_long
            ratio = np.where(np.isinf(ratio), np.nan, ratio)
        return ratio

    def calculate_hv_percentile(self, hv_values: np.ndarray, lookback: int) -> np.ndarray:
        """
        Рассчитывает HV Percentile - ранг текущего HV относительно истории.

        Percentile = (количество значений < текущего) / lookback × 100

        Args:
            hv_values: Массив HV значений
            lookback: Количество периодов для сравнения

        Returns:
            Массив HV Percentile (0-100)
        """
        percentile = np.full(len(hv_values), np.nan)

        for i in range(lookback, len(hv_values)):
            if np.isnan(hv_values[i]):
                continue

            # Получаем окно исторических значений
            window = hv_values[i - lookback:i]

            # Удаляем NaN
            valid_window = window[~np.isnan(window)]

            if len(valid_window) > 0:
                # Считаем сколько значений меньше текущего
                count_less = np.sum(valid_window < hv_values[i])
                percentile[i] = (count_less / len(valid_window)) * 100

        return percentile

    def load_all_data(
        self,
        timeframe: str,
        max_lookback: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Загружает ВСЕ данные свечей для расчёта HV.

        Args:
            timeframe: '1m', '15m', или '1h'
            max_lookback: Максимальный lookback для percentile
            start_date: Начальная дата
            end_date: Конечная дата

        Returns:
            DataFrame с колонками: timestamp, close
        """
        minutes = self.timeframe_minutes[timeframe]

        # Lookback для percentile 90 дней + буфер для HV периодов
        lookback_minutes = max_lookback * minutes + 100 * minutes

        if start_date:
            adjusted_start = start_date - timedelta(minutes=lookback_minutes)
        else:
            adjusted_start = None

        if end_date is None:
            end_date = datetime.now(timezone.utc)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            if timeframe == '1m':
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
                # Для агрегированных таймфреймов
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

    def process_timeframe(self, timeframe: str, batch_days: int = 1,
                         start_date: Optional[datetime] = None):
        """
        Обрабатывает HV для указанного таймфрейма.

        Args:
            timeframe: Таймфрейм ('1m', '15m', '1h')
            batch_days: Размер батча в днях (для записи в БД)
            start_date: Начальная дата
        """
        # Получаем периоды из конфига
        hv_config = self.config.get('indicators', {}).get('hv', {})
        periods = hv_config.get('periods', [7, 14, 30, 60, 90])
        batch_days = hv_config.get('batch_days', batch_days)

        logger.info(f"📊 Обработка HV для таймфрейма {timeframe}")
        logger.info(f"📈 Периоды HV: {periods}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча записи: {batch_days} дней")

        # Создаём колонки если нужно
        if not self.create_hv_columns(timeframe):
            return

        # Очищаем данные если force-reload
        if self.force_reload:
            logger.info(f"\n🔄 Включен режим force-reload - обнуление существующих HV данных")
            if not self.clear_hv_columns(timeframe):
                logger.error(f"❌ Не удалось обнулить HV столбцы для {timeframe}")
                return

        # Определяем диапазон дат
        end_date = datetime.now(timezone.utc)

        if start_date is None:
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

        # Получаем периоды для percentile
        percentile_periods = PERCENTILE_PERIODS.get(timeframe, {'7d': 168, '90d': 2160})
        max_lookback = percentile_periods['90d']

        # ШАГ 1: Загружаем ВСЕ данные
        logger.info(f"\n🔄 ШАГ 1/3: Загрузка всех данных...")
        df = self.load_all_data(timeframe, max_lookback, start_date, end_date)

        if df.empty:
            logger.error(f"❌ Нет данных для загрузки")
            return

        # ШАГ 2: Расчёт HV для всех периодов
        logger.info(f"\n🔄 ШАГ 2/3: Расчёт HV для всех периодов...")

        closes = df['close'].values

        # Получаем коэффициент аннуализации
        annualization = ANNUALIZATION_FACTORS.get(timeframe, np.sqrt(8760))

        # Рассчитываем HV для каждого периода
        hv_results = {}
        for period in tqdm(periods, desc=f"{self.symbol} {timeframe} - Расчёт HV"):
            hv_raw = self.calculate_hv(closes, period)
            # Аннуализируем и переводим в проценты
            hv_results[period] = hv_raw * annualization * 100

        # Рассчитываем производные метрики
        logger.info(f"   📈 Расчёт производных метрик...")

        # HV Ratio (7/30)
        hv_ratio_7_30 = self.calculate_hv_ratio(hv_results[7], hv_results[30])

        # HV Percentile (7 дней и 90 дней)
        # Используем HV_30 как базовый для percentile
        hv_percentile_7d = self.calculate_hv_percentile(
            hv_results[30], percentile_periods['7d']
        )
        hv_percentile_90d = self.calculate_hv_percentile(
            hv_results[30], percentile_periods['90d']
        )

        # Фильтруем до актуального диапазона данных
        df_write = df[df['timestamp'] >= start_date].copy()

        # Добавляем все HV колонки
        for period in periods:
            full_hv = hv_results[period]
            actual_hv = full_hv[len(full_hv) - len(df_write):]
            df_write[f'hv_{period}'] = actual_hv

        # Добавляем производные метрики
        df_write['hv_ratio_7_30'] = hv_ratio_7_30[len(hv_ratio_7_30) - len(df_write):]
        df_write['hv_percentile_7d'] = hv_percentile_7d[len(hv_percentile_7d) - len(df_write):]
        df_write['hv_percentile_90d'] = hv_percentile_90d[len(hv_percentile_90d) - len(df_write):]

        logger.info(f"   ✅ Рассчитано HV для {len(df_write):,} свечей")

        # ШАГ 3: Запись результатов в БД
        logger.info(f"\n🔄 ШАГ 3/3: Запись результатов в БД...")

        total_records = len(df_write)
        if self.force_reload:
            logger.info(f"   🔄 Режим force-reload: записываем все {total_records:,} записей")
            records_to_write = df_write
        else:
            null_timestamps = self.get_null_timestamps(timeframe)

            if not null_timestamps:
                logger.info(f"   ✅ Все HV данные актуальны для {timeframe} - пропускаем запись")
                logger.info(f"\n✅ Обработка HV для {timeframe} завершена!")
                return

            records_to_write = df_write[df_write['timestamp'].isin(null_timestamps)]
            skipped_records = total_records - len(records_to_write)

            logger.info(f"   📊 Статистика оптимизации:")
            logger.info(f"      • Всего рассчитано: {total_records:,} записей")
            logger.info(f"      • Нужно записать: {len(records_to_write):,} записей (HV IS NULL)")
            logger.info(f"      • Пропущено: {skipped_records:,} записей (уже заполнены)")

        if len(records_to_write) > 0:
            self.write_results_in_batches(timeframe, periods, records_to_write, batch_days)
        else:
            logger.info(f"   ✅ Нет записей для обновления")

        logger.info(f"\n✅ Обработка HV для {timeframe} завершена!")

    def write_results_in_batches(
        self,
        timeframe: str,
        periods: List[int],
        df: pd.DataFrame,
        batch_days: int
    ):
        """
        Записывает результаты HV в БД батчами.

        Args:
            timeframe: Таймфрейм
            periods: Периоды HV
            df: DataFrame с результатами
            batch_days: Размер батча в днях
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

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

                        # Все HV колонки
                        all_columns = [f'hv_{p}' for p in periods] + [
                            'hv_ratio_7_30', 'hv_percentile_7d', 'hv_percentile_90d'
                        ]

                        set_clauses = [f"{col} = data.{col}" for col in all_columns]

                        values = []
                        for _, row in batch_df.iterrows():
                            value_tuple = (self.symbol, row['timestamp'])
                            for col in all_columns:
                                val = row[col]
                                if pd.isna(val):
                                    value_tuple += (None,)
                                else:
                                    value_tuple += (float(val),)
                            values.append(value_tuple)

                        if values:
                            value_columns = ', '.join(all_columns)
                            placeholders = ', '.join(['%s'] * (2 + len(all_columns)))

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

        logger.info(f"   ✅ Записано {len(df):,} записей в {table_name}")

    def run(self, timeframes: List[str] = None, batch_days: int = 1,
            start_date: Optional[datetime] = None):
        """
        Запускает обработку HV для указанных таймфреймов.

        Args:
            timeframes: Список таймфреймов (если None, берет из конфига)
            batch_days: Размер батча в днях
            start_date: Начальная дата
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"🚀 Запуск HV Loader")
        logger.info(f"📊 Таймфреймы: {timeframes}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        if start_date:
            logger.info(f"📅 Начальная дата: {start_date}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe, batch_days, start_date)

        logger.info(f"✅ Обработка всех таймфреймов завершена!")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='Historical Volatility (HV) Loader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python hv_loader.py                          # Загрузка для всех символов
  python hv_loader.py --symbol BTCUSDT         # Только для BTCUSDT
  python hv_loader.py --timeframe 1h           # Только часовой таймфрейм
  python hv_loader.py --force-reload           # Полный пересчёт всех данных
        """
    )

    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую')
    parser.add_argument('--timeframe', type=str,
                       help='Один таймфрейм для обработки')
    parser.add_argument('--timeframes', type=str,
                       help='Несколько таймфреймов через запятую')
    parser.add_argument('--batch-days', type=int, default=1,
                       help='Размер батча в днях (по умолчанию: 1)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD')
    parser.add_argument('--force-reload', action='store_true',
                       help='Полный пересчёт всех данных')

    args = parser.parse_args()

    # Определяем символы
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        symbols = [args.symbol]
    else:
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
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

    # Парсим дату
    start_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"❌ Неверный формат даты: {args.start_date}. Используйте YYYY-MM-DD")
            sys.exit(1)

    logger.info(f"🎯 Обработка символов: {symbols}")

    start_time = time.time()

    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        if idx > 1:
            logger.info("")
            logger.info("=" * 80)

        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")

        try:
            loader = HVLoader(symbol=symbol)
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            loader.force_reload = args.force_reload
            loader.run(timeframes, args.batch_days, start_date)
            logger.info(f"✅ Символ {symbol} обработан")
        except KeyboardInterrupt:
            logger.info("⚠️ Прервано пользователем.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info(f"🎉 Все символы обработаны: {symbols}")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()
