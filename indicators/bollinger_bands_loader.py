#!/usr/bin/env python3
"""
Bollinger Bands Loader
======================
Загрузчик индикатора Bollinger Bands в базу данных PostgreSQL.

Bollinger Bands (BB) - индикатор волатильности, созданный Джоном Боллинджером в 1980-х.
Состоит из трёх линий:
- Middle Band (средняя полоса) = SMA или EMA
- Upper Band (верхняя полоса) = Middle + k × σ
- Lower Band (нижняя полоса) = Middle - k × σ

Где:
- k = множитель стандартного отклонения (обычно 2.0)
- σ = стандартное отклонение цены за период

Дополнительные метрики:
- %B (Percent B) = (Close - Lower) / (Upper - Lower) - позиция цены внутри полос
- Bandwidth = (Upper - Lower) / Middle × 100 - ширина полос в процентах
- Squeeze = Bandwidth < 5% - флаг сжатия полос

Конфигурации:
==============
13 конфигураций (11 SMA + 2 EMA):

SMA-based:
1. Ultra Fast (3, 2.0) - экстремально быстрый
2. Scalping (5, 2.0) - для скальпинга
3. Short (10, 1.5) - краткосрочный узкий
4. Intraday (14, 2.0) - для интрадея
5. Tight (20, 1.0) - узкие полосы
6. Golden (20, 1.618) - золотое сечение
7. Classic (20, 2.0) - стандарт индустрии
8. Wide (20, 3.0) - широкие полосы
9. Fibonacci (21, 2.0) - период Фибоначчи
10. Fibonacci Medium (34, 2.0) - среднесрочный
11. Fibonacci Long (89, 2.0) - долгосрочный

EMA-based:
12. Classic EMA (20, 2.0) - быстрая реакция
13. Golden EMA (20, 1.618) - золотое сечение EMA

Особенности реализации:
=======================
- Независимый расчёт (рассчитывает SMA/EMA самостоятельно)
- ОПТИМИЗИРОВАНО: Все конфигурации обрабатываются параллельно на одних данных
- Single data load + SMA/std caching для производительности (~3-4x ускорение)
- Safe division: NaN вместо infinity для bandwidth/percent_b при делении на ноль
- Batch processing по 1 дню
- Lookback × 3 для точности на границах
- Squeeze threshold = 5%
- Агрегация для 15m/1h: LAST(close) из минутных свечей
- MIN checkpoint sync: все конфигурации синхронизированы через MIN(last_date)

Использование:
==============
# Загрузка всех конфигураций и таймфреймов
python indicators/bollinger_bands_loader.py

# Конкретный таймфрейм
python indicators/bollinger_bands_loader.py --timeframe 1m --batch-days 1

# С увеличенным batch size
python indicators/bollinger_bands_loader.py --batch-days 7

Автор: Claude Code
Дата создания: 2025-10-16
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from tqdm import tqdm

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators.database import DatabaseConnection


# Конфигурации Bollinger Bands
BOLLINGER_CONFIGS = [
    # SMA-based конфигурации (11 штук)
    {'name': 'ultrafast', 'period': 3, 'std_dev': 2.0, 'base': 'sma', 'description': 'Extreme fast for squeeze detection'},
    {'name': 'scalping', 'period': 5, 'std_dev': 2.0, 'base': 'sma', 'description': 'Ultra-short for scalping'},
    {'name': 'short', 'period': 10, 'std_dev': 1.5, 'base': 'sma', 'description': 'Short-term tight combination'},
    {'name': 'intraday', 'period': 14, 'std_dev': 2.0, 'base': 'sma', 'description': 'Faster bands for intraday trading'},
    {'name': 'tight', 'period': 20, 'std_dev': 1.0, 'base': 'sma', 'description': 'Very tight bands for frequent signals'},
    {'name': 'golden', 'period': 20, 'std_dev': 1.618, 'base': 'sma', 'description': 'Golden ratio deviation (Fibonacci 1.618)'},
    {'name': 'classic', 'period': 20, 'std_dev': 2.0, 'base': 'sma', 'description': 'Classic Bollinger (20, 2) - industry standard'},
    {'name': 'wide', 'period': 20, 'std_dev': 3.0, 'base': 'sma', 'description': 'Wide bands for extreme detection'},
    {'name': 'fibonacci', 'period': 21, 'std_dev': 2.0, 'base': 'sma', 'description': 'Fibonacci period (21, 2)'},
    {'name': 'fibonacci_medium', 'period': 34, 'std_dev': 2.0, 'base': 'sma', 'description': 'Fibonacci medium-term (34, 2)'},
    {'name': 'fibonacci_long', 'period': 89, 'std_dev': 2.0, 'base': 'sma', 'description': 'Fibonacci long-term (89, 2) for smooth trends'},

    # EMA-based конфигурации (2 штуки)
    {'name': 'classic_ema', 'period': 20, 'std_dev': 2.0, 'base': 'ema', 'description': 'Classic BB on EMA for faster reaction'},
    {'name': 'golden_ema', 'period': 20, 'std_dev': 1.618, 'base': 'ema', 'description': 'Golden ratio on EMA base'},
]


class BollingerBandsLoader:
    """
    Загрузчик Bollinger Bands индикатора

    Рассчитывает BB для всех конфигураций последовательно с checkpoint системой.
    """

    def __init__(self, symbol: str = 'BTCUSDT', batch_days: int = 1,
                 lookback_multiplier: int = 3, squeeze_threshold: float = 5.0,
                 force_reload: bool = False, check_nulls: bool = False):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара (по умолчанию BTCUSDT)
            batch_days: Размер батча в днях (по умолчанию 1)
            lookback_multiplier: Множитель для lookback периода (по умолчанию 3)
            squeeze_threshold: Порог для определения squeeze в % (по умолчанию 5.0)
            force_reload: Полный пересчёт всех данных
            check_nulls: Проверить и заполнить NULL значения в середине данных
        """
        self.symbol = symbol
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.batch_days = batch_days
        self.lookback_multiplier = lookback_multiplier
        self.squeeze_threshold = squeeze_threshold
        self.force_reload = force_reload
        self.check_nulls = check_nulls
        self.db = DatabaseConnection()
        self.logger = logging.getLogger(__name__)

    def format_std_dev(self, std_dev: float) -> str:
        """
        Форматирует std_dev для имени колонки (заменяет точку на подчёркивание)

        Args:
            std_dev: Стандартное отклонение (например, 2.0, 1.618)

        Returns:
            Строка для имени колонки (например, '2_0', '1_618')
        """
        return str(std_dev).replace('.', '_')

    def get_column_names(self, period: int, std_dev: float, base: str) -> Dict[str, str]:
        """
        Генерирует имена колонок для конфигурации BB

        Args:
            period: Период BB
            std_dev: Стандартное отклонение
            base: База расчёта ('sma' или 'ema')

        Returns:
            Dict с именами колонок: upper, middle, lower, percent_b, bandwidth, squeeze
        """
        std_str = self.format_std_dev(std_dev)
        prefix = f"bollinger_bands_{base}_{period}_{std_str}"

        return {
            'upper': f"{prefix}_upper",
            'middle': f"{prefix}_middle",
            'lower': f"{prefix}_lower",
            'percent_b': f"{prefix}_percent_b",
            'bandwidth': f"{prefix}_bandwidth",
            'squeeze': f"{prefix}_squeeze",
        }

    def ensure_columns_exist(self, timeframe: str, config: Dict):
        """
        Проверяет и создаёт колонки для конфигурации BB

        Args:
            timeframe: Таймфрейм (например, '1m')
            config: Конфигурация BB
        """
        table_name = f"indicators_bybit_futures_{timeframe}"
        columns = self.get_column_names(config['period'], config['std_dev'], config['base'])

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            for col_type, col_name in columns.items():
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
                    # Определяем тип данных
                    if col_type == 'squeeze':
                        col_type_sql = 'BOOLEAN'
                    elif col_type in ['percent_b', 'bandwidth']:
                        col_type_sql = 'DECIMAL(10,4)'
                    else:
                        col_type_sql = 'DECIMAL(20,8)'

                    self.logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type_sql};")
                    conn.commit()
                    self.logger.info(f"✅ Колонка {col_name} создана")

            cur.close()

    def calculate_bollinger_bands(self, close_prices: pd.Series, period: int,
                                  std_dev: float, base: str = 'sma',
                                  cached_sma: pd.Series = None,
                                  cached_std: pd.Series = None) -> Dict[str, pd.Series]:
        """
        Рассчитывает Bollinger Bands и метрики с поддержкой кэширования

        Args:
            close_prices: Series с ценами закрытия
            period: Период для расчёта (например, 20)
            std_dev: Множитель стандартного отклонения (например, 2.0)
            base: База расчёта ('sma' или 'ema')
            cached_sma: Опциональный кэшированный SMA(period) - для ускорения
            cached_std: Опциональный кэшированный rolling std(period) - для ускорения

        Returns:
            Dict с Series: upper, middle, lower, percent_b, bandwidth, squeeze
        """
        # Конвертируем цены в float (из Decimal)
        close_prices = close_prices.astype(float)

        # 1. Рассчитываем среднюю полосу (SMA или EMA)
        if base == 'sma':
            # Используем кэш если есть, иначе рассчитываем
            if cached_sma is not None:
                middle_band = cached_sma
            else:
                middle_band = close_prices.rolling(window=period).mean()
        elif base == 'ema':
            # EMA не кэшируем (используется реже)
            middle_band = close_prices.ewm(span=period, adjust=False).mean()
        else:
            raise ValueError(f"Unknown base: {base}")

        # 2. Стандартное отклонение (всегда от close, не от EMA!)
        # Используем кэш если есть, иначе рассчитываем
        if cached_std is not None:
            rolling_std = cached_std
        else:
            rolling_std = close_prices.rolling(window=period).std()

        # 3. Верхняя и нижняя полосы
        upper_band = middle_band + (std_dev * rolling_std)
        lower_band = middle_band - (std_dev * rolling_std)

        # 4. Дополнительные метрики
        # %B = (Close - Lower) / (Upper - Lower)
        band_range = upper_band - lower_band

        # Безопасное деление для percent_b (защита от division by zero)
        # Когда band_range = 0 (upper = lower), возвращаем NaN вместо infinity
        percent_b = pd.Series(index=close_prices.index, dtype=float)
        mask_valid_range = band_range != 0
        percent_b[mask_valid_range] = (close_prices[mask_valid_range] - lower_band[mask_valid_range]) / band_range[mask_valid_range]
        percent_b[~mask_valid_range] = np.nan

        # Bandwidth = (Upper - Lower) / Middle × 100
        # Безопасное деление для bandwidth (защита от division by zero)
        # Когда middle_band = 0, возвращаем NaN вместо infinity
        bandwidth = pd.Series(index=close_prices.index, dtype=float)
        mask_valid_middle = middle_band != 0
        bandwidth[mask_valid_middle] = (band_range[mask_valid_middle] / middle_band[mask_valid_middle]) * 100
        bandwidth[~mask_valid_middle] = np.nan

        # Squeeze flag: bandwidth < threshold (только для валидных значений)
        squeeze = bandwidth < self.squeeze_threshold

        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band,
            'percent_b': percent_b,
            'bandwidth': bandwidth,
            'squeeze': squeeze
        }

    def get_last_processed_date(self, timeframe: str, config: Dict) -> Optional[datetime]:
        """
        Получает дату последней обработанной записи для конфигурации

        Args:
            timeframe: Таймфрейм
            config: Конфигурация BB

        Returns:
            Datetime последней обработанной записи или None
        """
        table_name = f"indicators_bybit_futures_{timeframe}"
        columns = self.get_column_names(config['period'], config['std_dev'], config['base'])
        upper_col = columns['upper']

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Ищем последнюю заполненную дату
            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM {table_name}
                WHERE symbol = %s AND {upper_col} IS NOT NULL
            """, (self.symbol,))

            result = cur.fetchone()
            cur.close()

            return result[0] if result and result[0] else None

    def get_all_last_processed_dates(self, timeframe: str, configs: List[Dict]) -> Optional[datetime]:
        """
        Получает минимальную дату среди всех конфигураций (для синхронизации)

        Args:
            timeframe: Таймфрейм
            configs: Список конфигураций BB

        Returns:
            MIN(last_processed_date) среди всех конфигураций или None
        """
        last_dates = []

        for config in configs:
            last_date = self.get_last_processed_date(timeframe, config)
            if last_date:
                last_dates.append(last_date)

        # Если хотя бы одна конфигурация обработана, возвращаем MIN
        if last_dates:
            return min(last_dates)

        # Если ни одна конфигурация не обработана
        return None

    def get_null_timestamps(self, timeframe: str, configs: List[Dict]) -> set:
        """
        Находит timestamps где хотя бы одна BB колонка IS NULL,
        исключая естественные NULL в начале данных.

        Args:
            timeframe: Таймфрейм
            configs: Список конфигураций BB

        Returns:
            set timestamps с NULL значениями
        """
        table_name = f"indicators_bybit_futures_{timeframe}"

        # Собираем все колонки upper для всех конфигов
        all_upper_cols = []
        for config in configs:
            columns = self.get_column_names(config['period'], config['std_dev'], config['base'])
            all_upper_cols.append(columns['upper'])

        null_conditions = ' OR '.join([f'{col} IS NULL' for col in all_upper_cols])

        # Определяем границу естественных NULL (max_period * timeframe_minutes)
        max_period = max(config['period'] for config in configs)
        timeframe_minutes_map = {'1m': 1, '15m': 15, '1h': 60, '4h': 240, '1d': 1440}
        minutes = timeframe_minutes_map.get(timeframe, 1)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Минимальная дата данных
            cur.execute(f"""
                SELECT MIN(timestamp)
                FROM {table_name}
                WHERE symbol = %s
            """, (self.symbol,))
            result = cur.fetchone()
            if not result or not result[0]:
                cur.close()
                return set()

            from datetime import timedelta
            boundary = result[0] + timedelta(minutes=max_period * minutes)

            # Ищем NULL после границы
            cur.execute(f"""
                SELECT timestamp
                FROM {table_name}
                WHERE symbol = %s
                  AND ({null_conditions})
                  AND timestamp >= %s
            """, (self.symbol, boundary))

            timestamps = {row[0] for row in cur.fetchall()}
            cur.close()
            return timestamps

    def get_max_lookback_period(self, configs: List[Dict]) -> int:
        """
        Вычисляет максимальный lookback период среди всех конфигураций

        Args:
            configs: Список конфигураций BB

        Returns:
            MAX(period × lookback_multiplier) среди всех конфигураций
        """
        max_period = max(config['period'] for config in configs)
        return max_period * self.lookback_multiplier

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Получает диапазон доступных данных в таблице candles

        Args:
            timeframe: Таймфрейм

        Returns:
            Tuple (min_date, max_date)
        """
        candles_table = f"candles_bybit_futures_{timeframe}"

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute(f"""
                SELECT MIN(timestamp), MAX(timestamp)
                FROM {candles_table}
                WHERE symbol = %s
            """, (self.symbol,))

            min_date, max_date = cur.fetchone()
            cur.close()

            return min_date, max_date

    def aggregate_1m_to_timeframe(self, df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Агрегирует 1m данные в старший таймфрейм

        Args:
            df_1m: DataFrame с минутными данными
            timeframe: Целевой таймфрейм ('15m', '1h', '4h', '1d')

        Returns:
            Агрегированный DataFrame
        """
        if timeframe == '1m':
            return df_1m

        # Определяем правило агрегации
        rule_map = {
            '15m': '15min',
            '1h': '1h',
            '4h': '4h',
            '1d': '1D',
        }

        if timeframe not in rule_map:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        rule = rule_map[timeframe]

        # Агрегируем: берём LAST close из каждого периода
        df_agg = df_1m.resample(rule, label='left', closed='left').agg({
            'close': 'last'
        }).dropna()

        return df_agg

    def load_batch_data_once(self, start_date: datetime, end_date: datetime,
                            timeframe: str, lookback_periods: int) -> pd.DataFrame:
        """
        Загружает данные из БД ОДИН РАЗ для батча (оптимизация)

        Args:
            start_date: Начальная дата (для записи в БД)
            end_date: Конечная дата
            timeframe: Таймфрейм ('1m', '15m', '1h')
            lookback_periods: Количество периодов для lookback

        Returns:
            DataFrame с агрегированными данными (с lookback)
        """
        candles_table = "candles_bybit_futures_1m"

        # Вычисляем lookback start (зависит от таймфрейма)
        if timeframe == '1m':
            lookback_start = start_date - timedelta(minutes=lookback_periods)
        elif timeframe == '15m':
            lookback_start = start_date - timedelta(minutes=lookback_periods * 15)
        elif timeframe == '1h':
            lookback_start = start_date - timedelta(hours=lookback_periods)
        elif timeframe == '4h':
            lookback_start = start_date - timedelta(hours=lookback_periods * 4)
        elif timeframe == '1d':
            lookback_start = start_date - timedelta(days=lookback_periods)
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Загружаем данные из БД
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            query = f"""
                SELECT timestamp, close
                FROM {candles_table}
                WHERE symbol = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                ORDER BY timestamp
            """

            cur.execute(query, (self.symbol, lookback_start, end_date))
            rows = cur.fetchall()
            cur.close()

        if not rows:
            return pd.DataFrame()

        # Создаём DataFrame
        df = pd.DataFrame(rows, columns=['timestamp', 'close'])
        df.set_index('timestamp', inplace=True)

        # Агрегируем в нужный таймфрейм
        df_agg = self.aggregate_1m_to_timeframe(df, timeframe)

        return df_agg

    def load_configuration(self, config: Dict, timeframe: str, start_date: datetime, end_date: datetime):
        """
        Загружает данные для одной конфигурации BB

        Args:
            config: Конфигурация BB
            timeframe: Таймфрейм
            start_date: Начальная дата
            end_date: Конечная дата
        """
        period = config['period']
        std_dev = config['std_dev']
        base = config['base']
        name = config['name']

        self.logger.info(f"🔄 Обработка конфигурации: {name} ({period}, {std_dev}) {base.upper()}")

        # Создаём колонки если нужно
        self.ensure_columns_exist(timeframe, config)

        # Определяем lookback период
        lookback_periods = period * self.lookback_multiplier

        # Получаем имена колонок
        columns = self.get_column_names(period, std_dev, base)

        # Таблицы
        candles_table = f"candles_bybit_futures_1m"  # Всегда берём из 1m
        indicators_table = f"indicators_bybit_futures_{timeframe}"

        # Обрабатываем данные батчами
        current_date = start_date

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Прогресс-бар
            total_days = (end_date - start_date).days
            pbar = tqdm(total=total_days,
                       desc=f"{self.symbol} {self.symbol_progress} BB-{name}({period},{std_dev},{base.upper()}) {timeframe.upper()}",
                       unit='day',
                       ncols=100,
                       bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')

            while current_date < end_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

                # Вычисляем lookback start
                if timeframe == '1m':
                    lookback_start = current_date - timedelta(minutes=lookback_periods)
                elif timeframe == '15m':
                    lookback_start = current_date - timedelta(minutes=lookback_periods * 15)
                elif timeframe == '1h':
                    lookback_start = current_date - timedelta(hours=lookback_periods)
                elif timeframe == '4h':
                    lookback_start = current_date - timedelta(hours=lookback_periods * 4)
                elif timeframe == '1d':
                    lookback_start = current_date - timedelta(days=lookback_periods)

                # Загружаем данные из 1m таблицы
                query = f"""
                    SELECT timestamp, close
                    FROM {candles_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp < %s
                    ORDER BY timestamp
                """

                cur.execute(query, (self.symbol, lookback_start, batch_end))
                rows = cur.fetchall()

                if not rows:
                    current_date = batch_end
                    pbar.update((batch_end - current_date).days + self.batch_days)
                    continue

                # Создаём DataFrame
                df = pd.DataFrame(rows, columns=['timestamp', 'close'])
                df.set_index('timestamp', inplace=True)

                # Агрегируем в нужный таймфрейм
                df_agg = self.aggregate_1m_to_timeframe(df, timeframe)

                if df_agg.empty:
                    current_date = batch_end
                    pbar.update((batch_end - current_date).days + self.batch_days)
                    continue

                # Рассчитываем BB
                bb_data = self.calculate_bollinger_bands(df_agg['close'], period, std_dev, base)

                # Фильтруем только данные текущего батча (без lookback)
                mask = (df_agg.index >= current_date) & (df_agg.index < batch_end)

                # Подготавливаем данные для UPDATE
                update_data = []
                for ts in df_agg.index[mask]:
                    if pd.notna(bb_data['upper'].loc[ts]):
                        update_data.append((
                            float(bb_data['upper'].loc[ts]),
                            float(bb_data['middle'].loc[ts]),
                            float(bb_data['lower'].loc[ts]),
                            float(bb_data['percent_b'].loc[ts]) if pd.notna(bb_data['percent_b'].loc[ts]) else None,
                            float(bb_data['bandwidth'].loc[ts]) if pd.notna(bb_data['bandwidth'].loc[ts]) else None,
                            bool(bb_data['squeeze'].loc[ts]) if pd.notna(bb_data['squeeze'].loc[ts]) else None,
                            self.symbol,
                            ts
                        ))

                # Batch UPDATE
                if update_data:
                    update_query = f"""
                        UPDATE {indicators_table}
                        SET {columns['upper']} = %s,
                            {columns['middle']} = %s,
                            {columns['lower']} = %s,
                            {columns['percent_b']} = %s,
                            {columns['bandwidth']} = %s,
                            {columns['squeeze']} = %s
                        WHERE symbol = %s AND timestamp = %s
                    """

                    cur.executemany(update_query, update_data)
                    conn.commit()

                # Обновляем прогресс
                pbar.update(self.batch_days)
                current_date = batch_end

            pbar.close()
            cur.close()

        self.logger.info(f"✅ Конфигурация {name} обработана")

    def load_timeframe(self, timeframe: str, configs: List[Dict]):
        """
        Загружает BB для всех конфигураций на одном таймфрейме (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ)

        Оптимизация:
        - Загружает данные из БД ОДИН РАЗ для всех конфигураций
        - Кэширует SMA и rolling std для одинаковых периодов
        - Значительно быстрее старой версии (~3-4x)

        Args:
            timeframe: Таймфрейм (например, '1m')
            configs: Список конфигураций BB
        """
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"⏰ Таймфрейм: {timeframe}")
        self.logger.info(f"{'='*80}")

        # Получаем диапазон данных
        min_date, max_date = self.get_data_range('1m')  # Всегда из 1m

        self.logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Ограничиваем max_date до последнего завершенного периода
        if timeframe == '15m':
            max_date = max_date.replace(minute=(max_date.minute // 15) * 15, second=0, microsecond=0)
        elif timeframe == '1h':
            max_date = max_date.replace(minute=0, second=0, microsecond=0)
        elif timeframe == '4h':
            max_date = max_date.replace(hour=(max_date.hour // 4) * 4, minute=0, second=0, microsecond=0)
        elif timeframe == '1d':
            max_date = max_date.replace(hour=0, minute=0, second=0, microsecond=0)

        self.logger.info(f"⏸️  Ограничение max_date до последнего завершенного периода: {max_date}")

        # Создаём колонки для всех конфигураций
        for config in configs:
            self.ensure_columns_exist(timeframe, config)

        # Определяем режим и начальную дату
        null_timestamps = None

        if self.force_reload:
            start_date = min_date
            self.logger.info(f"🔄 Режим force-reload: пересчёт с начала ({start_date})")
        elif self.check_nulls:
            # Режим --check-nulls: ищем NULL после естественной границы
            null_ts = self.get_null_timestamps(timeframe, configs)
            if not null_ts:
                self.logger.info(f"✅ Все BB данные актуальны для {timeframe} — пропускаем")
                return
            null_timestamps = null_ts
            start_date = min(null_ts).replace(hour=0, minute=0, second=0, microsecond=0)
            self.logger.info(f"🔍 Режим check-nulls: найдено {len(null_ts):,} NULL записей (от {start_date.strftime('%Y-%m-%d')})")
        else:
            # Режим по умолчанию: инкрементально
            last_date = self.get_all_last_processed_dates(timeframe, configs)

            if last_date:
                self.logger.info(f"📌 Последняя обработанная дата (MIN среди конфигураций): {last_date}")
                # Начинаем со следующего периода
                if timeframe == '1m':
                    start_date = last_date + timedelta(minutes=1)
                elif timeframe == '15m':
                    start_date = last_date + timedelta(minutes=15)
                elif timeframe == '1h':
                    start_date = last_date + timedelta(hours=1)
                elif timeframe == '4h':
                    start_date = last_date + timedelta(hours=4)
                elif timeframe == '1d':
                    start_date = last_date + timedelta(days=1)
            else:
                start_date = min_date
                self.logger.info(f"📌 Начинаем с начала: {start_date}")

        if start_date >= max_date:
            self.logger.info(f"✅ Все конфигурации уже актуальны")
            return

        # Вычисляем максимальный lookback период
        max_lookback = self.get_max_lookback_period(configs)
        self.logger.info(f"📏 Максимальный lookback: {max_lookback} периодов")

        # Обрабатываем данные батчами (по дням)
        current_date = start_date
        total_days = (max_date - start_date).days

        indicators_table = f"indicators_bybit_futures_{timeframe}"

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Прогресс-бар для батчей
            pbar = tqdm(total=total_days,
                       desc=f"{self.symbol} {self.symbol_progress} BB(ALL) {timeframe.upper()}",
                       unit='day',
                       ncols=100,
                       bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')

            while current_date < max_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), max_date)

                # ОПТИМИЗАЦИЯ: Загружаем данные ОДИН РАЗ для батча
                df_batch = self.load_batch_data_once(current_date, batch_end, timeframe, max_lookback)

                if df_batch.empty:
                    current_date = batch_end
                    pbar.update(self.batch_days)
                    continue

                # ОПТИМИЗАЦИЯ: Создаём кэш для SMA и rolling std
                sma_cache = {}
                std_cache = {}

                # Обрабатываем все конфигурации на одних и тех же данных
                for config in configs:
                    period = config['period']
                    std_dev = config['std_dev']
                    base = config['base']
                    name = config['name']

                    # Получаем имена колонок
                    columns = self.get_column_names(period, std_dev, base)

                    # Для SMA-based: используем кэш
                    cached_sma = None
                    cached_std = None

                    if base == 'sma':
                        # Проверяем кэш
                        if period not in sma_cache:
                            sma_cache[period] = df_batch['close'].rolling(window=period).mean()
                            std_cache[period] = df_batch['close'].rolling(window=period).std()

                        cached_sma = sma_cache[period]
                        cached_std = std_cache[period]

                    # Рассчитываем BB с использованием кэша
                    bb_data = self.calculate_bollinger_bands(
                        df_batch['close'],
                        period,
                        std_dev,
                        base,
                        cached_sma=cached_sma,
                        cached_std=cached_std
                    )

                    # Фильтруем только данные текущего батча (без lookback)
                    mask = (df_batch.index >= current_date) & (df_batch.index < batch_end)

                    # Подготавливаем данные для UPDATE
                    update_data = []
                    for ts in df_batch.index[mask]:
                        # В режиме check-nulls записываем только NULL timestamps
                        if null_timestamps is not None and ts not in null_timestamps:
                            continue
                        # Проверяем что основные значения валидны (не NaN и не infinity)
                        if np.isfinite(bb_data['upper'].loc[ts]):
                            # Для percent_b и bandwidth используем None если значение не валидно
                            percent_b_val = float(bb_data['percent_b'].loc[ts]) if np.isfinite(bb_data['percent_b'].loc[ts]) else None
                            bandwidth_val = float(bb_data['bandwidth'].loc[ts]) if np.isfinite(bb_data['bandwidth'].loc[ts]) else None
                            squeeze_val = bool(bb_data['squeeze'].loc[ts]) if pd.notna(bb_data['squeeze'].loc[ts]) else None

                            update_data.append((
                                float(bb_data['upper'].loc[ts]),
                                float(bb_data['middle'].loc[ts]),
                                float(bb_data['lower'].loc[ts]),
                                percent_b_val,
                                bandwidth_val,
                                squeeze_val,
                                self.symbol,
                                ts
                            ))

                    # Batch UPDATE
                    if update_data:
                        update_query = f"""
                            UPDATE {indicators_table}
                            SET {columns['upper']} = %s,
                                {columns['middle']} = %s,
                                {columns['lower']} = %s,
                                {columns['percent_b']} = %s,
                                {columns['bandwidth']} = %s,
                                {columns['squeeze']} = %s
                            WHERE symbol = %s AND timestamp = %s
                        """

                        cur.executemany(update_query, update_data)
                        conn.commit()  # Commit после каждой конфигурации

                # Обновляем прогресс
                pbar.update(self.batch_days)
                current_date = batch_end

            pbar.close()
            cur.close()

        self.logger.info(f"\n✅ Таймфрейм {timeframe} обработан полностью")


def setup_logging():
    """Настройка логирования"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'bollinger_bands_{timestamp}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"📝 Логирование настроено. Лог-файл: {log_file}")

    return logger


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Загрузка Bollinger Bands индикаторов')
    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str, help='Конкретный таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--timeframes', type=str, help='Таймфреймы через запятую (например, 1m,15m,1h)')
    parser.add_argument('--batch-days', type=int, default=1, help='Размер батча в днях (по умолчанию 1)')
    parser.add_argument('--config', type=str, help='Конкретная конфигурация (например, classic)')
    parser.add_argument('--force-reload', action='store_true',
                       help='Полный пересчёт всех данных с начала')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Проверить и заполнить NULL значения в середине данных')

    args = parser.parse_args()

    # Настройка логирования
    logger = setup_logging()

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

    # Определяем таймфреймы для обработки
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = args.timeframes.split(',')
    else:
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
                timeframes = cfg.get('timeframes', ['1m', '15m', '1h'])
        else:
            timeframes = ['1m', '15m', '1h']

    logger.info(f"⏰ Таймфреймы: {timeframes}")

    # Определяем конфигурации для обработки
    if args.config:
        configs = [c for c in BOLLINGER_CONFIGS if c['name'] == args.config]
        if not configs:
            logger.error(f"❌ Конфигурация {args.config} не найдена")
            return
    else:
        configs = BOLLINGER_CONFIGS

    logger.info(f"🎯 Обработка символов: {symbols}")
    logger.info(f"📊 Конфигурации: {[c['name'] for c in configs]}")
    logger.info(f"📦 Batch size: {args.batch_days} дней")
    if args.force_reload:
        logger.info(f"🔄 Режим FORCE RELOAD: все данные будут пересчитаны с начала")
    elif args.check_nulls:
        logger.info(f"🔍 Режим CHECK-NULLS: поиск и заполнение NULL значений")

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        # Создаём загрузчик для текущего символа
        loader = BollingerBandsLoader(symbol=symbol, batch_days=args.batch_days,
                                      force_reload=args.force_reload, check_nulls=args.check_nulls)
        loader.symbol_progress = f"[{idx}/{total_symbols}] "

        # Обрабатываем каждый таймфрейм
        for timeframe in timeframes:
            try:
                loader.load_timeframe(timeframe, configs)
            except KeyboardInterrupt:
                logger.warning("\n⚠️  Прервано пользователем. Прогресс сохранён.")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке {timeframe}: {e}", exc_info=True)
                continue

        logger.info(f"\n✅ Символ {symbol} обработан\n")

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")


if __name__ == '__main__':
    main()
