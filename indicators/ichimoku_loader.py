#!/usr/bin/env python3
"""
Ichimoku Cloud Loader
=====================
Загрузчик индикатора Ichimoku Kinko Hyo в базу данных PostgreSQL.

Ichimoku Kinko Hyo (一目均衡表) - "таблица равновесия одним взглядом"
Создан Гоичи Хосода (Ichimoku Sanjin) в 1930-х, опубликован в 1969.

Компоненты:
===========
1. Tenkan-sen (Conversion Line) = (Highest High + Lowest Low) / 2 за conversion_period
   - Быстрая линия, показывает краткосрочный тренд

2. Kijun-sen (Base Line) = (Highest High + Lowest Low) / 2 за base_period
   - Медленная линия, показывает среднесрочный тренд

3. Senkou Span A (Leading Span A) = (Tenkan + Kijun) / 2, сдвинутый вперёд на base_period
   - Первая граница облака (Kumo)
   - ХРАНЕНИЕ: "Effective" значение - облако, которое ПРИМЕНЯЕТСЯ к текущему timestamp

4. Senkou Span B (Leading Span B) = (Highest High + Lowest Low) / 2 за span_period, сдвинутый вперёд на base_period
   - Вторая граница облака (Kumo)
   - ХРАНЕНИЕ: "Effective" значение - облако, которое ПРИМЕНЯЕТСЯ к текущему timestamp

5. Chikou Span (Lagging Span) = Close, сдвинутый назад на base_period
   - Показывает текущую цену относительно цены base_period периодов назад
   - ХРАНЕНИЕ: Для удобства анализа

Производные колонки:
===================
6. cloud_thick = |Senkou A - Senkou B| / Close × 100
   - Толщина облака в процентах от цены

7. price_cloud = Позиция цены относительно облака
   - 1: цена выше облака (бычий сигнал)
   - 0: цена внутри облака (неопределённость)
   - -1: цена ниже облака (медвежий сигнал)

8. tk_cross = Сигнал пересечения Tenkan/Kijun
   - 1: Tenkan пересёк Kijun снизу вверх (бычий крест)
   - -1: Tenkan пересёк Kijun сверху вниз (медвежий крест)
   - 0: нет пересечения

Конфигурации:
=============
1. Crypto (9/26/52) - стандартная для криптовалют (24/7 рынок)
   - Оригинальные периоды Хосоды, адаптированные для крипто

2. Long (20/60/120) - долгосрочная для позиционной торговли
   - Увеличенные периоды для фильтрации шума

Всего: 2 конфигурации × 8 колонок = 16 колонок на таймфрейм

Использование:
==============
# Загрузка всех конфигураций и таймфреймов
python indicators/ichimoku_loader.py

# Конкретный символ
python indicators/ichimoku_loader.py --symbol BTCUSDT

# Конкретный таймфрейм
python indicators/ichimoku_loader.py --timeframe 1h

# Полная перезагрузка
python indicators/ichimoku_loader.py --force-reload

Автор: Claude Code
Дата создания: 2026-01-29
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
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators.database import DatabaseConnection


# Конфигурации Ichimoku
ICHIMOKU_CONFIGS = [
    {
        'name': 'crypto',
        'conversion_period': 9,   # Tenkan-sen
        'base_period': 26,        # Kijun-sen
        'span_period': 52,        # Senkou Span B
        'description': 'Crypto-optimized Ichimoku (9/26/52) - 24/7 market standard'
    },
    {
        'name': 'long',
        'conversion_period': 20,  # Tenkan-sen
        'base_period': 60,        # Kijun-sen
        'span_period': 120,       # Senkou Span B
        'description': 'Long-term Ichimoku (20/60/120) - position trading'
    },
]


class IchimokuLoader:
    """
    Загрузчик Ichimoku Cloud индикатора

    Рассчитывает Ichimoku для всех конфигураций с batch processing.
    """

    def __init__(self, symbol: str = 'BTCUSDT', batch_days: int = 1,
                 lookback_multiplier: int = 3):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара (по умолчанию BTCUSDT)
            batch_days: Размер батча в днях (по умолчанию 1)
            lookback_multiplier: Множитель для lookback периода (по умолчанию 3)
        """
        self.symbol = symbol
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.batch_days = batch_days
        self.lookback_multiplier = lookback_multiplier
        self.db = DatabaseConnection()
        self.logger = logging.getLogger(__name__)

    def get_column_names(self, config_name: str) -> Dict[str, str]:
        """
        Генерирует имена колонок для конфигурации Ichimoku

        Args:
            config_name: Имя конфигурации ('crypto' или 'long')

        Returns:
            Dict с именами колонок
        """
        prefix = f"ichimoku_{config_name}"

        return {
            'tenkan': f"{prefix}_tenkan",
            'kijun': f"{prefix}_kijun",
            'senkou_a': f"{prefix}_senkou_a",
            'senkou_b': f"{prefix}_senkou_b",
            'chikou': f"{prefix}_chikou",
            'cloud_thick': f"{prefix}_cloud_thick",
            'price_cloud': f"{prefix}_price_cloud",
            'tk_cross': f"{prefix}_tk_cross",
        }

    def ensure_columns_exist(self, timeframe: str, config: Dict):
        """
        Проверяет и создаёт колонки для конфигурации Ichimoku

        Args:
            timeframe: Таймфрейм (например, '1m')
            config: Конфигурация Ichimoku
        """
        table_name = f"indicators_bybit_futures_{timeframe}"
        columns = self.get_column_names(config['name'])

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
                    if col_type in ['price_cloud', 'tk_cross']:
                        col_type_sql = 'SMALLINT'  # -1, 0, 1
                    elif col_type == 'cloud_thick':
                        col_type_sql = 'DECIMAL(10,4)'  # Процент
                    else:
                        col_type_sql = 'DECIMAL(20,8)'  # Цены

                    self.logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type_sql};")
                    conn.commit()
                    self.logger.info(f"✅ Колонка {col_name} создана")

            cur.close()

    def calculate_ichimoku(self, df: pd.DataFrame, config: Dict) -> Dict[str, pd.Series]:
        """
        Рассчитывает компоненты Ichimoku

        ВАЖНО: Senkou Span A/B хранятся как "effective" значения.
        Это означает, что для timestamp T хранится облако, которое ПРИМЕНЯЕТСЯ к T.

        Традиционный расчёт: рассчитываем в T-base_period, сдвигаем на +base_period → применяется к T
        Наш расчёт: для записи в T берём значения, рассчитанные в T-base_period

        Args:
            df: DataFrame с колонками high, low, close (индекс = timestamp)
            config: Конфигурация Ichimoku

        Returns:
            Dict с Series: tenkan, kijun, senkou_a, senkou_b, chikou, cloud_thick, price_cloud, tk_cross
        """
        conversion_period = config['conversion_period']  # 9 или 20
        base_period = config['base_period']              # 26 или 60
        span_period = config['span_period']              # 52 или 120

        # Конвертируем в float
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)

        # 1. Tenkan-sen (Conversion Line) = (Highest High + Lowest Low) / 2 за conversion_period
        tenkan = (high.rolling(window=conversion_period).max() +
                  low.rolling(window=conversion_period).min()) / 2

        # 2. Kijun-sen (Base Line) = (Highest High + Lowest Low) / 2 за base_period
        kijun = (high.rolling(window=base_period).max() +
                 low.rolling(window=base_period).min()) / 2

        # 3. Senkou Span A (Leading Span A) - рассчитываем как "effective" значение
        # Традиционно: (Tenkan + Kijun) / 2, сдвинутый вперёд на base_period
        # Для "effective": берём значение, рассчитанное base_period периодов назад
        senkou_a_raw = (tenkan + kijun) / 2
        senkou_a = senkou_a_raw.shift(base_period)  # Сдвиг вперёд = shift с положительным значением
        # Это даёт нам значение, которое ПРИМЕНЯЕТСЯ к текущему timestamp

        # 4. Senkou Span B (Leading Span B) - рассчитываем как "effective" значение
        # Традиционно: (Highest High + Lowest Low) / 2 за span_period, сдвинутый вперёд на base_period
        senkou_b_raw = (high.rolling(window=span_period).max() +
                        low.rolling(window=span_period).min()) / 2
        senkou_b = senkou_b_raw.shift(base_period)  # Сдвиг вперёд

        # 5. Chikou Span (Lagging Span) = Close, сдвинутый назад на base_period
        # Для удобства храним: chikou[T] = close[T-base_period]
        # Но традиционно Chikou это текущая цена на графике сдвинутая назад
        # Для ML лучше хранить саму текущую close, а сдвиг применять при визуализации
        chikou = close  # Храним текущий close для удобства

        # 6. Cloud Thickness (производная) = |Senkou A - Senkou B| / Close × 100
        cloud_thick = pd.Series(index=df.index, dtype=float)
        mask_valid = (close != 0) & senkou_a.notna() & senkou_b.notna()
        cloud_thick[mask_valid] = (np.abs(senkou_a[mask_valid] - senkou_b[mask_valid]) /
                                   close[mask_valid]) * 100

        # 7. Price vs Cloud (производная)
        # 1: цена выше облака, -1: цена ниже облака, 0: цена внутри облака
        price_cloud = pd.Series(index=df.index, dtype=float)

        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1)
        cloud_bottom = pd.concat([senkou_a, senkou_b], axis=1).min(axis=1)

        mask_above = close > cloud_top
        mask_below = close < cloud_bottom
        mask_inside = ~mask_above & ~mask_below

        price_cloud[mask_above] = 1
        price_cloud[mask_below] = -1
        price_cloud[mask_inside] = 0

        # 8. Tenkan-Kijun Cross (производная)
        # 1: Tenkan пересёк Kijun снизу вверх (бычий крест)
        # -1: Tenkan пересёк Kijun сверху вниз (медвежий крест)
        # 0: нет пересечения
        tk_cross = pd.Series(0, index=df.index, dtype=float)

        # Tenkan выше Kijun сейчас и ниже/равен в предыдущий период = бычий крест
        tenkan_prev = tenkan.shift(1)
        kijun_prev = kijun.shift(1)

        bullish_cross = (tenkan > kijun) & (tenkan_prev <= kijun_prev)
        bearish_cross = (tenkan < kijun) & (tenkan_prev >= kijun_prev)

        tk_cross[bullish_cross] = 1
        tk_cross[bearish_cross] = -1

        return {
            'tenkan': tenkan,
            'kijun': kijun,
            'senkou_a': senkou_a,
            'senkou_b': senkou_b,
            'chikou': chikou,
            'cloud_thick': cloud_thick,
            'price_cloud': price_cloud,
            'tk_cross': tk_cross
        }

    def get_last_processed_date(self, timeframe: str, config: Dict) -> Optional[datetime]:
        """
        Получает дату последней обработанной записи для конфигурации

        Args:
            timeframe: Таймфрейм
            config: Конфигурация Ichimoku

        Returns:
            Datetime последней обработанной записи или None
        """
        table_name = f"indicators_bybit_futures_{timeframe}"
        columns = self.get_column_names(config['name'])
        tenkan_col = columns['tenkan']

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Ищем последнюю заполненную дату
            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM {table_name}
                WHERE symbol = %s AND {tenkan_col} IS NOT NULL
            """, (self.symbol,))

            result = cur.fetchone()
            cur.close()

            return result[0] if result and result[0] else None

    def get_all_last_processed_dates(self, timeframe: str, configs: List[Dict]) -> Optional[datetime]:
        """
        Получает минимальную дату среди всех конфигураций (для синхронизации)

        Args:
            timeframe: Таймфрейм
            configs: Список конфигураций Ichimoku

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

    def get_max_lookback_period(self, configs: List[Dict]) -> int:
        """
        Вычисляет максимальный lookback период среди всех конфигураций

        Для Ichimoku нужен lookback = max(span_period + base_period) для Senkou B

        Args:
            configs: Список конфигураций Ichimoku

        Returns:
            MAX((span_period + base_period) × lookback_multiplier) среди всех конфигураций
        """
        max_periods = []
        for config in configs:
            # Для Senkou Span B нужен span_period для расчёта + base_period для сдвига
            total_period = config['span_period'] + config['base_period']
            max_periods.append(total_period)

        return max(max_periods) * self.lookback_multiplier

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Получает диапазон доступных данных в таблице candles

        Args:
            timeframe: Таймфрейм

        Returns:
            Tuple (min_date, max_date)
        """
        candles_table = "candles_bybit_futures_1m"

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
            df_1m: DataFrame с минутными данными (high, low, close)
            timeframe: Целевой таймфрейм ('15m', '1h')

        Returns:
            Агрегированный DataFrame
        """
        if timeframe == '1m':
            return df_1m

        # Определяем правило агрегации
        rule_map = {
            '15m': '15min',
            '1h': '1h',
        }

        if timeframe not in rule_map:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        rule = rule_map[timeframe]

        # Агрегируем OHLC
        df_agg = df_1m.resample(rule, label='left', closed='left').agg({
            'high': 'max',
            'low': 'min',
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
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Загружаем данные из БД
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            query = f"""
                SELECT timestamp, high, low, close
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
        df = pd.DataFrame(rows, columns=['timestamp', 'high', 'low', 'close'])
        df.set_index('timestamp', inplace=True)

        # Агрегируем в нужный таймфрейм
        df_agg = self.aggregate_1m_to_timeframe(df, timeframe)

        return df_agg

    def load_timeframe(self, timeframe: str, configs: List[Dict], force_reload: bool = False):
        """
        Загружает Ichimoku для всех конфигураций на одном таймфрейме

        Args:
            timeframe: Таймфрейм (например, '1m')
            configs: Список конфигураций Ichimoku
            force_reload: Если True, перезаписывает все данные
        """
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"⏰ Таймфрейм: {timeframe}")
        self.logger.info(f"{'='*80}")

        # Получаем диапазон данных
        min_date, max_date = self.get_data_range('1m')

        self.logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Ограничиваем max_date до последнего завершенного периода
        if timeframe == '15m':
            max_date = max_date.replace(minute=(max_date.minute // 15) * 15, second=0, microsecond=0)
        elif timeframe == '1h':
            max_date = max_date.replace(minute=0, second=0, microsecond=0)

        self.logger.info(f"⏸️  Ограничение max_date до последнего завершенного периода: {max_date}")

        # Создаём колонки для всех конфигураций
        for config in configs:
            self.ensure_columns_exist(timeframe, config)

        # Определяем начальную дату
        if force_reload:
            start_date = min_date
            self.logger.info(f"🔄 Force reload: начинаем с {start_date}")
        else:
            # Получаем MIN(last_date) среди всех конфигураций (для синхронизации)
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
                       desc=f"{self.symbol} {self.symbol_progress} Ichimoku(ALL) {timeframe.upper()}",
                       unit='day')

            while current_date < max_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), max_date)

                # Загружаем данные ОДИН РАЗ для батча
                df_batch = self.load_batch_data_once(current_date, batch_end, timeframe, max_lookback)

                if df_batch.empty:
                    current_date = batch_end
                    pbar.update(self.batch_days)
                    continue

                # Обрабатываем все конфигурации на одних и тех же данных
                for config in configs:
                    name = config['name']

                    # Получаем имена колонок
                    columns = self.get_column_names(name)

                    # Рассчитываем Ichimoku
                    ichimoku_data = self.calculate_ichimoku(df_batch, config)

                    # Фильтруем только данные текущего батча (без lookback)
                    mask = (df_batch.index >= current_date) & (df_batch.index < batch_end)

                    # Подготавливаем данные для UPDATE
                    update_data = []
                    for ts in df_batch.index[mask]:
                        # Проверяем что основные значения валидны
                        tenkan_val = ichimoku_data['tenkan'].loc[ts]
                        if pd.notna(tenkan_val) and np.isfinite(tenkan_val):
                            # Собираем все значения
                            def safe_float(val):
                                if pd.notna(val) and np.isfinite(val):
                                    return float(val)
                                return None

                            def safe_int(val):
                                if pd.notna(val) and np.isfinite(val):
                                    return int(val)
                                return None

                            update_data.append((
                                safe_float(ichimoku_data['tenkan'].loc[ts]),
                                safe_float(ichimoku_data['kijun'].loc[ts]),
                                safe_float(ichimoku_data['senkou_a'].loc[ts]),
                                safe_float(ichimoku_data['senkou_b'].loc[ts]),
                                safe_float(ichimoku_data['chikou'].loc[ts]),
                                safe_float(ichimoku_data['cloud_thick'].loc[ts]),
                                safe_int(ichimoku_data['price_cloud'].loc[ts]),
                                safe_int(ichimoku_data['tk_cross'].loc[ts]),
                                self.symbol,
                                ts
                            ))

                    # Batch UPDATE
                    if update_data:
                        update_query = f"""
                            UPDATE {indicators_table}
                            SET {columns['tenkan']} = %s,
                                {columns['kijun']} = %s,
                                {columns['senkou_a']} = %s,
                                {columns['senkou_b']} = %s,
                                {columns['chikou']} = %s,
                                {columns['cloud_thick']} = %s,
                                {columns['price_cloud']} = %s,
                                {columns['tk_cross']} = %s
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
    log_file = os.path.join(log_dir, f'ichimoku_{timestamp}.log')

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
    parser = argparse.ArgumentParser(description='Загрузка Ichimoku Cloud индикаторов')
    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str, help='Конкретный таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--timeframes', type=str, help='Таймфреймы через запятую (например, 1m,15m,1h)')
    parser.add_argument('--batch-days', type=int, default=1, help='Размер батча в днях (по умолчанию 1)')
    parser.add_argument('--force-reload', action='store_true', help='Перезаписать все данные')

    args = parser.parse_args()

    # Настройка логирования
    logger = setup_logging()

    start_time = datetime.now()
    logger.info(f"🚀 Запуск загрузчика Ichimoku Cloud")
    logger.info(f"⏱️  Время запуска: {start_time}")

    # Загрузка конфигурации из YAML
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
    with open(config_path, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # Определяем символы
    if args.symbol:
        symbols = [args.symbol]
    elif args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    else:
        symbols = yaml_config.get('symbols', ['BTCUSDT'])

    # Определяем таймфреймы
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = [t.strip() for t in args.timeframes.split(',')]
    else:
        timeframes = yaml_config.get('timeframes', ['1m', '15m', '1h'])

    logger.info(f"📊 Символы: {symbols}")
    logger.info(f"⏰ Таймфреймы: {timeframes}")
    logger.info(f"📦 Batch size: {args.batch_days} дней")
    logger.info(f"🔄 Force reload: {args.force_reload}")
    logger.info(f"⚙️  Конфигурации Ichimoku:")
    for config in ICHIMOKU_CONFIGS:
        logger.info(f"   - {config['name']}: ({config['conversion_period']}/{config['base_period']}/{config['span_period']}) - {config['description']}")

    # Обработка каждого символа
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'#'*80}")
        logger.info(f"# Обработка символа: {symbol} ({idx}/{len(symbols)})")
        logger.info(f"{'#'*80}")

        loader = IchimokuLoader(
            symbol=symbol,
            batch_days=args.batch_days,
            lookback_multiplier=3
        )
        loader.symbol_progress = f"({idx}/{len(symbols)})"

        # Обработка каждого таймфрейма
        for timeframe in timeframes:
            loader.load_timeframe(timeframe, ICHIMOKU_CONFIGS, force_reload=args.force_reload)

        logger.info(f"\n{'='*80}")
        logger.info(f"✅ Символ {symbol} обработан")
        logger.info(f"{'='*80}")

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(f"\n{'#'*80}")
    logger.info(f"# ЗАГРУЗКА ЗАВЕРШЕНА")
    logger.info(f"{'#'*80}")
    logger.info(f"⏱️  Время завершения: {end_time}")
    logger.info(f"⏱️  Общая продолжительность: {duration}")
    logger.info(f"📊 Обработано символов: {len(symbols)}")
    logger.info(f"⏰ Обработано таймфреймов: {len(timeframes)}")
    logger.info(f"⚙️  Конфигураций Ichimoku: {len(ICHIMOKU_CONFIGS)}")
    logger.info(f"📝 Всего колонок: {len(ICHIMOKU_CONFIGS) * 8} на таймфрейм")


if __name__ == '__main__':
    main()
