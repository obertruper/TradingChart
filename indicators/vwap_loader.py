#!/usr/bin/env python3
"""
VWAP (Volume Weighted Average Price) Loader

Загрузка индикатора VWAP для множества символов и таймфреймов.

VWAP = Σ(Typical Price × Volume) / Σ(Volume)
где Typical Price = (High + Low + Close) / 3

Реализует два типа VWAP:
1. Daily VWAP - сбрасывается каждый день в 00:00 UTC
2. Rolling VWAP - скользящее окно фиксированного размера

Usage:
    python3 vwap_loader.py                                    # Все символы, все таймфреймы
    python3 vwap_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 vwap_loader.py --symbol BTCUSDT --timeframe 1m    # Символ + таймфрейм
    python3 vwap_loader.py --batch-days 7                     # Изменить размер батча
"""

import sys
import logging
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import pandas as pd
import numpy as np
from tqdm import tqdm

# Подавляем предупреждение pandas о DBAPI2 connection
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Добавляем путь к корню проекта
sys.path.append(str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# Настройка логирования
logger = logging.getLogger(__name__)


class VWAPLoader:
    """Загрузчик индикатора VWAP для торговых данных"""

    def __init__(self, symbol: str, timeframe: str, config: dict):
        """
        Инициализация загрузчика VWAP

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)

        # Настройки из конфига
        vwap_config = config['indicators']['vwap']
        self.daily_enabled = vwap_config['daily_enabled']
        self.rolling_periods = vwap_config['rolling_periods']
        self.batch_days = vwap_config.get('batch_days', 1)
        self.lookback_multiplier = vwap_config.get('lookback_multiplier', 2)

        # Вычисляем lookback
        max_period = max(self.rolling_periods)
        self.lookback_periods = max_period * self.lookback_multiplier

        # Для прогресс-бара (устанавливается извне)
        self.symbol_progress = ""

        # База данных
        self.db = DatabaseConnection()
        self.candles_table = "candles_bybit_futures_1m"  # Всегда используем 1m таблицу
        self.indicators_table = f"indicators_bybit_futures_{timeframe}"

        logger.info(f"Инициализирован VWAPLoader для {symbol} на {timeframe}")
        logger.info(f"Daily VWAP: {self.daily_enabled}, Rolling периоды: {len(self.rolling_periods)}")
        logger.info(f"Lookback: {self.lookback_periods} периодов")

    def _parse_timeframe(self, tf: str) -> int:
        """
        Конвертация таймфрейма в минуты

        Args:
            tf: Таймфрейм (1m, 15m, 1h, etc.)

        Returns:
            Количество минут
        """
        if tf.endswith('m'):
            return int(tf[:-1])
        elif tf.endswith('h'):
            return int(tf[:-1]) * 60
        elif tf.endswith('d'):
            return int(tf[:-1]) * 1440
        else:
            raise ValueError(f"Неизвестный формат таймфрейма: {tf}")

    def ensure_columns_exist(self):
        """Проверка и создание колонок VWAP в таблице indicators"""

        logger.info("Проверка наличия колонок VWAP в таблице...")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем список существующих колонок
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{self.indicators_table}'
                """)
                existing_columns = {row[0] for row in cur.fetchall()}

                # Список колонок для создания
                columns_to_add = []

                # Daily VWAP
                if self.daily_enabled and 'vwap_daily' not in existing_columns:
                    columns_to_add.append('vwap_daily DECIMAL(20,8)')
                    logger.info("  - vwap_daily (будет создана)")

                # Rolling VWAP
                for period in self.rolling_periods:
                    col_name = f'vwap_{period}'
                    if col_name not in existing_columns:
                        columns_to_add.append(f'{col_name} DECIMAL(20,8)')
                        logger.info(f"  - {col_name} (будет создана)")

                # Создаем колонки если нужно
                if columns_to_add:
                    logger.info(f"Создание {len(columns_to_add)} новых колонок...")

                    for col_def in columns_to_add:
                        col_name = col_def.split()[0]
                        sql = f"ALTER TABLE {self.indicators_table} ADD COLUMN IF NOT EXISTS {col_def}"
                        cur.execute(sql)
                        logger.info(f"  ✅ Создана колонка: {col_name}")

                    conn.commit()
                    logger.info("✅ Все колонки VWAP созданы")
                else:
                    logger.info("✅ Все необходимые колонки уже существуют")

    def get_date_range(self):
        """
        Определение диапазона дат для обработки

        Returns:
            tuple: (start_date, end_date) в UTC
        """

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Проверяем последнюю дату VWAP в indicators таблице
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s AND vwap_daily IS NOT NULL
                """, (self.symbol,))

                last_vwap_date = cur.fetchone()[0]

                # 2. Получаем диапазон данных в candles таблице
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {self.candles_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                min_candle_date, max_candle_date = cur.fetchone()

                if min_candle_date is None or max_candle_date is None:
                    logger.warning(f"⚠️  Нет данных для {self.symbol} в {self.candles_table}")
                    return None, None

                # 3. Определяем start_date
                if last_vwap_date is None:
                    # Данных нет - начинаем с начала
                    start_date = min_candle_date
                    logger.info(f"📅 Данных VWAP нет. Начинаем с: {start_date}")
                else:
                    # Продолжаем с последней даты
                    start_date = last_vwap_date + timedelta(minutes=self.timeframe_minutes)
                    logger.info(f"📅 Последняя дата VWAP: {last_vwap_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")

                # 4. Определяем end_date (последняя завершенная свеча)
                # Ограничиваем до последней полной свечи минус 1 период для безопасности
                end_date = max_candle_date

                # Выравниваем до начала периода
                if self.timeframe == '1m':
                    # Для 1m - ограничиваем до последней полной минуты
                    end_date = end_date.replace(second=0, microsecond=0)
                elif self.timeframe == '15m':
                    # Для 15m - выравниваем до кратного 15 минут
                    minutes = (end_date.minute // 15) * 15
                    end_date = end_date.replace(minute=minutes, second=0, microsecond=0)
                elif self.timeframe == '1h':
                    # Для 1h - выравниваем до начала часа
                    end_date = end_date.replace(minute=0, second=0, microsecond=0)
                elif self.timeframe == '4h':
                    # Для 4h - выравниваем до 4-часового блока
                    hour_block = (end_date.hour // 4) * 4
                    end_date = end_date.replace(hour=hour_block, minute=0, second=0, microsecond=0)
                elif self.timeframe == '1d':
                    # Для 1d - выравниваем до начала дня
                    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

                logger.info(f"📅 Диапазон данных в БД: {min_candle_date} - {max_candle_date}")
                logger.info(f"⏸️  Ограничение end_date до последней завершенной свечи: {end_date}")

                return start_date, end_date

    def calculate_daily_vwap(self, df: pd.DataFrame) -> pd.Series:
        """
        Расчет Daily VWAP с reset в 00:00 UTC

        Formula:
        - Typical Price (TP) = (High + Low + Close) / 3
        - VWAP = Σ(TP × Volume) / Σ(Volume)
        - Группировка по дате, cumsum внутри каждой группы

        Args:
            df: DataFrame с колонками high, low, close, volume, timestamp

        Returns:
            pd.Series с vwap_daily значениями (может содержать NaN)
        """

        # 1. Вычисляем Typical Price
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        # 2. Добавляем колонку date (без времени)
        df['date'] = df.index.date

        # 3. Фильтруем свечи с volume > 0
        df_filtered = df[df['volume'] > 0].copy()

        if len(df_filtered) == 0:
            # Все свечи с нулевым объемом
            return pd.Series(np.nan, index=df.index)

        # 4. Группируем по дате и делаем cumsum внутри каждой группы
        df_filtered['cum_tp_volume'] = df_filtered.groupby('date')['tp_volume'].cumsum()
        df_filtered['cum_volume'] = df_filtered.groupby('date')['volume'].cumsum()

        # 5. Вычисляем VWAP
        df_filtered['vwap_daily'] = df_filtered['cum_tp_volume'] / df_filtered['cum_volume']

        # 6. Возвращаем Series с правильным индексом (заполняем NaN для пропущенных)
        return df_filtered['vwap_daily'].reindex(df.index)

    def calculate_rolling_vwap(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Расчет Rolling VWAP для заданного периода

        Formula:
        - VWAP = Σ(TP × Volume)_last_N / Σ(Volume)_last_N
        - Rolling window размером period

        Args:
            df: DataFrame с колонками high, low, close, volume
                (tp и tp_volume уже должны быть вычислены)
            period: Размер окна (количество периодов)

        Returns:
            pd.Series с vwap_{period} значениями
        """

        # tp и tp_volume уже вычислены в calculate_daily_vwap
        # Если нет - вычисляем
        if 'tp' not in df.columns:
            df['tp'] = (df['high'] + df['low'] + df['close']) / 3
            df['tp_volume'] = df['tp'] * df['volume']

        # Rolling sum для tp_volume и volume
        # min_periods=1 означает что будем считать даже если данных меньше period
        rolling_tp_volume = df['tp_volume'].rolling(window=period, min_periods=1).sum()
        rolling_volume = df['volume'].rolling(window=period, min_periods=1).sum()

        # Избегаем деления на ноль
        vwap = np.where(
            rolling_volume > 0,
            rolling_tp_volume / rolling_volume,
            np.nan
        )

        # Первые (period-1) значений = NULL (недостаточно данных)
        vwap[:period-1] = np.nan

        return pd.Series(vwap, index=df.index, name=f'vwap_{period}')

    def load_candles_with_lookback(self, start_date, end_date):
        """
        Загрузка свечей из БД с lookback периодом

        Для таймфреймов 15m и 1h агрегирует данные из 1m свечей

        Args:
            start_date: Начало батча
            end_date: Конец батча

        Returns:
            pd.DataFrame с индексом timestamp и колонками: high, low, close, volume
        """

        # Вычисляем lookback_start
        lookback_start = start_date - timedelta(minutes=self.lookback_periods * self.timeframe_minutes)

        with self.db.get_connection() as conn:
            if self.timeframe == '1m':
                # Для 1m - читаем напрямую
                query = f"""
                    SELECT timestamp, high, low, close, volume
                    FROM {self.candles_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp ASC
                """

                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, lookback_start, end_date),
                    parse_dates=['timestamp']
                )
            elif self.timeframe == '1d':
                # Для 1d - агрегируем по дням
                query = f"""
                    SELECT
                        date_trunc('day', timestamp) as timestamp,
                        MAX(high) as high,
                        MIN(low) as low,
                        (array_agg(close ORDER BY timestamp DESC))[1] as close,
                        SUM(volume) as volume
                    FROM {self.candles_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    GROUP BY date_trunc('day', timestamp)
                    ORDER BY timestamp ASC
                """

                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, lookback_start, end_date),
                    parse_dates=['timestamp']
                )
            elif self.timeframe == '4h':
                # Для 4h - агрегируем по 4-часовым интервалам
                query = f"""
                    SELECT
                        date_trunc('day', timestamp) +
                        INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4) as timestamp,
                        MAX(high) as high,
                        MIN(low) as low,
                        (array_agg(close ORDER BY timestamp DESC))[1] as close,
                        SUM(volume) as volume
                    FROM {self.candles_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    GROUP BY date_trunc('day', timestamp) +
                             INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4)
                    ORDER BY timestamp ASC
                """

                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, lookback_start, end_date),
                    parse_dates=['timestamp']
                )
            else:
                # Для 15m и 1h - агрегируем из 1m данных
                interval_minutes = self.timeframe_minutes
                query = f"""
                    SELECT
                        date_trunc('hour', timestamp) +
                        INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as timestamp,
                        MAX(high) as high,
                        MIN(low) as low,
                        (array_agg(close ORDER BY timestamp DESC))[1] as close,
                        SUM(volume) as volume
                    FROM {self.candles_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    GROUP BY date_trunc('hour', timestamp) +
                             INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes})
                    ORDER BY timestamp ASC
                """

                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, lookback_start, end_date),
                    parse_dates=['timestamp']
                )

            if df.empty:
                return pd.DataFrame()

            # Устанавливаем timestamp как индекс
            df.set_index('timestamp', inplace=True)

            return df

    def save_to_db(self, df: pd.DataFrame, batch_start, batch_end, columns: list):
        """
        Сохранение данных в indicators таблицу

        Args:
            df: DataFrame с рассчитанными VWAP значениями
            batch_start: Начало батча (для фильтрации)
            batch_end: Конец батча (для фильтрации)
            columns: Список колонок для сохранения (например, ['vwap_daily'] или ['vwap_100'])
        """

        # Фильтруем только батч (без lookback)
        df_batch = df[(df.index >= batch_start) & (df.index <= batch_end)].copy()

        if df_batch.empty:
            return

        # Подготавливаем данные для UPDATE
        update_data = []
        for timestamp, row in df_batch.iterrows():
            for col in columns:
                if col in row and pd.notna(row[col]):
                    update_data.append({
                        'timestamp': timestamp,
                        'symbol': self.symbol,
                        'column': col,
                        'value': float(row[col])
                    })

        if not update_data:
            return

        # Bulk UPDATE
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for data in update_data:
                    sql = f"""
                        UPDATE {self.indicators_table}
                        SET {data['column']} = %s
                        WHERE timestamp = %s AND symbol = %s
                    """
                    cur.execute(sql, (data['value'], data['timestamp'], data['symbol']))

                conn.commit()

    def load_vwap_for_symbol(self):
        """Основной метод загрузки VWAP для символа"""

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Загрузка VWAP")
        logger.info("=" * 80)
        logger.info(f"⏰ Таймфрейм: {self.timeframe}")
        logger.info(f"📦 Batch size: {self.batch_days} день(дней)")

        periods_info = "daily" if self.daily_enabled else ""
        if self.rolling_periods:
            if periods_info:
                periods_info += f" + {len(self.rolling_periods)} rolling"
            else:
                periods_info = f"{len(self.rolling_periods)} rolling"
        logger.info(f"📊 Периоды: {periods_info}")

        # 1. Проверяем и создаем колонки
        self.ensure_columns_exist()

        # 2. Определяем диапазон дат
        start_date, end_date = self.get_date_range()

        if start_date is None or end_date is None:
            logger.warning(f"⚠️  Нет данных для обработки: {self.symbol}")
            return

        if start_date >= end_date:
            logger.info(f"✅ {self.symbol} - данные VWAP актуальны")
            return

        # 3. Рассчитываем количество батчей
        total_days = (end_date - start_date).days + 1
        total_batches = max(1, (total_days + self.batch_days - 1) // self.batch_days)

        logger.info(f"📅 Диапазон обработки: {start_date} → {end_date}")
        logger.info(f"📊 Всего дней: {total_days}, батчей: {total_batches}")
        logger.info("")

        # Определяем общее количество этапов
        total_indicators = (1 if self.daily_enabled else 0) + len(self.rolling_periods)

        # 4. Daily VWAP
        if self.daily_enabled:
            logger.info(f"[1/{total_indicators}] Daily VWAP")

            current_date = start_date
            batch_num = 0

            pbar = tqdm(
                total=total_batches,
                desc=f"{self.symbol} {self.symbol_progress} VWAP-daily {self.timeframe.upper()}",
                unit="батч"
            )

            while current_date < end_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

                # Загружаем данные с lookback
                df = self.load_candles_with_lookback(current_date, batch_end)

                if not df.empty:
                    # Рассчитываем Daily VWAP
                    df['vwap_daily'] = self.calculate_daily_vwap(df)

                    # Записываем в БД (только батч, без lookback)
                    self.save_to_db(df, current_date, batch_end, ['vwap_daily'])

                batch_num += 1
                pbar.update(1)
                current_date = batch_end

            pbar.close()

        # 5. Rolling VWAP (все периоды)
        for idx, period in enumerate(self.rolling_periods):
            indicator_num = (1 if self.daily_enabled else 0) + idx + 1
            logger.info(f"[{indicator_num}/{total_indicators}] Rolling VWAP (period={period})")

            current_date = start_date
            batch_num = 0

            pbar = tqdm(
                total=total_batches,
                desc=f"{self.symbol} {self.symbol_progress} VWAP-{period} {self.timeframe.upper()}",
                unit="батч"
            )

            while current_date < end_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

                # Загружаем данные с lookback
                df = self.load_candles_with_lookback(current_date, batch_end)

                if not df.empty:
                    # Рассчитываем Rolling VWAP для этого периода
                    col_name = f'vwap_{period}'
                    df[col_name] = self.calculate_rolling_vwap(df, period)

                    # Записываем в БД
                    self.save_to_db(df, current_date, batch_end, [col_name])

                batch_num += 1
                pbar.update(1)
                current_date = batch_end

            pbar.close()

        logger.info(f"✅ {self.symbol} завершен")
        logger.info("")


def setup_logging():
    """Настройка системы логирования"""

    # Создаем директорию для логов
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    # Имя файла лога с текущей датой и временем
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'vwap_loader_{timestamp}.log'

    # Настройка формата
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Конфигурируем root logger
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger.info(f"📝 Логирование настроено. Лог-файл: {log_file}")

    return log_file


def parse_args():
    """Парсинг аргументов командной строки"""

    parser = argparse.ArgumentParser(
        description='VWAP Loader - загрузка индикатора VWAP для торговых данных',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 vwap_loader.py                                    # Все символы, все таймфреймы
  python3 vwap_loader.py --symbol BTCUSDT                   # Конкретный символ
  python3 vwap_loader.py --symbol BTCUSDT --timeframe 1m    # Символ + таймфрейм
  python3 vwap_loader.py --symbol BTCUSDT ETHUSDT           # Несколько символов
  python3 vwap_loader.py --batch-days 7                     # Изменить размер батча
        """
    )

    parser.add_argument(
        '--symbol',
        nargs='+',
        help='Символ(ы) для обработки (например, BTCUSDT ETHUSDT). По умолчанию - все из конфига'
    )

    parser.add_argument(
        '--timeframe',
        help='Таймфрейм для обработки (1m, 15m, 1h). По умолчанию - все из конфига'
    )

    parser.add_argument(
        '--batch-days',
        type=int,
        help='Размер батча в днях. По умолчанию - из конфига (обычно 1)'
    )

    return parser.parse_args()


def load_config():
    """Загрузка конфигурации из YAML файла"""

    config_path = Path(__file__).parent / 'indicators_config.yaml'

    if not config_path.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def main():
    """Главная функция"""

    # 1. Настройка логирования
    log_file = setup_logging()

    logger.info("=" * 80)
    logger.info("🚀 VWAP Loader - Запуск")
    logger.info("=" * 80)

    # 2. Парсинг аргументов
    args = parse_args()

    # 3. Загрузка конфигурации
    try:
        config = load_config()
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

    # 4. Определяем символы
    if args.symbol:
        symbols = args.symbol
        logger.info(f"🎯 Обработка символов из аргументов: {symbols}")
    else:
        symbols = config.get('symbols', [])
        logger.info(f"🎯 Обработка символов из конфига: {symbols}")

    if not symbols:
        logger.error("❌ Не указаны символы для обработки")
        sys.exit(1)

    # 5. Определяем таймфреймы
    if args.timeframe:
        timeframes = [args.timeframe]
        logger.info(f"⏰ Обработка таймфрейма из аргументов: {timeframes}")
    else:
        timeframes = config.get('timeframes', ['1m', '15m', '1h'])
        logger.info(f"⏰ Обработка таймфреймов из конфига: {timeframes}")

    # 6. Переопределяем batch_days если указан
    if args.batch_days:
        config['indicators']['vwap']['batch_days'] = args.batch_days
        logger.info(f"📦 Размер батча из аргументов: {args.batch_days} дней")

    logger.info(f"📊 Индикатор: VWAP")
    logger.info("")

    # 7. Обработка
    total_symbols = len(symbols)

    for symbol_idx, symbol in enumerate(symbols, start=1):
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{symbol_idx}/{total_symbols}]")
        logger.info("=" * 80)
        logger.info("")

        for timeframe in timeframes:
            try:
                # Создаем экземпляр загрузчика
                loader = VWAPLoader(symbol, timeframe, config)
                loader.symbol_progress = f"[{symbol_idx}/{total_symbols}]"

                # Запускаем загрузку
                loader.load_vwap_for_symbol()

            except KeyboardInterrupt:
                logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Ошибка обработки {symbol} на {timeframe}: {e}", exc_info=True)
                continue

    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ VWAP Loader - Завершено")
    logger.info(f"📝 Лог-файл: {log_file}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
