#!/usr/bin/env python3
"""
MFI (Money Flow Index) Loader

Загрузка индикатора MFI для множества символов и таймфреймов.

MFI = 100 - (100 / (1 + Money Flow Ratio))
где Money Flow Ratio = Σ(Positive MF, N) / Σ(Negative MF, N)

Money Flow = Typical Price × Volume
Typical Price = (High + Low + Close) / 3

Usage:
    python3 mfi_loader.py                                    # Все символы, все таймфреймы
    python3 mfi_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 mfi_loader.py --symbol BTCUSDT --timeframe 1m    # Символ + таймфрейм
    python3 mfi_loader.py --batch-days 7                     # Изменить размер батча
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


class MFILoader:
    """Загрузчик индикатора MFI (Money Flow Index) для торговых данных"""

    def __init__(self, symbol: str, timeframe: str, config: dict, force_reload: bool = False):
        """
        Инициализация загрузчика MFI

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
            force_reload: Пересчитать все данные с начала (по умолчанию False)
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)
        self.force_reload = force_reload

        # Настройки из конфига
        mfi_config = config['indicators']['mfi']
        self.periods = mfi_config['periods']
        self.batch_days = mfi_config.get('batch_days', 1)
        self.lookback_multiplier = mfi_config.get('lookback_multiplier', 2)

        # Вычисляем lookback
        max_period = max(self.periods)
        self.lookback_periods = max_period * self.lookback_multiplier

        # Для прогресс-бара (устанавливается извне)
        self.symbol_progress = ""

        # База данных
        self.db = DatabaseConnection()
        self.candles_table = "candles_bybit_futures_1m"  # Всегда используем 1m таблицу
        self.indicators_table = f"indicators_bybit_futures_{timeframe}"

        logger.info(f"Инициализирован MFILoader для {symbol} на {timeframe}")
        logger.info(f"Периоды: {self.periods}, Lookback: {self.lookback_periods} периодов")
        if force_reload:
            logger.info("⚠️  Режим FORCE RELOAD: пересчет всех данных с начала")

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
        """Проверка и создание колонок MFI в таблице indicators"""

        logger.info("Проверка наличия колонок MFI в таблице...")

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

                for period in self.periods:
                    col_name = f'mfi_{period}'
                    if col_name not in existing_columns:
                        columns_to_add.append(f'{col_name} DECIMAL(10,2)')
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
                    logger.info("✅ Все колонки MFI созданы")
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
                # 1. Получаем диапазон данных в candles таблице
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {self.candles_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                min_candle_date, max_candle_date = cur.fetchone()

                if min_candle_date is None or max_candle_date is None:
                    logger.warning(f"⚠️  Нет данных для {self.symbol} в {self.candles_table}")
                    return None, None

                # 2. Определяем start_date в зависимости от режима
                if self.force_reload:
                    # Режим force_reload - начинаем с самого начала
                    start_date = min_candle_date
                    logger.info(f"🔄 Режим FORCE RELOAD: начинаем с начала данных")
                    logger.info(f"📅 Начальная дата: {start_date}")
                else:
                    # Обычный режим - проверяем последнюю дату MFI
                    cur.execute(f"""
                        SELECT MAX(timestamp)
                        FROM {self.indicators_table}
                        WHERE symbol = %s AND mfi_14 IS NOT NULL
                    """, (self.symbol,))

                    last_mfi_date = cur.fetchone()[0]

                    if last_mfi_date is None:
                        # Данных нет - начинаем с начала
                        start_date = min_candle_date
                        logger.info(f"📅 Данных MFI нет. Начинаем с: {start_date}")
                    else:
                        # Продолжаем с последней даты
                        start_date = last_mfi_date + timedelta(minutes=self.timeframe_minutes)
                        logger.info(f"📅 Последняя дата MFI: {last_mfi_date}")
                        logger.info(f"▶️  Продолжаем с: {start_date}")

                # 4. Определяем end_date (последняя ЗАВЕРШЕННАЯ свеча)
                end_date = max_candle_date

                # Выравниваем до начала периода И вычитаем один период (исключаем незавершенную свечу)
                if self.timeframe == '1m':
                    end_date = end_date.replace(second=0, microsecond=0)
                    end_date = end_date - timedelta(minutes=1)  # Exclude last incomplete 1m candle
                elif self.timeframe == '15m':
                    minutes = (end_date.minute // 15) * 15
                    end_date = end_date.replace(minute=minutes, second=0, microsecond=0)
                    end_date = end_date - timedelta(minutes=15)  # Exclude last incomplete 15m candle
                elif self.timeframe == '1h':
                    end_date = end_date.replace(minute=0, second=0, microsecond=0)
                    end_date = end_date - timedelta(hours=1)  # Exclude last incomplete 1h candle

                logger.info(f"📅 Диапазон данных в БД: {min_candle_date} - {max_candle_date}")
                logger.info(f"⏸️  Ограничение end_date до последней ЗАВЕРШЕННОЙ свечи: {end_date}")
                logger.info(f"   (исключена незавершенная свеча {max_candle_date})")

                return start_date, end_date

    def calculate_mfi(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Расчет MFI для заданного периода

        Formula:
        1. Typical Price (TP) = (High + Low + Close) / 3
        2. Money Flow (MF) = TP × Volume
        3. Positive/Negative разделение:
           - Если TP > TP_prev: Positive_MF = MF, Negative_MF = 0
           - Если TP < TP_prev: Positive_MF = 0, Negative_MF = MF
           - Если TP == TP_prev: оба = 0 (игнорируем)
        4. Rolling sum за period периодов
        5. Money Flow Ratio = Σ(Positive_MF) / Σ(Negative_MF)
        6. MFI = 100 - (100 / (1 + Ratio))

        Edge cases:
        - Volume = 0: MF = 0 (учитываем в rolling window, но вклад = 0)
        - Negative_Sum = 0: MFI = 100.0 (только покупки)
        - Positive_Sum = 0: MFI = 0.0 (только продажи)
        - Оба = 0: MFI = NaN (нет движения)
        - Первые (period-1) свечей: MFI = NaN

        Args:
            df: DataFrame с колонками high, low, close, volume
            period: Период MFI (7, 10, 14, 20, 25)

        Returns:
            pd.Series с MFI значениями
        """

        # Конвертируем колонки в float64 для корректной работы с PostgreSQL Decimal типами
        # (аналогично RSI loader fix для предотвращения TypeError)
        high = df['high'].astype(np.float64)
        low = df['low'].astype(np.float64)
        close = df['close'].astype(np.float64)
        volume = df['volume'].astype(np.float64)

        # 1. Typical Price
        tp = (high + low + close) / 3

        # 2. Money Flow (не фильтруем volume = 0, просто считаем)
        money_flow = tp * volume

        # 3. Разделение на Positive/Negative
        tp_diff = tp.diff()  # TP - TP_prev

        # Если TP > TP_prev (растет) → positive_mf = money_flow, negative_mf = 0
        # Если TP < TP_prev (падает) → positive_mf = 0, negative_mf = money_flow
        # Если TP == TP_prev (не изменилась) → оба = 0
        positive_mf = pd.Series(np.where(tp_diff > 0, money_flow, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, money_flow, 0), index=df.index)

        # 4. Rolling sum (min_periods=period → NULL для первых period-1 свечей)
        positive_sum = positive_mf.rolling(window=period, min_periods=period).sum()
        negative_sum = negative_mf.rolling(window=period, min_periods=period).sum()

        # 5. MFI с обработкой деления на ноль
        mfi = pd.Series(index=df.index, dtype=float)

        for i in range(len(df)):
            pos_sum = positive_sum.iloc[i]
            neg_sum = negative_sum.iloc[i]

            if pd.isna(pos_sum) or pd.isna(neg_sum):
                # Первые (period-1) свечей - недостаточно данных
                mfi.iloc[i] = np.nan
            elif pos_sum == 0 and neg_sum == 0:
                # Нет движения цены за весь период
                mfi.iloc[i] = np.nan
            elif neg_sum == 0:
                # Только покупки (только рост TP)
                mfi.iloc[i] = 100.0
            elif pos_sum == 0:
                # Только продажи (только падение TP)
                mfi.iloc[i] = 0.0
            else:
                # Нормальный расчет
                ratio = pos_sum / neg_sum
                mfi.iloc[i] = 100 - (100 / (1 + ratio))

        return mfi

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
            df: DataFrame с рассчитанными MFI значениями
            batch_start: Начало батча (для фильтрации)
            batch_end: Конец батча (для фильтрации)
            columns: Список колонок для сохранения (например, ['mfi_14'])
        """

        # Фильтруем только батч (без lookback)
        df_batch = df[(df.index >= batch_start) & (df.index <= batch_end)].copy()

        if df_batch.empty:
            return

        # Группируем данные по (timestamp, symbol) для батчевой вставки
        records_by_time = {}
        for timestamp, row in df_batch.iterrows():
            key = (timestamp, self.symbol)
            if key not in records_by_time:
                records_by_time[key] = {
                    'timestamp': timestamp,
                    'symbol': self.symbol
                }

            # Добавляем все MFI колонки для этого timestamp
            for col in columns:
                if col in row and pd.notna(row[col]):
                    records_by_time[key][col] = float(row[col])

        if not records_by_time:
            return

        # Формируем INSERT...ON CONFLICT запрос (UPSERT)
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Подготавливаем данные для батчевой вставки
                records = []
                for key, record in records_by_time.items():
                    # Получаем колонки для этой записи
                    record_columns = list(record.keys())
                    values = [record[col] for col in record_columns]

                    # Только MFI колонки для UPDATE части
                    mfi_columns = [col for col in record_columns if col.startswith('mfi_')]

                    if mfi_columns:
                        records.append((record_columns, values, mfi_columns))

                # Выполняем INSERT...ON CONFLICT для каждой записи
                for record_columns, values, mfi_columns in records:
                    placeholders = ','.join(['%s'] * len(record_columns))
                    update_set = ','.join([f"{col} = EXCLUDED.{col}" for col in mfi_columns])

                    sql = f"""
                        INSERT INTO {self.indicators_table} ({','.join(record_columns)})
                        VALUES ({placeholders})
                        ON CONFLICT (timestamp, symbol) DO UPDATE SET
                        {update_set}
                    """

                    cur.execute(sql, values)

                conn.commit()

    def load_mfi_for_symbol(self):
        """Основной метод загрузки MFI для символа"""

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Загрузка MFI")
        logger.info("=" * 80)
        logger.info(f"⏰ Таймфрейм: {self.timeframe}")
        logger.info(f"📦 Batch size: {self.batch_days} день(дней)")
        logger.info(f"📊 Периоды: {self.periods}")

        # 1. Проверяем и создаем колонки
        self.ensure_columns_exist()

        # 2. Определяем диапазон дат
        start_date, end_date = self.get_date_range()

        if start_date is None or end_date is None:
            logger.warning(f"⚠️  Нет данных для обработки: {self.symbol}")
            return

        if start_date >= end_date:
            logger.info(f"✅ {self.symbol} - данные MFI актуальны")
            return

        # 3. Рассчитываем количество батчей
        total_days = (end_date - start_date).days + 1
        total_batches = max(1, (total_days + self.batch_days - 1) // self.batch_days)

        logger.info(f"📅 Диапазон обработки: {start_date} → {end_date}")
        logger.info(f"📊 Всего дней: {total_days}, батчей: {total_batches}")
        logger.info("")

        # 4. Обработка каждого периода MFI
        for idx, period in enumerate(self.periods, start=1):
            logger.info(f"[{idx}/{len(self.periods)}] MFI период {period}")

            current_date = start_date
            batch_num = 0

            pbar = tqdm(
                total=total_batches,
                desc=f"{self.symbol} {self.symbol_progress} MFI-{period} {self.timeframe.upper()}",
                unit="батч"
            )

            while current_date < end_date:
                batch_end = min(current_date + timedelta(days=self.batch_days), end_date)

                # Загружаем данные с lookback
                df = self.load_candles_with_lookback(current_date, batch_end)

                if not df.empty:
                    # Рассчитываем MFI для этого периода
                    col_name = f'mfi_{period}'
                    df[col_name] = self.calculate_mfi(df, period)

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
    log_file = logs_dir / f'mfi_loader_{timestamp}.log'

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
        description='MFI Loader - загрузка индикатора MFI для торговых данных',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 mfi_loader.py                                    # Все символы, все таймфреймы
  python3 mfi_loader.py --symbol BTCUSDT                   # Конкретный символ
  python3 mfi_loader.py --symbol BTCUSDT --timeframe 1m    # Символ + таймфрейм
  python3 mfi_loader.py --symbol BTCUSDT ETHUSDT           # Несколько символов
  python3 mfi_loader.py --batch-days 7                     # Изменить размер батча
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

    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Пересчитать все данные с начала (заполнить пробелы внутри истории)'
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
    logger.info("🚀 MFI Loader - Запуск")
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
        config['indicators']['mfi']['batch_days'] = args.batch_days
        logger.info(f"📦 Размер батча из аргументов: {args.batch_days} дней")

    # 7. Режим force_reload
    force_reload = args.force_reload if hasattr(args, 'force_reload') else False
    if force_reload:
        logger.info("🔄 РЕЖИМ FORCE RELOAD АКТИВИРОВАН")
        logger.info("   Будут пересчитаны ВСЕ данные с начала истории")
        logger.info("   Это заполнит пробелы внутри данных и обновит существующие значения")
        logger.info("")

    logger.info(f"📊 Индикатор: MFI")
    logger.info("")

    # 8. Обработка
    total_symbols = len(symbols)

    for symbol_idx, symbol in enumerate(symbols, start=1):
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{symbol_idx}/{total_symbols}]")
        logger.info("=" * 80)
        logger.info("")

        for timeframe in timeframes:
            try:
                # Создаем экземпляр загрузчика с force_reload
                loader = MFILoader(symbol, timeframe, config, force_reload=force_reload)
                loader.symbol_progress = f"[{symbol_idx}/{total_symbols}]"

                # Запускаем загрузку
                loader.load_mfi_for_symbol()

            except KeyboardInterrupt:
                logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Ошибка обработки {symbol} на {timeframe}: {e}", exc_info=True)
                continue

    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ MFI Loader - Завершено")
    logger.info(f"📝 Лог-файл: {log_file}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
