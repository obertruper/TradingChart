#!/usr/bin/env python3
"""
OBV (On-Balance Volume) Loader

Загрузка индикатора OBV для множества символов и таймфреймов.

OBV - кумулятивный индикатор объема, разработанный Joe Granville (1963).
Принцип: объем добавляется при росте цены и вычитается при падении.

Формула:
    If Close(t) > Close(t-1):  OBV(t) = OBV(t-1) + Volume(t)
    If Close(t) < Close(t-1):  OBV(t) = OBV(t-1) - Volume(t)
    If Close(t) = Close(t-1):  OBV(t) = OBV(t-1)
    Initial: OBV(0) = 0

Особенности реализации:
    - Кумулятивный расчет: требуется обработка всех исторических данных
    - Полный пересчет: при каждом запуске пересчитывается весь OBV от начала
    - Инкрементальная запись: в БД записываются только новые данные
    - Без чекпойнтов: возобновление происходит от MAX(timestamp) в БД
    - Производительность: расчет 5 лет данных ~1-2 минуты

Usage:
    python3 obv_loader.py                                    # Все символы, все таймфреймы
    python3 obv_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 obv_loader.py --symbol BTCUSDT --timeframe 1m    # Символ + таймфрейм
    python3 obv_loader.py --batch-days 7                     # Изменить размер батча
"""

import sys
import logging
import argparse
import warnings
from pathlib import Path
from datetime import datetime
import time
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


class OBVLoader:
    """Загрузчик индикатора OBV (On-Balance Volume) для торговых данных"""

    def __init__(self, symbol: str, timeframe: str, config: dict):
        """
        Инициализация загрузчика OBV

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)

        # Настройки из конфига
        obv_config = config['indicators']['obv']
        self.batch_days = obv_config.get('batch_days', 1)

        # Для прогресс-бара (устанавливается извне)
        self.symbol_progress = ""

        # База данных
        self.db = DatabaseConnection()
        self.candles_table = "candles_bybit_futures_1m"  # Всегда используем 1m таблицу
        self.indicators_table = f"indicators_bybit_futures_{timeframe}"

        logger.info(f"Инициализирован OBVLoader для {symbol} на {timeframe}")
        logger.info(f"Batch size: {self.batch_days} день(дней)")

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

    def create_indicators_table(self) -> bool:
        """
        Создает таблицу для индикаторов если её нет

        Returns:
            True если таблица создана или уже существует
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Проверяем существование таблицы
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = %s
                        );
                    """, (self.indicators_table,))

                    exists = cur.fetchone()[0]

                    if not exists:
                        logger.info(f"🔨 Создаю таблицу {self.indicators_table}")

                        # Создаем таблицу с базовой структурой
                        create_query = f"""
                        CREATE TABLE {self.indicators_table} (
                            timestamp TIMESTAMPTZ NOT NULL,
                            symbol VARCHAR(20) NOT NULL,
                            PRIMARY KEY (timestamp, symbol)
                        );

                        -- Создаем индексы
                        CREATE INDEX idx_{self.timeframe}_symbol_timestamp
                        ON {self.indicators_table} (symbol, timestamp);

                        CREATE INDEX idx_{self.timeframe}_timestamp
                        ON {self.indicators_table} (timestamp);
                        """

                        cur.execute(create_query)
                        conn.commit()
                        logger.info(f"✅ Таблица {self.indicators_table} создана")
                    else:
                        logger.info(f"✓ Таблица {self.indicators_table} уже существует")

                    return True

                except Exception as e:
                    logger.error(f"❌ Ошибка при создании таблицы: {e}")
                    conn.rollback()
                    return False

    def ensure_columns_exist(self):
        """Проверка и создание колонки OBV в таблице indicators"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Проверяем существование колонки obv
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = 'obv'
                """, (self.indicators_table,))

                exists = cur.fetchone() is not None

                if not exists:
                    logger.info(f"Создание колонки obv в таблице {self.indicators_table}")
                    cur.execute(f"""
                        ALTER TABLE {self.indicators_table}
                        ADD COLUMN IF NOT EXISTS obv DECIMAL(30, 8)
                    """)
                    conn.commit()
                    logger.info("✓ Колонка obv создана")
                else:
                    logger.info("✓ Колонка obv уже существует")

    def get_last_obv_date(self) -> datetime:
        """
        Получить дату последней записи OBV в БД

        Returns:
            Последняя дата с заполненным OBV или None если данных нет
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s AND obv IS NOT NULL
                """, (self.symbol,))

                result = cur.fetchone()
                last_date = result[0] if result and result[0] else None

                if last_date:
                    logger.info(f"Последняя дата OBV в БД: {last_date}")
                else:
                    logger.info("OBV данные отсутствуют в БД - будет полная загрузка")

                return last_date

    def get_earliest_candle_date(self) -> datetime:
        """
        Получить дату самой ранней свечи для данного символа

        Returns:
            Дата самой ранней свечи
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM {self.candles_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()
                if result and result[0]:
                    return result[0]
                else:
                    raise ValueError(f"Нет данных для символа {self.symbol} в таблице {self.candles_table}")

    def calculate_obv(self, df: pd.DataFrame) -> pd.Series:
        """
        Расчет OBV для всего DataFrame

        Args:
            df: DataFrame с колонками 'close' и 'volume'

        Returns:
            Series с значениями OBV
        """
        # Направление изменения цены
        price_change = df['close'].diff()

        # Signed volume (положительный при росте цены, отрицательный при падении)
        signed_volume = np.where(
            price_change > 0, df['volume'],
            np.where(price_change < 0, -df['volume'], 0)
        )

        # Кумулятивная сумма (быстрая операция)
        obv = pd.Series(signed_volume, index=df.index).cumsum()

        # Первое значение = 0 (стандарт индустрии)
        obv.iloc[0] = 0

        return obv

    def load_all_candles(self, start_date: datetime = None) -> pd.DataFrame:
        """
        Загрузка всех свечей для расчета OBV

        Args:
            start_date: Начальная дата (если None, берется самая ранняя)

        Returns:
            DataFrame с колонками timestamp, close, volume
        """
        if start_date is None:
            start_date = self.get_earliest_candle_date()

        with self.db.get_connection() as conn:
            # Выбираем нужный таймфрейм из 1m данных
            if self.timeframe == '1m':
                # Для 1m просто берем все свечи
                query = f"""
                    SELECT timestamp, close, volume
                    FROM {self.candles_table}
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
                # Для 15m и 1h - агрегируем из 1m данных
                interval_minutes = self.timeframe_minutes
                query = f"""
                    SELECT
                        date_trunc('hour', timestamp) +
                        INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as timestamp,
                        (array_agg(close ORDER BY timestamp DESC))[1] as close,
                        SUM(volume) as volume
                    FROM {self.candles_table}
                    WHERE symbol = %s AND timestamp >= %s
                    GROUP BY date_trunc('hour', timestamp) +
                             INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes})
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(self.symbol, start_date),
                    parse_dates=['timestamp']
                )

            logger.info(f"Загружено {len(df):,} свечей от {df['timestamp'].min()} до {df['timestamp'].max()}")
            return df

    def batch_update_obv(self, df: pd.DataFrame):
        """
        Батч-обновление OBV в БД по дням

        Args:
            df: DataFrame с колонками timestamp, obv
        """
        if df.empty:
            logger.info("Нет данных для обновления")
            return

        # Группируем по дням
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        grouped = df.groupby('date')

        total_days = len(grouped)
        logger.info(f"Начало обновления БД: {total_days} дней")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Progress bar для батч-обновления
                progress_desc = f"{self.symbol} {self.symbol_progress}{self.timeframe.upper()}"
                pbar = tqdm(
                    total=total_days,
                    desc=f"📊 {progress_desc} - Обновление БД",
                    unit=" день",
                    leave=False,
                    ncols=120,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
                )

                try:
                    for date, day_data in grouped:
                        # Батч UPDATE для одного дня
                        update_values = [
                            (float(row['obv']), row['timestamp'], self.symbol)
                            for _, row in day_data.iterrows()
                        ]

                        cur.executemany(f"""
                            UPDATE {self.indicators_table}
                            SET obv = %s
                            WHERE timestamp = %s AND symbol = %s
                        """, update_values)

                        pbar.update(1)

                    pbar.close()
                    conn.commit()
                    logger.info(f"✓ Обновлено {len(df):,} записей OBV")

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Ошибка при обновлении БД: {e}")
                    raise

    def load_obv_for_timeframe(self):
        """
        Основная функция загрузки OBV для конкретного таймфрейма

        Алгоритм:
        1. Определить последнюю дату OBV в БД
        2. Загрузить ВСЕ свечи от начала истории
        3. Рассчитать OBV для всех данных (кумулятивно)
        4. Отфильтровать только новые данные для записи
        5. Записать батчами по дням
        """
        logger.info("=" * 80)
        logger.info(f"Загрузка OBV для {self.symbol} на {self.timeframe}")
        logger.info("=" * 80)

        # Создаем таблицу если её нет
        if not self.create_indicators_table():
            logger.error(f"❌ Не удалось создать таблицу {self.indicators_table}")
            return

        # Проверяем наличие колонки obv
        self.ensure_columns_exist()

        # Определяем последнюю дату OBV в БД
        last_obv_date = self.get_last_obv_date()

        # Вывод сообщения о полном пересчете
        print()
        print(f"🔄 [{self.symbol}] {self.symbol_progress} [{self.timeframe}] OBV: Кумулятивный расчёт от начала истории до текущей даты")
        print("ℹ️  Это нормальная особенность индикатора OBV (требуется для точности)")
        print()

        # Загружаем ВСЕ свечи от начала истории
        logger.info("Загрузка всех исторических свечей...")
        start_time = time.time()

        df = self.load_all_candles()

        if df.empty:
            logger.warning(f"Нет данных для расчета OBV для {self.symbol}")
            return

        # Расчет OBV для всех данных
        logger.info("Расчет OBV (cumulative)...")
        print(f"🔢 Начинаю расчёт OBV для {len(df):,} свечей...")
        df['obv'] = self.calculate_obv(df)

        calc_time = time.time() - start_time

        # Красивый вывод времени расчета
        if calc_time < 60:
            time_str = f"{calc_time:.1f} секунд"
        else:
            minutes = int(calc_time // 60)
            seconds = int(calc_time % 60)
            time_str = f"{minutes} минут {seconds} секунд"

        print(f"⏱️  Расчёт OBV завершён за {time_str}")
        print()

        logger.info(f"✓ OBV рассчитан для {len(df):,} записей за {calc_time:.2f}s")

        # Фильтруем только новые данные для записи
        if last_obv_date:
            df_to_update = df[df['timestamp'] > last_obv_date].copy()
            logger.info(f"Найдено {len(df_to_update):,} новых записей для обновления (после {last_obv_date})")
        else:
            df_to_update = df.copy()
            logger.info(f"Полная загрузка: {len(df_to_update):,} записей")

        if df_to_update.empty:
            logger.info("✓ Все данные OBV уже актуальны")
            return

        # Батч-обновление БД
        self.batch_update_obv(df_to_update)

        logger.info("✓ Загрузка OBV завершена")


def setup_logging(log_dir: Path):
    """
    Настройка логирования

    Args:
        log_dir: Директория для лог-файлов
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"obv_loader_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger.info(f"📝 Логирование настроено. Лог-файл: {log_file}")


def load_config(config_path: Path) -> dict:
    """
    Загрузка конфигурации из YAML

    Args:
        config_path: Путь к файлу конфигурации

    Returns:
        Словарь с конфигурацией
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    logger.info(f"✓ Конфигурация загружена из {config_path}")
    return config


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='OBV (On-Balance Volume) Loader')
    parser.add_argument('--symbol', type=str, help='Символ для обработки (например, BTCUSDT)')
    parser.add_argument('--timeframe', type=str, help='Таймфрейм для обработки (1m, 15m, 1h)')
    parser.add_argument('--batch-days', type=int, help='Размер батча в днях (по умолчанию из конфига)')

    args = parser.parse_args()

    # Пути
    base_dir = Path(__file__).parent
    config_path = base_dir / 'indicators_config.yaml'
    log_dir = base_dir / 'logs'

    # Настройка логирования
    setup_logging(log_dir)

    logger.info("=" * 80)
    logger.info("OBV (On-Balance Volume) Loader")
    logger.info("=" * 80)

    # Загрузка конфигурации
    config = load_config(config_path)

    # Переопределение batch_days из аргументов командной строки
    if args.batch_days:
        config['indicators']['obv']['batch_days'] = args.batch_days
        logger.info(f"Размер батча переопределен: {args.batch_days} день(дней)")

    # Определяем символы и таймфреймы для обработки
    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = config['symbols']

    if args.timeframe:
        timeframes = [args.timeframe]
    else:
        timeframes = config['indicators']['obv']['timeframes']

    logger.info(f"Символы для обработки: {symbols}")
    logger.info(f"Таймфреймы для обработки: {timeframes}")

    # Засекаем время начала обработки
    start_time = time.time()

    # Подсчет общего количества символов
    total_symbols = len(symbols)
    current_symbol = 0

    # Обработка каждого символа и таймфрейма
    for symbol in symbols:
        current_symbol += 1

        for timeframe in timeframes:
            # Прогресс в формате [1/10] - номер символа из общего количества
            symbol_progress = f"[{current_symbol}/{total_symbols}] "

            logger.info("")
            logger.info("*" * 80)
            logger.info(f"{symbol_progress}Обработка {symbol} на {timeframe}")
            logger.info("*" * 80)

            try:
                loader = OBVLoader(symbol, timeframe, config)
                loader.symbol_progress = symbol_progress
                loader.load_obv_for_timeframe()

            except Exception as e:
                logger.error(f"❌ Ошибка при обработке {symbol} на {timeframe}: {e}", exc_info=True)
                continue

    # Вычисляем общее время обработки
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info("")
    logger.info("=" * 80)
    logger.info("✓ Загрузка OBV завершена для всех символов и таймфреймов")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
