#!/usr/bin/env python3
"""
Premium Index Loader

Загрузка индикатора Premium Index от Bybit для множества символов и таймфреймов.

Premium Index - это разница между ценой фьючерса и спота:
  Premium Index = (Futures Price - Spot Price) / Spot Price

Интерпретация:
- Положительный = фьючерс дороже спота (бычий сентимент, лонги доминируют)
- Отрицательный = фьючерс дешевле спота (медвежий сентимент, шорты доминируют)
- Экстремальные значения часто предшествуют развороту

Связь с Funding Rate:
- Premium Index - опережающий индикатор для Funding Rate
- Funding Rate = усреднённый Premium Index за 8 часов + Interest Rate

Источник данных: Bybit API /v5/market/premium-index-price-kline
Исторические данные доступны с марта 2020 (зависит от символа).

Колонки:
- premium_index: Значение Premium Index (close)

Usage:
    python3 premium_index_loader.py                                    # Все символы, все таймфреймы
    python3 premium_index_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 premium_index_loader.py --symbol BTCUSDT --timeframe 1h    # Символ + таймфрейм
    python3 premium_index_loader.py --force-reload                     # Перезагрузка всех данных
"""

import sys
import logging
import argparse
import warnings
import signal
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import requests
import time
from tqdm import tqdm
import pytz

# Подавляем предупреждение pandas о DBAPI2 connection
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Глобальный флаг для graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Обработчик сигнала прерывания (Ctrl+C)"""
    global shutdown_requested
    if shutdown_requested:
        # Повторное нажатие - принудительный выход
        print("\n⚠️  Принудительное завершение...")
        sys.exit(1)
    shutdown_requested = True
    print("\n⚠️  Получен сигнал прерывания. Завершаем после текущей операции...")
    print("   (Нажмите Ctrl+C ещё раз для принудительного выхода)")


# Регистрируем обработчик сигнала
signal.signal(signal.SIGINT, signal_handler)

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# Настройка логирования
logger = logging.getLogger(__name__)

# Константы
BYBIT_API_BASE = "https://api.bybit.com"
BYBIT_API_ENDPOINT = "/v5/market/premium-index-price-kline"

# Mapping таймфреймов на интервалы API
TIMEFRAME_TO_INTERVAL = {
    '1m': '1',
    '15m': '15',
    '1h': '60',
    '4h': '240',
    '1d': 'D',
}


class PremiumIndexLoader:
    """Загрузчик индикатора Premium Index от Bybit"""

    def __init__(self, symbol: str, timeframe: str, config: dict):
        """
        Инициализация загрузчика Premium Index

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)
        self.api_interval = TIMEFRAME_TO_INTERVAL.get(timeframe, '60')

        # Настройки из конфига
        premium_config = config['indicators']['premium_index']
        # Bybit API ограничивает лимит до 1000 записей за запрос
        self.batch_size = min(premium_config.get('batch_size', 1000), 1000)
        self.api_retry_attempts = premium_config.get('api_retry_attempts', 3)
        self.api_retry_delay = premium_config.get('api_retry_delay', 2)

        # Earliest API date для этого символа (для проверки пробелов)
        earliest_dates = premium_config.get('earliest_api_dates', {})
        earliest_str = earliest_dates.get(symbol)
        if earliest_str:
            self.earliest_api_date = datetime.strptime(earliest_str, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
        else:
            # Если не указано, используем дефолтную дату (2020-03-25 для безопасности)
            self.earliest_api_date = datetime(2020, 3, 25, tzinfo=pytz.UTC)
            logger.warning(f"⚠️  earliest_api_date не указана для {symbol}, используем {self.earliest_api_date.date()}")

        # Для прогресс-бара (устанавливается извне)
        self.symbol_progress = ""

        # Флаг принудительной перезагрузки (устанавливается извне)
        self.force_reload = False

        # База данных
        self.db = DatabaseConnection()
        self.indicators_table = f"indicators_bybit_futures_{timeframe}"

        logger.info(f"Инициализирован PremiumIndexLoader для {symbol} на {timeframe}")

    def _parse_timeframe(self, tf: str) -> int:
        """Конвертация таймфрейма в минуты"""
        if tf.endswith('m'):
            return int(tf[:-1])
        elif tf.endswith('h'):
            return int(tf[:-1]) * 60
        elif tf.endswith('d'):
            return int(tf[:-1]) * 1440
        else:
            raise ValueError(f"Неизвестный формат таймфрейма: {tf}")

    def ensure_columns_exist(self):
        """Проверка и создание колонки Premium Index в таблице indicators"""

        logger.info("Проверка наличия колонки Premium Index в таблице...")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем список существующих колонок
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{self.indicators_table}'
                """)
                existing_columns = {row[0] for row in cur.fetchall()}

                # Колонка для создания
                column_name = 'premium_index'
                column_type = 'DECIMAL(20,10)'

                if column_name not in existing_columns:
                    logger.info(f"  - {column_name} (будет создана)")
                    logger.info(f"  ⏳ ALTER TABLE {self.indicators_table}... (это может занять 1-2 минуты для большой таблицы)")
                    sql = f"ALTER TABLE {self.indicators_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    cur.execute(sql)
                    conn.commit()
                    logger.info(f"  ✅ Создана колонка: {column_name}")
                else:
                    logger.info("✅ Колонка premium_index уже существует")

    def get_date_range(self):
        """
        Определение диапазона дат для обработки

        Returns:
            tuple: (start_date, end_date) в UTC
        """

        logger.info(f"🔍 Определение диапазона дат для {self.symbol} {self.timeframe}...")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Проверяем последнюю дату Premium Index в indicators таблице
                logger.info(f"   Запрос MAX(timestamp) WHERE premium_index IS NOT NULL...")
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s AND premium_index IS NOT NULL
                """, (self.symbol,))

                last_premium_date = cur.fetchone()[0]

                # 2. Получаем минимальную и максимальную дату в indicators таблице
                logger.info(f"   Запрос MIN/MAX(timestamp)...")
                cur.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                row = cur.fetchone()
                min_indicator_date, max_indicator_date = row[0], row[1]

                if max_indicator_date is None:
                    logger.warning(f"⚠️  Нет записей для {self.symbol} в {self.indicators_table}")
                    return None, None

                # 3. Определяем start_date
                if self.force_reload:
                    # Принудительная перезагрузка - начинаем с начала данных в таблице
                    # Но не раньше earliest_api_date
                    start_date = max(min_indicator_date, self.earliest_api_date)
                    logger.info(f"🔄 Режим force-reload: начинаем с {start_date}")
                elif last_premium_date is None:
                    # Данных нет - начинаем с начала таблицы или earliest_api_date
                    start_date = max(min_indicator_date, self.earliest_api_date)
                    logger.info(f"📅 Данных Premium Index нет. Начинаем с: {start_date}")
                else:
                    # Продолжаем с последней даты
                    start_date = last_premium_date + timedelta(minutes=self.timeframe_minutes)
                    logger.info(f"📅 Последняя дата Premium Index: {last_premium_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")

                # 4. end_date = последняя свеча в таблице
                end_date = max_indicator_date

                logger.info(f"📅 Диапазон в таблице: {min_indicator_date} → {max_indicator_date}")

                return start_date, end_date

    def fetch_day_data(self, day_start: datetime, day_end: datetime) -> list:
        """
        Получение данных Premium Index за один день

        Args:
            day_start: Начало дня (UTC)
            day_end: Конец дня (UTC)

        Returns:
            list: Список записей [[timestamp, open, high, low, close], ...]
        """
        all_records = []
        start_ts = int(day_start.timestamp() * 1000)
        end_ts = int(day_end.timestamp() * 1000)

        params = {
            'category': 'linear',
            'symbol': self.symbol,
            'interval': self.api_interval,
            'limit': self.batch_size,
            'start': start_ts,
            'end': end_ts,
        }

        max_pages = 10  # Для одного дня достаточно (1440 минут / 1000 = 2 страницы max)
        current_end = end_ts

        for page in range(max_pages):
            if shutdown_requested:
                break

            params['end'] = current_end

            for attempt in range(self.api_retry_attempts):
                try:
                    response = requests.get(
                        f"{BYBIT_API_BASE}{BYBIT_API_ENDPOINT}",
                        params=params,
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()

                    if data.get('retCode') != 0:
                        logger.error(f"API Error: {data.get('retMsg')}")
                        return all_records

                    records = data['result']['list']
                    if not records:
                        return all_records

                    all_records.extend(records)

                    # Проверяем, достигли ли начала диапазона
                    oldest_ts_val = int(records[-1][0])
                    if oldest_ts_val <= start_ts:
                        break

                    current_end = oldest_ts_val - 1
                    break

                except requests.exceptions.RequestException as e:
                    logger.warning(f"API attempt {attempt + 1}/{self.api_retry_attempts} failed: {e}")
                    if attempt < self.api_retry_attempts - 1:
                        time.sleep(self.api_retry_delay)
            else:
                # Все попытки неудачны
                break

            # Достигли начала
            if oldest_ts_val <= start_ts:
                break

            time.sleep(0.02)

        # Фильтруем по диапазону дня
        filtered = [r for r in all_records if start_ts <= int(r[0]) <= end_ts]
        return sorted(filtered, key=lambda x: int(x[0]))

    def save_day_to_db(self, premium_data: list) -> int:
        """
        Сохранение данных одного дня в БД

        Args:
            premium_data: Список записей за день

        Returns:
            int: Количество обновлённых записей
        """
        if not premium_data:
            return 0

        saved_count = 0

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for record in premium_data:
                    ts_ms = int(record[0])
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
                    close_value = float(record[4])

                    cur.execute(f"""
                        UPDATE {self.indicators_table}
                        SET premium_index = %s
                        WHERE timestamp = %s AND symbol = %s
                    """, (close_value, ts, self.symbol))

                    if cur.rowcount > 0:
                        saved_count += 1

                conn.commit()

        return saved_count

    def find_gaps(self) -> list:
        """
        Поиск пробелов (NULL) в данных Premium Index в диапазоне API данных

        Returns:
            list: Список дат (DATE) с NULL значениями premium_index
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Находим все даты где premium_index IS NULL
                # но только в диапазоне где API имеет данные (>= earliest_api_date)
                cur.execute(f"""
                    SELECT DISTINCT DATE(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND premium_index IS NULL
                    ORDER BY 1
                """, (self.symbol, self.earliest_api_date))

                gaps = [row[0] for row in cur.fetchall()]

        return gaps

    def fill_gaps(self, gap_dates: list):
        """
        Заполнение пробелов в данных Premium Index

        Args:
            gap_dates: Список дат с NULL значениями
        """
        if not gap_dates:
            return

        logger.info(f"🔧 Заполнение пробелов за {len(gap_dates)} дней...")

        for gap_date in tqdm(gap_dates, desc=f"🔧 {self.symbol} {self.symbol_progress} {self.timeframe} gaps", unit=" days"):
            if shutdown_requested:
                logger.info("⚠️  Прервано пользователем.")
                return

            # Определяем диапазон для этой даты
            start_dt = datetime.combine(gap_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = start_dt + timedelta(days=1)

            # Загружаем данные для этой даты
            premium_data = self.fetch_day_data(start_dt, end_dt)

            if premium_data:
                # Сохраняем данные (только для NULL записей)
                self._save_gaps_batch(premium_data)

    def _save_gaps_batch(self, premium_data: list):
        """
        Сохранение данных для заполнения пробелов

        Args:
            premium_data: Список записей [[timestamp, open, high, low, close], ...]
        """
        if not premium_data:
            return

        updates = []
        for record in premium_data:
            ts_ms = int(record[0])
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
            close_value = float(record[4])
            updates.append((ts, self.symbol, close_value))

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for ts, symbol, premium_index in updates:
                    # Используем UPDATE для обновления только существующих строк
                    # WHERE premium_index IS NULL гарантирует что не перезапишем данные
                    cur.execute(f"""
                        UPDATE {self.indicators_table}
                        SET premium_index = %s
                        WHERE timestamp = %s AND symbol = %s
                          AND premium_index IS NULL
                    """, (premium_index, ts, symbol))

                conn.commit()

    def check_and_fill_gaps(self):
        """
        Проверка и заполнение пробелов в данных Premium Index

        Returns:
            int: Количество найденных дней с пробелами
        """
        if shutdown_requested:
            return 0

        # 1. Ищем пробелы
        gaps = self.find_gaps()

        if not gaps:
            logger.info(f"✅ Пробелов не найдено для {self.symbol} {self.timeframe}")
            return 0

        logger.info(f"🔍 Найдено {len(gaps)} дней с пробелами для {self.symbol} {self.timeframe}")
        logger.info(f"   Диапазон: {gaps[0]} → {gaps[-1]}")

        # 2. Заполняем пробелы
        self.fill_gaps(gaps)

        return len(gaps)

    def load_premium_index_for_symbol(self):
        """
        Основной метод загрузки Premium Index для символа.

        Использует ежедневную загрузку от старых данных к новым:
        - Если прервётся, следующий запуск продолжит с последней даты
        - Надёжное восстановление после сбоев
        """

        if shutdown_requested:
            logger.info("⚠️  Пропуск - получен сигнал прерывания")
            return

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Загрузка Premium Index")
        logger.info("=" * 80)
        logger.info(f"⏰ Таймфрейм: {self.timeframe}")

        # 1. Проверяем и создаем колонки
        self.ensure_columns_exist()

        # 2. Определяем диапазон дат
        start_date, end_date = self.get_date_range()

        if start_date is None or end_date is None:
            logger.warning(f"⚠️  Нет данных для обработки: {self.symbol}")
            return

        # Этап 1: Загрузка данных по дням (от старых к новым)
        if start_date >= end_date:
            logger.info(f"✅ {self.symbol} - новых данных нет")
        else:
            logger.info(f"📅 Диапазон обработки: {start_date.date()} → {end_date.date()}")

            # Вычисляем количество дней
            total_days = (end_date.date() - start_date.date()).days + 1
            logger.info(f"📆 Всего дней для обработки: {total_days}")

            # Прогресс-бар по дням
            total_saved = 0
            current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

            with tqdm(
                total=total_days,
                desc=f"📅 {self.symbol} {self.symbol_progress} {self.timeframe}",
                unit=" дней",
                dynamic_ncols=True,
                leave=True
            ) as pbar:
                while current_date.date() <= end_date.date():
                    if shutdown_requested:
                        logger.info(f"⚠️  Прервано на дате {current_date.date()}. Следующий запуск продолжит отсюда.")
                        break

                    # Границы дня
                    day_start = current_date
                    day_end = current_date + timedelta(days=1) - timedelta(milliseconds=1)

                    # Ограничиваем end_date если последний день
                    if day_end > end_date:
                        day_end = end_date

                    # Загружаем данные за день
                    day_data = self.fetch_day_data(day_start, day_end)

                    # Сохраняем в БД
                    if day_data:
                        saved = self.save_day_to_db(day_data)
                        total_saved += saved

                    # Обновляем прогресс-бар
                    pbar.update(1)
                    pbar.set_postfix_str(f"{current_date.strftime('%Y-%m-%d')} | saved: {total_saved:,}")

                    # Следующий день
                    current_date += timedelta(days=1)

            logger.info(f"✅ Загружено и сохранено: {total_saved:,} записей")

        logger.info(f"✅ {self.symbol} {self.timeframe} завершен")
        logger.info("")


def setup_logging():
    """Настройка системы логирования"""

    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'premium_index_{timestamp}.log'

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

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
        description='Premium Index Loader - загрузка индикатора от Bybit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 premium_index_loader.py                                    # Все символы, все таймфреймы
  python3 premium_index_loader.py --symbol BTCUSDT                   # Конкретный символ
  python3 premium_index_loader.py --symbol BTCUSDT --timeframe 1h    # Символ + таймфрейм
  python3 premium_index_loader.py --symbol BTCUSDT ETHUSDT           # Несколько символов
  python3 premium_index_loader.py --force-reload                     # Перезагрузка всех данных

Premium Index показывает разницу между ценой фьючерса и спота.
Это опережающий индикатор для Funding Rate.
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
        '--force-reload',
        action='store_true',
        help='Принудительная перезагрузка всех данных (перезапишет существующие)'
    )

    parser.add_argument(
        '--check-nulls',
        action='store_true',
        help='Проверить и заполнить NULL значения (пробелы) в данных Premium Index'
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
    logger.info("🚀 Premium Index Loader - Запуск")
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

    logger.info(f"📊 Индикатор: Premium Index")
    if args.force_reload:
        logger.info(f"🔄 Режим: FORCE-RELOAD")
    if args.check_nulls:
        logger.info(f"🔍 Режим: CHECK-NULLS (проверка и заполнение пробелов)")
    logger.info("")

    # Засекаем время
    start_time = time.time()

    # 6. Обработка
    total_symbols = len(symbols)

    for symbol_idx, symbol in enumerate(symbols, start=1):
        if shutdown_requested:
            logger.info("⚠️  Прерывание по запросу пользователя")
            break

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{symbol_idx}/{total_symbols}]")
        logger.info("=" * 80)
        logger.info("")

        for timeframe in timeframes:
            if shutdown_requested:
                break

            try:
                loader = PremiumIndexLoader(symbol, timeframe, config)
                loader.symbol_progress = f"[{symbol_idx}/{total_symbols}]"
                loader.force_reload = args.force_reload

                if args.check_nulls:
                    logger.info(f"🔍 Проверка пробелов для {symbol} {timeframe}...")
                    loader.check_and_fill_gaps()
                else:
                    loader.load_premium_index_for_symbol()

            except Exception as e:
                logger.error(f"❌ Ошибка обработки {symbol} на {timeframe}: {e}", exc_info=True)
                continue

    # Время выполнения
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info("")
    logger.info("=" * 80)
    if shutdown_requested:
        logger.info("⚠️  Premium Index Loader - Прервано пользователем")
    else:
        logger.info("✅ Premium Index Loader - Завершено")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")
    logger.info(f"📝 Лог-файл: {log_file}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
