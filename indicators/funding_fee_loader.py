#!/usr/bin/env python3
"""
Funding Rate Loader

Загрузка индикатора Funding Rate от Bybit для множества символов и таймфреймов.

Funding Rate - это ставка финансирования для perpetual контрактов.
Рассчитывается каждые 8 часов (00:00, 08:00, 16:00 UTC).
Положительный funding = лонги платят шортам (бычий перегрев).
Отрицательный funding = шорты платят лонгам (медвежий перегрев).

Источник данных: Bybit API /v5/market/funding/history
Исторические данные доступны с марта 2020 (зависит от символа).

Колонки:
- funding_rate_next: Ставка funding, которая будет применена в следующий расчёт
- funding_time_next: Время следующего расчёта funding (TIMESTAMPTZ)

Логика заполнения (backward-fill):
- Для периода 00:00-07:59 записываем ставку, которая будет в 08:00
- Для периода 08:00-15:59 записываем ставку, которая будет в 16:00
- И так далее

Usage:
    python3 funding_rate_loader.py                                    # Все символы, все таймфреймы
    python3 funding_rate_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 funding_rate_loader.py --symbol BTCUSDT --timeframe 1h    # Символ + таймфрейм
    python3 funding_rate_loader.py --force-reload                     # Перезагрузка всех данных
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
BYBIT_API_ENDPOINT = "/v5/market/funding/history"

# Funding рассчитывается каждые 8 часов
FUNDING_INTERVAL_HOURS = 8
FUNDING_TIMES_UTC = [0, 8, 16]  # 00:00, 08:00, 16:00 UTC


class FundingRateLoader:
    """Загрузчик индикатора Funding Rate от Bybit"""

    def __init__(self, symbol: str, timeframe: str, config: dict):
        """
        Инициализация загрузчика Funding Rate

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)

        # Настройки из конфига
        funding_config = config['indicators']['funding_rate']
        # Bybit API ограничивает лимит до 200 записей за запрос
        self.batch_size = min(funding_config.get('batch_size', 200), 200)
        self.api_retry_attempts = funding_config.get('api_retry_attempts', 3)
        self.api_retry_delay = funding_config.get('api_retry_delay', 2)

        # Earliest API date для этого символа (для проверки пробелов)
        earliest_dates = funding_config.get('earliest_api_dates', {})
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

        logger.info(f"Инициализирован FundingRateLoader для {symbol} на {timeframe}")

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
        """Проверка и создание колонок Funding Rate в таблице indicators"""

        logger.info("Проверка наличия колонок Funding Rate в таблице...")

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
                required_columns = {
                    'funding_rate_next': 'DECIMAL(12,10)',
                    'funding_time_next': 'TIMESTAMPTZ',
                }

                for col_name, col_type in required_columns.items():
                    if col_name not in existing_columns:
                        columns_to_add.append(f'{col_name} {col_type}')
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
                    logger.info("✅ Все колонки Funding Rate созданы")
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
                # 1. Проверяем последнюю дату Funding Rate в indicators таблице
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s AND funding_rate_next IS NOT NULL
                """, (self.symbol,))

                last_funding_date = cur.fetchone()[0]

                # 2. Получаем минимальную и максимальную дату в indicators таблице
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
                    start_date = min_indicator_date
                    logger.info(f"🔄 Режим force-reload: начинаем с {start_date}")
                elif last_funding_date is None:
                    # Данных нет - начинаем с начала таблицы
                    start_date = min_indicator_date
                    logger.info(f"📅 Данных Funding Rate нет. Начинаем с: {start_date}")
                else:
                    # Продолжаем с последней даты
                    start_date = last_funding_date + timedelta(minutes=self.timeframe_minutes)
                    logger.info(f"📅 Последняя дата Funding Rate: {last_funding_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")

                # 4. end_date = последняя свеча в таблице
                end_date = max_indicator_date

                logger.info(f"📅 Диапазон в таблице: {min_indicator_date} → {max_indicator_date}")

                return start_date, end_date

    def fetch_all_funding_history(self) -> list:
        """
        Получение всей истории Funding Rate от Bybit API

        Returns:
            list: Список словарей с данными [{fundingRate, fundingRateTimestamp}, ...]
                  отсортированный по времени (от старых к новым)
        """

        all_records = []
        params = {
            'category': 'linear',
            'symbol': self.symbol,
            'limit': self.batch_size,
        }

        max_pages = 500  # Ограничение для безопасности

        # Прогресс-бар для API загрузки
        with tqdm(
            desc=f"📡 {self.symbol} API",
            unit=" записей",
            dynamic_ncols=True,
            leave=True
        ) as pbar:
            pages = 0
            while pages < max_pages and not shutdown_requested:
                # Попытки с retry
                success = False
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
                            pbar.close()
                            return sorted(all_records, key=lambda x: int(x['fundingRateTimestamp']))

                        records = data['result']['list']
                        if not records:
                            # Достигли конца истории
                            success = True
                            break

                        all_records.extend(records)
                        pbar.update(len(records))

                        # Показываем диапазон дат в описании
                        if len(all_records) > 0:
                            newest_ts = datetime.fromtimestamp(
                                int(all_records[0]['fundingRateTimestamp']) / 1000, tz=pytz.UTC
                            )
                            oldest_ts = datetime.fromtimestamp(
                                int(records[-1]['fundingRateTimestamp']) / 1000, tz=pytz.UTC
                            )
                            pbar.set_postfix_str(f"{oldest_ts.strftime('%Y-%m-%d')} → {newest_ts.strftime('%Y-%m-%d')}")

                        # Получаем самый старый timestamp для следующего запроса
                        oldest_ts_val = int(records[-1]['fundingRateTimestamp'])
                        params['endTime'] = oldest_ts_val - 1

                        pages += 1
                        success = True
                        break

                    except requests.exceptions.RequestException as e:
                        logger.warning(f"Попытка {attempt + 1}/{self.api_retry_attempts} не удалась: {e}")
                        if attempt < self.api_retry_attempts - 1:
                            time.sleep(self.api_retry_delay)

                if not success or not records:
                    break

                # Пауза между запросами
                time.sleep(0.05)

        logger.info(f"✅ Загружено {len(all_records)} записей Funding Rate")

        # Сортируем по времени (от старых к новым)
        return sorted(all_records, key=lambda x: int(x['fundingRateTimestamp']))

    def get_next_funding_time(self, current_time: datetime) -> datetime:
        """
        Определение времени следующего расчёта funding

        Args:
            current_time: Текущее время (UTC)

        Returns:
            datetime: Время следующего расчёта funding
        """
        current_hour = current_time.hour

        # Находим следующий час расчёта funding
        for funding_hour in FUNDING_TIMES_UTC:
            if current_hour < funding_hour:
                return current_time.replace(hour=funding_hour, minute=0, second=0, microsecond=0)

        # Если текущий час >= 16, следующий funding в 00:00 следующего дня
        next_day = current_time + timedelta(days=1)
        return next_day.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_funding_period_start(self, funding_time: datetime) -> datetime:
        """
        Определение начала периода для данного funding time

        Args:
            funding_time: Время расчёта funding (00:00, 08:00 или 16:00)

        Returns:
            datetime: Начало периода (8 часов назад)
        """
        return funding_time - timedelta(hours=FUNDING_INTERVAL_HOURS)

    def save_to_db(self, start_date: datetime, end_date: datetime, funding_data: list):
        """
        Сохранение данных в indicators таблицу с backward-fill логикой

        Args:
            start_date: Начало диапазона для обновления
            end_date: Конец диапазона для обновления
            funding_data: Список записей funding (отсортированный по времени)
        """

        if not funding_data:
            logger.warning("Нет данных funding для сохранения")
            return

        # Создаём словарь funding_time -> funding_rate
        funding_dict = {}
        for record in funding_data:
            ts_ms = int(record['fundingRateTimestamp'])
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
            rate = float(record['fundingRate'])
            funding_dict[ts] = rate

        # Получаем все timestamps из таблицы для обновления
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT timestamp
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp
                """, (self.symbol, start_date, end_date))

                timestamps = [row[0] for row in cur.fetchall()]

        if not timestamps:
            logger.warning(f"Нет timestamps для обновления в диапазоне {start_date} → {end_date}")
            return

        logger.info(f"📝 Обновление {len(timestamps)} записей...")

        # Подготавливаем данные для batch update
        updates = []
        for ts in timestamps:
            # Определяем время следующего funding для этого timestamp
            next_funding_time = self.get_next_funding_time(ts)

            # Находим ставку для этого funding time
            funding_rate = funding_dict.get(next_funding_time)

            if funding_rate is not None:
                updates.append((funding_rate, next_funding_time, ts, self.symbol))

        if not updates:
            logger.warning("Нет данных для обновления (funding rates не найдены)")
            return

        # Batch update с прогресс-баром
        updated_count = 0
        batch_size = 1000
        total_batches = (len(updates) + batch_size - 1) // batch_size

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                with tqdm(
                    total=len(updates),
                    desc=f"💾 {self.symbol} {self.timeframe} DB",
                    unit=" rows",
                    dynamic_ncols=True,
                    leave=True
                ) as pbar:
                    for i in range(0, len(updates), batch_size):
                        if shutdown_requested:
                            conn.commit()  # Сохраняем то, что успели
                            logger.info("⚠️  Прервано пользователем. Данные сохранены.")
                            return

                        batch = updates[i:i + batch_size]

                        for funding_rate, next_funding_time, ts, symbol in batch:
                            cur.execute(f"""
                                UPDATE {self.indicators_table}
                                SET funding_rate_next = %s,
                                    funding_time_next = %s
                                WHERE timestamp = %s AND symbol = %s
                                  AND funding_rate_next IS NULL
                            """, (funding_rate, next_funding_time, ts, symbol))
                            updated_count += cur.rowcount

                        conn.commit()
                        pbar.update(len(batch))

        logger.info(f"✅ Обновлено {updated_count} записей")

    def find_gaps(self) -> list:
        """
        Поиск пробелов (NULL) в данных Funding Rate в диапазоне API данных

        Returns:
            list: Список timestamps с NULL значениями funding_rate_next
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # Находим все timestamps где funding_rate_next IS NULL
                # но только в диапазоне где API имеет данные (>= earliest_api_date)
                cur.execute(f"""
                    SELECT timestamp
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND funding_rate_next IS NULL
                    ORDER BY timestamp
                """, (self.symbol, self.earliest_api_date))

                gaps = [row[0] for row in cur.fetchall()]

        return gaps

    def fill_gaps(self, gap_timestamps: list, funding_data: list):
        """
        Заполнение пробелов в данных Funding Rate

        Args:
            gap_timestamps: Список timestamps с NULL значениями
            funding_data: Данные funding из API (отсортированные по времени)
        """
        if not gap_timestamps or not funding_data:
            return

        logger.info(f"🔧 Заполнение {len(gap_timestamps)} пробелов...")

        # Создаём словарь funding_time -> funding_rate
        funding_dict = {}
        for record in funding_data:
            ts_ms = int(record['fundingRateTimestamp'])
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
            rate = float(record['fundingRate'])
            funding_dict[ts] = rate

        # Подготавливаем данные для обновления
        updates = []
        for ts in gap_timestamps:
            next_funding_time = self.get_next_funding_time(ts)
            funding_rate = funding_dict.get(next_funding_time)

            if funding_rate is not None:
                updates.append((funding_rate, next_funding_time, ts, self.symbol))

        if not updates:
            logger.warning("⚠️  Не удалось найти данные для заполнения пробелов")
            return

        # Batch update с прогресс-баром
        updated_count = 0
        batch_size = 1000

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                with tqdm(
                    total=len(updates),
                    desc=f"🔧 {self.symbol} {self.timeframe} gaps",
                    unit=" rows",
                    dynamic_ncols=True,
                    leave=True
                ) as pbar:
                    for i in range(0, len(updates), batch_size):
                        if shutdown_requested:
                            conn.commit()
                            logger.info("⚠️  Прервано пользователем. Данные сохранены.")
                            return

                        batch = updates[i:i + batch_size]

                        for funding_rate, next_funding_time, ts, symbol in batch:
                            cur.execute(f"""
                                UPDATE {self.indicators_table}
                                SET funding_rate_next = %s,
                                    funding_time_next = %s
                                WHERE timestamp = %s AND symbol = %s
                                  AND funding_rate_next IS NULL
                            """, (funding_rate, next_funding_time, ts, symbol))
                            updated_count += cur.rowcount

                        conn.commit()
                        pbar.update(len(batch))

        logger.info(f"✅ Заполнено {updated_count} пробелов")

    def check_and_fill_gaps(self):
        """
        Проверка и заполнение пробелов в данных Funding Rate

        Returns:
            int: Количество найденных и заполненных пробелов
        """
        if shutdown_requested:
            return 0

        # 1. Ищем пробелы
        gaps = self.find_gaps()

        if not gaps:
            logger.info(f"✅ Пробелов не найдено для {self.symbol} {self.timeframe}")
            return 0

        logger.info(f"🔍 Найдено {len(gaps)} пробелов для {self.symbol} {self.timeframe}")
        logger.info(f"   Диапазон: {gaps[0]} → {gaps[-1]}")

        # 2. Загружаем данные с API (если ещё не загружены)
        funding_data = self.fetch_all_funding_history()

        if not funding_data:
            logger.warning(f"⚠️  Нет данных funding от API для заполнения пробелов")
            return 0

        # 3. Заполняем пробелы
        self.fill_gaps(gaps, funding_data)

        return len(gaps)

    def load_funding_for_symbol(self):
        """Основной метод загрузки Funding Rate для символа"""

        if shutdown_requested:
            logger.info("⚠️  Пропуск - получен сигнал прерывания")
            return

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Загрузка Funding Rate")
        logger.info("=" * 80)
        logger.info(f"⏰ Таймфрейм: {self.timeframe}")

        # 1. Проверяем и создаем колонки
        self.ensure_columns_exist()

        # 2. Определяем диапазон дат
        start_date, end_date = self.get_date_range()

        if start_date is None or end_date is None:
            logger.warning(f"⚠️  Нет данных для обработки: {self.symbol}")
            return

        # Этап 1: Загрузка новых данных (если есть)
        if start_date >= end_date:
            logger.info(f"✅ {self.symbol} - новых данных нет")
        else:
            logger.info(f"📅 Диапазон обработки: {start_date} → {end_date}")

            # 3. Загружаем всю историю funding с API
            funding_data = self.fetch_all_funding_history()

            if not funding_data:
                logger.warning(f"⚠️  Нет данных funding от API для {self.symbol}")
            else:
                # Показываем диапазон загруженных данных
                oldest_ts = datetime.fromtimestamp(int(funding_data[0]['fundingRateTimestamp']) / 1000, tz=pytz.UTC)
                newest_ts = datetime.fromtimestamp(int(funding_data[-1]['fundingRateTimestamp']) / 1000, tz=pytz.UTC)
                logger.info(f"📡 Данные API: {oldest_ts} → {newest_ts} ({len(funding_data)} записей)")

                # 4. Сохраняем в БД с backward-fill
                self.save_to_db(start_date, end_date, funding_data)

        # Этап 2: Проверка и заполнение пробелов
        if not shutdown_requested:
            logger.info(f"🔍 Проверка пробелов для {self.symbol} {self.timeframe}...")
            self.check_and_fill_gaps()

        logger.info(f"✅ {self.symbol} {self.timeframe} завершен")
        logger.info("")


def setup_logging():
    """Настройка системы логирования"""

    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'funding_rate_{timestamp}.log'

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
        description='Funding Rate Loader - загрузка индикатора от Bybit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 funding_rate_loader.py                                    # Все символы, все таймфреймы
  python3 funding_rate_loader.py --symbol BTCUSDT                   # Конкретный символ
  python3 funding_rate_loader.py --symbol BTCUSDT --timeframe 1h    # Символ + таймфрейм
  python3 funding_rate_loader.py --symbol BTCUSDT ETHUSDT           # Несколько символов
  python3 funding_rate_loader.py --force-reload                     # Перезагрузка всех данных

Funding Rate рассчитывается каждые 8 часов (00:00, 08:00, 16:00 UTC).
Данные заполняются методом backward-fill: для каждой минуты записывается
ставка, которая будет применена в следующий расчёт.
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
        help='Принудительная перезагрузка всех данных (заполнит пропуски)'
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
    logger.info("🚀 Funding Rate Loader - Запуск")
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

    logger.info(f"📊 Индикатор: Funding Rate")
    if args.force_reload:
        logger.info(f"🔄 Режим: FORCE-RELOAD")
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
                loader = FundingRateLoader(symbol, timeframe, config)
                loader.symbol_progress = f"[{symbol_idx}/{total_symbols}]"
                loader.force_reload = args.force_reload

                loader.load_funding_for_symbol()

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
        logger.info("⚠️  Funding Rate Loader - Прервано пользователем")
    else:
        logger.info("✅ Funding Rate Loader - Завершено")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")
    logger.info(f"📝 Лог-файл: {log_file}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
