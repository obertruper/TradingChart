#!/usr/bin/env python3
"""
Open Interest Loader

Загрузка индикатора Open Interest от Bybit для множества символов и таймфреймов.

Open Interest показывает общее количество открытых фьючерсных контрактов.
Используется для анализа силы тренда и прогнозирования разворотов.

Источник данных: Bybit API /v5/market/open-interest
Исторические данные доступны примерно с октября 2023 года (~27 месяцев).

Usage:
    python3 open_interest_loader.py                                    # Все символы, все таймфреймы
    python3 open_interest_loader.py --symbol BTCUSDT                   # Конкретный символ
    python3 open_interest_loader.py --symbol BTCUSDT --timeframe 15m   # Символ + таймфрейм
    python3 open_interest_loader.py --force-reload                     # Перезагрузка всех данных с октября 2023
"""

import sys
import logging
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import requests
import time
from tqdm import tqdm
import pytz

# Подавляем предупреждение pandas о DBAPI2 connection
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# Настройка логирования
logger = logging.getLogger(__name__)

# Константы
BYBIT_API_BASE = "https://api.bybit.com"
BYBIT_API_ENDPOINT = "/v5/market/open-interest"
# API Open Interest доступен примерно с октября 2023 (проверено через API)
EARLIEST_DATA_DATE = datetime(2023, 10, 1, tzinfo=pytz.UTC)


class OpenInterestLoader:
    """Загрузчик индикатора Open Interest от Bybit"""

    def __init__(self, symbol: str, timeframe: str, config: dict):
        """
        Инициализация загрузчика Open Interest

        Args:
            symbol: Торговая пара (например, BTCUSDT)
            timeframe: Таймфрейм (1m, 15m, 1h)
            config: Конфигурация из indicators_config.yaml
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_minutes = self._parse_timeframe(timeframe)

        # Настройки из конфига
        oi_config = config['indicators']['open_interest']
        # ВАЖНО: Bybit API ограничивает лимит до 200 записей за запрос для Open Interest
        self.batch_size = min(oi_config.get('batch_size', 200), 200)
        self.api_retry_attempts = oi_config.get('api_retry_attempts', 3)
        self.api_retry_delay = oi_config.get('api_retry_delay', 2)

        # Для прогресс-бара (устанавливается извне)
        self.symbol_progress = ""

        # Флаг принудительной перезагрузки (устанавливается извне)
        self.force_reload = False

        # База данных
        self.db = DatabaseConnection()
        self.indicators_table = f"indicators_bybit_futures_{timeframe}"

        # Маппинг таймфреймов для API Bybit
        # Open Interest API поддерживает: 5min, 15min, 30min, 1h, 4h, 1d
        self.api_interval_map = {
            '1m': None,  # API не поддерживает 1m, будем использовать 5min и интерполировать или NULL
            '15m': '15min',
            '1h': '1h'
        }

        logger.info(f"Инициализирован OpenInterestLoader для {symbol} на {timeframe}")

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
        """Проверка и создание колонок Open Interest в таблице indicators"""

        logger.info("Проверка наличия колонок Open Interest в таблице...")

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
                    'open_interest': 'DECIMAL(30,8)',  # OI в базовой валюте (BTC, ETH, etc.)
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
                    logger.info("✅ Все колонки Open Interest созданы")
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
                # 1. Проверяем последнюю дату Open Interest в indicators таблице
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s AND open_interest IS NOT NULL
                """, (self.symbol,))

                last_oi_date = cur.fetchone()[0]

                # 2. Получаем максимальную дату в indicators таблице (для end_date)
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                max_indicator_date = cur.fetchone()[0]

                if max_indicator_date is None:
                    logger.warning(f"⚠️  Нет записей для {self.symbol} в {self.indicators_table}")
                    return None, None

                # 3. Определяем start_date
                if self.force_reload:
                    # Принудительная перезагрузка - начинаем с самой ранней даты API
                    start_date = EARLIEST_DATA_DATE
                    logger.info(f"🔄 Режим force-reload: начинаем с {start_date}")
                elif last_oi_date is None:
                    # Данных нет - начинаем с самой ранней доступной даты
                    start_date = EARLIEST_DATA_DATE
                    logger.info(f"📅 Данных Open Interest нет. Начинаем с: {start_date}")
                else:
                    # Продолжаем с последней даты
                    start_date = last_oi_date + timedelta(minutes=self.timeframe_minutes)
                    logger.info(f"📅 Последняя дата Open Interest: {last_oi_date}")
                    logger.info(f"▶️  Продолжаем с: {start_date}")

                # 4. Определяем end_date (последняя завершенная свеча)
                end_date = max_indicator_date

                # Выравниваем до начала периода
                if self.timeframe == '1m':
                    end_date = end_date.replace(second=0, microsecond=0)
                elif self.timeframe == '15m':
                    minutes = (end_date.minute // 15) * 15
                    end_date = end_date.replace(minute=minutes, second=0, microsecond=0)
                elif self.timeframe == '1h':
                    end_date = end_date.replace(minute=0, second=0, microsecond=0)

                logger.info(f"📅 Максимальная дата в indicators: {max_indicator_date}")
                logger.info(f"⏸️  Ограничение end_date до последней завершенной свечи: {end_date}")

                return start_date, end_date

    def fetch_from_bybit_api(self, cursor: str = None) -> tuple:
        """
        Получение данных Open Interest от Bybit API

        API возвращает данные в обратном порядке (от новых к старым).
        Используем cursor-based пагинацию для получения исторических данных.

        Args:
            cursor: Курсор для пагинации (из предыдущего ответа). None для первого запроса.

        Returns:
            tuple: (list of records, next_cursor or None)
        """

        # Параметры запроса
        params = {
            'category': 'linear',
            'symbol': self.symbol,
            'intervalTime': self.api_interval_map[self.timeframe],
            'limit': self.batch_size,
        }

        if cursor:
            params['cursor'] = cursor

        url = f"{BYBIT_API_BASE}{BYBIT_API_ENDPOINT}"

        # Попытки с retry
        for attempt in range(self.api_retry_attempts):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                # Проверка ответа
                if data.get('retCode') != 0:
                    logger.error(f"API Error: {data.get('retMsg')}")
                    return [], None

                result = data.get('result', {})
                result_list = result.get('list', [])
                next_cursor = result.get('nextPageCursor')

                logger.debug(f"Получено {len(result_list)} записей от API")
                return result_list, next_cursor

            except requests.exceptions.RequestException as e:
                logger.warning(f"Попытка {attempt + 1}/{self.api_retry_attempts} не удалась: {e}")
                if attempt < self.api_retry_attempts - 1:
                    time.sleep(self.api_retry_delay)
                else:
                    logger.error(f"Не удалось получить данные после {self.api_retry_attempts} попыток")
                    return [], None

        return [], None

    def save_to_db(self, data: list):
        """
        Сохранение данных в indicators таблицу

        Использует INSERT...ON CONFLICT для создания/обновления записей.
        Обновляет только те записи, где open_interest IS NULL.

        Args:
            data: Список словарей с данными от API
        """

        if not data:
            return

        inserted_count = 0

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for record in data:
                    # Парсинг данных
                    timestamp_ms = int(record['timestamp'])
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                    open_interest = float(record['openInterest'])

                    # INSERT...ON CONFLICT для создания/обновления записей
                    sql = f"""
                        INSERT INTO {self.indicators_table}
                            (timestamp, symbol, open_interest)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (timestamp, symbol)
                        DO UPDATE SET
                            open_interest = EXCLUDED.open_interest
                        WHERE {self.indicators_table}.open_interest IS NULL
                    """

                    cur.execute(sql, (timestamp, self.symbol, open_interest))

                    if cur.rowcount > 0:
                        inserted_count += 1

                conn.commit()

        if inserted_count > 0:
            logger.debug(f"Записано: {inserted_count} записей в БД")

    def set_null_for_existing_records(self):
        """
        Установка NULL значений для таймфрейма 1m

        API Bybit не поддерживает интервал 1m для Open Interest.
        Минимальный интервал - 5min.
        """

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Open Interest - 1m (NULL)")
        logger.info("=" * 80)
        logger.info("⚠️  API Bybit не поддерживает интервал 1m для Open Interest")
        logger.info("   Минимальный доступный интервал: 5min")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Находим последнюю запись в таблице вообще
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                """, (self.symbol,))

                max_timestamp = cur.fetchone()[0]

                if max_timestamp is None:
                    logger.warning(f"⚠️  Нет записей для {self.symbol} в {self.indicators_table}")
                    logger.info("")
                    return

                # 2. Находим последнюю обработанную запись (где уже установлены NULL)
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.indicators_table}
                    WHERE symbol = %s
                      AND open_interest IS NULL
                """, (self.symbol,))

                last_null_timestamp = cur.fetchone()[0]

                # 3. Определяем, что нужно обновить
                if last_null_timestamp is None:
                    # Первый запуск - обрабатываем все записи
                    logger.info("📝 Первая загрузка - устанавливаем NULL для всех записей")
                    logger.info(f"📊 Диапазон: начало → {max_timestamp}")

                    sql = f"""
                        UPDATE {self.indicators_table}
                        SET
                            open_interest = NULL
                        WHERE symbol = %s
                    """
                    cur.execute(sql, (self.symbol,))

                elif last_null_timestamp < max_timestamp:
                    # Есть новые записи - обрабатываем только их
                    next_timestamp = last_null_timestamp + timedelta(minutes=1)
                    logger.info(f"📅 Последняя обработанная дата: {last_null_timestamp}")
                    logger.info(f"▶️  Обновляем новые записи: {next_timestamp} → {max_timestamp}")

                    sql = f"""
                        UPDATE {self.indicators_table}
                        SET
                            open_interest = NULL
                        WHERE symbol = %s
                          AND timestamp > %s
                    """
                    cur.execute(sql, (self.symbol, last_null_timestamp))

                else:
                    # last_null_timestamp == max_timestamp - все уже обработано
                    logger.info(f"✅ Все записи уже обработаны (до {max_timestamp})")
                    logger.info("📊 Нет новых данных для обновления")
                    logger.info("")
                    return

                updated_rows = cur.rowcount
                conn.commit()

                if updated_rows > 0:
                    logger.info(f"✅ Установлено NULL для {updated_rows} новых записей")
                else:
                    logger.info(f"ℹ️  Нет записей для обновления")

                logger.info("")

    def load_oi_for_symbol(self):
        """Основной метод загрузки Open Interest для символа"""

        # Проверка: API поддерживает этот таймфрейм?
        if self.api_interval_map[self.timeframe] is None:
            logger.warning(f"⚠️  API Bybit не поддерживает интервал {self.timeframe}")
            self.set_null_for_existing_records()
            return

        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📊 {self.symbol} {self.symbol_progress} Загрузка Open Interest")
        logger.info("=" * 80)
        logger.info(f"⏰ Таймфрейм: {self.timeframe}")
        logger.info(f"📦 Batch size: {self.batch_size} записей")

        # 1. Проверяем и создаем колонки
        self.ensure_columns_exist()

        # 2. Определяем диапазон дат
        start_date, end_date = self.get_date_range()

        if start_date is None or end_date is None:
            logger.warning(f"⚠️  Нет данных для обработки: {self.symbol}")
            return

        if start_date >= end_date:
            logger.info(f"✅ {self.symbol} - данные Open Interest актуальны")
            return

        # 3. Рассчитываем количество батчей (приблизительно)
        total_periods = int((end_date - start_date).total_seconds() / (self.timeframe_minutes * 60))
        total_batches = max(1, (total_periods + self.batch_size - 1) // self.batch_size)

        logger.info(f"📅 Диапазон обработки: {start_date} → {end_date}")
        logger.info(f"📊 Ожидаемое количество батчей: ~{total_batches}")
        logger.info("")

        # 4. Загрузка данных с cursor-based пагинацией (от новых к старым)
        cursor = None
        batch_num = 0
        total_saved = 0
        reached_start = False

        pbar = tqdm(
            total=total_batches,
            desc=f"{self.symbol} {self.symbol_progress} OPEN-INTEREST {self.timeframe.upper()}",
            unit="батч"
        )

        while not reached_start:
            # Получаем данные от API с cursor-based пагинацией
            api_data, next_cursor = self.fetch_from_bybit_api(cursor)

            if not api_data:
                # Если нет данных, возможно достигли начала истории
                logger.debug("Нет данных от API - достигли конца истории")
                break

            # Фильтруем записи, которые старше start_date
            filtered_data = []
            for record in api_data:
                timestamp_ms = int(record['timestamp'])
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                if timestamp >= start_date:
                    filtered_data.append(record)
                else:
                    reached_start = True

            # Сохраняем в БД
            if filtered_data:
                self.save_to_db(filtered_data)
                total_saved += len(filtered_data)

            batch_num += 1
            pbar.update(1)

            # Проверяем, есть ли следующая страница
            if not next_cursor:
                logger.debug("Нет следующей страницы - достигли конца истории")
                break

            cursor = next_cursor

            # Небольшая пауза между запросами для rate limit
            time.sleep(0.1)

        pbar.close()

        logger.info(f"✅ {self.symbol} завершен. Записано: {total_saved} записей")
        logger.info("")


def setup_logging():
    """Настройка системы логирования"""

    # Создаем директорию для логов
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    # Имя файла лога с текущей датой и временем
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'open_interest_{timestamp}.log'

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
        description='Open Interest Loader - загрузка индикатора от Bybit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 open_interest_loader.py                                    # Все символы, все таймфреймы
  python3 open_interest_loader.py --symbol BTCUSDT                   # Конкретный символ
  python3 open_interest_loader.py --symbol BTCUSDT --timeframe 15m   # Символ + таймфрейм
  python3 open_interest_loader.py --symbol BTCUSDT ETHUSDT           # Несколько символов
  python3 open_interest_loader.py --force-reload                     # Перезагрузка с октября 2023
  python3 open_interest_loader.py --symbol BTCUSDT --force-reload    # Перезагрузка для BTCUSDT

ВНИМАНИЕ: API Bybit не поддерживает интервал 1m. Для таймфрейма 1m будут установлены NULL значения.
Минимальный поддерживаемый интервал: 5min.
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
        help='Принудительная перезагрузка всех данных с октября 2023 (заполнит пропуски)'
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
    logger.info("🚀 Open Interest Loader - Запуск")
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

    logger.info(f"📊 Индикатор: Open Interest")
    if args.force_reload:
        logger.info(f"🔄 Режим: FORCE-RELOAD (перезагрузка с {EARLIEST_DATA_DATE.date()})")
    logger.info("")

    # Засекаем время начала обработки
    start_time = time.time()

    # 6. Обработка: символ → таймфреймы (последовательно)
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
                loader = OpenInterestLoader(symbol, timeframe, config)
                loader.symbol_progress = f"[{symbol_idx}/{total_symbols}]"
                loader.force_reload = args.force_reload

                # Запускаем загрузку
                if timeframe == '1m':
                    # Для 1m устанавливаем NULL
                    loader.ensure_columns_exist()
                    loader.set_null_for_existing_records()
                else:
                    # Для 15m и 1h загружаем реальные данные
                    loader.load_oi_for_symbol()

            except KeyboardInterrupt:
                logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Ошибка обработки {symbol} на {timeframe}: {e}", exc_info=True)
                continue

    # Вычисляем общее время обработки
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ Open Interest Loader - Завершено")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")
    logger.info(f"📝 Лог-файл: {log_file}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
