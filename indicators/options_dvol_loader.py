#!/usr/bin/env python3
"""
Options DVOL Loader — Deribit Volatility Index (DVOL)
=====================================================
Загружает DVOL (implied volatility index) OHLC свечи с Deribit API.
DVOL — криптоаналог VIX, показывает ожидаемую 30-дневную волатильность.

Источник: Deribit public API (аутентификация не требуется)
Таблицы: options_deribit_dvol_1m, options_deribit_dvol_1h
Валюты: BTC, ETH
Резолюции: 1m (60), 1h (3600)
История: 1h — с 2021-03-24 (запуск DVOL), 1m — скользящее окно ~186 дней
Батчинг: 1 день на чанк, коммит после каждого дня

Использование:
    python3 options_dvol_loader.py                        # BTC, 1m + 1h
    python3 options_dvol_loader.py --currency ETH         # ETH, 1m + 1h
    python3 options_dvol_loader.py --timeframe 1h         # BTC, только 1h
    python3 options_dvol_loader.py --timeframe 1m         # BTC, только 1m
    python3 options_dvol_loader.py --force-reload         # Полная перезагрузка
    python3 options_dvol_loader.py --currency BTCUSDT --timeframe 1m
"""

import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from typing import Optional, List
import logging
import argparse
import signal
import time
import sys
import os
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators.database import DatabaseConnection

# ─── Константы ───────────────────────────────────────────────────────────────

DERIBIT_API_URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"

# Конфигурация таймфреймов
TIMEFRAME_CONFIG = {
    '1m': {
        'table': 'options_deribit_dvol_1m',
        'resolution': '60',
        'interval': timedelta(minutes=1),
    },
    '1h': {
        'table': 'options_deribit_dvol_1h',
        'resolution': '3600',
        'interval': timedelta(hours=1),
    },
}

ALL_TIMEFRAMES = ['1m', '1h']

# Дата запуска DVOL на Deribit (проверено через API)
DVOL_EARLIEST_1H = {
    'BTC': datetime(2021, 3, 24, 0, 0, tzinfo=timezone.utc),
    'ETH': datetime(2021, 3, 24, 0, 0, tzinfo=timezone.utc),
}

# 1m данные доступны только за последние ~186 дней (скользящее окно Deribit)
DVOL_1M_WINDOW_DAYS = 180  # консервативно от 186

CHUNK_DAYS = 1
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
REQUEST_TIMEOUT = 30

# ─── Graceful Shutdown ───────────────────────────────────────────────────────

shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    if shutdown_requested:
        logger.warning("Повторный Ctrl+C — принудительный выход")
        sys.exit(1)
    shutdown_requested = True
    logger.info("Ctrl+C — завершаю после текущего чанка...")


signal.signal(signal.SIGINT, signal_handler)

# ─── Логирование ─────────────────────────────────────────────────────────────


def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f'options_dvol_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    )

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename, encoding='utf-8')
        ]
    )

    _logger = logging.getLogger(__name__)
    _logger.info(f"Log: {log_filename}")
    return _logger


logger = setup_logging()

# ─── Loader ──────────────────────────────────────────────────────────────────


class OptionsDvolLoader:
    """Загрузчик DVOL свечей с Deribit API"""

    # Нормализация: BTCUSDT/ETHUSDT → BTC/ETH
    CURRENCY_ALIASES = {
        'BTCUSDT': 'BTC', 'ETHUSDT': 'ETH',
        'BTC': 'BTC', 'ETH': 'ETH',
    }

    def __init__(self, currency: str = 'BTC', timeframe: str = '1h',
                 force_reload: bool = False):
        self.db = DatabaseConnection()
        raw = currency.upper()
        self.currency = self.CURRENCY_ALIASES.get(raw, raw)
        self.timeframe = timeframe
        self.force_reload = force_reload
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'TradingChart/1.0'})

        if self.currency not in DVOL_EARLIEST_1H:
            raise ValueError(f"Неподдерживаемая валюта: {self.currency}. "
                             f"Доступны: {list(DVOL_EARLIEST_1H.keys())}")

        if self.timeframe not in TIMEFRAME_CONFIG:
            raise ValueError(f"Неподдерживаемый таймфрейм: {self.timeframe}. "
                             f"Доступны: {list(TIMEFRAME_CONFIG.keys())}")

        cfg = TIMEFRAME_CONFIG[self.timeframe]
        self.table_name = cfg['table']
        self.resolution = cfg['resolution']
        self.interval = cfg['interval']

    def _get_earliest_date(self) -> datetime:
        """Определяет самую раннюю доступную дату для текущего таймфрейма"""
        if self.timeframe == '1m':
            # 1m данные — скользящее окно от текущей даты
            return (datetime.now(timezone.utc) - timedelta(days=DVOL_1M_WINDOW_DAYS)
                    ).replace(minute=0, second=0, microsecond=0)
        else:
            return DVOL_EARLIEST_1H[self.currency]

    def ensure_table(self):
        """Проверяет существование таблицы"""
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    );
                """, (self.table_name,))
                exists = cur.fetchone()[0]

                if not exists:
                    raise RuntimeError(
                        f"Таблица {self.table_name} не существует. "
                        f"Создайте её вручную на VPS через sudo -u postgres psql"
                    )

                logger.info(f"Таблица {self.table_name} OK")
            finally:
                cur.close()

    def get_last_timestamp(self) -> Optional[datetime]:
        """Получает последний timestamp для текущей валюты"""
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {self.table_name}
                    WHERE currency = %s
                """, (self.currency,))
                result = cur.fetchone()
                return result[0] if result and result[0] else None
            finally:
                cur.close()

    def get_record_count(self) -> int:
        """Получает количество записей для текущей валюты"""
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM {self.table_name}
                    WHERE currency = %s
                """, (self.currency,))
                return cur.fetchone()[0]
            finally:
                cur.close()

    def fetch_chunk(self, start_ms: int, end_ms: int) -> List[list]:
        """
        Загружает один чанк данных из API.
        API возвращает max 1000 записей. Для 1m чанка в 1 день (1440 записей)
        потребуется пагинация (2 запроса).

        Returns:
            Список [timestamp_ms, open, high, low, close] отсортированный по timestamp
        """
        all_records = []
        current_end = end_ms

        while True:
            params = {
                'currency': self.currency,
                'start_timestamp': start_ms,
                'end_timestamp': current_end,
                'resolution': self.resolution,
            }

            for attempt in range(RETRY_ATTEMPTS):
                try:
                    resp = self.session.get(
                        DERIBIT_API_URL,
                        params=params,
                        timeout=REQUEST_TIMEOUT
                    )
                    resp.raise_for_status()
                    result = resp.json().get('result', {})
                    data = result.get('data', [])
                    continuation = result.get('continuation')

                    all_records.extend(data)

                    if continuation is None or not data:
                        # Сортируем по timestamp (от старых к новым)
                        all_records.sort(key=lambda x: x[0])
                        return all_records

                    # Пагинация назад
                    current_end = continuation
                    break

                except requests.exceptions.RequestException as e:
                    if attempt < RETRY_ATTEMPTS - 1:
                        wait = RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"API ошибка: {e}. Повтор через {wait}с...")
                        time.sleep(wait)
                    else:
                        logger.error(f"API ошибка после {RETRY_ATTEMPTS} попыток: {e}")
                        raise

        # На случай если цикл завершился без return
        all_records.sort(key=lambda x: x[0])
        return all_records

    def save_chunk(self, records: List[list]) -> int:
        """
        Сохраняет записи в БД.
        INSERT ON CONFLICT DO UPDATE — безопасно при перезапуске.

        Args:
            records: Список [timestamp_ms, open, high, low, close]

        Returns:
            Количество сохранённых записей
        """
        if not records:
            return 0

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            try:
                query = f"""
                    INSERT INTO {self.table_name} (timestamp, currency, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, currency) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close
                """

                data = [
                    (
                        datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc),
                        self.currency,
                        r[1],   # open
                        r[2],   # high
                        r[3],   # low
                        r[4],   # close
                    )
                    for r in records
                ]

                psycopg2.extras.execute_batch(cur, query, data, page_size=1000)
                conn.commit()
                return len(data)

            except Exception as e:
                conn.rollback()
                logger.error(f"Ошибка записи в БД: {e}")
                raise
            finally:
                cur.close()

    def run(self):
        """Основной цикл загрузки"""
        global shutdown_requested

        start_time = datetime.now()
        logger.info(f"{'='*60}")
        logger.info(f"OPTIONS DVOL LOADER — {self.currency} {self.timeframe}")
        logger.info(f"{'='*60}")

        # 1. Проверяем таблицу
        self.ensure_table()

        # 2. Определяем start_date
        earliest = self._get_earliest_date()

        if self.force_reload:
            start_date = earliest
            logger.info(f"FORCE RELOAD: загрузка с {start_date}")
        else:
            last_ts = self.get_last_timestamp()
            if last_ts:
                start_date = last_ts + self.interval
                logger.info(f"Последняя запись: {last_ts}")
                logger.info(f"Продолжаем с: {start_date}")
            else:
                start_date = earliest
                logger.info(f"Таблица пуста, загрузка с: {start_date}")

        # 3. Определяем end_date
        if self.timeframe == '1m':
            end_date = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        else:
            end_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        if start_date >= end_date:
            count = self.get_record_count()
            logger.info(f"Данные актуальны ({count:,} записей)")
            self._log_finish(start_time)
            return

        # 4. Разбиваем на чанки по CHUNK_DAYS дней
        total_days = (end_date - start_date).days + 1
        chunks = []
        chunk_start = start_date

        while chunk_start < end_date:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_date)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + self.interval

        logger.info(f"Период: {start_date.strftime('%Y-%m-%d %H:%M')} — "
                     f"{end_date.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"Всего дней: {total_days}, чанков: {len(chunks)}")

        # 5. Загружаем чанки с прогресс-баром
        total_saved = 0

        with tqdm(
            total=len(chunks),
            desc=f"DVOL {self.currency} {self.timeframe}",
            unit="day",
            bar_format='{desc}: {percentage:3.0f}%|{bar:30}| '
                       '{n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}'
        ) as pbar:

            for chunk_start, chunk_end in chunks:
                if shutdown_requested:
                    logger.info(f"Остановка по запросу. Сохранено: "
                                f"{total_saved:,} записей")
                    break

                # Конвертируем в ms для API
                start_ms = int(chunk_start.timestamp() * 1000)
                end_ms = int(chunk_end.timestamp() * 1000)

                # Загружаем и сохраняем
                records = self.fetch_chunk(start_ms, end_ms)
                saved = self.save_chunk(records)
                total_saved += saved

                pbar.update(1)
                pbar.set_postfix({
                    'записей': f'{total_saved:,}',
                    'дата': chunk_start.strftime('%Y-%m-%d')
                })

                # Пауза между запросами (rate limit)
                time.sleep(0.1)

        # 6. Итог
        final_count = self.get_record_count()
        logger.info(f"Загружено новых: {total_saved:,}")
        logger.info(f"Всего в БД ({self.currency} {self.timeframe}): {final_count:,}")
        self._log_finish(start_time)

    def _log_finish(self, start_time: datetime):
        """Форматированный вывод времени выполнения"""
        duration = datetime.now() - start_time
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours > 0:
            time_str = f"{hours}ч {minutes}м {seconds}с"
        elif minutes > 0:
            time_str = f"{minutes}м {seconds}с"
        else:
            time_str = f"{seconds}с"

        logger.info(f"{'='*60}")
        logger.info(f"OPTIONS DVOL LOADER — {self.currency} {self.timeframe} "
                     f"— Завершено за {time_str}")
        logger.info(f"{'='*60}")


# ─── CLI ─────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(
        description='Options DVOL Loader — загрузка Deribit Volatility Index'
    )
    parser.add_argument(
        '--currency', type=str, default='BTC',
        help='Валюта: BTC, ETH, BTCUSDT, ETHUSDT (по умолчанию: BTC)'
    )
    parser.add_argument(
        '--timeframe', type=str, default=None,
        choices=['1m', '1h'],
        help='Таймфрейм: 1m, 1h (по умолчанию: оба)'
    )
    parser.add_argument(
        '--force-reload', action='store_true',
        help='Полная перезагрузка (1h: с 2021-03-24, 1m: за последние 180 дней)'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    timeframes = [args.timeframe] if args.timeframe else ALL_TIMEFRAMES

    logger.info(f"Валюта: {args.currency}")
    logger.info(f"Таймфреймы: {', '.join(timeframes)}")
    if args.force_reload:
        logger.info("Режим: FORCE RELOAD")

    for tf in timeframes:
        if shutdown_requested:
            logger.info("Остановка по запросу пользователя")
            break

        loader = OptionsDvolLoader(
            currency=args.currency,
            timeframe=tf,
            force_reload=args.force_reload
        )
        loader.run()


if __name__ == "__main__":
    main()
