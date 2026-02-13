#!/usr/bin/env python3
"""
Options Deribit Raw WebSocket Collector
=======================================

Daemon-скрипт для сбора raw данных по опционам с Deribit через WebSocket.
Подписывается на ticker всех активных BTC + ETH опционов,
каждые N минут записывает снапшот в таблицу options_deribit_raw.

Запуск:
    python3 options_deribit_raw_ws_collector.py
    python3 options_deribit_raw_ws_collector.py --interval 5       # каждые 5 минут
    python3 options_deribit_raw_ws_collector.py --dry-run           # без записи в БД
    python3 options_deribit_raw_ws_collector.py --single-snapshot   # один снапшот и выход

Deployment (VPS):
    systemd service или cron @reboot
    Автоматический reconnect при потере связи
    Graceful shutdown по SIGTERM/SIGINT

Автор: Trading System
Дата: 2026-02-13
"""

import asyncio
import json
import signal
import ssl
import sys
import os
import logging
import time
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

# =============================================================================
# Константы
# =============================================================================

DERIBIT_WS_URL = "wss://www.deribit.com/ws/api/v2"
DERIBIT_REST_URL = "https://www.deribit.com/api/v2"

CURRENCIES = ["BTC", "ETH"]

# Интервал записи снапшотов в БД (минуты)
DEFAULT_SNAPSHOT_INTERVAL_MINUTES = 15

# WebSocket ticker interval (100ms = самые частые обновления)
WS_TICKER_INTERVAL = "100ms"

# Reconnect
RECONNECT_DELAY_SECONDS = 5
MAX_RECONNECT_DELAY_SECONDS = 300  # 5 минут максимум

# Обновление списка инструментов (для новых контрактов / экспираций)
INSTRUMENTS_REFRESH_MINUTES = 60

# Таблица БД
TABLE_NAME = "options_deribit_raw"

# Database config
DB_CONFIG = {
    'host': '82.25.115.144',
    'port': 5432,
    'database': 'trading_data',
    'user': 'trading_admin',
    'password': 'TrAdm!n2025$Kx9Lm'
}

# =============================================================================
# Парсинг instrument_name
# =============================================================================

MONTHS = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}


def parse_instrument_name(name: str) -> dict:
    """
    Парсит instrument_name в компоненты.
    BTC-27MAR26-100000-C → {currency: BTC, expiration: 2026-03-27, strike: 100000, option_type: call}
    """
    parts = name.split('-')
    currency = parts[0]
    exp_str = parts[1]
    strike = float(parts[2])
    option_type = 'call' if parts[3] == 'C' else 'put'

    # Parse expiration: 27MAR26 → 2026-03-27
    day = int(exp_str[:-5])
    month = MONTHS[exp_str[-5:-2]]
    year = 2000 + int(exp_str[-2:])

    from datetime import date
    expiration = date(year, month, day)

    return {
        'currency': currency,
        'expiration': expiration,
        'strike': strike,
        'option_type': option_type,
    }


# =============================================================================
# Logging
# =============================================================================

def setup_logging(log_dir: Optional[Path] = None) -> logging.Logger:
    """Настройка логирования в консоль + файл"""
    logger = logging.getLogger('options_ws_collector')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File
    if log_dir is None:
        log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'ws_collector_{timestamp}.log'

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Log: {log_file}")
    return logger


# =============================================================================
# Options WebSocket Collector
# =============================================================================

class OptionsWebSocketCollector:
    """
    WebSocket collector для опционных данных Deribit.

    Подписывается на ticker всех активных BTC+ETH опционов,
    буферизирует обновления в памяти, каждые N минут пишет снапшот в БД.
    """

    def __init__(
        self,
        snapshot_interval_minutes: int = DEFAULT_SNAPSHOT_INTERVAL_MINUTES,
        dry_run: bool = False,
        single_snapshot: bool = False,
    ):
        self.snapshot_interval = snapshot_interval_minutes
        self.dry_run = dry_run
        self.single_snapshot = single_snapshot
        self.logger = logging.getLogger('options_ws_collector')

        # Shutdown
        self.shutdown_requested = False

        # WebSocket state
        self.ws = None
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        # Instrument tracking
        self.active_instruments: Set[str] = set()
        self.subscribed_instruments: Set[str] = set()
        self.last_instruments_refresh: Optional[datetime] = None

        # Ticker buffer: instrument_name → latest ticker data
        self.ticker_buffer: Dict[str, dict] = {}

        # Stats
        self.total_messages = 0
        self.total_snapshots_saved = 0
        self.ws_connect_count = 0
        self.start_time: Optional[float] = None
        self.last_status_time: float = 0
        self.messages_at_last_status: int = 0
        self.last_snapshot_time: Optional[datetime] = None
        self.next_snapshot_time: Optional[datetime] = None
        self.last_db_status: str = "—"

    # -------------------------------------------------------------------------
    # Instrument management (REST)
    # -------------------------------------------------------------------------

    def fetch_active_instruments(self) -> Set[str]:
        """Получает список всех активных опционов через REST API"""
        instruments = set()

        for currency in CURRENCIES:
            try:
                url = f"{DERIBIT_REST_URL}/public/get_book_summary_by_currency"
                resp = requests.get(url, params={
                    'currency': currency,
                    'kind': 'option'
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                for item in data.get('result', []):
                    instruments.add(item['instrument_name'])

                self.logger.info(
                    f"Инструменты {currency}: {sum(1 for i in instruments if i.startswith(currency))}"
                )
            except Exception as e:
                self.logger.error(f"Ошибка загрузки инструментов {currency}: {e}")

        self.last_instruments_refresh = datetime.now(timezone.utc)
        return instruments

    def need_instruments_refresh(self) -> bool:
        """Нужно ли обновить список инструментов?"""
        if self.last_instruments_refresh is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self.last_instruments_refresh).total_seconds()
        return elapsed > INSTRUMENTS_REFRESH_MINUTES * 60

    # -------------------------------------------------------------------------
    # WebSocket connection
    # -------------------------------------------------------------------------

    async def connect_and_subscribe(self):
        """Подключиться к WebSocket и подписаться на тикеры"""
        import websockets

        self.logger.info(f"Подключение к {DERIBIT_WS_URL}...")
        self.ws = await websockets.connect(
            DERIBIT_WS_URL,
            ssl=self.ssl_context,
            max_size=10 * 1024 * 1024,  # 10 MB
            ping_interval=30,
            ping_timeout=10,
        )
        self.ws_connect_count += 1
        self.logger.info("WebSocket подключён")

        # Подписываемся на тикеры батчами по валютам
        for currency in CURRENCIES:
            currency_instruments = [
                i for i in self.active_instruments if i.startswith(currency)
            ]
            if not currency_instruments:
                continue

            channels = [
                f"ticker.{name}.{WS_TICKER_INTERVAL}"
                for name in currency_instruments
            ]

            subscribe_msg = {
                'jsonrpc': '2.0',
                'method': 'public/subscribe',
                'id': hash(currency) % 10000,
                'params': {'channels': channels}
            }

            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.info(
                f"Подписка {currency}: {len(channels)} каналов"
            )

        self.subscribed_instruments = set(self.active_instruments)

    async def resubscribe_new_instruments(self):
        """Подписаться на новые инструменты (появившиеся после старта)"""
        new_instruments = self.active_instruments - self.subscribed_instruments
        removed_instruments = self.subscribed_instruments - self.active_instruments

        if removed_instruments:
            # Отписываемся от истёкших
            channels = [
                f"ticker.{name}.{WS_TICKER_INTERVAL}"
                for name in removed_instruments
            ]
            unsubscribe_msg = {
                'jsonrpc': '2.0',
                'method': 'public/unsubscribe',
                'id': 9999,
                'params': {'channels': channels}
            }
            try:
                await self.ws.send(json.dumps(unsubscribe_msg))
                self.logger.info(f"Отписка от {len(removed_instruments)} истёкших контрактов")
                # Удаляем из буфера
                for name in removed_instruments:
                    self.ticker_buffer.pop(name, None)
            except Exception as e:
                self.logger.warning(f"Ошибка отписки: {e}")

        if new_instruments:
            for currency in CURRENCIES:
                currency_new = [
                    i for i in new_instruments if i.startswith(currency)
                ]
                if not currency_new:
                    continue

                channels = [
                    f"ticker.{name}.{WS_TICKER_INTERVAL}"
                    for name in currency_new
                ]
                subscribe_msg = {
                    'jsonrpc': '2.0',
                    'method': 'public/subscribe',
                    'id': hash(f"new_{currency}") % 10000,
                    'params': {'channels': channels}
                }
                try:
                    await self.ws.send(json.dumps(subscribe_msg))
                    self.logger.info(
                        f"Новая подписка {currency}: {len(currency_new)} контрактов"
                    )
                except Exception as e:
                    self.logger.warning(f"Ошибка подписки на новые: {e}")

        self.subscribed_instruments = set(self.active_instruments)

    # -------------------------------------------------------------------------
    # Message processing
    # -------------------------------------------------------------------------

    def process_ticker_message(self, ticker: dict):
        """Обработка одного ticker сообщения — обновляем буфер"""
        name = ticker.get('instrument_name')
        if name:
            self.ticker_buffer[name] = ticker
            self.total_messages += 1

    # -------------------------------------------------------------------------
    # Snapshot: буфер → БД
    # -------------------------------------------------------------------------

    def _get_current_period_start(self) -> datetime:
        """Возвращает начало текущего периода (timestamp свечи)"""
        now = datetime.now(timezone.utc)
        minute = (now.minute // self.snapshot_interval) * self.snapshot_interval
        return now.replace(minute=minute, second=0, microsecond=0)

    def _get_next_snapshot_time(self) -> datetime:
        """
        Возвращает время следующего снапшота — секунда 59 последней минуты периода.

        Для interval=15: снапшот в XX:14:59, XX:29:59, XX:44:59, XX:59:59
        Для interval=1:  снапшот в XX:XX:59
        Для interval=5:  снапшот в XX:04:59, XX:09:59, XX:14:59, ...
        Для interval=60: снапшот в XX:59:59
        """
        now = datetime.now(timezone.utc)
        period_start = self._get_current_period_start()
        # Конец периода = начало + interval минут - 1 секунда
        snapshot_time = period_start + timedelta(minutes=self.snapshot_interval, seconds=-1)

        if now >= snapshot_time:
            # Уже прошли момент снапшота — следующий период
            snapshot_time += timedelta(minutes=self.snapshot_interval)

        return snapshot_time

    def take_snapshot(self, period_start_ts: Optional[datetime] = None) -> List[tuple]:
        """
        Берёт текущее состояние буфера и формирует строки для БД.
        Timestamp = начало периода (Bybit standard: timestamp = START of period).
        """
        if period_start_ts is None:
            period_start_ts = self._get_current_period_start()
        snapshot_ts = period_start_ts

        rows = []
        for name, ticker in self.ticker_buffer.items():
            try:
                parsed = parse_instrument_name(name)
                greeks = ticker.get('greeks', {})
                stats = ticker.get('stats', {})

                row = (
                    snapshot_ts,                            # timestamp
                    name,                                   # instrument_name
                    parsed['currency'],                     # currency
                    parsed['expiration'],                   # expiration
                    parsed['strike'],                       # strike
                    parsed['option_type'],                  # option_type
                    ticker.get('mark_iv'),                  # mark_iv
                    ticker.get('bid_iv'),                   # bid_iv
                    ticker.get('ask_iv'),                   # ask_iv
                    greeks.get('delta'),                    # delta
                    greeks.get('gamma'),                    # gamma
                    greeks.get('theta'),                    # theta
                    greeks.get('vega'),                     # vega
                    greeks.get('rho'),                      # rho
                    ticker.get('open_interest'),            # open_interest
                    stats.get('volume'),                    # volume_24h
                    stats.get('volume_usd'),                # volume_usd_24h
                    ticker.get('mark_price'),               # mark_price
                    ticker.get('last_price'),               # last_price
                    ticker.get('best_bid_price'),           # best_bid_price
                    ticker.get('best_ask_price'),           # best_ask_price
                    ticker.get('best_bid_amount'),          # best_bid_amount
                    ticker.get('best_ask_amount'),          # best_ask_amount
                    ticker.get('underlying_price'),         # underlying_price
                    ticker.get('index_price'),              # index_price
                    ticker.get('settlement_price'),         # settlement_price
                    stats.get('high'),                      # high_24h
                    stats.get('low'),                       # low_24h
                    ticker.get('interest_rate'),            # interest_rate
                )
                rows.append(row)

            except Exception as e:
                self.logger.warning(f"Ошибка парсинга {name}: {e}")

        return rows

    def save_snapshot_to_db(self, rows: List[tuple]):
        """Записывает снапшот в БД"""
        if not rows:
            return

        if self.dry_run:
            self.total_snapshots_saved += 1
            return

        upsert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                timestamp, instrument_name, currency, expiration, strike, option_type,
                mark_iv, bid_iv, ask_iv,
                delta, gamma, theta, vega, rho,
                open_interest, volume_24h, volume_usd_24h,
                mark_price, last_price, best_bid_price, best_ask_price,
                best_bid_amount, best_ask_amount,
                underlying_price, index_price, settlement_price,
                high_24h, low_24h, interest_rate
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (timestamp, instrument_name) DO UPDATE SET
                mark_iv = EXCLUDED.mark_iv,
                bid_iv = EXCLUDED.bid_iv,
                ask_iv = EXCLUDED.ask_iv,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                theta = EXCLUDED.theta,
                vega = EXCLUDED.vega,
                rho = EXCLUDED.rho,
                open_interest = EXCLUDED.open_interest,
                volume_24h = EXCLUDED.volume_24h,
                volume_usd_24h = EXCLUDED.volume_usd_24h,
                mark_price = EXCLUDED.mark_price,
                last_price = EXCLUDED.last_price,
                best_bid_price = EXCLUDED.best_bid_price,
                best_ask_price = EXCLUDED.best_ask_price,
                best_bid_amount = EXCLUDED.best_bid_amount,
                best_ask_amount = EXCLUDED.best_ask_amount,
                underlying_price = EXCLUDED.underlying_price,
                index_price = EXCLUDED.index_price,
                settlement_price = EXCLUDED.settlement_price,
                high_24h = EXCLUDED.high_24h,
                low_24h = EXCLUDED.low_24h,
                interest_rate = EXCLUDED.interest_rate
        """

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
                conn.commit()
                self.total_snapshots_saved += 1
                self.last_db_status = f"OK ({len(rows)} rows)"
            except Exception as e:
                conn.rollback()
                self.last_db_status = f"ERROR: {e}"
                self.logger.error(f"Ошибка записи в БД: {e}")
                raise
            finally:
                conn.close()
        except psycopg2.OperationalError as e:
            self.last_db_status = f"CONN ERROR: {e}"
            self.logger.error(f"Ошибка подключения к БД: {e}")

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    async def run(self):
        """Основной цикл: подключение → сбор → снапшоты → reconnect"""
        self.logger.info("=" * 70)
        self.logger.info("OPTIONS DERIBIT RAW WS COLLECTOR")
        self.logger.info("=" * 70)
        self.logger.info(f"Интервал снапшотов: {self.snapshot_interval} мин")
        self.logger.info(f"Валюты: {', '.join(CURRENCIES)}")
        self.logger.info(f"Dry run: {self.dry_run}")
        self.logger.info(f"Single snapshot: {self.single_snapshot}")
        self.logger.info("")

        reconnect_delay = RECONNECT_DELAY_SECONDS
        self.start_time = time.time()

        while not self.shutdown_requested:
            try:
                # 1. Загружаем список инструментов
                self.active_instruments = self.fetch_active_instruments()
                total = len(self.active_instruments)
                if total == 0:
                    self.logger.error("Нет активных инструментов!")
                    await asyncio.sleep(60)
                    continue

                self.logger.info(f"Всего активных инструментов: {total}")

                # 2. Подключаемся к WebSocket
                await self.connect_and_subscribe()
                reconnect_delay = RECONNECT_DELAY_SECONDS  # Reset on success

                # 3. Ждём первый полный снапшот
                self.logger.info("Ожидание данных...")
                await self._collect_initial_snapshot()

                # 4. Если single_snapshot — записываем и выходим
                if self.single_snapshot:
                    rows = self.take_snapshot()
                    self._log_snapshot_stats(rows)
                    self.save_snapshot_to_db(rows)
                    return

                # 5. Основной цикл: слушаем WS + пишем снапшоты по расписанию
                await self._main_loop()

            except asyncio.CancelledError:
                self.logger.info("Получен сигнал отмены")
                break

            except Exception as e:
                if self.shutdown_requested:
                    break
                rc = self.ws_connect_count - 1 if self.ws_connect_count > 0 else 0
                now_utc = datetime.now(timezone.utc)
                print(
                    f"!!! RECONNECT {now_utc.strftime('%H:%M:%S %Y-%m-%d')} | "
                    f"Error: {e} | "
                    f"Retry in {reconnect_delay}s | rc:{rc + 1}"
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * 2,
                    MAX_RECONNECT_DELAY_SECONDS
                )

            finally:
                if self.ws:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass
                    self.ws = None

        rc = self.ws_connect_count - 1 if self.ws_connect_count > 0 else 0
        print(
            f"\n--- SHUTDOWN | msgs:{self.total_messages:,} | "
            f"saved:{self.total_snapshots_saved} | rc:{rc} ---"
        )

    async def _collect_initial_snapshot(self):
        """Собираем первый полный снапшот (все инструменты хотя бы по 1 разу)"""
        target = len(self.active_instruments)
        start = time.time()

        while len(self.ticker_buffer) < target:
            if self.shutdown_requested:
                return
            if time.time() - start > 60:
                self.logger.warning(
                    f"Таймаут первого снапшота: "
                    f"{len(self.ticker_buffer)}/{target}"
                )
                break

            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=5)
                data = json.loads(msg)

                if 'params' in data and 'data' in data['params']:
                    self.process_ticker_message(data['params']['data'])

            except asyncio.TimeoutError:
                continue

        elapsed = time.time() - start
        self.logger.info(
            f"Первый снапшот: {len(self.ticker_buffer)}/{target} "
            f"инструментов за {elapsed:.1f}s"
        )

    async def _main_loop(self):
        """
        Основной цикл: слушаем WS, пишем снапшоты привязанные к таймфреймам свечей.

        Снапшот берётся на 59-й секунде последней минуты периода:
          15m: XX:14:59, XX:29:59, XX:44:59, XX:59:59
          1m:  XX:XX:59
          5m:  XX:04:59, XX:09:59, ...
        Timestamp в БД = начало периода (Bybit standard).
        """
        self.last_status_time = time.time()
        self.messages_at_last_status = self.total_messages
        last_status_print = time.time()

        next_snapshot = self._get_next_snapshot_time()
        # period_start для этого снапшота
        period_start = next_snapshot - timedelta(minutes=self.snapshot_interval) + timedelta(seconds=1)

        self.logger.info(
            f"Следующий снапшот: {next_snapshot.strftime('%H:%M:%S')} UTC "
            f"(период {period_start.strftime('%H:%M')})"
        )

        # Для отображения countdown в статусе
        self.next_snapshot_time = next_snapshot

        # Первый статус сразу после сбора начальных данных
        self._print_status()

        while not self.shutdown_requested:
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=1)
                data = json.loads(msg)

                if 'params' in data and 'data' in data['params']:
                    self.process_ticker_message(data['params']['data'])

            except asyncio.TimeoutError:
                pass

            now_utc = datetime.now(timezone.utc)
            now_mono = time.time()

            # Статус в консоль каждые 60 секунд
            if now_mono - last_status_print >= 60:
                self._print_status()
                last_status_print = now_mono

            # Обновление списка инструментов
            if self.need_instruments_refresh():
                self.active_instruments = self.fetch_active_instruments()
                await self.resubscribe_new_instruments()

            # Время снапшота? (привязка к wall clock)
            if now_utc >= next_snapshot:
                rows = self.take_snapshot(period_start)
                self._log_snapshot_stats(rows)
                self.save_snapshot_to_db(rows)
                self.last_snapshot_time = now_utc

                # Вычисляем следующий снапшот
                next_snapshot = self._get_next_snapshot_time()
                period_start = next_snapshot - timedelta(minutes=self.snapshot_interval) + timedelta(seconds=1)
                self.next_snapshot_time = next_snapshot

                self.logger.info(
                    f"Следующий снапшот: {next_snapshot.strftime('%H:%M:%S')} UTC "
                    f"(период {period_start.strftime('%H:%M')})"
                )

    def _get_currency_summary(self, currency: str) -> dict:
        """Собирает сводку по валюте из буфера"""
        contracts = {
            k: v for k, v in self.ticker_buffer.items()
            if k.startswith(currency)
        }
        if not contracts:
            return {}

        # Index price (одинаковый для всех контрактов валюты)
        index_price = None
        for t in contracts.values():
            ip = t.get('index_price')
            if ip and ip > 0:
                index_price = ip
                break

        # ATM IV: ближайшая экспирация + страйк ближайший к index_price
        atm_iv = None
        if index_price:
            now = datetime.now(timezone.utc)
            best_contract = None
            best_score = float('inf')

            for name, t in contracts.items():
                parsed = parse_instrument_name(name)
                # Только call для ATM IV
                if parsed['option_type'] != 'call':
                    continue
                days_to_exp = (parsed['expiration'] - now.date()).days
                if days_to_exp < 1:
                    continue
                strike_dist = abs(parsed['strike'] - index_price) / index_price
                # score: предпочитаем ближайшую экспирацию + ближайший страйк
                score = days_to_exp * 0.01 + strike_dist
                if score < best_score:
                    best_score = score
                    best_contract = t
                    atm_iv = t.get('mark_iv')

        # Total OI (в USD)
        total_oi_usd = 0
        for t in contracts.values():
            oi = t.get('open_interest', 0) or 0
            up = t.get('underlying_price', 0) or 0
            total_oi_usd += oi * up

        return {
            'index_price': index_price,
            'atm_iv': atm_iv,
            'contracts': len(contracts),
            'total_oi_usd': total_oi_usd,
        }

    def _format_usd(self, value: float) -> str:
        """Форматирует USD: 12400000000 → 12.4B, 350000000 → 350.0M"""
        if value >= 1e9:
            return f"{value / 1e9:.1f}B"
        elif value >= 1e6:
            return f"{value / 1e6:.1f}M"
        elif value >= 1e3:
            return f"{value / 1e3:.0f}K"
        return f"{value:.0f}"

    def _format_uptime(self, seconds: float) -> str:
        """Форматирует uptime: 8145 → 2h 15m"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        if hours > 0:
            return f"{hours}h {minutes:02d}m"
        return f"{minutes}m"

    def _get_nearest_expiration_str(self, now_utc: datetime) -> Optional[str]:
        """Возвращает строку с countdown до ближайшей экспирации"""
        if not self.ticker_buffer:
            return None

        # Собираем экспирации и считаем контракты
        expirations: Dict[str, int] = {}  # date_str → count
        for name in self.ticker_buffer:
            try:
                parsed = parse_instrument_name(name)
                exp_date = parsed['expiration']
                exp_key = exp_date.isoformat()
                expirations[exp_key] = expirations.get(exp_key, 0) + 1
            except Exception:
                continue

        if not expirations:
            return None

        # Ближайшая экспирация (08:00 UTC)
        nearest_date = min(expirations.keys())
        nearest_count = expirations[nearest_date]

        from datetime import date as date_type
        exp_date = date_type.fromisoformat(nearest_date)
        exp_dt = datetime(exp_date.year, exp_date.month, exp_date.day, 8, 0, 0, tzinfo=timezone.utc)

        remaining = (exp_dt - now_utc).total_seconds()
        if remaining < 0:
            return None

        hours = int(remaining // 3600)
        minutes = int((remaining % 3600) // 60)

        if hours >= 24:
            days = hours // 24
            hours = hours % 24
            return f"Exp:{days}d{hours:02d}h({nearest_count}ct)"
        else:
            return f"Exp:{hours}h{minutes:02d}m({nearest_count}ct)"

    def _print_status(self):
        """Выводит однострочный статус в консоль"""
        now = time.time()
        now_utc = datetime.now(timezone.utc)

        # Messages per second
        elapsed = now - self.last_status_time if self.last_status_time else 1
        msg_delta = self.total_messages - self.messages_at_last_status
        msg_per_sec = msg_delta / elapsed if elapsed > 0 else 0
        self.messages_at_last_status = self.total_messages
        self.last_status_time = now

        # Uptime
        uptime = now - self.start_time if self.start_time else 0

        # Next snapshot countdown
        next_snap = getattr(self, 'next_snapshot_time', None)
        if next_snap:
            next_in = (next_snap - now_utc).total_seconds()
            if next_in < 0:
                next_in = 0
            snap_str = f"{int(next_in // 60)}m{int(next_in % 60):02d}s"
        else:
            snap_str = "—"

        # BTC/ETH summaries
        parts = [
            f"{now_utc.strftime('%H:%M:%S %Y-%m-%d')}",
        ]

        for currency in CURRENCIES:
            summary = self._get_currency_summary(currency)
            if not summary:
                parts.append(f"{currency} —")
                continue
            price = summary.get('index_price')
            iv = summary.get('atm_iv')
            price_str = f"${price:,.0f}" if price else "—"
            iv_str = f"IV:{iv:.1f}%" if iv else "IV:—"
            cnt = summary.get('contracts', 0)
            parts.append(f"{currency} {price_str} {iv_str} Contr:{cnt}")

        # Nearest expiration countdown
        exp_str = self._get_nearest_expiration_str(now_utc)
        if exp_str:
            parts.append(exp_str)

        rc = self.ws_connect_count - 1 if self.ws_connect_count > 0 else 0
        parts.append(f"{msg_per_sec:,.0f}/s")
        parts.append(f"▸snap:{snap_str}")
        parts.append(f"Up:{self._format_uptime(uptime)}")
        parts.append(f"saved:{self.total_snapshots_saved}")
        parts.append(f"rc:{rc}")

        print(" | ".join(parts))

    def _log_snapshot_stats(self, rows: List[tuple]):
        """Выводит однострочный лог снапшота в том же формате что и статус"""
        if not rows:
            self.logger.warning("Пустой снапшот!")
            return

        btc_count = sum(1 for r in rows if r[2] == 'BTC')
        eth_count = sum(1 for r in rows if r[2] == 'ETH')
        ts = rows[0][0]  # period start (timestamp в БД)
        now_utc = datetime.now(timezone.utc)
        db_mode = "[DRY RUN] " if self.dry_run else ""

        print(
            f">>> {db_mode}{now_utc.strftime('%H:%M:%S')} SNAPSHOT period:{ts.strftime('%H:%M %Y-%m-%d')} | "
            f"{len(rows)} rows (BTC:{btc_count} ETH:{eth_count}) | "
            f"saved:{self.total_snapshots_saved + 1}"
        )

    # -------------------------------------------------------------------------
    # Shutdown
    # -------------------------------------------------------------------------

    def request_shutdown(self):
        """Запрос на остановку"""
        if self.shutdown_requested:
            self.logger.warning("Повторный сигнал — принудительный выход")
            sys.exit(1)
        self.shutdown_requested = True
        self.logger.info("Запрошена остановка, завершаем после текущей операции...")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Options Deribit Raw WebSocket Collector'
    )
    parser.add_argument(
        '--interval', type=int,
        default=DEFAULT_SNAPSHOT_INTERVAL_MINUTES,
        help=f'Интервал снапшотов в минутах (default: {DEFAULT_SNAPSHOT_INTERVAL_MINUTES})'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Не записывать в БД (только логирование)'
    )
    parser.add_argument(
        '--single-snapshot', action='store_true',
        help='Сделать один снапшот и выйти'
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging()

    collector = OptionsWebSocketCollector(
        snapshot_interval_minutes=args.interval,
        dry_run=args.dry_run,
        single_snapshot=args.single_snapshot,
    )

    # Graceful shutdown
    def signal_handler(sig, frame):
        collector.request_shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")


if __name__ == '__main__':
    main()
