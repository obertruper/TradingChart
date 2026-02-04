#!/usr/bin/env python3
"""
Orderbook Loader
================
Загрузка и агрегация данных orderbook из исторических архивов Bybit.

Источник: https://quote-saver.bycsi.com/orderbook/linear/{SYMBOL}/
Формат: ZIP архивы с JSON Lines (snapshot + delta сообщения)

Алгоритм:
1. Определяем последнюю запись в БД (MAX timestamp)
2. Скачиваем ZIP в RAM (BytesIO), без записи на диск
3. Восстанавливаем стакан из snapshot + delta
4. Агрегируем ~600 сообщений/мин → 1 строка БД (58 колонок)
5. Записываем 1440 строк/день, COMMIT после каждого дня

Данные: 2023-01-18 — текущая дата (ob500 → ob200 с 2025-08-21)
Таблица: orderbook_bybit_futures_1m (56 числовых + 2 JSONB колонок)

Usage:
    python3 orderbook_loader.py                        # Продолжить загрузку
    python3 orderbook_loader.py --symbol BTCUSDT       # Конкретный символ
    python3 orderbook_loader.py --force-reload          # Перезагрузить всё с 2023-01-18
"""

import sys
import os
import io
import json
import logging
import argparse
import warnings
import signal
import zipfile
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import List, Dict, Optional, Tuple
import time

import requests
import psycopg2
import psycopg2.extras
from tqdm import tqdm

# Подавляем предупреждения
warnings.filterwarnings('ignore')

# Глобальный флаг для graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Обработчик сигнала прерывания (Ctrl+C)"""
    global shutdown_requested
    if shutdown_requested:
        print("\n⚠️  Принудительное завершение...")
        sys.exit(1)
    shutdown_requested = True
    print("\n⚠️  Получен сигнал прерывания. Завершаем после текущего дня...")
    print("   (Нажмите Ctrl+C ещё раз для принудительного выхода)")


signal.signal(signal.SIGINT, signal_handler)

# Добавляем путь к корню проекта
sys.path.insert(0, str(Path(__file__).parent.parent))

from indicators.database import DatabaseConnection

# Настройка логирования
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# Константы
# ═══════════════════════════════════════════════════════════════════

BASE_URL = "https://quote-saver.bycsi.com/orderbook/linear"
EARLIEST_DATE = datetime(2023, 1, 18, tzinfo=timezone.utc)
TABLE_NAME = "orderbook_bybit_futures_1m"
DEFAULT_SYMBOL = "BTCUSDT"

# HTTP
DOWNLOAD_TIMEOUT = 300  # 5 минут на скачивание файла
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # секунд между попытками

# Top-N уровней для JSONB
TOP_LEVELS = 50

# Slippage amounts в USD
SLIPPAGE_AMOUNTS = [10_000, 50_000, 100_000]

# ═══════════════════════════════════════════════════════════════════
# SQL
# ═══════════════════════════════════════════════════════════════════

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    -- Первичный ключ
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    -- PRICE (6 колонок)
    best_bid DECIMAL(20,8),
    best_ask DECIMAL(20,8),
    mid_price DECIMAL(20,8),
    microprice DECIMAL(20,8),
    vwap_bid DECIMAL(20,8),
    vwap_ask DECIMAL(20,8),

    -- SPREAD (6 колонок)
    spread DECIMAL(20,8),
    spread_pct DECIMAL(10,6),
    spread_min DECIMAL(20,8),
    spread_max DECIMAL(20,8),
    spread_avg DECIMAL(20,8),
    spread_std DECIMAL(20,8),

    -- BID VOLUME (6 колонок)
    bid_vol_01pct DECIMAL(20,8),
    bid_vol_05pct DECIMAL(20,8),
    bid_vol_10pct DECIMAL(20,8),
    bid_volume DECIMAL(20,8),
    bid_vol_avg DECIMAL(20,8),
    bid_vol_std DECIMAL(20,8),

    -- ASK VOLUME (6 колонок)
    ask_vol_01pct DECIMAL(20,8),
    ask_vol_05pct DECIMAL(20,8),
    ask_vol_10pct DECIMAL(20,8),
    ask_volume DECIMAL(20,8),
    ask_vol_avg DECIMAL(20,8),
    ask_vol_std DECIMAL(20,8),

    -- IMBALANCE (7 колонок)
    imbalance DECIMAL(10,6),
    imbalance_01pct DECIMAL(10,6),
    imbalance_min DECIMAL(10,6),
    imbalance_max DECIMAL(10,6),
    imbalance_avg DECIMAL(10,6),
    imbalance_std DECIMAL(10,6),
    imbalance_range DECIMAL(10,6),

    -- PRESSURE (2 колонки)
    buy_pressure DECIMAL(10,6),
    depth_ratio DECIMAL(10,6),

    -- WALLS (6 колонок)
    bid_wall_price DECIMAL(20,8),
    bid_wall_volume DECIMAL(20,8),
    bid_wall_distance_pct DECIMAL(10,6),
    ask_wall_price DECIMAL(20,8),
    ask_wall_volume DECIMAL(20,8),
    ask_wall_distance_pct DECIMAL(10,6),

    -- SLIPPAGE (6 колонок)
    slippage_buy_10k DECIMAL(20,8),
    slippage_buy_50k DECIMAL(20,8),
    slippage_buy_100k DECIMAL(20,8),
    slippage_sell_10k DECIMAL(20,8),
    slippage_sell_50k DECIMAL(20,8),
    slippage_sell_100k DECIMAL(20,8),

    -- LIQUIDITY (3 колонки)
    liquidity_score DECIMAL(20,8),
    bid_concentration DECIMAL(10,6),
    ask_concentration DECIMAL(10,6),

    -- VOLATILITY (3 колонки)
    mid_price_range DECIMAL(20,8),
    mid_price_std DECIMAL(20,8),
    price_momentum DECIMAL(10,6),

    -- ACTIVITY (5 колонок)
    ob_bid_levels INT,
    ob_ask_levels INT,
    ob_update_count INT,
    best_bid_changes INT,
    best_ask_changes INT,

    -- DEPTH LEVELS (2 JSONB колонки)
    bid_levels JSONB,
    ask_levels JSONB,

    PRIMARY KEY (timestamp, symbol)
);
"""

CREATE_INDEXES_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_ob_symbol_ts ON {TABLE_NAME} (symbol, timestamp);",
    f"CREATE INDEX IF NOT EXISTS idx_ob_timestamp ON {TABLE_NAME} (timestamp);",
]

# Все колонки в порядке для INSERT
ALL_COLUMNS = [
    'timestamp', 'symbol',
    # PRICE
    'best_bid', 'best_ask', 'mid_price', 'microprice', 'vwap_bid', 'vwap_ask',
    # SPREAD
    'spread', 'spread_pct', 'spread_min', 'spread_max', 'spread_avg', 'spread_std',
    # BID VOLUME
    'bid_vol_01pct', 'bid_vol_05pct', 'bid_vol_10pct', 'bid_volume',
    'bid_vol_avg', 'bid_vol_std',
    # ASK VOLUME
    'ask_vol_01pct', 'ask_vol_05pct', 'ask_vol_10pct', 'ask_volume',
    'ask_vol_avg', 'ask_vol_std',
    # IMBALANCE
    'imbalance', 'imbalance_01pct', 'imbalance_min', 'imbalance_max',
    'imbalance_avg', 'imbalance_std', 'imbalance_range',
    # PRESSURE
    'buy_pressure', 'depth_ratio',
    # WALLS
    'bid_wall_price', 'bid_wall_volume', 'bid_wall_distance_pct',
    'ask_wall_price', 'ask_wall_volume', 'ask_wall_distance_pct',
    # SLIPPAGE
    'slippage_buy_10k', 'slippage_buy_50k', 'slippage_buy_100k',
    'slippage_sell_10k', 'slippage_sell_50k', 'slippage_sell_100k',
    # LIQUIDITY
    'liquidity_score', 'bid_concentration', 'ask_concentration',
    # VOLATILITY
    'mid_price_range', 'mid_price_std', 'price_momentum',
    # ACTIVITY
    'ob_bid_levels', 'ob_ask_levels', 'ob_update_count',
    'best_bid_changes', 'best_ask_changes',
    # DEPTH LEVELS
    'bid_levels', 'ask_levels',
]

# Колонки для UPDATE (все кроме timestamp и symbol)
UPDATE_COLUMNS = [c for c in ALL_COLUMNS if c not in ('timestamp', 'symbol')]


def _build_upsert_sql() -> str:
    """Формирует SQL для INSERT...ON CONFLICT DO UPDATE"""
    cols = ', '.join(ALL_COLUMNS)
    placeholders = ', '.join(['%s'] * len(ALL_COLUMNS))
    updates = ', '.join([f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS])
    return f"""
        INSERT INTO {TABLE_NAME} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (timestamp, symbol) DO UPDATE SET {updates}
    """


UPSERT_SQL = _build_upsert_sql()


# ═══════════════════════════════════════════════════════════════════
# Вычисление метрик
# ═══════════════════════════════════════════════════════════════════

def calc_volume_in_range(levels: dict, mid: float, pct: float) -> float:
    """Объём в пределах pct% от mid"""
    threshold = mid * pct / 100.0
    lower = mid - threshold
    upper = mid + threshold
    return sum(v for p, v in levels.items() if lower <= p <= upper)


def calc_vwap(levels: dict) -> float:
    """VWAP = sum(price * volume) / sum(volume)"""
    total_vol = sum(levels.values())
    if total_vol == 0:
        return 0.0
    return sum(p * v for p, v in levels.items()) / total_vol


def calc_slippage(levels: dict, amount_usd: float, side: str) -> float:
    """
    Slippage при исполнении маркет-ордера на amount_usd.
    side='buy' → идём по ask (от лучшего вверх)
    side='sell' → идём по bid (от лучшего вниз)
    Возвращает: среднюю цену исполнения - лучшую цену (в $).
    """
    if not levels:
        return 0.0

    if side == 'buy':
        sorted_levels = sorted(levels.items(), key=lambda x: x[0])  # ASC для ask
    else:
        sorted_levels = sorted(levels.items(), key=lambda x: -x[0])  # DESC для bid

    best_price = sorted_levels[0][0]
    remaining_usd = amount_usd
    filled_value = 0.0
    filled_qty = 0.0

    for price, vol in sorted_levels:
        level_value = price * vol
        if level_value >= remaining_usd:
            qty = remaining_usd / price
            filled_qty += qty
            filled_value += qty * price
            remaining_usd = 0
            break
        else:
            filled_qty += vol
            filled_value += level_value
            remaining_usd -= level_value

    if filled_qty == 0:
        return 0.0

    avg_price = filled_value / filled_qty
    return abs(avg_price - best_price)


def calc_concentration(levels: dict, top_n: int = 3) -> float:
    """Доля объёма в top_n крупнейших уровнях от общего объёма"""
    if not levels:
        return 0.0
    total = sum(levels.values())
    if total == 0:
        return 0.0
    top_volumes = sorted(levels.values(), reverse=True)[:top_n]
    return sum(top_volumes) / total


def finalize_minute(
    bids: dict, asks: dict, symbol: str, minute: datetime,
    spreads: list, mid_prices: list, imbalances: list,
    bid_volumes: list, ask_volumes: list,
    bid_change_count: int, ask_change_count: int, msg_count: int,
) -> tuple:
    """
    Формирует одну строку для БД из состояния стакана + аккумуляторов.
    Возвращает tuple в порядке ALL_COLUMNS.
    """
    best_bid = max(bids.keys())
    best_ask = min(asks.keys())
    mid = (best_bid + best_ask) / 2.0
    spread_val = best_ask - best_bid

    # ── PRICE ──
    # Microprice
    bb_vol = bids.get(best_bid, 0)
    ba_vol = asks.get(best_ask, 0)
    if bb_vol + ba_vol > 0:
        microprice = (best_bid * ba_vol + best_ask * bb_vol) / (bb_vol + ba_vol)
    else:
        microprice = mid

    vwap_bid_val = calc_vwap(bids)
    vwap_ask_val = calc_vwap(asks)

    # ── SPREAD ──
    spread_pct_val = (spread_val / mid * 100.0) if mid > 0 else 0.0
    spread_min_val = min(spreads)
    spread_max_val = max(spreads)
    spread_avg_val = mean(spreads)
    spread_std_val = stdev(spreads) if len(spreads) > 1 else 0.0

    # ── VOLUMES ──
    bid_vol_01 = calc_volume_in_range(bids, mid, 0.1)
    bid_vol_05 = calc_volume_in_range(bids, mid, 0.5)
    bid_vol_10 = calc_volume_in_range(bids, mid, 1.0)
    bid_vol_total = sum(bids.values())

    ask_vol_01 = calc_volume_in_range(asks, mid, 0.1)
    ask_vol_05 = calc_volume_in_range(asks, mid, 0.5)
    ask_vol_10 = calc_volume_in_range(asks, mid, 1.0)
    ask_vol_total = sum(asks.values())

    bid_vol_avg_val = mean(bid_volumes)
    bid_vol_std_val = stdev(bid_volumes) if len(bid_volumes) > 1 else 0.0
    ask_vol_avg_val = mean(ask_volumes)
    ask_vol_std_val = stdev(ask_volumes) if len(ask_volumes) > 1 else 0.0

    # ── IMBALANCE ──
    total_vol = bid_vol_total + ask_vol_total
    imb_val = (bid_vol_total - ask_vol_total) / total_vol if total_vol > 0 else 0.0

    total_01 = bid_vol_01 + ask_vol_01
    imb_01_val = (bid_vol_01 - ask_vol_01) / total_01 if total_01 > 0 else 0.0

    imb_min_val = min(imbalances)
    imb_max_val = max(imbalances)
    imb_avg_val = mean(imbalances)
    imb_std_val = stdev(imbalances) if len(imbalances) > 1 else 0.0
    imb_range_val = imb_max_val - imb_min_val

    # ── PRESSURE ──
    buy_pressure_val = (bid_vol_01 / ask_vol_01) if ask_vol_01 > 0 else 0.0
    depth_ratio_val = (bid_vol_10 / ask_vol_10) if ask_vol_10 > 0 else 0.0

    # ── WALLS ──
    if bids:
        bid_wall_price, bid_wall_vol = max(bids.items(), key=lambda x: x[1])
        bid_wall_dist = abs(mid - bid_wall_price) / mid * 100.0 if mid > 0 else 0.0
    else:
        bid_wall_price = bid_wall_vol = bid_wall_dist = 0.0

    if asks:
        ask_wall_price, ask_wall_vol = max(asks.items(), key=lambda x: x[1])
        ask_wall_dist = abs(ask_wall_price - mid) / mid * 100.0 if mid > 0 else 0.0
    else:
        ask_wall_price = ask_wall_vol = ask_wall_dist = 0.0

    # ── SLIPPAGE ──
    slip_buy_10k = calc_slippage(asks, 10_000, 'buy')
    slip_buy_50k = calc_slippage(asks, 50_000, 'buy')
    slip_buy_100k = calc_slippage(asks, 100_000, 'buy')
    slip_sell_10k = calc_slippage(bids, 10_000, 'sell')
    slip_sell_50k = calc_slippage(bids, 50_000, 'sell')
    slip_sell_100k = calc_slippage(bids, 100_000, 'sell')

    # ── LIQUIDITY ──
    liquidity = (bid_vol_total + ask_vol_total) / 2.0
    bid_conc = calc_concentration(bids, 3)
    ask_conc = calc_concentration(asks, 3)

    # ── VOLATILITY ──
    mid_range_val = max(mid_prices) - min(mid_prices)
    mid_std_val = stdev(mid_prices) if len(mid_prices) > 1 else 0.0
    first_mid = mid_prices[0]
    last_mid = mid_prices[-1]
    momentum_val = (last_mid - first_mid) / first_mid if first_mid > 0 else 0.0

    # ── ACTIVITY ──
    ob_bid_levels_count = len(bids)
    ob_ask_levels_count = len(asks)

    # ── DEPTH LEVELS (JSONB) ──
    sorted_bids = sorted(bids.items(), key=lambda x: -x[0])[:TOP_LEVELS]
    sorted_asks = sorted(asks.items(), key=lambda x: x[0])[:TOP_LEVELS]

    bid_levels_json = json.dumps([
        {"p": round(p, 2), "v": round(v, 8)} for p, v in sorted_bids
    ])
    ask_levels_json = json.dumps([
        {"p": round(p, 2), "v": round(v, 8)} for p, v in sorted_asks
    ])

    # Возвращаем tuple в порядке ALL_COLUMNS
    return (
        minute, symbol,
        # PRICE
        best_bid, best_ask, mid, microprice, vwap_bid_val, vwap_ask_val,
        # SPREAD
        spread_val, spread_pct_val, spread_min_val, spread_max_val,
        spread_avg_val, spread_std_val,
        # BID VOLUME
        bid_vol_01, bid_vol_05, bid_vol_10, bid_vol_total,
        bid_vol_avg_val, bid_vol_std_val,
        # ASK VOLUME
        ask_vol_01, ask_vol_05, ask_vol_10, ask_vol_total,
        ask_vol_avg_val, ask_vol_std_val,
        # IMBALANCE
        imb_val, imb_01_val, imb_min_val, imb_max_val,
        imb_avg_val, imb_std_val, imb_range_val,
        # PRESSURE
        buy_pressure_val, depth_ratio_val,
        # WALLS
        bid_wall_price, bid_wall_vol, bid_wall_dist,
        ask_wall_price, ask_wall_vol, ask_wall_dist,
        # SLIPPAGE
        slip_buy_10k, slip_buy_50k, slip_buy_100k,
        slip_sell_10k, slip_sell_50k, slip_sell_100k,
        # LIQUIDITY
        liquidity, bid_conc, ask_conc,
        # VOLATILITY
        mid_range_val, mid_std_val, momentum_val,
        # ACTIVITY
        ob_bid_levels_count, ob_ask_levels_count, msg_count,
        bid_change_count, ask_change_count,
        # DEPTH LEVELS
        bid_levels_json, ask_levels_json,
    )


# ═══════════════════════════════════════════════════════════════════
# Обработка данных
# ═══════════════════════════════════════════════════════════════════

def process_day_from_zip(zip_bytes: io.BytesIO, symbol: str) -> List[tuple]:
    """
    Обрабатывает ZIP из RAM и возвращает список 1-минутных строк для БД.
    Каждая строка — tuple в порядке ALL_COLUMNS.
    """
    orderbook_bids = {}
    orderbook_asks = {}

    # Аккумуляторы для текущей минуты
    current_minute = None
    spreads = []
    mid_prices = []
    imbalances = []
    bid_volumes = []
    ask_volumes = []
    best_bid_prev = None
    best_ask_prev = None
    bid_change_count = 0
    ask_change_count = 0
    msg_count = 0

    results = []

    with zipfile.ZipFile(zip_bytes) as zf:
        inner_name = zf.namelist()[0]
        with zf.open(inner_name) as f:
            for raw_line in f:
                line = raw_line.decode('utf-8').strip()
                if not line:
                    continue

                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue

                ts_ms = msg.get('ts')
                if ts_ms is None:
                    continue

                ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                minute = ts.replace(second=0, microsecond=0)

                # Новая минута — сохраняем предыдущую
                if current_minute is not None and minute != current_minute:
                    if msg_count > 0 and orderbook_bids and orderbook_asks:
                        row = finalize_minute(
                            orderbook_bids, orderbook_asks, symbol, current_minute,
                            spreads, mid_prices, imbalances,
                            bid_volumes, ask_volumes,
                            bid_change_count, ask_change_count, msg_count,
                        )
                        results.append(row)

                    # Сброс аккумуляторов
                    spreads = []
                    mid_prices = []
                    imbalances = []
                    bid_volumes = []
                    ask_volumes = []
                    bid_change_count = 0
                    ask_change_count = 0
                    msg_count = 0

                current_minute = minute

                # Применяем snapshot или delta
                data = msg.get('data', {})
                msg_type = msg.get('type', '')

                if msg_type == 'snapshot':
                    orderbook_bids = {
                        float(b[0]): float(b[1]) for b in data.get('b', [])
                    }
                    orderbook_asks = {
                        float(a[0]): float(a[1]) for a in data.get('a', [])
                    }
                elif msg_type == 'delta':
                    for b in data.get('b', []):
                        p, s = float(b[0]), float(b[1])
                        if s == 0:
                            orderbook_bids.pop(p, None)
                        else:
                            orderbook_bids[p] = s
                    for a in data.get('a', []):
                        p, s = float(a[0]), float(a[1])
                        if s == 0:
                            orderbook_asks.pop(p, None)
                        else:
                            orderbook_asks[p] = s
                else:
                    continue

                # Вычисляем метрики текущего состояния
                if orderbook_bids and orderbook_asks:
                    best_bid = max(orderbook_bids.keys())
                    best_ask = min(orderbook_asks.keys())

                    if best_bid >= best_ask:
                        # Crossed book — пропускаем
                        continue

                    mid = (best_bid + best_ask) / 2.0
                    spread = best_ask - best_bid
                    bid_vol = sum(orderbook_bids.values())
                    ask_vol = sum(orderbook_asks.values())
                    total = bid_vol + ask_vol
                    imb = (bid_vol - ask_vol) / total if total > 0 else 0.0

                    spreads.append(spread)
                    mid_prices.append(mid)
                    imbalances.append(imb)
                    bid_volumes.append(bid_vol)
                    ask_volumes.append(ask_vol)

                    if best_bid_prev is not None and best_bid != best_bid_prev:
                        bid_change_count += 1
                    if best_ask_prev is not None and best_ask != best_ask_prev:
                        ask_change_count += 1
                    best_bid_prev = best_bid
                    best_ask_prev = best_ask
                    msg_count += 1

    # Последняя минута
    if current_minute is not None and msg_count > 0 and orderbook_bids and orderbook_asks:
        row = finalize_minute(
            orderbook_bids, orderbook_asks, symbol, current_minute,
            spreads, mid_prices, imbalances,
            bid_volumes, ask_volumes,
            bid_change_count, ask_change_count, msg_count,
        )
        results.append(row)

    return results


# ═══════════════════════════════════════════════════════════════════
# Основной класс
# ═══════════════════════════════════════════════════════════════════

class OrderbookLoader:
    """Загрузчик данных orderbook из исторических архивов Bybit"""

    def __init__(self, symbol: str = DEFAULT_SYMBOL, force_reload: bool = False):
        self.symbol = symbol
        self.force_reload = force_reload
        self.db = DatabaseConnection()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TradingChart-OrderbookLoader/1.0'
        })

    def ensure_table(self):
        """Проверяет существование таблицы, создаёт если нет"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                """, (TABLE_NAME,))
                exists = cur.fetchone()[0]

                if exists:
                    logger.info(f"Таблица {TABLE_NAME} существует")
                    return

                # Таблица не существует — создаём
                logger.info(f"Создаём таблицу {TABLE_NAME}...")
                cur.execute(CREATE_TABLE_SQL)
                for idx_sql in CREATE_INDEXES_SQL:
                    cur.execute(idx_sql)
                conn.commit()
                logger.info(f"Таблица {TABLE_NAME} создана")

    def get_start_date(self) -> datetime:
        """
        Определяет дату начала загрузки из MAX(timestamp) в БД.
        Если день загружен частично — возвращает начало этого дня (перезагрузка).
        """
        if self.force_reload:
            logger.info(f"Force-reload: начинаем с {EARLIEST_DATE.date()}")
            return EARLIEST_DATE

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX(timestamp) FROM {TABLE_NAME} WHERE symbol = %s",
                    (self.symbol,)
                )
                result = cur.fetchone()[0]

        if result is None:
            logger.info(f"Данных нет в БД. Начинаем с {EARLIEST_DATE.date()}")
            return EARLIEST_DATE

        # result может быть naive или aware
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        # Проверяем: если последняя запись = 23:59 → день полный, начинаем со следующего
        if result.hour == 23 and result.minute == 59:
            start = (result + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            logger.info(f"Последняя запись: {result} (день полный). Продолжаем с {start.date()}")
        else:
            # Частичный день — перезагружаем его
            start = result.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"Последняя запись: {result} (день частичный). Перезагружаем {start.date()}")

        return start

    def get_available_files(self) -> List[Tuple[datetime, str]]:
        """
        Получает список доступных ZIP файлов с directory listing.
        Возвращает список (date, url) отсортированный по дате.
        """
        listing_url = f"{BASE_URL}/{self.symbol}/"
        logger.info(f"Загружаем список файлов: {listing_url}")

        response = self._http_get(listing_url)
        html = response.text

        # Парсим ссылки на ZIP файлы
        # Формат: 2023-01-18_BTCUSDT_ob500.data.zip или 2026-02-01_BTCUSDT_ob200.data.zip
        pattern = rf'href="(\d{{4}}-\d{{2}}-\d{{2}}_{re.escape(self.symbol)}_ob\d+\.data\.zip)"'
        matches = re.findall(pattern, html)

        files = []
        for filename in matches:
            date_str = filename[:10]  # "2023-01-18"
            try:
                file_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                url = f"{BASE_URL}/{self.symbol}/{filename}"
                files.append((file_date, url))
            except ValueError:
                continue

        files.sort(key=lambda x: x[0])
        logger.info(f"Найдено {len(files)} файлов (от {files[0][0].date()} до {files[-1][0].date()})")
        return files

    def _http_get(self, url: str, stream: bool = False) -> requests.Response:
        """HTTP GET с retry логикой. При неудаче — crash."""
        last_error = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                response = self.session.get(url, timeout=DOWNLOAD_TIMEOUT, stream=stream)
                response.raise_for_status()
                return response
            except Exception as e:
                last_error = e
                if attempt < RETRY_ATTEMPTS:
                    delay = RETRY_DELAY * attempt
                    logger.warning(
                        f"Ошибка загрузки (попытка {attempt}/{RETRY_ATTEMPTS}): {e}. "
                        f"Повтор через {delay}с..."
                    )
                    time.sleep(delay)

        logger.error(f"Все {RETRY_ATTEMPTS} попыток провалились для {url}: {last_error}")
        raise last_error

    def download_to_ram(self, url: str) -> io.BytesIO:
        """Скачивает ZIP в RAM и возвращает BytesIO"""
        response = self._http_get(url)
        return io.BytesIO(response.content)

    def save_day_to_db(self, rows: List[tuple], conn):
        """Записывает строки одного дня в БД"""
        if not rows:
            return

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=500)
        conn.commit()

    def run(self):
        """Основной цикл загрузки"""
        global shutdown_requested

        logger.info("=" * 80)
        logger.info(f"Orderbook Loader — {self.symbol}")
        logger.info("=" * 80)

        # 1. Создаём таблицу
        self.ensure_table()

        # 2. Определяем начальную дату
        start_date = self.get_start_date()

        # 3. Получаем список файлов
        all_files = self.get_available_files()

        # 4. Фильтруем по дате
        files_to_process = [
            (d, url) for d, url in all_files if d >= start_date
        ]

        if not files_to_process:
            logger.info("Все данные актуальны. Нечего загружать.")
            return

        # Исключаем сегодняшний день (файл ещё формируется)
        today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        files_to_process = [(d, url) for d, url in files_to_process if d < today]

        if not files_to_process:
            logger.info("Все данные актуальны (сегодняшний файл ещё не готов).")
            return

        logger.info(f"К обработке: {len(files_to_process)} дней "
                     f"({files_to_process[0][0].date()} → {files_to_process[-1][0].date()})")
        logger.info("")

        # 5. Основной цикл
        total_rows = 0
        total_messages = 0
        conn = psycopg2.connect(**self.db.config)

        try:
            pbar = tqdm(
                files_to_process,
                desc=f"Orderbook {self.symbol}",
                unit="day",
                ncols=100,
            )

            for file_date, url in pbar:
                if shutdown_requested:
                    logger.info("Прерывание по запросу пользователя")
                    break

                date_str = file_date.strftime('%Y-%m-%d')
                pbar.set_postfix_str(date_str)

                # Скачиваем в RAM
                try:
                    zip_data = self.download_to_ram(url)
                except Exception as e:
                    logger.error(f"Не удалось скачать {date_str}: {e}")
                    conn.close()
                    raise

                # Обрабатываем
                try:
                    rows = process_day_from_zip(zip_data, self.symbol)
                except Exception as e:
                    logger.error(f"Ошибка обработки {date_str}: {e}")
                    conn.close()
                    raise

                # Освобождаем RAM
                del zip_data

                # Записываем в БД
                if rows:
                    try:
                        self.save_day_to_db(rows, conn)
                    except Exception as e:
                        logger.error(f"Ошибка записи в БД {date_str}: {e}")
                        conn.close()
                        raise

                    total_rows += len(rows)

                logger.debug(f"{date_str}: {len(rows)} строк записано")

            pbar.close()

        except Exception:
            if not conn.closed:
                conn.close()
            raise
        finally:
            if not conn.closed:
                conn.close()

        # Итоги
        logger.info("")
        logger.info("=" * 80)
        if shutdown_requested:
            logger.info(f"Orderbook Loader — Прервано пользователем")
        else:
            logger.info(f"Orderbook Loader — Завершено")
        logger.info(f"Записано строк: {total_rows:,}")
        logger.info("=" * 80)


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def setup_logging() -> str:
    """Настройка логирования в файл и консоль"""
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'orderbook_{timestamp}.log'

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ]
    )

    logger.info(f"Логирование настроено. Лог-файл: {log_file}")
    return str(log_file)


def parse_args():
    """Парсинг аргументов"""
    parser = argparse.ArgumentParser(
        description='Orderbook Loader — загрузка данных orderbook из архивов Bybit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 orderbook_loader.py                        # Продолжить загрузку BTCUSDT
  python3 orderbook_loader.py --symbol BTCUSDT       # Конкретный символ
  python3 orderbook_loader.py --force-reload          # Перезагрузить всё с 2023-01-18

Источник: https://quote-saver.bycsi.com/orderbook/linear/
Таблица: orderbook_bybit_futures_1m (58 колонок)
        """
    )

    parser.add_argument(
        '--symbol',
        default=DEFAULT_SYMBOL,
        help=f'Символ для обработки (по умолчанию: {DEFAULT_SYMBOL})'
    )

    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Перезагрузить все данные с 2023-01-18'
    )

    return parser.parse_args()


def main():
    """Главная функция"""
    log_file = setup_logging()

    args = parse_args()

    start_time = time.time()

    try:
        loader = OrderbookLoader(
            symbol=args.symbol,
            force_reload=args.force_reload,
        )
        loader.run()
    except KeyboardInterrupt:
        logger.info("Прервано пользователем (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)

    logger.info(f"Время выполнения: {hours}h {minutes}m {seconds}s")
    logger.info(f"Лог-файл: {log_file}")


if __name__ == '__main__':
    main()
