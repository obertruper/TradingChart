#!/usr/bin/env python3
"""
Orderbook Binance Loader
========================
Загрузка и агрегация данных orderbook из публичных архивов Binance (data.binance.vision).

Источники:
1. bookDepth (2023-01-01 → настоящее):
   - URL: https://data.binance.vision/data/futures/um/daily/bookDepth/{SYMBOL}/{SYMBOL}-bookDepth-{YYYY-MM-DD}.zip
   - CSV: timestamp, percentage, depth, notional
   - ~2800 снимков/день (~30 сек интервал), 10-12 строк/снимок
   - ~500 KB/день ZIP

2. bookTicker (2023-05-16 → 2024-03-30):
   - URL: https://data.binance.vision/data/futures/um/daily/bookTicker/{SYMBOL}/{SYMBOL}-bookTicker-{YYYY-MM-DD}.zip
   - CSV: update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time
   - ~4.5M тиков/день, 50-130 MB/день ZIP
   - ВНИМАНИЕ: Binance прекратил публикацию после 2024-03-30 (GitHub issue #372)

Алгоритм:
1. Определяем последнюю запись в БД (MAX timestamp)
2. Для каждого дня: скачиваем bookDepth ZIP → агрегируем → скачиваем bookTicker ZIP → агрегируем
3. Мержим оба источника → 1440 строк/день
4. INSERT...ON CONFLICT DO UPDATE → COMMIT после каждого дня

Таблица: orderbook_binance_futures_1m (46 колонок)

Usage:
    python3 orderbook_binance_loader.py                        # Продолжить загрузку
    python3 orderbook_binance_loader.py --symbol BTCUSDT       # Конкретный символ
    python3 orderbook_binance_loader.py --force-reload          # Перезагрузить всё с 2023-01-01
"""

import sys
import os
import io
import logging
import argparse
import warnings
import signal
import zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
import time

import yaml
import numpy as np
import pandas as pd
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


def format_elapsed(seconds: float) -> str:
    """Форматирует время в формат Xm YYs или Xs"""
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m{secs:02d}s"


# Настройка логирования
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# Константы
# ═══════════════════════════════════════════════════════════════════

TABLE_NAME = "orderbook_binance_futures_1m"
DEFAULT_SYMBOL = "BTCUSDT"

# URL шаблоны для data.binance.vision
BOOK_DEPTH_URL = "https://data.binance.vision/data/futures/um/daily/bookDepth/{symbol}/{symbol}-bookDepth-{date}.zip"
BOOK_TICKER_URL = "https://data.binance.vision/data/futures/um/daily/bookTicker/{symbol}/{symbol}-bookTicker-{date}.zip"
BOOK_TICKER_MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/bookTicker/{symbol}/{symbol}-bookTicker-{month}.zip"

# Даты начала данных
BOOK_DEPTH_EARLIEST = datetime(2023, 1, 1, tzinfo=timezone.utc)
BOOK_TICKER_EARLIEST = datetime(2023, 5, 16, tzinfo=timezone.utc)
# Binance прекратил публикацию daily bookTicker после 2024-03-30
# (GitHub issue: binance/binance-public-data#372)
BOOK_TICKER_LAST = datetime(2024, 3, 30, tzinfo=timezone.utc)
# Monthly архивы: 2023-05 → 2024-04 (последний).
# Fallback ищет любые месяцы с NULL bookTicker начиная с BOOK_TICKER_EARLIEST.

# HTTP
DOWNLOAD_TIMEOUT = 300  # 5 минут на скачивание файла
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # секунд между попытками

# Процентные уровни bookDepth
# 10 уровней всегда доступны (±1%, ±2%, ±3%, ±4%, ±5%)
# 12 уровней с 2026-01-15 (добавлены ±0.2%)
DEPTH_LEVELS_10_DATE = datetime(2026, 1, 15, tzinfo=timezone.utc)

# ═══════════════════════════════════════════════════════════════════
# SQL
# ═══════════════════════════════════════════════════════════════════

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    -- Первичный ключ
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    -- === bookTicker колонки (22) — NULL до 2023-05-16 ===

    -- PRICE (LAST значения за минуту)
    best_bid DECIMAL(20,8),
    best_ask DECIMAL(20,8),
    best_bid_qty DECIMAL(20,8),
    best_ask_qty DECIMAL(20,8),
    mid_price DECIMAL(20,8),
    microprice DECIMAL(20,8),

    -- SPREAD
    spread DECIMAL(20,8),
    spread_pct DECIMAL(10,6),
    spread_min DECIMAL(20,8),
    spread_max DECIMAL(20,8),
    spread_avg DECIMAL(20,8),
    spread_std DECIMAL(20,8),

    -- VOLATILITY (mid_price за минуту)
    mid_price_range DECIMAL(20,8),
    mid_price_std DECIMAL(20,8),
    price_momentum DECIMAL(10,6),

    -- ACTIVITY
    best_bid_changes INT,
    best_ask_changes INT,
    tick_count INT,

    -- QUANTITY STATS
    best_bid_qty_avg DECIMAL(20,8),
    best_bid_qty_max DECIMAL(20,8),
    best_ask_qty_avg DECIMAL(20,8),
    best_ask_qty_max DECIMAL(20,8),

    -- === bookDepth колонки (22) — доступны с 2023-01-01 ===

    -- BID DEPTH (объём в базовой валюте на уровне %)
    bid_depth_1pct DECIMAL(20,8),
    bid_depth_2pct DECIMAL(20,8),
    bid_depth_3pct DECIMAL(20,8),
    bid_depth_4pct DECIMAL(20,8),
    bid_depth_5pct DECIMAL(20,8),

    -- ASK DEPTH
    ask_depth_1pct DECIMAL(20,8),
    ask_depth_2pct DECIMAL(20,8),
    ask_depth_3pct DECIMAL(20,8),
    ask_depth_4pct DECIMAL(20,8),
    ask_depth_5pct DECIMAL(20,8),

    -- NOTIONAL (USD на уровне 5%)
    bid_notional_5pct DECIMAL(20,4),
    ask_notional_5pct DECIMAL(20,4),

    -- ±0.2% уровни (NULL до 2026-01-15)
    bid_depth_02pct DECIMAL(20,8),
    ask_depth_02pct DECIMAL(20,8),

    -- IMBALANCE (рассчитывается из depth)
    imbalance_1pct DECIMAL(10,6),
    imbalance_2pct DECIMAL(10,6),
    imbalance_3pct DECIMAL(10,6),
    imbalance_5pct DECIMAL(10,6),

    -- PRESSURE / RATIO
    depth_ratio DECIMAL(20,8),
    buy_pressure DECIMAL(20,8),

    -- LIQUIDITY
    liquidity_score DECIMAL(20,4),

    -- ACTIVITY
    snapshot_count INT,

    PRIMARY KEY (timestamp, symbol)
);
"""

CREATE_INDEXES_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_ob_binance_symbol_ts ON {TABLE_NAME} (symbol, timestamp);",
    f"CREATE INDEX IF NOT EXISTS idx_ob_binance_timestamp ON {TABLE_NAME} (timestamp);",
]

# Все колонки в порядке для INSERT
ALL_COLUMNS = [
    'timestamp', 'symbol',
    # bookTicker (22 колонки)
    'best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty',
    'mid_price', 'microprice',
    'spread', 'spread_pct',
    'spread_min', 'spread_max', 'spread_avg', 'spread_std',
    'mid_price_range', 'mid_price_std', 'price_momentum',
    'best_bid_changes', 'best_ask_changes', 'tick_count',
    'best_bid_qty_avg', 'best_bid_qty_max',
    'best_ask_qty_avg', 'best_ask_qty_max',
    # bookDepth (22 колонки)
    'bid_depth_1pct', 'bid_depth_2pct', 'bid_depth_3pct',
    'bid_depth_4pct', 'bid_depth_5pct',
    'ask_depth_1pct', 'ask_depth_2pct', 'ask_depth_3pct',
    'ask_depth_4pct', 'ask_depth_5pct',
    'bid_notional_5pct', 'ask_notional_5pct',
    'bid_depth_02pct', 'ask_depth_02pct',
    'imbalance_1pct', 'imbalance_2pct', 'imbalance_3pct', 'imbalance_5pct',
    'depth_ratio', 'buy_pressure',
    'liquidity_score',
    'snapshot_count',
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

# bookTicker колонки (для UPDATE при monthly fallback)
TICKER_COLUMNS = [
    'best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty',
    'mid_price', 'microprice',
    'spread', 'spread_pct',
    'spread_min', 'spread_max', 'spread_avg', 'spread_std',
    'mid_price_range', 'mid_price_std', 'price_momentum',
    'best_bid_changes', 'best_ask_changes', 'tick_count',
    'best_bid_qty_avg', 'best_bid_qty_max',
    'best_ask_qty_avg', 'best_ask_qty_max',
]

TICKER_UPDATE_SQL = f"""
    UPDATE {TABLE_NAME}
    SET {', '.join(f'{c} = %s' for c in TICKER_COLUMNS)}
    WHERE timestamp = %s AND symbol = %s
"""


# ═══════════════════════════════════════════════════════════════════
# Обработка bookDepth
# ═══════════════════════════════════════════════════════════════════

def process_book_depth(zip_bytes: io.BytesIO) -> Dict[datetime, dict]:
    """
    Обрабатывает bookDepth ZIP и возвращает dict {minute: metrics}.

    CSV формат: timestamp, percentage, depth, notional
    - timestamp: миллисекунды
    - percentage: процентный уровень (напр. "1", "2", ..., "5", или "0.2")
    - depth: объём в базовой валюте (BTC)
    - notional: объём в USD

    Берём LAST снимок за каждую минуту.
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        inner_name = zf.namelist()[0]
        with zf.open(inner_name) as f:
            df = pd.read_csv(
                f,
                names=['timestamp', 'percentage', 'depth', 'notional'],
                header=0,  # CSV имеет заголовок
                dtype={'percentage': str,
                       'depth': np.float64, 'notional': np.float64}
            )

    if df.empty:
        return {}

    # timestamp — строка вида "2023-01-01 00:06:05"
    df['ts'] = pd.to_datetime(df['timestamp'], utc=True)
    df['minute'] = df['ts'].dt.floor('min')

    # Для каждой минуты берём LAST снимок (максимальный timestamp)
    # Берём все строки с максимальным ts в каждой минуте
    idx_last = df.groupby('minute')['ts'].transform('max') == df['ts']
    last_snapshots = df[idx_last].copy()

    result = {}
    snapshot_counts = df.groupby('minute')['ts'].nunique()

    for minute, group in last_snapshots.groupby('minute'):
        minute_dt = minute.to_pydatetime()
        metrics = {}

        # Маппинг процентного уровня → колонки
        for _, row in group.iterrows():
            pct = str(row['percentage']).strip()
            depth_val = row['depth']
            notional_val = row['notional']

            # Определяем сторону по знаку percentage
            # Отрицательные = bid (ниже цены), положительные = ask (выше цены)
            if pct.startswith('-'):
                side = 'bid'
                pct_abs = pct[1:]  # убираем минус
            else:
                side = 'ask'
                pct_abs = pct

            # Нормализуем строку процента: "1.00" → "1", "0.20" → "0.2"
            pct_abs = pct_abs.rstrip('0').rstrip('.')

            # Маппинг на колонки
            if pct_abs == '0.2':
                metrics[f'{side}_depth_02pct'] = depth_val
            elif pct_abs == '1':
                metrics[f'{side}_depth_1pct'] = depth_val
            elif pct_abs == '2':
                metrics[f'{side}_depth_2pct'] = depth_val
            elif pct_abs == '3':
                metrics[f'{side}_depth_3pct'] = depth_val
            elif pct_abs == '4':
                metrics[f'{side}_depth_4pct'] = depth_val
            elif pct_abs == '5':
                metrics[f'{side}_depth_5pct'] = depth_val
                # Сохраняем notional для 5% уровня
                metrics[f'{side}_notional_5pct'] = notional_val

        # Рассчитываем производные метрики
        bid_1 = metrics.get('bid_depth_1pct', 0) or 0
        ask_1 = metrics.get('ask_depth_1pct', 0) or 0
        bid_2 = metrics.get('bid_depth_2pct', 0) or 0
        ask_2 = metrics.get('ask_depth_2pct', 0) or 0
        bid_3 = metrics.get('bid_depth_3pct', 0) or 0
        ask_3 = metrics.get('ask_depth_3pct', 0) or 0
        bid_5 = metrics.get('bid_depth_5pct', 0) or 0
        ask_5 = metrics.get('ask_depth_5pct', 0) or 0

        # Imbalance = (bid - ask) / (bid + ask)
        for pct_label, bid_val, ask_val in [
            ('1pct', bid_1, ask_1),
            ('2pct', bid_2, ask_2),
            ('3pct', bid_3, ask_3),
            ('5pct', bid_5, ask_5),
        ]:
            total = bid_val + ask_val
            metrics[f'imbalance_{pct_label}'] = (bid_val - ask_val) / total if total > 0 else None

        # depth_ratio = bid_5pct / ask_5pct (cap at 9999 for extreme cases)
        if ask_5 > 0:
            metrics['depth_ratio'] = min(bid_5 / ask_5, 9999.0)
        else:
            metrics['depth_ratio'] = None

        # buy_pressure = bid_1pct / ask_1pct (cap at 9999 for extreme cases)
        if ask_1 > 0:
            metrics['buy_pressure'] = min(bid_1 / ask_1, 9999.0)
        else:
            metrics['buy_pressure'] = None

        # liquidity_score = bid_notional_5pct + ask_notional_5pct
        bid_not = metrics.get('bid_notional_5pct', 0) or 0
        ask_not = metrics.get('ask_notional_5pct', 0) or 0
        metrics['liquidity_score'] = bid_not + ask_not

        # snapshot_count
        metrics['snapshot_count'] = int(snapshot_counts.get(minute, 0))

        result[minute_dt] = metrics

    return result


# ═══════════════════════════════════════════════════════════════════
# Обработка bookTicker
# ═══════════════════════════════════════════════════════════════════

BOOK_TICKER_CSV_COLUMNS = [
    'update_id', 'best_bid_price', 'best_bid_qty',
    'best_ask_price', 'best_ask_qty',
    'transaction_time', 'event_time',
]

BOOK_TICKER_CSV_DTYPE = {
    'update_id': np.int64,
    'best_bid_price': np.float64,
    'best_bid_qty': np.float64,
    'best_ask_price': np.float64,
    'best_ask_qty': np.float64,
    'transaction_time': np.int64,
    'event_time': np.int64,
}


def _aggregate_ticker_df(df: pd.DataFrame) -> Dict[datetime, dict]:
    """
    Агрегирует raw bookTicker DataFrame в per-minute метрики.

    Общая логика для daily и monthly обработки.
    df должен содержать колонки: best_bid_price, best_bid_qty,
    best_ask_price, best_ask_qty, transaction_time, event_time, update_id.
    """
    if df.empty:
        return {}

    # Рассчитываем производные колонки
    df['mid_price'] = (df['best_bid_price'] + df['best_ask_price']) / 2.0
    df['spread'] = df['best_ask_price'] - df['best_bid_price']
    df['spread_pct'] = df['spread'] / df['mid_price'] * 100.0

    # Минутная группировка (по transaction_time)
    df['minute'] = pd.to_datetime(df['transaction_time'], unit='ms', utc=True).dt.floor('min')

    # Определяем изменения best_bid и best_ask
    df['bid_changed'] = df['best_bid_price'].diff().ne(0).astype(int)
    df['ask_changed'] = df['best_ask_price'].diff().ne(0).astype(int)
    # Первая строка — не считаем изменением
    df.iloc[0, df.columns.get_loc('bid_changed')] = 0
    df.iloc[0, df.columns.get_loc('ask_changed')] = 0

    # Агрегация по минутам
    agg = df.groupby('minute').agg(
        best_bid=('best_bid_price', 'last'),
        best_ask=('best_ask_price', 'last'),
        best_bid_qty=('best_bid_qty', 'last'),
        best_ask_qty=('best_ask_qty', 'last'),
        mid_price=('mid_price', 'last'),
        spread=('spread', 'last'),
        spread_pct=('spread_pct', 'last'),
        spread_min=('spread', 'min'),
        spread_max=('spread', 'max'),
        spread_avg=('spread', 'mean'),
        spread_std=('spread', 'std'),
        mid_price_max=('mid_price', 'max'),
        mid_price_min=('mid_price', 'min'),
        mid_price_std=('mid_price', 'std'),
        mid_price_first=('mid_price', 'first'),
        mid_price_last=('mid_price', 'last'),
        best_bid_changes=('bid_changed', 'sum'),
        best_ask_changes=('ask_changed', 'sum'),
        tick_count=('update_id', 'count'),
        best_bid_qty_avg=('best_bid_qty', 'mean'),
        best_bid_qty_max=('best_bid_qty', 'max'),
        best_ask_qty_avg=('best_ask_qty', 'mean'),
        best_ask_qty_max=('best_ask_qty', 'max'),
    ).reset_index()

    # Рассчитываем производные
    agg['mid_price_range'] = agg['mid_price_max'] - agg['mid_price_min']
    agg['price_momentum'] = np.where(
        agg['mid_price_first'] > 0,
        (agg['mid_price_last'] - agg['mid_price_first']) / agg['mid_price_first'],
        0.0
    )

    # Microprice = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)
    total_qty = agg['best_bid_qty'] + agg['best_ask_qty']
    agg['microprice'] = np.where(
        total_qty > 0,
        (agg['best_bid'] * agg['best_ask_qty'] + agg['best_ask'] * agg['best_bid_qty']) / total_qty,
        agg['mid_price']
    )

    # Заменяем NaN на None
    agg = agg.where(agg.notna(), None)

    # Конвертируем в словарь
    result = {}
    for _, row in agg.iterrows():
        minute_dt = row['minute'].to_pydatetime()
        metrics = {}
        for col in TICKER_COLUMNS:
            val = row[col]
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                if isinstance(val, (np.integer,)):
                    metrics[col] = int(val)
                elif isinstance(val, (np.floating,)):
                    metrics[col] = float(val)
                else:
                    metrics[col] = val
            else:
                metrics[col] = None
        result[minute_dt] = metrics

    return result


def process_book_ticker(zip_bytes: io.BytesIO) -> Dict[datetime, dict]:
    """
    Обрабатывает bookTicker daily ZIP и возвращает dict {minute: metrics}.
    Для daily файлов (~4.5M строк) — загрузка целиком в память.
    """
    with zipfile.ZipFile(zip_bytes) as zf:
        inner_name = zf.namelist()[0]
        with zf.open(inner_name) as f:
            df = pd.read_csv(
                f,
                names=BOOK_TICKER_CSV_COLUMNS,
                header=0,
                dtype=BOOK_TICKER_CSV_DTYPE,
            )

    return _aggregate_ticker_df(df)


# ═══════════════════════════════════════════════════════════════════
# Merge и формирование строк
# ═══════════════════════════════════════════════════════════════════

def merge_and_build_rows(
    depth_data: Dict[datetime, dict],
    ticker_data: Optional[Dict[datetime, dict]],
    symbol: str,
) -> List[tuple]:
    """
    Мержит bookDepth и bookTicker данные, формирует строки для БД.

    bookDepth — база (всегда есть).
    bookTicker — может быть None (до 2023-05-16) или пустым.
    """
    rows = []

    # bookDepth колонки (в порядке ALL_COLUMNS)
    depth_column_order = [
        'bid_depth_1pct', 'bid_depth_2pct', 'bid_depth_3pct',
        'bid_depth_4pct', 'bid_depth_5pct',
        'ask_depth_1pct', 'ask_depth_2pct', 'ask_depth_3pct',
        'ask_depth_4pct', 'ask_depth_5pct',
        'bid_notional_5pct', 'ask_notional_5pct',
        'bid_depth_02pct', 'ask_depth_02pct',
        'imbalance_1pct', 'imbalance_2pct', 'imbalance_3pct', 'imbalance_5pct',
        'depth_ratio', 'buy_pressure',
        'liquidity_score',
        'snapshot_count',
    ]

    # bookTicker колонки (в порядке ALL_COLUMNS)
    ticker_column_order = [
        'best_bid', 'best_ask', 'best_bid_qty', 'best_ask_qty',
        'mid_price', 'microprice',
        'spread', 'spread_pct',
        'spread_min', 'spread_max', 'spread_avg', 'spread_std',
        'mid_price_range', 'mid_price_std', 'price_momentum',
        'best_bid_changes', 'best_ask_changes', 'tick_count',
        'best_bid_qty_avg', 'best_bid_qty_max',
        'best_ask_qty_avg', 'best_ask_qty_max',
    ]

    for minute_dt in sorted(depth_data.keys()):
        depth_metrics = depth_data[minute_dt]

        # bookTicker данные (если есть)
        ticker_metrics = None
        if ticker_data is not None:
            ticker_metrics = ticker_data.get(minute_dt)

        # Формируем tuple в порядке ALL_COLUMNS
        row = [minute_dt, symbol]

        # bookTicker колонки
        for col in ticker_column_order:
            if ticker_metrics is not None:
                row.append(ticker_metrics.get(col))
            else:
                row.append(None)

        # bookDepth колонки
        for col in depth_column_order:
            row.append(depth_metrics.get(col))

        rows.append(tuple(row))

    return rows


# ═══════════════════════════════════════════════════════════════════
# Конфигурация
# ═══════════════════════════════════════════════════════════════════

def load_config() -> dict:
    """Загружает конфигурацию из indicators_config.yaml"""
    config_path = Path(__file__).parent / 'indicators_config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════════════════
# Основной класс
# ═══════════════════════════════════════════════════════════════════

class OrderbookBinanceLoader:
    """Загрузчик данных orderbook из публичных архивов Binance"""

    def __init__(self, symbol: Optional[str] = None, force_reload: bool = False,
                 check_nulls: bool = False):
        """
        Args:
            symbol: Конкретный символ или None для всех из конфига
            force_reload: Начать загрузку с самой ранней даты
            check_nulls: Найти и перезагрузить дни с NULL данными
        """
        self.force_reload = force_reload
        self.check_nulls = check_nulls
        self.db = DatabaseConnection()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TradingChart-OrderbookBinanceLoader/1.0'
        })

        # Загружаем символы из конфига
        config = load_config()
        config_symbols = config.get('indicators', {}).get(
            'binance_orderbook', {}
        ).get('symbols', [DEFAULT_SYMBOL])

        if symbol:
            self.symbols = [symbol]
        else:
            self.symbols = config_symbols

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

                logger.info(f"Создаём таблицу {TABLE_NAME}...")
                cur.execute(CREATE_TABLE_SQL)
                for idx_sql in CREATE_INDEXES_SQL:
                    cur.execute(idx_sql)
                conn.commit()
                logger.info(f"Таблица {TABLE_NAME} создана")

    def get_start_date(self, symbol: str) -> datetime:
        """
        Определяет дату начала загрузки из MAX(timestamp) в БД для символа.
        Если день загружен частично — возвращает начало этого дня (перезагрузка).
        """
        if self.force_reload:
            logger.info(f"Force-reload: начинаем с {BOOK_DEPTH_EARLIEST.date()}")
            return BOOK_DEPTH_EARLIEST

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX(timestamp) FROM {TABLE_NAME} WHERE symbol = %s",
                    (symbol,)
                )
                result = cur.fetchone()[0]

        if result is None:
            logger.info(f"Данных нет в БД. Начинаем с {BOOK_DEPTH_EARLIEST.date()}")
            return BOOK_DEPTH_EARLIEST

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        # Если последняя запись = 23:59 → день полный, начинаем со следующего
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

    def download_to_ram(self, url: str, show_progress: bool = False) -> Optional[io.BytesIO]:
        """
        Скачивает ZIP в RAM и возвращает BytesIO.
        Возвращает None при 404 (нормально для отсутствующих дат).
        show_progress: показывать tqdm прогресс-бар (для больших файлов).
        """
        last_error = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                response = self.session.get(
                    url, timeout=DOWNLOAD_TIMEOUT,
                    stream=show_progress,
                )
                if response.status_code == 404:
                    return None
                if response.status_code == 403:
                    # Binance иногда возвращает 403 вместо 404
                    return None
                response.raise_for_status()

                if show_progress:
                    total = int(response.headers.get('content-length', 0))
                    buf = io.BytesIO()
                    with tqdm(total=total, unit='B', unit_scale=True,
                              desc='    Download', leave=False) as pbar:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            buf.write(chunk)
                            pbar.update(len(chunk))
                    buf.seek(0)
                    return buf

                return io.BytesIO(response.content)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (404, 403):
                    return None
                last_error = e
                if attempt < RETRY_ATTEMPTS:
                    delay = RETRY_DELAY * attempt
                    logger.warning(
                        f"Ошибка загрузки (попытка {attempt}/{RETRY_ATTEMPTS}): {e}. "
                        f"Повтор через {delay}с..."
                    )
                    time.sleep(delay)
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

    def save_day_to_db(self, rows: List[tuple], conn):
        """Записывает строки одного дня в БД"""
        if not rows:
            return

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=500)
        conn.commit()

    def get_null_dates(self, symbol: str) -> List[datetime]:
        """
        Находит даты, в которых строки существуют но bookDepth данные = NULL.
        Используется для --check-nulls.
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DISTINCT DATE(timestamp) as day
                    FROM {TABLE_NAME}
                    WHERE symbol = %s
                      AND bid_depth_1pct IS NULL
                    ORDER BY day
                """, (symbol,))
                rows = cur.fetchall()

        null_dates = []
        for row in rows:
            dt = datetime.combine(row[0], datetime.min.time()).replace(tzinfo=timezone.utc)
            null_dates.append(dt)

        return null_dates

    def run_symbol(self, symbol: str) -> Tuple[int, int, int]:
        """
        Загрузка данных для одного символа.

        Returns:
            (total_rows, days_processed, days_skipped)
        """
        global shutdown_requested

        logger.info("")
        logger.info("─" * 80)
        logger.info(f"  {symbol}")
        logger.info("─" * 80)

        # 1. Определяем список дат
        if self.check_nulls:
            null_dates = self.get_null_dates(symbol)
            if not null_dates:
                logger.info(f"✅ {symbol}: NULL данных не найдено")
                return 0, 0, 0
            logger.info(f"Найдено {len(null_dates)} дней с NULL данными "
                        f"({null_dates[0].date()} → {null_dates[-1].date()})")
            dates_to_process = null_dates
        else:
            start_date = self.get_start_date(symbol)

            today = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            dates_to_process = []
            current = start_date
            while current < today:
                dates_to_process.append(current)
                current += timedelta(days=1)

        if not dates_to_process:
            logger.info(f"✅ {symbol}: все данные актуальны")
            return 0, 0, 0

        logger.info(f"К обработке: {len(dates_to_process)} дней "
                     f"({dates_to_process[0].date()} → {dates_to_process[-1].date()})")

        # 3. Основной цикл
        total_rows = 0
        days_processed = 0
        days_skipped = 0
        conn = psycopg2.connect(**self.db.config)

        try:
            pbar = tqdm(
                dates_to_process,
                desc=f"Binance OB {symbol}",
                unit="day",
                ncols=100,
            )

            for file_date in pbar:
                if shutdown_requested:
                    logger.info("Прерывание по запросу пользователя")
                    break

                date_str = file_date.strftime('%Y-%m-%d')
                pbar.set_postfix_str(date_str)
                day_start_time = time.time()

                # === bookDepth ===
                depth_url = BOOK_DEPTH_URL.format(symbol=symbol, date=date_str)
                try:
                    depth_zip = self.download_to_ram(depth_url)
                except Exception as e:
                    logger.error(f"Не удалось скачать bookDepth {date_str}: {e}")
                    conn.close()
                    raise

                if depth_zip is None:
                    days_skipped += 1
                    logger.debug(f"{date_str}: bookDepth не найден, пропуск")
                    continue

                try:
                    depth_data = process_book_depth(depth_zip)
                except Exception as e:
                    logger.error(f"Ошибка обработки bookDepth {date_str}: {e}")
                    conn.close()
                    raise
                finally:
                    del depth_zip

                if not depth_data:
                    days_skipped += 1
                    logger.debug(f"{date_str}: bookDepth пустой, пропуск")
                    continue

                # === bookTicker ===
                ticker_data = None
                if BOOK_TICKER_EARLIEST <= file_date <= BOOK_TICKER_LAST:
                    ticker_url = BOOK_TICKER_URL.format(symbol=symbol, date=date_str)
                    try:
                        ticker_zip = self.download_to_ram(ticker_url)
                    except Exception as e:
                        logger.warning(f"Не удалось скачать bookTicker {date_str}: {e}. "
                                       f"Продолжаем без bookTicker.")
                        ticker_zip = None

                    if ticker_zip is not None:
                        try:
                            ticker_data = process_book_ticker(ticker_zip)
                        except Exception as e:
                            logger.warning(f"Ошибка обработки bookTicker {date_str}: {e}. "
                                           f"Продолжаем без bookTicker.")
                            ticker_data = None
                        finally:
                            del ticker_zip

                # === Merge ===
                rows = merge_and_build_rows(depth_data, ticker_data, symbol)

                del depth_data
                del ticker_data

                # === Записываем в БД ===
                if rows:
                    try:
                        self.save_day_to_db(rows, conn)
                    except Exception as e:
                        logger.error(f"Ошибка записи в БД {date_str}: {e}")
                        conn.close()
                        raise

                    total_rows += len(rows)
                    days_processed += 1

                day_elapsed = time.time() - day_start_time
                pbar.set_postfix_str(
                    f"{date_str} | {len(rows)}r | {format_elapsed(day_elapsed)}"
                )

                logger.debug(f"{date_str}: {len(rows)} строк записано")

            pbar.close()

        except Exception:
            if not conn.closed:
                conn.close()
            raise
        finally:
            if not conn.closed:
                conn.close()

        # Итоги по символу
        logger.info(f"  {symbol}: дней={days_processed:,}, пропущено={days_skipped:,}, строк={total_rows:,}")

        return total_rows, days_processed, days_skipped

    def _update_ticker_rows(self, ticker_data: Dict[datetime, dict],
                            symbol: str, conn) -> int:
        """UPDATE bookTicker колонки для существующих строк."""
        rows = []
        for minute_dt, metrics in ticker_data.items():
            values = [metrics.get(col) for col in TICKER_COLUMNS]
            values.append(minute_dt)   # WHERE timestamp = %s
            values.append(symbol)      # WHERE symbol = %s
            rows.append(tuple(values))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, TICKER_UPDATE_SQL, rows, page_size=500)
        conn.commit()
        return len(rows)

    def _get_null_ticker_dates(self, symbol: str, month_str: str) -> set:
        """Возвращает set дат (date) в конкретном месяце, где bookTicker = NULL."""
        year, month = month_str.split('-')
        month_start = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
        if int(month) == 12:
            month_end = datetime(int(year) + 1, 1, 1, tzinfo=timezone.utc)
        else:
            month_end = datetime(int(year), int(month) + 1, 1, tzinfo=timezone.utc)

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DISTINCT DATE(timestamp) as day
                    FROM {TABLE_NAME}
                    WHERE symbol = %s
                      AND timestamp >= %s AND timestamp < %s
                      AND bid_depth_1pct IS NOT NULL
                      AND best_bid IS NULL
                    ORDER BY day
                """, (symbol, month_start, month_end))
                return {row[0] for row in cur.fetchall()}

    def fill_ticker_from_monthly(self, symbol: str) -> int:
        """
        Фаза 2: Заполняет bookTicker колонки из monthly архивов.

        Находит месяцы с NULL bookTicker, скачивает monthly ZIP,
        читает CSV чанками, фильтрует только нужные дни (NULL в БД),
        накапливает данные per-day, затем обрабатывает и пишет в БД.

        Returns:
            Количество обновлённых строк
        """
        # Находим месяцы с NULL bookTicker (начиная с первых данных bookTicker)
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DISTINCT TO_CHAR(timestamp, 'YYYY-MM') as month
                    FROM {TABLE_NAME}
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND bid_depth_1pct IS NOT NULL
                      AND best_bid IS NULL
                    ORDER BY month
                """, (symbol, BOOK_TICKER_EARLIEST))
                months = [row[0] for row in cur.fetchall()]

        if not months:
            return 0

        logger.info(f"  bookTicker monthly fallback: {len(months)} мес. ({', '.join(months)})")

        total_updated = 0

        for month_str in months:
            if shutdown_requested:
                break

            # Узнаём конкретные NULL-даты для этого месяца
            null_dates = self._get_null_ticker_dates(symbol, month_str)
            if not null_dates:
                continue

            url = BOOK_TICKER_MONTHLY_URL.format(symbol=symbol, month=month_str)
            logger.info(f"  Скачиваем monthly bookTicker {month_str} ({len(null_dates)} дн. с NULL)...")

            try:
                zip_bytes = self.download_to_ram(url, show_progress=True)
            except Exception as e:
                logger.warning(f"  Ошибка скачивания monthly {month_str}: {e}")
                continue

            if zip_bytes is None:
                logger.info(f"  Monthly bookTicker {month_str} не найден (404)")
                continue

            # Читаем чанки, фильтруем только нужные дни, накапливаем per-day
            logger.info(f"  Обработка monthly bookTicker {month_str}...")
            month_updated = 0
            day_buffers = {}  # {date: [DataFrame, ...]}

            try:
                with zipfile.ZipFile(zip_bytes) as zf:
                    inner_name = zf.namelist()[0]
                    with zf.open(inner_name) as f:
                        reader = pd.read_csv(
                            f,
                            names=BOOK_TICKER_CSV_COLUMNS,
                            header=0,
                            dtype=BOOK_TICKER_CSV_DTYPE,
                            chunksize=5_000_000,
                        )

                        for chunk in tqdm(reader, desc=f'    Чтение {month_str}',
                                          unit=' чанк', leave=False):
                            chunk['day'] = pd.to_datetime(
                                chunk['transaction_time'], unit='ms', utc=True
                            ).dt.date

                            # Оставляем только строки для NULL-дат
                            chunk = chunk[chunk['day'].isin(null_dates)]
                            if chunk.empty:
                                continue

                            for day_val, day_group in chunk.groupby('day'):
                                if day_val not in day_buffers:
                                    day_buffers[day_val] = []
                                day_buffers[day_val].append(day_group)

                del zip_bytes

                # Обрабатываем накопленные дни по порядку
                if day_buffers:
                    days_count = len(day_buffers)
                    conn = psycopg2.connect(**self.db.config)
                    try:
                        for day_val in sorted(day_buffers.keys()):
                            full_day = pd.concat(day_buffers[day_val], ignore_index=True)
                            del day_buffers[day_val]

                            ticker_data = _aggregate_ticker_df(full_day)
                            del full_day
                            if ticker_data:
                                n = self._update_ticker_rows(ticker_data, symbol, conn)
                                month_updated += n
                            del ticker_data

                        total_updated += month_updated
                        logger.info(f"  ✅ {month_str}: обновлено {month_updated:,} строк ({days_count} дн.)")
                    except Exception as e:
                        logger.error(f"  Ошибка записи monthly {month_str}: {e}")
                        if not conn.closed:
                            conn.rollback()
                    finally:
                        if not conn.closed:
                            conn.close()
                else:
                    logger.info(f"  {month_str}: нет данных для NULL-дат в архиве")

            except Exception as e:
                logger.error(f"  Ошибка обработки monthly {month_str}: {e}")

        return total_updated

    def run(self):
        """Основной цикл загрузки для всех символов"""
        global shutdown_requested

        logger.info("=" * 80)
        logger.info(f"Orderbook Binance Loader")
        logger.info(f"Символы: {', '.join(self.symbols)}")
        logger.info("=" * 80)

        # 1. Создаём таблицу
        self.ensure_table()

        # 2. Обрабатываем каждый символ
        grand_total_rows = 0
        grand_total_days = 0
        grand_total_skipped = 0
        symbols_done = 0

        for i, symbol in enumerate(self.symbols, 1):
            if shutdown_requested:
                break

            if len(self.symbols) > 1:
                logger.info(f"\n[{i}/{len(self.symbols)}] {symbol}")

            rows, days, skipped = self.run_symbol(symbol)
            grand_total_rows += rows
            grand_total_days += days
            grand_total_skipped += skipped
            symbols_done += 1

        # 3. Фаза 2: monthly bookTicker fallback
        grand_monthly_rows = 0
        if not shutdown_requested:
            for symbol in self.symbols:
                if shutdown_requested:
                    break
                monthly_rows = self.fill_ticker_from_monthly(symbol)
                grand_monthly_rows += monthly_rows

        # Общие итоги
        logger.info("")
        logger.info("=" * 80)
        if shutdown_requested:
            logger.info(f"Orderbook Binance Loader — Прервано пользователем")
        else:
            logger.info(f"Orderbook Binance Loader — Завершено")
        if len(self.symbols) > 1:
            logger.info(f"Символов: {symbols_done}/{len(self.symbols)}")
        logger.info(f"Дней обработано: {grand_total_days:,}")
        logger.info(f"Дней пропущено (нет данных): {grand_total_skipped:,}")
        logger.info(f"Записано строк: {grand_total_rows:,}")
        if grand_monthly_rows > 0:
            logger.info(f"bookTicker из monthly: {grand_monthly_rows:,} строк обновлено")
        logger.info("=" * 80)


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def setup_logging() -> str:
    """Настройка логирования в файл и консоль"""
    logs_dir = Path(__file__).parent / 'logs'
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'orderbook_binance_{timestamp}.log'

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
        description='Orderbook Binance Loader — загрузка данных orderbook из архивов Binance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 orderbook_binance_loader.py                        # Все символы из конфига
  python3 orderbook_binance_loader.py --symbol BTCUSDT       # Конкретный символ
  python3 orderbook_binance_loader.py --force-reload          # Перезагрузить всё с 2023-01-01

Источники:
  bookDepth:  https://data.binance.vision/data/futures/um/daily/bookDepth/
  bookTicker: https://data.binance.vision/data/futures/um/daily/bookTicker/
Таблица: orderbook_binance_futures_1m (46 колонок)
        """
    )

    parser.add_argument(
        '--symbol',
        default=None,
        help='Символ для обработки (по умолчанию: все из indicators_config.yaml)'
    )

    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Перезагрузить все данные с 2023-01-01'
    )

    parser.add_argument(
        '--check-nulls',
        action='store_true',
        help='Найти и перезагрузить дни с NULL данными в bookDepth колонках'
    )

    return parser.parse_args()


def main():
    """Главная функция"""
    log_file = setup_logging()

    args = parse_args()

    start_time = time.time()

    try:
        loader = OrderbookBinanceLoader(
            symbol=args.symbol,
            force_reload=args.force_reload,
            check_nulls=args.check_nulls,
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
