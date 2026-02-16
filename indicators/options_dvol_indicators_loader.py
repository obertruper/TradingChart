#!/usr/bin/env python3
"""
Options DVOL Indicators Loader (Per-Group Architecture)
=======================================================
Рассчитывает технические индикаторы и метрики из DVOL свечей (1h).
Каждая группа индикаторов обрабатывается отдельно: проверяется последняя
заполненная дата, рассчитываются только недостающие данные, запись батчами по 1 день.

Источник данных:
  - options_deribit_dvol_1h (DVOL OHLC свечи, 5 лет истории)
  - indicators_bybit_futures_1h (HV_30 для iv_hv_spread)

Выход:
  - options_deribit_dvol_indicators_1h (22 расчётные колонки)

Группы индикаторов (8 групп, 22 колонки):
  1. Trend (4):     dvol_sma_24, dvol_sma_168, dvol_ema_12, dvol_ema_26
  2. Momentum (3):  dvol_change_24h, dvol_change_pct_24h, dvol_roc_24h
  3. Levels (3):    dvol_percentile_30d, dvol_percentile_90d, dvol_zscore_30d
  4. IV vs HV (2):  iv_hv_spread_30, iv_hv_ratio_30
  5. BTC/ETH (3):   dvol_btc_eth_spread, dvol_btc_eth_ratio, dvol_btc_eth_corr_24h
  6. RSI (1):       dvol_rsi_14
  7. Bollinger (3): dvol_bb_upper_20_2, dvol_bb_lower_20_2, dvol_bb_pct_b_20_2
  8. MACD (3):      dvol_macd_line_12_26, dvol_macd_signal_12_26_9, dvol_macd_hist_12_26_9

Запуск:
    python3 options_dvol_indicators_loader.py                          # Все группы, BTC + ETH
    python3 options_dvol_indicators_loader.py --currency BTC           # Только BTC
    python3 options_dvol_indicators_loader.py --group rsi              # Только группа RSI
    python3 options_dvol_indicators_loader.py --group macd --currency ETH  # MACD только для ETH
    python3 options_dvol_indicators_loader.py --force-reload           # Полная перезагрузка всех групп

Доступные группы (--group):
    trend, momentum, levels, iv_hv, cross, rsi, bollinger, macd

Автор: Trading System
Дата: 2026-02-16
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
import logging
import argparse
import time
import warnings

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from tqdm import tqdm

from indicators.database import DatabaseConnection

warnings.filterwarnings('ignore')

# =============================================================================
# Константы
# =============================================================================

TABLE_NAME = 'options_deribit_dvol_indicators_1h'
SOURCE_TABLE = 'options_deribit_dvol_1h'
HV_TABLE = 'indicators_bybit_futures_1h'

CURRENCIES = ['BTC', 'ETH']

CURRENCY_SYMBOL_MAP = {
    'BTC': 'BTCUSDT',
    'ETH': 'ETHUSDT',
}

# SQL для создания таблицы
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    timestamp             TIMESTAMPTZ NOT NULL,
    currency              VARCHAR(10) NOT NULL,
    dvol_sma_24           DECIMAL(10, 4),
    dvol_sma_168          DECIMAL(10, 4),
    dvol_ema_12           DECIMAL(10, 4),
    dvol_ema_26           DECIMAL(10, 4),
    dvol_change_24h       DECIMAL(10, 4),
    dvol_change_pct_24h   DECIMAL(10, 4),
    dvol_roc_24h          DECIMAL(10, 4),
    dvol_percentile_30d   DECIMAL(10, 4),
    dvol_percentile_90d   DECIMAL(10, 4),
    dvol_zscore_30d       DECIMAL(10, 4),
    iv_hv_spread_30       DECIMAL(10, 4),
    iv_hv_ratio_30        DECIMAL(10, 4),
    dvol_btc_eth_spread   DECIMAL(10, 4),
    dvol_btc_eth_ratio    DECIMAL(10, 4),
    dvol_btc_eth_corr_24h DECIMAL(10, 4),
    dvol_rsi_14           DECIMAL(10, 4),
    dvol_bb_upper_20_2    DECIMAL(10, 4),
    dvol_bb_lower_20_2    DECIMAL(10, 4),
    dvol_bb_pct_b_20_2    DECIMAL(10, 4),
    dvol_macd_line_12_26      DECIMAL(10, 4),
    dvol_macd_signal_12_26_9  DECIMAL(10, 4),
    dvol_macd_hist_12_26_9    DECIMAL(10, 4),
    PRIMARY KEY (timestamp, currency)
);
"""

CREATE_INDEXES_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_dvol_ind_1h_currency_ts ON {TABLE_NAME} (currency, timestamp);",
    f"CREATE INDEX IF NOT EXISTS idx_dvol_ind_1h_ts ON {TABLE_NAME} (timestamp);",
]


# =============================================================================
# Логирование
# =============================================================================

def setup_logging():
    """Настраивает логирование в консоль и файл"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'dvol_indicators_{timestamp}.log')

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger_inst = logging.getLogger(__name__)
    logger_inst.setLevel(logging.INFO)
    logger_inst.addHandler(file_handler)
    logger_inst.addHandler(console_handler)

    return logger_inst


logger = setup_logging()


# =============================================================================
# Основной класс
# =============================================================================

class DvolIndicatorsLoader:
    """
    Загрузчик индикаторов DVOL с per-group архитектурой.

    Каждая группа индикаторов обрабатывается отдельно:
    1. Проверяется последняя дата для колонок группы
    2. Рассчитываются недостающие данные (полный расчёт, запись только новых)
    3. Запись в БД батчами по 1 день, только колонки этой группы
    """

    # Допустимые имена групп для --group флага
    GROUP_NAMES = ['trend', 'momentum', 'levels', 'iv_hv', 'cross', 'rsi', 'bollinger', 'macd']

    def __init__(self, force_reload: bool = False, currency: str = None, group: str = None):
        self.db = DatabaseConnection()
        self.force_reload = force_reload
        self.currencies = [currency] if currency else CURRENCIES
        self.group_filter = group
        self.indicator_groups = self._define_groups()

    # -------------------------------------------------------------------------
    # Определение групп индикаторов
    # -------------------------------------------------------------------------

    def _define_groups(self) -> list:
        """
        Определяет группы индикаторов.
        Каждая группа обрабатывается и записывается отдельно.
        Добавление нового индикатора = новый элемент в этом списке.
        """
        groups = [
            {
                'key': 'trend',
                'name': 'Trend',
                'columns': ['dvol_sma_24', 'dvol_sma_168', 'dvol_ema_12', 'dvol_ema_26'],
                'calculate': self._calc_trend,
            },
            {
                'key': 'momentum',
                'name': 'Momentum',
                'columns': ['dvol_change_24h', 'dvol_change_pct_24h', 'dvol_roc_24h'],
                'calculate': self._calc_momentum,
            },
            {
                'key': 'levels',
                'name': 'Levels',
                'columns': ['dvol_percentile_30d', 'dvol_percentile_90d', 'dvol_zscore_30d'],
                'calculate': self._calc_levels,
            },
            {
                'key': 'iv_hv',
                'name': 'IV vs HV',
                'columns': ['iv_hv_spread_30', 'iv_hv_ratio_30'],
                'calculate': self._calc_iv_hv,
                'needs_hv': True,
            },
            {
                'key': 'cross',
                'name': 'BTC/ETH Cross',
                'columns': ['dvol_btc_eth_spread', 'dvol_btc_eth_ratio', 'dvol_btc_eth_corr_24h'],
                'calculate': self._calc_cross_currency,
                'cross_currency': True,
            },
            {
                'key': 'rsi',
                'name': 'RSI',
                'columns': ['dvol_rsi_14'],
                'calculate': self._calc_rsi,
            },
            {
                'key': 'bollinger',
                'name': 'Bollinger Bands',
                'columns': ['dvol_bb_upper_20_2', 'dvol_bb_lower_20_2', 'dvol_bb_pct_b_20_2'],
                'calculate': self._calc_bollinger,
            },
            {
                'key': 'macd',
                'name': 'MACD',
                'columns': ['dvol_macd_line_12_26', 'dvol_macd_signal_12_26_9', 'dvol_macd_hist_12_26_9'],
                'calculate': self._calc_macd,
            },
        ]

        # Фильтрация по --group
        if self.group_filter:
            groups = [g for g in groups if g['key'] == self.group_filter]

        return groups

    # -------------------------------------------------------------------------
    # Функции расчёта для каждой группы
    # -------------------------------------------------------------------------

    @staticmethod
    def _calc_trend(close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)
        df['dvol_sma_24'] = close.rolling(24).mean()
        df['dvol_sma_168'] = close.rolling(168).mean()
        df['dvol_ema_12'] = close.ewm(span=12, adjust=False).mean()
        df['dvol_ema_26'] = close.ewm(span=26, adjust=False).mean()
        return df

    @staticmethod
    def _calc_momentum(close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)
        prev = close.shift(24)
        df['dvol_change_24h'] = close - prev
        df['dvol_change_pct_24h'] = ((close - prev) / prev.replace(0, np.nan)) * 100.0
        df['dvol_roc_24h'] = ((close / prev.replace(0, np.nan)) - 1.0) * 100.0
        return df

    @staticmethod
    def _calc_levels(close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)

        # Percentile: какой % значений в окне меньше текущего (0-100)
        df['dvol_percentile_30d'] = close.rolling(720, min_periods=168).apply(
            lambda x: (np.sum(x < x[-1]) / len(x)) * 100.0, raw=True
        )
        df['dvol_percentile_90d'] = close.rolling(2160, min_periods=720).apply(
            lambda x: (np.sum(x < x[-1]) / len(x)) * 100.0, raw=True
        )

        # Z-score: отклонение от средней в стандартных отклонениях
        rolling_mean = close.rolling(720).mean()
        rolling_std = close.rolling(720).std()
        df['dvol_zscore_30d'] = (close - rolling_mean) / rolling_std.replace(0, np.nan)

        return df

    @staticmethod
    def _calc_iv_hv(close: pd.Series, hv_series: pd.Series = None, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)

        if hv_series is not None:
            hv = hv_series.reindex(close.index)
            df['iv_hv_spread_30'] = close - hv
            df['iv_hv_ratio_30'] = close / hv.replace(0, np.nan)
        else:
            df['iv_hv_spread_30'] = np.nan
            df['iv_hv_ratio_30'] = np.nan

        return df

    @staticmethod
    def _calc_cross_currency(close: pd.Series = None, dvol_data: dict = None, **kwargs) -> pd.DataFrame:
        """Рассчитывает cross-currency метрики BTC vs ETH"""
        btc_close = dvol_data['BTC']['close']
        eth_close = dvol_data['ETH']['close']

        common_idx = btc_close.index.intersection(eth_close.index)
        btc = btc_close.loc[common_idx]
        eth = eth_close.loc[common_idx]

        df = pd.DataFrame(index=common_idx)
        df['dvol_btc_eth_spread'] = eth - btc
        df['dvol_btc_eth_ratio'] = eth / btc.replace(0, np.nan)
        df['dvol_btc_eth_corr_24h'] = btc.rolling(24).corr(eth)

        return df

    @staticmethod
    def _calc_rsi_values(closes: np.ndarray, period: int) -> np.ndarray:
        """Рассчитывает RSI методом Wilder smoothing"""
        if len(closes) < period + 1:
            return np.full(len(closes), np.nan)

        closes = np.asarray(closes, dtype=np.float64)
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        rsi_values = np.full(len(closes), np.nan)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            if avg_loss == 0:
                rsi_values[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_values[i + 1] = 100.0 - (100.0 / (1.0 + rs))

        return rsi_values

    @classmethod
    def _calc_rsi(cls, close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)
        df['dvol_rsi_14'] = cls._calc_rsi_values(close.values, 14)
        return df

    @staticmethod
    def _calc_bollinger(close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)
        bb_middle = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        df['dvol_bb_upper_20_2'] = bb_middle + 2.0 * bb_std
        df['dvol_bb_lower_20_2'] = bb_middle - 2.0 * bb_std
        bb_range = df['dvol_bb_upper_20_2'] - df['dvol_bb_lower_20_2']
        df['dvol_bb_pct_b_20_2'] = (close - df['dvol_bb_lower_20_2']) / bb_range.replace(0, np.nan)
        return df

    @staticmethod
    def _calc_macd(close: pd.Series, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame(index=close.index)
        ema_fast = close.ewm(span=12, adjust=False).mean()
        ema_slow = close.ewm(span=26, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        df['dvol_macd_line_12_26'] = macd_line
        df['dvol_macd_signal_12_26_9'] = macd_signal
        df['dvol_macd_hist_12_26_9'] = macd_line - macd_signal
        return df

    # -------------------------------------------------------------------------
    # Таблица и БД
    # -------------------------------------------------------------------------

    def ensure_table(self):
        """Создаёт таблицу и индексы если не существуют"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                for idx_sql in CREATE_INDEXES_SQL:
                    cur.execute(idx_sql)
                conn.commit()
        logger.info(f"Таблица {TABLE_NAME} готова")

    def get_group_last_timestamp(self, currency: str, columns: list):
        """
        Получить последний заполненный timestamp для группы колонок.
        Проверяет ВСЕ колонки группы через AND — если хоть одна NULL,
        эта строка не считается заполненной.
        """
        conditions = ' AND '.join([f'{col} IS NOT NULL' for col in columns])
        query = f"""
            SELECT MAX(timestamp) FROM {TABLE_NAME}
            WHERE currency = %s AND {conditions}
        """
        result = self.db.execute_query(query, (currency,))
        return result[0][0] if result and result[0][0] else None

    @staticmethod
    def build_upsert_sql(columns: list) -> str:
        """Строит UPSERT SQL для конкретной группы колонок"""
        all_cols = ['timestamp', 'currency'] + columns
        placeholders = ', '.join(['%s'] * len(all_cols))
        col_names = ', '.join(all_cols)
        updates = ', '.join([f'{c} = EXCLUDED.{c}' for c in columns])
        return (f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders}) "
                f"ON CONFLICT (timestamp, currency) DO UPDATE SET {updates}")

    # -------------------------------------------------------------------------
    # Загрузка данных
    # -------------------------------------------------------------------------

    def load_dvol_data(self) -> dict:
        """Загружает DVOL close для BTC и ETH (всегда обе валюты)"""
        dvol_data = {}

        for currency in CURRENCIES:
            query = f"""
                SELECT timestamp, close FROM {SOURCE_TABLE}
                WHERE currency = %s ORDER BY timestamp
            """
            rows = self.db.execute_query(query, (currency,))
            if not rows:
                logger.warning(f"  {currency}: нет DVOL данных")
                continue

            df = pd.DataFrame(rows, columns=['timestamp', 'close'])
            df['close'] = df['close'].astype(float)
            df = df.set_index('timestamp')
            dvol_data[currency] = df

            logger.info(f"  {currency}: {len(df)} DVOL свечей "
                        f"({df.index.min()} — {df.index.max()})")

        return dvol_data

    def load_hv_data(self) -> dict:
        """Загружает HV_30 (маппинг BTC→BTCUSDT, ETH→ETHUSDT)"""
        hv_data = {}

        for currency, symbol in CURRENCY_SYMBOL_MAP.items():
            query = f"""
                SELECT timestamp, hv_30 FROM {HV_TABLE}
                WHERE symbol = %s AND hv_30 IS NOT NULL ORDER BY timestamp
            """
            rows = self.db.execute_query(query, (symbol,))
            if not rows:
                logger.warning(f"  {currency} ({symbol}): нет HV_30 данных")
                continue

            df = pd.DataFrame(rows, columns=['timestamp', 'hv_30'])
            df['hv_30'] = df['hv_30'].astype(float)
            df = df.set_index('timestamp')
            hv_data[currency] = df['hv_30']

            logger.info(f"  {currency} ({symbol}): {len(df)} HV_30 значений")

        return hv_data

    # -------------------------------------------------------------------------
    # Запись в БД (per-group, daily batching)
    # -------------------------------------------------------------------------

    def save_group_to_db(self, df: pd.DataFrame, currency: str, group: dict):
        """
        Записывает результаты одной группы индикаторов.
        UPSERT только колонок этой группы — остальные не затрагиваются.
        Коммит после каждого дня.
        """
        if df.empty:
            return

        columns = group['columns']
        upsert_sql = self.build_upsert_sql(columns)

        # Группируем строки по дате
        rows_by_date = {}
        for ts in df.index:
            date = ts.date()
            row_values = [ts, currency]
            for col in columns:
                val = df.loc[ts, col]
                row_values.append(None if pd.isna(val) else round(float(val), 4))

            if date not in rows_by_date:
                rows_by_date[date] = []
            rows_by_date[date].append(tuple(row_values))

        # Запись по дням
        dates = sorted(rows_by_date.keys())
        written = 0

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for date in tqdm(dates, desc=f"    {group['name']}", unit='day'):
                    batch = rows_by_date[date]
                    psycopg2.extras.execute_batch(cur, upsert_sql, batch, page_size=100)
                    conn.commit()
                    written += len(batch)

        logger.info(f"    записано {written} строк ({len(dates)} дней)")

    # -------------------------------------------------------------------------
    # Обработка одной группы
    # -------------------------------------------------------------------------

    def process_group(self, group: dict, currency: str, close: pd.Series,
                      dvol_last_ts, hv_data: dict = None, dvol_data: dict = None):
        """
        Обрабатывает одну группу индикаторов для одной валюты.

        1. Проверяет последнюю заполненную дату
        2. Рассчитывает индикаторы (полный расчёт на всех данных)
        3. Фильтрует только новые строки
        4. Записывает в БД батчами по 1 день
        """
        col_count = len(group['columns'])

        # 1. Проверяем последнюю дату для этой группы
        last_ts = self.get_group_last_timestamp(currency, group['columns'])

        if last_ts and last_ts >= dvol_last_ts and not self.force_reload:
            logger.info(f"  {group['name']} ({col_count} col): данные актуальны")
            return

        if last_ts and not self.force_reload:
            # Считаем количество дней для заполнения
            days_needed = (dvol_last_ts - last_ts).days
            logger.info(f"  {group['name']} ({col_count} col): "
                        f"последние данные {last_ts.strftime('%Y-%m-%d %H:%M')} "
                        f"-> нужно {days_needed} дней")
        else:
            logger.info(f"  {group['name']} ({col_count} col): "
                        f"данных нет -> полная загрузка")

        # 2. Расчёт (всегда на полных данных для корректности EMA/RSI)
        if group.get('cross_currency'):
            result = group['calculate'](dvol_data=dvol_data)
            # Reindex для текущей валюты
            if currency in dvol_data:
                result = result.reindex(dvol_data[currency].index)
        elif group.get('needs_hv'):
            result = group['calculate'](close, hv_series=hv_data.get(currency))
        else:
            result = group['calculate'](close)

        # 3. Фильтруем — только новые строки
        if last_ts and not self.force_reload:
            result = result[result.index > last_ts]

        if result.empty:
            logger.info(f"    нет новых данных")
            return

        # 4. Запись в БД
        self.save_group_to_db(result, currency, group)

    # -------------------------------------------------------------------------
    # Основной запуск
    # -------------------------------------------------------------------------

    def run(self):
        """Основной метод запуска"""
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("DVOL Indicators Loader (Per-Group)")
        logger.info("=" * 60)
        logger.info(f"Валюты: {self.currencies}")
        logger.info(f"Force reload: {self.force_reload}")
        if self.group_filter:
            logger.info(f"Группа: {self.group_filter}")
        logger.info(f"Таблица: {TABLE_NAME}")
        logger.info(f"Групп индикаторов: {len(self.indicator_groups)}")
        logger.info("")

        # 1. Таблица
        self.ensure_table()

        # 2. Загрузка данных (один раз для всех групп)
        logger.info("Загрузка данных...")
        dvol_data = self.load_dvol_data()
        if not dvol_data:
            logger.error("Нет DVOL данных. Завершение.")
            return

        hv_data = self.load_hv_data()
        logger.info("")

        # 3. Обработка per-currency групп
        for currency in self.currencies:
            if currency not in dvol_data:
                logger.warning(f"{currency}: нет данных, пропускаем")
                continue

            close = dvol_data[currency]['close']
            dvol_last_ts = dvol_data[currency].index.max()

            logger.info(f"{'=' * 50}")
            logger.info(f"{currency} (DVOL до {dvol_last_ts.strftime('%Y-%m-%d %H:%M')})")
            logger.info(f"{'=' * 50}")

            for group in self.indicator_groups:
                if group.get('cross_currency'):
                    continue  # Обрабатываем отдельно ниже

                self.process_group(
                    group=group,
                    currency=currency,
                    close=close,
                    dvol_last_ts=dvol_last_ts,
                    hv_data=hv_data,
                )

            logger.info("")

        # 4. Обработка cross-currency групп
        cross_groups = [g for g in self.indicator_groups if g.get('cross_currency')]

        if cross_groups and 'BTC' in dvol_data and 'ETH' in dvol_data:
            logger.info(f"{'=' * 50}")
            logger.info("Cross-currency: BTC/ETH")
            logger.info(f"{'=' * 50}")

            for group in cross_groups:
                for currency in self.currencies:
                    if currency not in dvol_data:
                        continue

                    dvol_last_ts = dvol_data[currency].index.max()
                    close = dvol_data[currency]['close']

                    logger.info(f"  [{currency}]")
                    self.process_group(
                        group=group,
                        currency=currency,
                        close=close,
                        dvol_last_ts=dvol_last_ts,
                        dvol_data=dvol_data,
                    )

            logger.info("")

        # 5. Итого
        elapsed = time.time() - start_time
        logger.info(f"Завершено за {elapsed:.1f} сек")
        logger.info("=" * 60)


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='DVOL Indicators Loader')
    parser.add_argument('--force-reload', action='store_true',
                        help='Пересчитать и перезаписать все данные')
    parser.add_argument('--currency', type=str, choices=['BTC', 'ETH'],
                        help='Обработать только одну валюту')
    parser.add_argument('--group', type=str,
                        choices=DvolIndicatorsLoader.GROUP_NAMES,
                        help='Обработать только одну группу индикаторов')
    return parser.parse_args()


def main():
    args = parse_args()

    loader = DvolIndicatorsLoader(
        force_reload=args.force_reload,
        currency=args.currency,
        group=args.group,
    )
    loader.run()


if __name__ == '__main__':
    main()
