#!/usr/bin/env python3
"""
Options Aggregated Metrics Loader
==================================
Рассчитывает агрегированные метрики из raw снапшотов опционов (15m).
Каждая группа метрик обрабатывается отдельно: проверяется последняя
заполненная дата, рассчитываются только недостающие данные, запись батчами по 1 день.

Источник данных:
  - options_deribit_raw (снапшоты ~1500 контрактов каждые 15 мин)

Выход:
  - options_deribit_aggregated_15m (24 расчётные колонки)

Группы метрик (7 групп, 24 колонки):
  1. Volume/OI (5):   put_call_ratio_volume/oi, total_volume_24h, total_oi, oi_change_pct_24h
  2. IV Metrics (6):  iv_atm_30d, iv_25d_put/call_30d, iv_skew, iv_smile, iv_term_structure
  3. Max Pain (4):    max_pain_nearest/monthly + distance_pct
  4. Greeks (4):      gex, net_delta, net_gamma, vega_exposure
  5. Expiration (2):  days_to_expiry_nearest, notional_expiring_7d
  6. Liquidity (2):   bid_ask_spread_avg_atm, max_oi_strike
  7. Positioning (1): gamma_flip_level

Запуск:
    python3 options_aggregated_loader.py                                # Все группы, BTC + ETH
    python3 options_aggregated_loader.py --currency BTC                 # Только BTC
    python3 options_aggregated_loader.py --group iv                     # Только IV Metrics
    python3 options_aggregated_loader.py --group maxpain --currency ETH # Max Pain для ETH
    python3 options_aggregated_loader.py --force-reload                 # Полная перезагрузка

Доступные группы (--group):
    volume, iv, maxpain, greeks, expiry, liquidity, positioning
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, date
import calendar
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

TABLE_NAME = 'options_deribit_aggregated_15m'
SOURCE_TABLE = 'options_deribit_raw'
CURRENCIES = ['BTC', 'ETH']

NUMERIC_COLS = [
    'mark_iv', 'bid_iv', 'ask_iv', 'delta', 'gamma', 'theta', 'vega', 'rho',
    'open_interest', 'volume_24h', 'volume_usd_24h', 'mark_price', 'last_price',
    'best_bid_price', 'best_ask_price', 'best_bid_amount', 'best_ask_amount',
    'underlying_price', 'index_price', 'strike', 'settlement_price',
    'high_24h', 'low_24h', 'interest_rate',
]


# =============================================================================
# Логирование
# =============================================================================

def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'options_aggregated_{timestamp}.log')

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

class OptionsAggregatedLoader:
    """
    Загрузчик агрегированных опционных метрик с per-group архитектурой.
    """

    GROUP_NAMES = ['volume', 'iv', 'maxpain', 'greeks', 'expiry', 'liquidity', 'positioning']

    def __init__(self, force_reload: bool = False, currency: str = None, group: str = None):
        self.db = DatabaseConnection()
        self.force_reload = force_reload
        self.currencies = [currency] if currency else CURRENCIES
        self.group_filter = group
        self.indicator_groups = self._define_groups()

    # -------------------------------------------------------------------------
    # Определение групп
    # -------------------------------------------------------------------------

    def _define_groups(self) -> list:
        groups = [
            {
                'key': 'volume',
                'name': 'Volume/OI',
                'columns': ['put_call_ratio_volume', 'put_call_ratio_oi',
                            'total_volume_24h', 'total_open_interest', 'oi_change_pct_24h'],
                'calculate': self._calc_volume,
                'lookback_hours': 24,
            },
            {
                'key': 'iv',
                'name': 'IV Metrics',
                'columns': ['iv_atm_30d', 'iv_25d_put_30d', 'iv_25d_call_30d',
                            'iv_skew_25d_30d', 'iv_smile_steepness_30d', 'iv_term_structure_7d_30d'],
                'calculate': self._calc_iv,
            },
            {
                'key': 'maxpain',
                'name': 'Max Pain',
                'columns': ['max_pain_nearest', 'max_pain_nearest_distance_pct',
                            'max_pain_monthly', 'max_pain_monthly_distance_pct'],
                'calculate': self._calc_maxpain,
            },
            {
                'key': 'greeks',
                'name': 'Greeks Exposure',
                'columns': ['gex', 'net_delta', 'net_gamma', 'vega_exposure'],
                'calculate': self._calc_greeks,
            },
            {
                'key': 'expiry',
                'name': 'Expiration',
                'columns': ['days_to_expiry_nearest', 'notional_expiring_7d'],
                'calculate': self._calc_expiry,
            },
            {
                'key': 'liquidity',
                'name': 'Liquidity',
                'columns': ['bid_ask_spread_avg_atm', 'max_oi_strike'],
                'calculate': self._calc_liquidity,
            },
            {
                'key': 'positioning',
                'name': 'Positioning',
                'columns': ['gamma_flip_level'],
                'calculate': self._calc_positioning,
            },
        ]

        if self.group_filter:
            groups = [g for g in groups if g['key'] == self.group_filter]

        return groups

    # =========================================================================
    # Вспомогательные методы для IV расчётов
    # =========================================================================

    @staticmethod
    def _get_atm_iv(contracts_exp):
        """ATM IV для одной экспирации: ближайший страйк к underlying_price"""
        valid = contracts_exp[contracts_exp['mark_iv'] > 0]
        if valid.empty:
            return None

        underlying = valid['underlying_price'].iloc[0]
        distances = (valid['strike'] - underlying).abs()
        closest_idx = distances.nsmallest(2).index
        return float(valid.loc[closest_idx, 'mark_iv'].mean())

    @staticmethod
    def _get_delta_iv(contracts_exp, target_delta):
        """
        IV при заданном delta с линейной интерполяцией.
        target_delta > 0 → calls, target_delta < 0 → puts.
        """
        if target_delta > 0:
            valid = contracts_exp[
                (contracts_exp['option_type'] == 'call') & (contracts_exp['mark_iv'] > 0)
            ].copy()
        else:
            valid = contracts_exp[
                (contracts_exp['option_type'] == 'put') & (contracts_exp['mark_iv'] > 0)
            ].copy()

        if len(valid) < 2:
            return None

        valid = valid.sort_values('delta')
        deltas = valid['delta'].values
        ivs = valid['mark_iv'].values

        # Ищем пару, которая окружает target_delta
        for i in range(len(deltas) - 1):
            d1, d2 = deltas[i], deltas[i + 1]
            if (d1 <= target_delta <= d2) or (d2 <= target_delta <= d1):
                denom = d2 - d1
                if abs(denom) < 1e-10:
                    return float(ivs[i])
                w = (target_delta - d1) / denom
                return float(ivs[i] + w * (ivs[i + 1] - ivs[i]))

        # Не окружает — берём ближайший
        idx = np.argmin(np.abs(deltas - target_delta))
        return float(ivs[idx])

    def _interpolate_to_maturity(self, snapshot, target_days, metric_func):
        """
        Интерполяция метрики к постоянной дюрации (например 30 дней).
        VIX-style: линейная интерполяция между T1 <= target и T2 > target.
        """
        ts = snapshot['timestamp'].iloc[0]
        today = ts.date() if hasattr(ts, 'date') else ts

        expirations = sorted(snapshot['expiration'].unique())
        exp_days = {exp: (exp - today).days for exp in expirations}

        # Только будущие экспирации (>= 1 день)
        exp_days = {k: v for k, v in exp_days.items() if v >= 1}

        if not exp_days:
            return None

        # Ищем T1 (ближайшая <= target) и T2 (ближайшая > target)
        t1_exp, t1_days = None, 0
        t2_exp, t2_days = None, float('inf')

        for exp, days in exp_days.items():
            if days <= target_days and days > t1_days:
                t1_exp, t1_days = exp, days
            if days > target_days and days < t2_days:
                t2_exp, t2_days = exp, days

        # Если не можем интерполировать — берём ближайшую
        if t1_exp is None and t2_exp is None:
            return None
        if t1_exp is None:
            return metric_func(snapshot[snapshot['expiration'] == t2_exp])
        if t2_exp is None:
            return metric_func(snapshot[snapshot['expiration'] == t1_exp])

        v1 = metric_func(snapshot[snapshot['expiration'] == t1_exp])
        v2 = metric_func(snapshot[snapshot['expiration'] == t2_exp])

        if v1 is None and v2 is None:
            return None
        if v1 is None:
            return v2
        if v2 is None:
            return v1

        # Линейная интерполяция
        w = (t2_days - target_days) / (t2_days - t1_days)
        return w * v1 + (1 - w) * v2

    @staticmethod
    def _is_monthly_expiry(exp_date):
        """Проверяет, является ли дата последней пятницей месяца (monthly expiry)"""
        year, month = exp_date.year, exp_date.month
        last_day = calendar.monthrange(year, month)[1]
        d = date(year, month, last_day)
        while d.weekday() != 4:  # 4 = Friday
            d -= timedelta(days=1)
        return exp_date == d

    @staticmethod
    def _calc_max_pain_for_expiry(contracts_exp):
        """Max Pain для одной экспирации: страйк с минимальной суммарной болью"""
        calls = contracts_exp[contracts_exp['option_type'] == 'call']
        puts = contracts_exp[contracts_exp['option_type'] == 'put']

        strikes = sorted(contracts_exp['strike'].unique())
        if not strikes:
            return None

        call_strikes = calls['strike'].values
        call_oi = calls['open_interest'].values
        put_strikes = puts['strike'].values
        put_oi = puts['open_interest'].values

        min_pain = float('inf')
        max_pain_strike = None

        for test_price in strikes:
            call_pain = np.sum(call_oi * np.maximum(0, test_price - call_strikes))
            put_pain = np.sum(put_oi * np.maximum(0, put_strikes - test_price))
            total_pain = call_pain + put_pain

            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = test_price

        return float(max_pain_strike) if max_pain_strike is not None else None

    # =========================================================================
    # Функции расчёта для каждой группы
    # =========================================================================

    def _calc_volume(self, snapshot, prev_snapshot=None, **kwargs):
        """Группа 1: Volume/OI"""
        calls = snapshot[snapshot['option_type'] == 'call']
        puts = snapshot[snapshot['option_type'] == 'put']

        vol_calls = calls['volume_24h'].sum()
        vol_puts = puts['volume_24h'].sum()
        oi_calls = calls['open_interest'].sum()
        oi_puts = puts['open_interest'].sum()
        total_oi = snapshot['open_interest'].sum()

        result = {
            'put_call_ratio_volume': float(vol_puts / vol_calls) if vol_calls > 0 else None,
            'put_call_ratio_oi': float(oi_puts / oi_calls) if oi_calls > 0 else None,
            'total_volume_24h': float(snapshot['volume_24h'].sum()),
            'total_open_interest': float(total_oi),
            'oi_change_pct_24h': None,
        }

        if prev_snapshot is not None and not prev_snapshot.empty:
            prev_total_oi = prev_snapshot['open_interest'].sum()
            if prev_total_oi > 0:
                result['oi_change_pct_24h'] = float(((total_oi - prev_total_oi) / prev_total_oi) * 100)

        return result

    def _calc_iv(self, snapshot, **kwargs):
        """Группа 2: IV Metrics с 30d интерполяцией"""
        # IV ATM 30d
        iv_atm_30d = self._interpolate_to_maturity(snapshot, 30, self._get_atm_iv)

        # IV 25d put/call 30d
        iv_25d_put_30d = self._interpolate_to_maturity(
            snapshot, 30, lambda exp_df: self._get_delta_iv(exp_df, -0.25)
        )
        iv_25d_call_30d = self._interpolate_to_maturity(
            snapshot, 30, lambda exp_df: self._get_delta_iv(exp_df, 0.25)
        )

        # Derived
        iv_skew = None
        iv_smile = None
        if iv_25d_put_30d is not None and iv_25d_call_30d is not None:
            iv_skew = iv_25d_put_30d - iv_25d_call_30d
            if iv_atm_30d is not None:
                iv_smile = (iv_25d_put_30d + iv_25d_call_30d) / 2 - iv_atm_30d

        # IV Term Structure: 7d ATM - 30d ATM
        iv_atm_7d = self._interpolate_to_maturity(snapshot, 7, self._get_atm_iv)
        iv_term = None
        if iv_atm_7d is not None and iv_atm_30d is not None:
            iv_term = iv_atm_7d - iv_atm_30d

        return {
            'iv_atm_30d': iv_atm_30d,
            'iv_25d_put_30d': iv_25d_put_30d,
            'iv_25d_call_30d': iv_25d_call_30d,
            'iv_skew_25d_30d': iv_skew,
            'iv_smile_steepness_30d': iv_smile,
            'iv_term_structure_7d_30d': iv_term,
        }

    def _calc_maxpain(self, snapshot, **kwargs):
        """Группа 3: Max Pain (nearest + monthly)"""
        ts = snapshot['timestamp'].iloc[0]
        today = ts.date() if hasattr(ts, 'date') else ts
        underlying = snapshot['underlying_price'].iloc[0]

        expirations = sorted(snapshot['expiration'].unique())
        future_exps = [e for e in expirations if (e - today).days >= 1]

        result = {
            'max_pain_nearest': None,
            'max_pain_nearest_distance_pct': None,
            'max_pain_monthly': None,
            'max_pain_monthly_distance_pct': None,
        }

        if not future_exps:
            return result

        # Nearest expiry
        nearest_exp = future_exps[0]
        mp_nearest = self._calc_max_pain_for_expiry(
            snapshot[snapshot['expiration'] == nearest_exp]
        )
        if mp_nearest is not None:
            result['max_pain_nearest'] = mp_nearest
            if underlying > 0:
                result['max_pain_nearest_distance_pct'] = \
                    float(((underlying - mp_nearest) / underlying) * 100)

        # Monthly expiry
        monthly_exps = [e for e in future_exps if self._is_monthly_expiry(e)]
        if monthly_exps:
            monthly_exp = monthly_exps[0]
            mp_monthly = self._calc_max_pain_for_expiry(
                snapshot[snapshot['expiration'] == monthly_exp]
            )
            if mp_monthly is not None:
                result['max_pain_monthly'] = mp_monthly
                if underlying > 0:
                    result['max_pain_monthly_distance_pct'] = \
                        float(((underlying - mp_monthly) / underlying) * 100)

        return result

    def _calc_greeks(self, snapshot, **kwargs):
        """Группа 4: Greeks Exposure"""
        S = snapshot['underlying_price'].iloc[0]
        calls = snapshot[snapshot['option_type'] == 'call']
        puts = snapshot[snapshot['option_type'] == 'put']

        # GEX: дилер short options → long gamma от calls, short gamma от puts
        gex_calls = (calls['gamma'] * calls['open_interest'] * S * S * 0.01).sum()
        gex_puts = (puts['gamma'] * puts['open_interest'] * S * S * 0.01).sum()
        gex = float(gex_calls - gex_puts)

        # Net Delta: направление позиций
        net_delta = float(
            (calls['delta'] * calls['open_interest']).sum()
            - (puts['delta'].abs() * puts['open_interest']).sum()
        )

        # Net Gamma
        net_gamma = float(snapshot['gamma'].mul(snapshot['open_interest']).sum())

        # Vega Exposure
        vega_exposure = float(snapshot['vega'].mul(snapshot['open_interest']).sum())

        return {
            'gex': gex,
            'net_delta': net_delta,
            'net_gamma': net_gamma,
            'vega_exposure': vega_exposure,
        }

    def _calc_expiry(self, snapshot, **kwargs):
        """Группа 5: Expiration"""
        ts = snapshot['timestamp'].iloc[0]
        today = ts.date() if hasattr(ts, 'date') else ts

        expirations = sorted(snapshot['expiration'].unique())
        future_exps = [e for e in expirations if (e - today).days >= 1]

        result = {
            'days_to_expiry_nearest': None,
            'notional_expiring_7d': None,
        }

        if future_exps:
            result['days_to_expiry_nearest'] = float((future_exps[0] - today).days)

        # Notional expiring within 7 days
        week_ahead = today + timedelta(days=7)
        expiring_7d = snapshot[
            (snapshot['expiration'] >= today) & (snapshot['expiration'] <= week_ahead)
        ]
        if not expiring_7d.empty:
            result['notional_expiring_7d'] = float(
                (expiring_7d['open_interest'] * expiring_7d['strike']).sum()
            )
        else:
            result['notional_expiring_7d'] = 0.0

        return result

    def _calc_liquidity(self, snapshot, **kwargs):
        """Группа 6: Liquidity"""
        underlying = snapshot['underlying_price'].iloc[0]

        # ATM контракты: strike в пределах ±5% от underlying
        atm_range = underlying * 0.05
        atm = snapshot[
            (snapshot['strike'] >= underlying - atm_range)
            & (snapshot['strike'] <= underlying + atm_range)
            & (snapshot['best_bid_price'] > 0)
            & (snapshot['best_ask_price'] > 0)
        ]

        spread_avg = None
        if not atm.empty:
            spreads = atm['best_ask_price'] - atm['best_bid_price']
            spread_avg = float(spreads.mean())

        # Max OI strike
        oi_by_strike = snapshot.groupby('strike')['open_interest'].sum()
        max_oi_strike = None
        if not oi_by_strike.empty:
            max_oi_strike = float(oi_by_strike.idxmax())

        return {
            'bid_ask_spread_avg_atm': spread_avg,
            'max_oi_strike': max_oi_strike,
        }

    def _calc_positioning(self, snapshot, **kwargs):
        """Группа 7: Gamma Flip Level — цена где net GEX = 0"""
        calls = snapshot[snapshot['option_type'] == 'call']
        puts = snapshot[snapshot['option_type'] == 'put']

        strikes = sorted(snapshot['strike'].unique())
        if len(strikes) < 2:
            return {'gamma_flip_level': None}

        call_gamma = calls.set_index('strike')['gamma'].mul(
            calls.set_index('strike')['open_interest']
        )
        put_gamma = puts.set_index('strike')['gamma'].mul(
            puts.set_index('strike')['open_interest']
        )

        # Агрегируем gamma × OI по страйкам
        call_gex_by_strike = call_gamma.groupby(level=0).sum()
        put_gex_by_strike = put_gamma.groupby(level=0).sum()

        prev_net_gex = None
        prev_price = None

        for test_price in strikes:
            # GEX при данном уровне цены
            gex_c = (call_gex_by_strike * test_price * test_price * 0.01).sum()
            gex_p = (put_gex_by_strike * test_price * test_price * 0.01).sum()
            net_gex = float(gex_c - gex_p)

            if prev_net_gex is not None and prev_net_gex * net_gex < 0:
                # Смена знака — линейная интерполяция
                denom = abs(net_gex - prev_net_gex)
                if denom < 1e-20:
                    return {'gamma_flip_level': float(test_price)}
                w = abs(prev_net_gex) / denom
                flip = prev_price + w * (test_price - prev_price)
                return {'gamma_flip_level': float(flip)}

            prev_net_gex = net_gex
            prev_price = test_price

        return {'gamma_flip_level': None}

    # =========================================================================
    # БД операции
    # =========================================================================

    def ensure_table(self):
        """Проверяет что таблица существует"""
        check_sql = """
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s LIMIT 1
        """
        result = self.db.execute_query(check_sql, (TABLE_NAME,))
        if not result:
            logger.error(f"Таблица {TABLE_NAME} не существует. "
                         f"Создайте вручную через sudo -u postgres psql.")
            sys.exit(1)
        logger.info(f"Таблица {TABLE_NAME} готова")

    def get_group_last_timestamp(self, currency: str, columns: list):
        """Последний заполненный timestamp для группы (все колонки NOT NULL)"""
        conditions = ' AND '.join([f'{col} IS NOT NULL' for col in columns])
        query = f"""
            SELECT MAX(timestamp) FROM {TABLE_NAME}
            WHERE currency = %s AND {conditions}
        """
        result = self.db.execute_query(query, (currency,))
        return result[0][0] if result and result[0][0] else None

    @staticmethod
    def build_upsert_sql(columns: list) -> str:
        """UPSERT SQL для конкретной группы колонок"""
        all_cols = ['timestamp', 'currency'] + columns
        placeholders = ', '.join(['%s'] * len(all_cols))
        col_names = ', '.join(all_cols)
        updates = ', '.join([f'{c} = EXCLUDED.{c}' for c in columns])
        return (f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders}) "
                f"ON CONFLICT (timestamp, currency) DO UPDATE SET {updates}")

    def save_group_to_db(self, results: list, currency: str, group: dict):
        """Записывает результаты группы батчами по 1 день"""
        if not results:
            return

        columns = group['columns']
        upsert_sql = self.build_upsert_sql(columns)

        rows_by_date = {}
        for row in results:
            ts = row['timestamp']
            d = ts.date()
            values = [ts, currency]
            for col in columns:
                val = row.get(col)
                values.append(None if val is None or (isinstance(val, float) and np.isnan(val))
                              else round(float(val), 4))

            if d not in rows_by_date:
                rows_by_date[d] = []
            rows_by_date[d].append(tuple(values))

        dates = sorted(rows_by_date.keys())
        written = 0

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for d in tqdm(dates, desc=f"    {group['name']}", unit='day'):
                    batch = rows_by_date[d]
                    psycopg2.extras.execute_batch(cur, upsert_sql, batch, page_size=100)
                    conn.commit()
                    written += len(batch)

        logger.info(f"    записано {written} строк ({len(dates)} дней)")

    # =========================================================================
    # Загрузка данных
    # =========================================================================

    def load_raw_data(self, currency: str, start_ts, end_ts=None):
        """Загружает raw данные из options_deribit_raw"""
        if end_ts:
            query = f"""
                SELECT * FROM {SOURCE_TABLE}
                WHERE currency = %s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp
            """
            rows = self.db.execute_query(query, (currency, start_ts, end_ts))
        else:
            query = f"""
                SELECT * FROM {SOURCE_TABLE}
                WHERE currency = %s AND timestamp >= %s
                ORDER BY timestamp
            """
            rows = self.db.execute_query(query, (currency, start_ts))

        if not rows:
            return pd.DataFrame()

        # Получаем имена колонок
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {SOURCE_TABLE} LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=col_names)

        # Конвертируем Decimal → float
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

        return df

    def get_raw_date_range(self, currency: str):
        """Возвращает min/max даты в raw таблице"""
        query = f"""
            SELECT MIN(timestamp)::date, MAX(timestamp)::date
            FROM {SOURCE_TABLE} WHERE currency = %s
        """
        result = self.db.execute_query(query, (currency,))
        if result and result[0][0]:
            return result[0][0], result[0][1]
        return None, None

    # =========================================================================
    # Обработка одной группы
    # =========================================================================

    def process_group(self, group: dict, currency: str):
        """
        Обрабатывает одну группу для одной валюты:
        1. Проверяет последнюю дату
        2. Загружает raw данные по дням
        3. Рассчитывает метрики для каждого снапшота
        4. Записывает батчами по 1 день
        """
        col_count = len(group['columns'])

        # 1. Проверяем последнюю дату
        last_ts = self.get_group_last_timestamp(currency, group['columns'])
        raw_min, raw_max = self.get_raw_date_range(currency)

        if raw_min is None:
            logger.info(f"  {group['name']} ({col_count} col): нет raw данных")
            return

        if last_ts and not self.force_reload:
            # Проверяем актуальность
            raw_max_ts_query = f"""
                SELECT MAX(timestamp) FROM {SOURCE_TABLE} WHERE currency = %s
            """
            raw_max_ts_result = self.db.execute_query(raw_max_ts_query, (currency,))
            raw_max_ts = raw_max_ts_result[0][0] if raw_max_ts_result and raw_max_ts_result[0][0] else None

            if raw_max_ts and last_ts >= raw_max_ts:
                logger.info(f"  {group['name']} ({col_count} col): данные актуальны")
                return

            start_date = last_ts.date()
            days_needed = (raw_max - start_date).days
            logger.info(f"  {group['name']} ({col_count} col): "
                        f"последние данные {last_ts.strftime('%Y-%m-%d %H:%M')} "
                        f"-> нужно {days_needed} дней")
        else:
            start_date = raw_min
            logger.info(f"  {group['name']} ({col_count} col): "
                        f"данных нет -> полная загрузка с {raw_min}")

        # 2. Обработка по дням
        lookback_hours = group.get('lookback_hours', 0)
        current_date = start_date
        end_date = raw_max + timedelta(days=1)
        all_results = []

        dates_list = []
        d = current_date
        while d <= raw_max:
            dates_list.append(d)
            d += timedelta(days=1)

        for day in tqdm(dates_list, desc=f"    {group['name']} calc", unit='day'):
            # Загружаем raw за день + lookback
            load_start = datetime.combine(day, datetime.min.time())
            if lookback_hours:
                load_start -= timedelta(hours=lookback_hours)
            load_end = datetime.combine(day + timedelta(days=1), datetime.min.time())

            raw_df = self.load_raw_data(currency, load_start, load_end)
            if raw_df.empty:
                continue

            # Снапшоты за текущий день
            day_start = datetime.combine(day, datetime.min.time())
            day_end = datetime.combine(day + timedelta(days=1), datetime.min.time())

            day_timestamps = sorted(
                raw_df[(raw_df['timestamp'] >= pd.Timestamp(day_start, tz='UTC'))
                       & (raw_df['timestamp'] < pd.Timestamp(day_end, tz='UTC'))]['timestamp'].unique()
            )

            for ts in day_timestamps:
                # Пропускаем уже обработанные
                if last_ts and ts <= last_ts and not self.force_reload:
                    continue

                snapshot = raw_df[raw_df['timestamp'] == ts]
                if snapshot.empty:
                    continue

                # Подготавливаем kwargs
                calc_kwargs = {}
                if lookback_hours:
                    target_prev = ts - timedelta(hours=24)
                    prev_candidates = raw_df[
                        (raw_df['timestamp'] <= target_prev)
                    ]['timestamp'].unique()
                    if len(prev_candidates) > 0:
                        prev_ts = max(prev_candidates)
                        calc_kwargs['prev_snapshot'] = raw_df[raw_df['timestamp'] == prev_ts]

                try:
                    row = group['calculate'](snapshot, **calc_kwargs)
                    row['timestamp'] = ts
                    all_results.append(row)
                except Exception as e:
                    logger.warning(f"    Ошибка расчёта {group['name']} для {ts}: {e}")
                    continue

        # 3. Запись
        if all_results:
            self.save_group_to_db(all_results, currency, group)
        else:
            logger.info(f"    нет новых данных")

    # =========================================================================
    # Основной запуск
    # =========================================================================

    def run(self):
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("Options Aggregated Metrics Loader (Per-Group)")
        logger.info("=" * 60)
        logger.info(f"Валюты: {self.currencies}")
        logger.info(f"Force reload: {self.force_reload}")
        if self.group_filter:
            logger.info(f"Группа: {self.group_filter}")
        logger.info(f"Таблица: {TABLE_NAME}")
        logger.info(f"Групп метрик: {len(self.indicator_groups)}")
        logger.info("")

        # 1. Таблица
        self.ensure_table()

        # 2. Обработка per-currency
        for currency in self.currencies:
            logger.info(f"{'=' * 50}")
            logger.info(f"{currency}")
            logger.info(f"{'=' * 50}")

            for group in self.indicator_groups:
                self.process_group(group, currency)

            logger.info("")

        # 3. Итого
        elapsed = time.time() - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        logger.info(f"Завершено за {minutes}m {seconds}s")
        logger.info("=" * 60)


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='Options Aggregated Metrics Loader')
    parser.add_argument('--force-reload', action='store_true',
                        help='Пересчитать и перезаписать все данные')
    parser.add_argument('--currency', type=str, choices=['BTC', 'ETH'],
                        help='Обработать только одну валюту')
    parser.add_argument('--group', type=str,
                        choices=OptionsAggregatedLoader.GROUP_NAMES,
                        help='Обработать только одну группу метрик')
    return parser.parse_args()


def main():
    args = parse_args()

    loader = OptionsAggregatedLoader(
        force_reload=args.force_reload,
        currency=args.currency,
        group=args.group,
    )
    loader.run()


if __name__ == '__main__':
    main()
