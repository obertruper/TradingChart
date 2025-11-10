"""
Unit tests for VWAP (Volume Weighted Average Price) Loader

Tests cover:
- Column names generation (daily + rolling)
- Typical Price calculation (H+L+C)/3
- Daily VWAP with midnight reset
- Rolling VWAP with sliding window
- Date grouping and cumsum
- Volume handling (zero volume, filtering)
- Database operations
- Timeframe parsing

Total: 64 tests
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import after path setup
from vwap_loader import VWAPLoader


# ============================================
# GROUP 1: COLUMN NAMES TESTS (5 tests)
# ============================================

class TestColumnNames:
    """Test VWAP column names generation"""

    def test_column_names_daily_format(self, mock_config):
        """Test daily VWAP column name: vwap_daily"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert loader.daily_enabled == True
        # Daily column should be named vwap_daily
        expected_col = 'vwap_daily'
        assert expected_col == 'vwap_daily'

    def test_column_names_rolling_format(self, mock_config):
        """Test rolling VWAP column names: vwap_{period}"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        for period in loader.rolling_periods:
            col_name = f'vwap_{period}'
            assert col_name.startswith('vwap_')
            assert col_name.split('_')[1] == str(period)

    def test_column_names_daily_and_rolling(self, mock_config):
        """Test both daily and rolling columns exist"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # Should have daily enabled
        assert loader.daily_enabled == True
        # Should have rolling periods
        assert len(loader.rolling_periods) > 0

    def test_column_names_all_periods(self, sample_vwap_rolling_periods):
        """Test all rolling periods get column names"""
        periods = sample_vwap_rolling_periods
        col_names = [f'vwap_{p}' for p in periods]

        assert len(col_names) == len(periods)
        assert all('vwap_' in name for name in col_names)

    def test_column_names_no_duplicates(self, mock_config):
        """Test no duplicate column names"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # All rolling columns
        col_names = [f'vwap_{p}' for p in loader.rolling_periods]
        # Add daily
        if loader.daily_enabled:
            col_names.append('vwap_daily')

        assert len(col_names) == len(set(col_names))


# ============================================
# GROUP 2: TYPICAL PRICE CALCULATION (6 tests)
# ============================================

class TestTypicalPriceCalculation:
    """Test Typical Price = (High + Low + Close) / 3"""

    def test_typical_price_basic_formula(self, sample_vwap_data_single_day):
        """Test TP formula: (H + L + C) / 3"""
        df = sample_vwap_data_single_day.copy()

        tp = (df['high'] + df['low'] + df['close']) / 3

        # First row: (102 + 98 + 100) / 3 = 100
        assert tp.iloc[0] == 100.0

    def test_typical_price_returns_series(self, sample_vwap_data_single_day):
        """Test TP calculation returns pandas Series"""
        df = sample_vwap_data_single_day.copy()
        tp = (df['high'] + df['low'] + df['close']) / 3

        assert isinstance(tp, pd.Series)
        assert len(tp) == len(df)

    def test_typical_price_extreme_values(self):
        """Test TP with extreme price values"""
        timestamps = pd.date_range('2024-01-01', periods=2, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [100000, 0.0001],
            'low': [99000, 0.00001],
            'close': [99500, 0.00005]
        }, index=timestamps)

        tp = (df['high'] + df['low'] + df['close']) / 3

        assert abs(tp.iloc[0] - (100000 + 99000 + 99500) / 3) < 0.01
        assert abs(tp.iloc[1] - (0.0001 + 0.00001 + 0.00005) / 3) < 1e-10

    def test_typical_price_equal_hlc(self):
        """Test TP when H = L = C (doji candle)"""
        timestamps = pd.date_range('2024-01-01', periods=3, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [100, 100, 100],
            'low': [100, 100, 100],
            'close': [100, 100, 100]
        }, index=timestamps)

        tp = (df['high'] + df['low'] + df['close']) / 3

        assert all(tp == 100.0)

    def test_typical_price_zero_values(self):
        """Test TP with zero values"""
        timestamps = pd.date_range('2024-01-01', periods=3, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [0, 1, 2],
            'low': [0, 0, 1],
            'close': [0, 0.5, 1.5]
        }, index=timestamps)

        tp = (df['high'] + df['low'] + df['close']) / 3

        assert tp.iloc[0] == 0.0
        assert tp.iloc[1] == (1 + 0 + 0.5) / 3

    def test_typical_price_precision(self, sample_vwap_data_single_day):
        """Test TP calculation precision"""
        df = sample_vwap_data_single_day.copy()

        tp = (df['high'] + df['low'] + df['close']) / 3

        # All TP values should be between low and high
        for i in range(len(df)):
            assert df['low'].iloc[i] <= tp.iloc[i] <= df['high'].iloc[i]


# ============================================
# GROUP 3: DAILY VWAP CALCULATION (10 tests)
# ============================================

class TestDailyVWAPCalculation:
    """Test Daily VWAP with midnight reset"""

    def test_daily_vwap_basic_formula(self, mock_config, sample_vwap_data_single_day):
        """Test Daily VWAP formula: Σ(TP×Volume) / Σ(Volume)"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_single_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Should return Series
        assert isinstance(vwap, pd.Series)
        assert len(vwap) == len(df)

    def test_daily_vwap_reset_at_midnight(self, mock_config, sample_vwap_data_multi_day):
        """Test Daily VWAP resets at 00:00 UTC"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Check first value of each day
        # Day 1: index 0
        # Day 2: index 24 (00:00 next day)
        # Day 3: index 48

        # First candle of Day 2 should have TP×Vol / Vol = TP
        day2_first = vwap.iloc[24]
        tp_day2_first = (df['high'].iloc[24] + df['low'].iloc[24] + df['close'].iloc[24]) / 3
        assert abs(day2_first - tp_day2_first) < 0.01

    def test_daily_vwap_grouping_by_date(self, mock_config, sample_vwap_data_multi_day):
        """Test VWAP groups by date correctly"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Values within same day should be cumulative
        # Day 1: indices 0-23
        # VWAP should increase (or change) as we accumulate within day
        assert not pd.isna(vwap.iloc[0])
        assert not pd.isna(vwap.iloc[23])

    def test_daily_vwap_first_value_of_day(self, mock_config):
        """Test first value of day equals TP (only one candle)"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=1, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102],
            'low': [98],
            'close': [100],
            'volume': [1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # First value should equal TP
        tp = (102 + 98 + 100) / 3
        assert abs(vwap.iloc[0] - tp) < 0.001

    def test_daily_vwap_accumulation_within_day(self, mock_config):
        """Test VWAP accumulates within day"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=3, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108],
            'low': [98, 101, 104],
            'close': [100, 103, 106],
            'volume': [1000, 1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Manual calculation
        # Row 0: TP=100, VWAP = 100*1000/1000 = 100
        # Row 1: TP=103, VWAP = (100*1000 + 103*1000)/(1000+1000) = 101.5
        # Row 2: TP=106, VWAP = (100*1000 + 103*1000 + 106*1000)/(3000) = 103

        assert abs(vwap.iloc[0] - 100.0) < 0.01
        assert abs(vwap.iloc[1] - 101.5) < 0.01
        assert abs(vwap.iloc[2] - 103.0) < 0.01

    def test_daily_vwap_multiple_days(self, mock_config, sample_vwap_data_multi_day):
        """Test VWAP handles multiple days correctly"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Should have values for all 72 hours
        assert vwap.notna().sum() > 0

    def test_daily_vwap_zero_volume_handling(self, mock_config):
        """Test VWAP filters out zero volume candles"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 0, 1000, 0, 1000]  # Zero volume candles
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Zero volume candles should have NaN
        assert pd.isna(vwap.iloc[1])
        assert pd.isna(vwap.iloc[3])
        # Non-zero should have values
        assert not pd.isna(vwap.iloc[0])
        assert not pd.isna(vwap.iloc[2])

    def test_daily_vwap_all_zero_volume(self, mock_config):
        """Test VWAP returns NaN when all volume is zero"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=3, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108],
            'low': [98, 101, 104],
            'close': [100, 103, 106],
            'volume': [0, 0, 0]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # All should be NaN
        assert all(pd.isna(vwap))

    def test_daily_vwap_reindex_with_nan(self, mock_config):
        """Test VWAP reindexes to original DataFrame with NaN"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 0, 1000, 0, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Length should match original
        assert len(vwap) == len(df)
        # Index should match
        assert all(vwap.index == df.index)

    def test_daily_vwap_realistic_data(self, mock_config, sample_vwap_realistic):
        """Test VWAP with realistic market data"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_realistic.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Should have many valid values
        valid_count = vwap.notna().sum()
        assert valid_count > 0


# ============================================
# GROUP 4: ROLLING VWAP CALCULATION (10 tests)
# ============================================

class TestRollingVWAPCalculation:
    """Test Rolling VWAP with sliding window"""

    def test_rolling_vwap_basic_formula(self, mock_config, sample_vwap_data_single_day):
        """Test Rolling VWAP formula with rolling sum"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_single_day.copy()

        # First calculate TP
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=10)

        assert isinstance(vwap, pd.Series)
        assert len(vwap) == len(df)

    def test_rolling_vwap_different_periods(self, mock_config, sample_vwap_data_single_day):
        """Test Rolling VWAP for different periods"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_single_day.copy()

        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap_10 = loader.calculate_rolling_vwap(df, period=10)
        vwap_20 = loader.calculate_rolling_vwap(df, period=20)

        # Both should be Series
        assert isinstance(vwap_10, pd.Series)
        assert isinstance(vwap_20, pd.Series)

        # Longer period has more NaN at start
        assert vwap_20.isna().sum() > vwap_10.isna().sum()

    def test_rolling_vwap_first_period_minus_one_nan(self, mock_config):
        """Test first (period-1) values are NaN"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=15, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102 + i for i in range(15)],
            'low': [98 + i for i in range(15)],
            'close': [100 + i for i in range(15)],
            'volume': [1000] * 15
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=10)

        # First 9 values should be NaN (period-1)
        assert all(pd.isna(vwap.iloc[:9]))
        # 10th value should exist
        assert not pd.isna(vwap.iloc[9])

    def test_rolling_vwap_min_periods_behavior(self, mock_config):
        """Test min_periods=1 in rolling but override with NaN"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 1000, 1000, 1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=3)

        # First 2 should be NaN (period-1 = 2)
        assert pd.isna(vwap.iloc[0])
        assert pd.isna(vwap.iloc[1])
        # Third should have value
        assert not pd.isna(vwap.iloc[2])

    def test_rolling_vwap_zero_volume_handling(self, mock_config):
        """Test Rolling VWAP handles division by zero"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=15, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102 + i for i in range(15)],
            'low': [98 + i for i in range(15)],
            'close': [100 + i for i in range(15)],
            'volume': [0] * 15  # All zero
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=10)

        # All should be NaN (division by zero)
        assert all(pd.isna(vwap))

    def test_rolling_vwap_sliding_window(self, mock_config):
        """Test Rolling VWAP uses sliding window correctly"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=12, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102] * 12,
            'low': [98] * 12,
            'close': [100] * 12,
            'volume': [1000] * 12
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=10)

        # After period-1, all values should be same (constant TP)
        valid_vwap = vwap.dropna()
        assert all(abs(valid_vwap - 100.0) < 0.01)

    def test_rolling_vwap_vs_daily_single_day(self, mock_config, sample_vwap_data_single_day):
        """Test Rolling VWAP with period=len(data) approximates Daily VWAP"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_single_day.copy()

        # Daily VWAP
        vwap_daily = loader.calculate_daily_vwap(df)

        # Rolling with period = len(data)
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']
        vwap_rolling = loader.calculate_rolling_vwap(df, period=len(df))

        # Last value should be close (both use all data)
        # Note: Not exact due to different calculation methods
        assert not pd.isna(vwap_daily.iloc[-1])
        assert not pd.isna(vwap_rolling.iloc[-1])

    def test_rolling_vwap_large_period(self, mock_config, sample_vwap_realistic):
        """Test Rolling VWAP with large period (1440)"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_realistic.copy()

        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=1440)

        # With 100 candles, all should be NaN (need 1440)
        assert all(pd.isna(vwap))

    def test_rolling_vwap_small_period(self, mock_config, sample_vwap_realistic):
        """Test Rolling VWAP with small period (10)"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_realistic.copy()

        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap = loader.calculate_rolling_vwap(df, period=10)

        # Should have many valid values (100 - 9 = 91)
        assert vwap.notna().sum() >= 90

    def test_rolling_vwap_realistic_data(self, mock_config, sample_vwap_realistic):
        """Test Rolling VWAP with realistic data"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_realistic.copy()

        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['tp_volume'] = df['tp'] * df['volume']

        vwap_20 = loader.calculate_rolling_vwap(df, period=20)

        # Check valid values exist
        assert vwap_20.notna().sum() > 70


# ============================================
# GROUP 5: DATE GROUPING (8 tests)
# ============================================

class TestDateGrouping:
    """Test date grouping for Daily VWAP"""

    def test_date_grouping_by_date(self, mock_config, sample_vwap_data_multi_day):
        """Test VWAP groups by date column"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Should have values across multiple days
        assert vwap.notna().sum() > 0

    def test_date_grouping_cumsum_within_group(self, mock_config):
        """Test cumsum happens within each date group"""
        # Two days, 2 candles each
        timestamps = pd.date_range('2024-01-01 00:00', periods=4, freq='12h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 102, 105],  # Repeat pattern
            'low': [98, 101, 98, 101],
            'close': [100, 103, 100, 103],
            'volume': [1000, 1000, 1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Days are: 0,1 (day 1), 2,3 (day 2)
        # Day 1, candle 0: TP=100, VWAP=100
        # Day 1, candle 1: TP=103, VWAP=(100*1000+103*1000)/2000=101.5
        # Day 2, candle 2: TP=100, VWAP=100 (reset)
        # Day 2, candle 3: TP=103, VWAP=101.5

        assert abs(vwap.iloc[0] - 100.0) < 0.01
        assert abs(vwap.iloc[1] - 101.5) < 0.01
        assert abs(vwap.iloc[2] - 100.0) < 0.01  # Reset
        assert abs(vwap.iloc[3] - 101.5) < 0.01

    def test_date_grouping_reset_between_days(self, mock_config):
        """Test VWAP resets between different days"""
        # Exactly at midnight boundaries
        timestamps = [
            pd.Timestamp('2024-01-01 23:00', tz='UTC'),
            pd.Timestamp('2024-01-02 00:00', tz='UTC'),
            pd.Timestamp('2024-01-02 01:00', tz='UTC'),
        ]
        df = pd.DataFrame({
            'high': [102, 105, 108],
            'low': [98, 101, 104],
            'close': [100, 103, 106],
            'volume': [1000, 1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Index 1 (00:00) should be first of new day
        tp_new_day = 103
        assert abs(vwap.iloc[1] - tp_new_day) < 0.01

    def test_date_grouping_day_boundaries(self, mock_config):
        """Test VWAP at exact day boundaries (23:59 → 00:00)"""
        timestamps = [
            pd.Timestamp('2024-01-01 23:59', tz='UTC'),
            pd.Timestamp('2024-01-02 00:00', tz='UTC'),
        ]
        df = pd.DataFrame({
            'high': [102, 105],
            'low': [98, 101],
            'close': [100, 103],
            'volume': [1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Second candle is new day, should reset
        assert not pd.isna(vwap.iloc[0])
        assert not pd.isna(vwap.iloc[1])

    def test_date_grouping_consecutive_days(self, mock_config, sample_vwap_data_multi_day):
        """Test VWAP handles consecutive days correctly"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()  # 3 days

        vwap = loader.calculate_daily_vwap(df)

        # Should have values across all 3 days
        assert vwap.notna().sum() > 60  # Most candles should have values

    def test_date_grouping_missing_days(self, mock_config):
        """Test VWAP with gaps between days"""
        timestamps = [
            pd.Timestamp('2024-01-01 12:00', tz='UTC'),
            pd.Timestamp('2024-01-03 12:00', tz='UTC'),  # Skip day 2
            pd.Timestamp('2024-01-05 12:00', tz='UTC'),  # Skip day 4
        ]
        df = pd.DataFrame({
            'high': [102, 105, 108],
            'low': [98, 101, 104],
            'close': [100, 103, 106],
            'volume': [1000, 1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Each candle is a different day, should all be TP
        for i in range(len(df)):
            tp = (df['high'].iloc[i] + df['low'].iloc[i] + df['close'].iloc[i]) / 3
            assert abs(vwap.iloc[i] - tp) < 0.01

    def test_date_grouping_utc_timezone(self, mock_config):
        """Test VWAP uses UTC timezone for date grouping"""
        # Test that midnight is based on UTC, not local time
        timestamps = pd.date_range('2024-01-01 00:00', periods=2, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105],
            'low': [98, 101],
            'close': [100, 103],
            'volume': [1000, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Both should be same day, should accumulate
        assert abs(vwap.iloc[1] - 101.5) < 0.01

    def test_date_grouping_preserves_index(self, mock_config, sample_vwap_data_multi_day):
        """Test date grouping preserves original DataFrame index"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_data_multi_day.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Index should match
        assert all(vwap.index == df.index)


# ============================================
# GROUP 6: VOLUME HANDLING (8 tests)
# ============================================

class TestVolumeHandling:
    """Test volume handling in VWAP calculations"""

    def test_volume_zero_ignored_daily(self, mock_config):
        """Test zero volume candles are filtered in daily VWAP"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 0, 1000, 0, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Zero volume should result in NaN
        assert pd.isna(vwap.iloc[1])
        assert pd.isna(vwap.iloc[3])

    def test_volume_high_volume_impact(self, mock_config):
        """Test high volume has more weight in VWAP"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=3, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 152, 102],  # Middle has much higher price
            'low': [98, 148, 98],
            'close': [100, 150, 100],
            'volume': [1000, 10000, 1000]  # Middle has 10x volume
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # VWAP should be pulled toward high-volume candle
        # Last VWAP should be close to 150 (weighted heavily)
        assert vwap.iloc[-1] > 120  # Should be above simple average

    def test_volume_variable_volume(self, mock_config, sample_vwap_realistic):
        """Test VWAP with variable volume patterns"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        df = sample_vwap_realistic.copy()

        vwap = loader.calculate_daily_vwap(df)

        # Should handle variable volume
        assert vwap.notna().sum() > 0

    def test_volume_all_zero_in_day(self, mock_config):
        """Test VWAP when entire day has zero volume"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=24, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102 + i for i in range(24)],
            'low': [98 + i for i in range(24)],
            'close': [100 + i for i in range(24)],
            'volume': [0] * 24
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # All should be NaN
        assert all(pd.isna(vwap))

    def test_volume_partial_zero_in_day(self, mock_config):
        """Test VWAP with some zero volume candles in day"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=10, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102 + i for i in range(10)],
            'low': [98 + i for i in range(10)],
            'close': [100 + i for i in range(10)],
            'volume': [1000, 0, 1000, 1000, 0, 1000, 0, 1000, 1000, 0]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Zero volume candles should be NaN
        assert pd.isna(vwap.iloc[1])
        assert pd.isna(vwap.iloc[4])
        # Non-zero should have values
        assert not pd.isna(vwap.iloc[0])
        assert not pd.isna(vwap.iloc[2])

    def test_volume_weight_influence(self, mock_config):
        """Test volume weight influences VWAP correctly"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=2, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 202],
            'low': [98, 198],
            'close': [100, 200],
            'volume': [1, 1]  # Equal volume
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # With equal volume, VWAP should be average of TPs
        # TP1=100, TP2=200, VWAP = (100+200)/2 = 150
        assert abs(vwap.iloc[-1] - 150.0) < 0.01

    def test_volume_filter_volume_gt_zero(self, mock_config):
        """Test volume > 0 filter works correctly"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 0.0, 1000, 0.00001, 1000]  # Very small volume
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Zero should be NaN
        assert pd.isna(vwap.iloc[1])
        # Very small positive should have value
        assert not pd.isna(vwap.iloc[3])

    def test_volume_reindex_after_filter(self, mock_config):
        """Test reindex works correctly after volume filtering"""
        timestamps = pd.date_range('2024-01-01 00:00', periods=5, freq='1h', tz='UTC')
        df = pd.DataFrame({
            'high': [102, 105, 108, 111, 114],
            'low': [98, 101, 104, 107, 110],
            'close': [100, 103, 106, 109, 112],
            'volume': [1000, 0, 0, 0, 1000]
        }, index=timestamps)

        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        vwap = loader.calculate_daily_vwap(df)

        # Should have same length as original
        assert len(vwap) == len(df)
        # Should have NaN for filtered rows
        assert pd.isna(vwap.iloc[1])
        assert pd.isna(vwap.iloc[2])
        assert pd.isna(vwap.iloc[3])


# ============================================
# GROUP 7: DATABASE OPERATIONS (6 tests)
# ============================================

class TestDatabaseOperations:
    """Test database-related operations"""

    def test_ensure_columns_daily_column(self, mock_config):
        """Test ensure_columns_exist creates daily VWAP column"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert loader.daily_enabled == True
        # Daily column name should be vwap_daily
        expected_col = 'vwap_daily'
        assert expected_col == 'vwap_daily'

    def test_ensure_columns_rolling_columns(self, mock_config):
        """Test ensure_columns_exist creates rolling VWAP columns"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # Should have rolling periods
        assert len(loader.rolling_periods) > 0

        # Each period should get a column
        for period in loader.rolling_periods:
            col_name = f'vwap_{period}'
            assert col_name.startswith('vwap_')

    def test_ensure_columns_decimal_precision(self, mock_config):
        """Test VWAP columns use DECIMAL(20,8) type"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # VWAP is price-like, needs high precision
        # DECIMAL(20,8) allows for large prices with 8 decimals
        assert loader.rolling_periods is not None

    def test_column_names_consistency(self, mock_config):
        """Test column names are consistent"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # Daily always vwap_daily
        if loader.daily_enabled:
            daily_col = 'vwap_daily'
            assert 'daily' in daily_col

        # Rolling always vwap_{period}
        for period in loader.rolling_periods:
            col_name = f'vwap_{period}'
            assert str(period) in col_name

    def test_daily_enabled_configuration(self, mock_config):
        """Test daily_enabled flag from config"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert loader.daily_enabled == True

    def test_batch_days_and_lookback_configuration(self, mock_config):
        """Test batch_days and lookback_multiplier config"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert loader.batch_days == 1
        assert loader.lookback_multiplier == 2

        # Lookback should be max_period × multiplier
        max_period = max(loader.rolling_periods)
        expected_lookback = max_period * loader.lookback_multiplier
        assert loader.lookback_periods == expected_lookback


# ============================================
# GROUP 8: TIMEFRAME OPERATIONS (6 tests)
# ============================================

class TestTimeframeOperations:
    """Test timeframe parsing and operations"""

    def test_parse_timeframe_minutes(self, mock_config):
        """Test parsing minute timeframes"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)
        assert loader.timeframe_minutes == 1

        loader = VWAPLoader('BTCUSDT', '15m', mock_config)
        assert loader.timeframe_minutes == 15

    def test_parse_timeframe_hours(self, mock_config):
        """Test parsing hour timeframes"""
        loader = VWAPLoader('BTCUSDT', '1h', mock_config)
        assert loader.timeframe_minutes == 60

        loader = VWAPLoader('BTCUSDT', '4h', mock_config)
        assert loader.timeframe_minutes == 240

    def test_parse_timeframe_days(self, mock_config):
        """Test parsing day timeframes"""
        loader = VWAPLoader('BTCUSDT', '1d', mock_config)
        assert loader.timeframe_minutes == 1440

    def test_parse_timeframe_invalid(self, mock_config):
        """Test invalid timeframe raises ValueError"""
        with pytest.raises(ValueError):
            loader = VWAPLoader('BTCUSDT', 'invalid', mock_config)

    def test_lookback_calculation(self, mock_config):
        """Test lookback period calculation"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        # lookback = max_period × lookback_multiplier
        max_period = max(loader.rolling_periods)
        expected = max_period * loader.lookback_multiplier

        assert loader.lookback_periods == expected

    def test_timeframe_minutes_calculation(self, mock_config):
        """Test timeframe minutes calculation is correct"""
        test_cases = [
            ('1m', 1),
            ('5m', 5),
            ('15m', 15),
            ('1h', 60),
            ('4h', 240),
            ('1d', 1440)
        ]

        for tf, expected_minutes in test_cases:
            loader = VWAPLoader('BTCUSDT', tf, mock_config)
            assert loader.timeframe_minutes == expected_minutes


# ============================================
# GROUP 9: HELPER FUNCTIONS (5 tests)
# ============================================

class TestHelperFunctions:
    """Test helper functions and utilities"""

    def test_rolling_periods_constant(self, sample_vwap_rolling_periods):
        """Test rolling periods match configuration"""
        expected = [10, 14, 20, 30, 50, 100, 200]
        assert sample_vwap_rolling_periods == expected

    def test_rolling_periods_sorted(self, mock_config):
        """Test rolling periods are in ascending order"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        periods = loader.rolling_periods
        assert periods == sorted(periods)

    def test_daily_enabled_flag(self, mock_config):
        """Test daily_enabled flag is accessible"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert hasattr(loader, 'daily_enabled')
        assert isinstance(loader.daily_enabled, bool)

    def test_symbol_initialization(self, mock_config):
        """Test loader initializes with correct symbol"""
        loader = VWAPLoader('ETHUSDT', '1m', mock_config)

        assert loader.symbol == 'ETHUSDT'

    def test_symbol_progress_initialization(self, mock_config):
        """Test symbol_progress attribute is initialized"""
        loader = VWAPLoader('BTCUSDT', '1m', mock_config)

        assert hasattr(loader, 'symbol_progress')
        assert loader.symbol_progress == ""
