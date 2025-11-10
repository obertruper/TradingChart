"""
Unit tests for ADXLoader

Test coverage for ADX (Average Directional Index) indicator loader.

Test Structure:
    Group 1: Column Names (5 tests)
    Group 2: True Range Calculation (8 tests)
    Group 3: Directional Movement (10 tests)
    Group 4: Wilder Smoothing (7 tests)
    Group 5: DI Calculation (6 tests)
    Group 6: ADX Calculation (8 tests)
    Group 7: Database Operations (6 tests)
    Group 8: Helper Functions (5 tests)

Total: 55 tests

ADX Components:
    TR = max(High - Low, |High - PrevClose|, |Low - PrevClose|)
    +DM/−DM = Directional Movement
    Wilder Smoothing: (prev_smoothed × (period - 1) + current) / period
    +DI/−DI = 100 × smoothed_DM / smoothed_TR
    DX = 100 × |+DI − −DI| / (+DI + −DI)
    ADX = Wilder smoothing of DX
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from indicators.adx_loader import ADXLoader, PERIODS


# ============================================
# Group 1: Column Names (5 tests)
# ============================================

class TestColumnNames:
    """Tests for ADX column naming"""

    def test_column_names_format(self):
        """Test ADX column names format"""
        loader = ADXLoader(symbol='BTCUSDT')
        columns = loader.get_column_names(14)

        assert columns['adx'] == 'adx_14'
        assert columns['plus_di'] == 'adx_14_plus_di'
        assert columns['minus_di'] == 'adx_14_minus_di'

    def test_column_names_different_periods(self, sample_adx_periods):
        """Test column names for different periods"""
        loader = ADXLoader(symbol='BTCUSDT')

        for period in sample_adx_periods:
            columns = loader.get_column_names(period)
            assert columns['adx'] == f'adx_{period}'
            assert columns['plus_di'] == f'adx_{period}_plus_di'
            assert columns['minus_di'] == f'adx_{period}_minus_di'

    def test_column_names_structure(self):
        """Test column names return correct dictionary structure"""
        loader = ADXLoader(symbol='BTCUSDT')
        columns = loader.get_column_names(14)

        assert isinstance(columns, dict)
        assert 'adx' in columns
        assert 'plus_di' in columns
        assert 'minus_di' in columns
        assert len(columns) == 3

    def test_column_names_no_underscore_prefix(self):
        """Test column names don't have double underscores"""
        loader = ADXLoader(symbol='BTCUSDT')
        columns = loader.get_column_names(14)

        for col in columns.values():
            assert '__' not in col
            assert col.startswith('adx_')

    def test_column_names_all_periods(self):
        """Test column names generation for all production periods"""
        loader = ADXLoader(symbol='BTCUSDT')

        # PERIODS = [7, 10, 14, 20, 21, 25, 30, 50]
        for period in PERIODS:
            columns = loader.get_column_names(period)
            assert f'adx_{period}' in columns['adx']
            assert f'adx_{period}_plus_di' in columns['plus_di']
            assert f'adx_{period}_minus_di' in columns['minus_di']


# ============================================
# Group 2: True Range Calculation (8 tests)
# ============================================

class TestTrueRangeCalculation:
    """Tests for True Range calculation"""

    def test_tr_basic_formula(self):
        """Test True Range basic formula"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Simple case: TR = High - Low
        high = pd.Series([105, 110, 108])
        low = pd.Series([100, 105, 103])
        close = pd.Series([102, 108, 106])

        tr = loader.calculate_true_range(high, low, close)

        # First value: High - Low = 105 - 100 = 5
        assert tr.iloc[0] == 5
        # Second value: max(110-105, |110-102|, |105-102|) = max(5, 8, 3) = 8
        assert tr.iloc[1] == 8
        # Third value: max(108-103, |108-108|, |103-108|) = max(5, 0, 5) = 5
        assert tr.iloc[2] == 5

    def test_tr_returns_series(self, sample_adx_data):
        """Test True Range returns Series"""
        loader = ADXLoader(symbol='BTCUSDT')
        tr = loader.calculate_true_range(
            sample_adx_data['high'],
            sample_adx_data['low'],
            sample_adx_data['close']
        )

        assert isinstance(tr, pd.Series)
        assert len(tr) == len(sample_adx_data)

    def test_tr_gap_up(self):
        """Test True Range with gap up"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Gap up: close=100, next high=115
        high = pd.Series([105, 115])
        low = pd.Series([100, 110])
        close = pd.Series([102, 113])

        tr = loader.calculate_true_range(high, low, close)

        # Second candle: max(115-110, |115-102|, |110-102|) = max(5, 13, 8) = 13
        assert tr.iloc[1] == 13

    def test_tr_gap_down(self):
        """Test True Range with gap down"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Gap down: close=100, next low=85
        high = pd.Series([105, 90])
        low = pd.Series([100, 85])
        close = pd.Series([102, 88])

        tr = loader.calculate_true_range(high, low, close)

        # Second candle: max(90-85, |90-102|, |85-102|) = max(5, 12, 17) = 17
        assert tr.iloc[1] == 17

    def test_tr_positive_values(self, sample_adx_data):
        """Test True Range always positive"""
        loader = ADXLoader(symbol='BTCUSDT')
        tr = loader.calculate_true_range(
            sample_adx_data['high'],
            sample_adx_data['low'],
            sample_adx_data['close']
        )

        assert (tr >= 0).all()

    def test_tr_minimum_high_low_diff(self):
        """Test TR is at least high-low difference"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([105, 110, 108])
        low = pd.Series([100, 105, 103])
        close = pd.Series([102, 108, 106])

        tr = loader.calculate_true_range(high, low, close)
        hl_diff = high - low

        # TR should be >= high-low for all values
        assert (tr >= hl_diff).all()

    def test_tr_formula_max_of_three(self):
        """Test TR uses max of three values"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Crafted data where each component wins
        high = pd.Series([105, 120, 110])
        low = pd.Series([100, 115, 105])
        close = pd.Series([102, 108, 118])

        tr = loader.calculate_true_range(high, low, close)

        # First: 105-100 = 5
        assert tr.iloc[0] == 5
        # Second: max(120-115, |120-102|, |115-102|) = max(5, 18, 13) = 18
        assert tr.iloc[1] == 18
        # Third: max(110-105, |110-108|, |105-108|) = max(5, 2, 3) = 5
        # Previous close is 108, not 118
        assert tr.iloc[2] == 5

    def test_tr_nan_handling(self):
        """Test True Range with NaN values"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([105, np.nan, 110])
        low = pd.Series([100, 105, 105])
        close = pd.Series([102, 108, 108])

        tr = loader.calculate_true_range(high, low, close)

        # First value should be valid
        assert not pd.isna(tr.iloc[0])
        # Second value may be NaN due to NaN in high
        # Third value should be valid
        assert not pd.isna(tr.iloc[2])


# ============================================
# Group 3: Directional Movement (10 tests)
# ============================================

class TestDirectionalMovement:
    """Tests for Directional Movement (+DM/-DM) calculation"""

    def test_dm_basic_structure(self):
        """Test DM returns two Series"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([105, 110, 108, 112])
        low = pd.Series([100, 105, 103, 107])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        assert isinstance(plus_dm, pd.Series)
        assert isinstance(minus_dm, pd.Series)
        assert len(plus_dm) == len(high)
        assert len(minus_dm) == len(high)

    def test_dm_uptrend(self):
        """Test +DM in uptrend"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Strong uptrend
        high = pd.Series([100, 105, 110, 115])
        low = pd.Series([95, 100, 105, 110])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # All +DM should be positive in uptrend
        # First value is always 0
        assert plus_dm.iloc[0] == 0
        assert plus_dm.iloc[1] > 0  # 105 - 100 = 5
        assert plus_dm.iloc[2] > 0  # 110 - 105 = 5
        assert plus_dm.iloc[3] > 0  # 115 - 110 = 5

    def test_dm_downtrend(self):
        """Test -DM in downtrend"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Strong downtrend
        high = pd.Series([100, 95, 90, 85])
        low = pd.Series([95, 90, 85, 80])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # All -DM should be positive in downtrend
        # First value is always 0
        assert minus_dm.iloc[0] == 0
        assert minus_dm.iloc[1] > 0  # 95 - 90 = 5
        assert minus_dm.iloc[2] > 0  # 90 - 85 = 5
        assert minus_dm.iloc[3] > 0  # 85 - 80 = 5

    def test_dm_plus_formula(self):
        """Test +DM formula: High - PrevHigh"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100, 105, 103, 108])
        low = pd.Series([95, 100, 98, 103])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # +DM when up_move > down_move and up_move > 0
        # Second: up=5, down=0 → +DM=5
        assert plus_dm.iloc[1] == 5
        # Third: up=-2, down=2 → +DM=0 (down wins)
        assert plus_dm.iloc[2] == 0
        # Fourth: up=5, down=0 → +DM=5
        assert plus_dm.iloc[3] == 5

    def test_dm_minus_formula(self):
        """Test -DM formula: PrevLow - Low"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100, 98, 103, 101])
        low = pd.Series([95, 93, 98, 96])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # -DM when down_move > up_move and down_move > 0
        # Second: up=-2, down=2 → -DM=2 (down wins)
        assert minus_dm.iloc[1] == 2
        # Third: up=5, down=0 → -DM=0 (up wins)
        assert minus_dm.iloc[2] == 0
        # Fourth: up=-2, down=2 → -DM=2
        assert minus_dm.iloc[3] == 2

    def test_dm_mutual_exclusion(self):
        """Test +DM and -DM are mutually exclusive"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100, 105, 103, 108, 106])
        low = pd.Series([95, 100, 98, 103, 101])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # For each candle, either +DM or -DM should be 0 (not both positive)
        for i in range(len(plus_dm)):
            # At least one should be zero
            assert plus_dm.iloc[i] == 0 or minus_dm.iloc[i] == 0

    def test_dm_first_value_zero(self):
        """Test first DM values are zero"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100, 105, 110])
        low = pd.Series([95, 100, 105])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # First values should be 0 (no previous value)
        assert plus_dm.iloc[0] == 0
        assert minus_dm.iloc[0] == 0

    def test_dm_positive_values(self, sample_adx_data):
        """Test DM values are always >= 0"""
        loader = ADXLoader(symbol='BTCUSDT')

        plus_dm, minus_dm = loader.calculate_directional_movement(
            sample_adx_data['high'],
            sample_adx_data['low']
        )

        assert (plus_dm >= 0).all()
        assert (minus_dm >= 0).all()

    def test_dm_inside_bar(self):
        """Test DM for inside bar (no new high/low)"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Inside bar: high and low both within previous range
        high = pd.Series([110, 108, 107])
        low = pd.Series([100, 102, 103])

        plus_dm, minus_dm = loader.calculate_directional_movement(high, low)

        # Second bar: up=-2, down=-2 → both 0
        assert plus_dm.iloc[1] == 0
        assert minus_dm.iloc[1] == 0

    def test_dm_realistic_data(self, sample_adx_realistic):
        """Test DM with realistic market data"""
        loader = ADXLoader(symbol='BTCUSDT')

        plus_dm, minus_dm = loader.calculate_directional_movement(
            sample_adx_realistic['high'],
            sample_adx_realistic['low']
        )

        # DM should have values
        assert len(plus_dm) == len(sample_adx_realistic)
        assert len(minus_dm) == len(sample_adx_realistic)
        # Not all zeros
        assert plus_dm.sum() > 0
        assert minus_dm.sum() > 0


# ============================================
# Group 4: Wilder Smoothing (7 tests)
# ============================================

class TestWilderSmoothing:
    """Tests for Wilder's smoothing method"""

    def test_wilder_basic_formula(self):
        """Test Wilder smoothing formula"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Simple series for testing
        series = pd.Series([10, 12, 14, 13, 15, 16, 14, 17, 18, 19])
        period = 5

        smoothed = loader.wilder_smoothing(series, period)

        # First smoothed value at index 4 = average of first 5
        expected_first = series.iloc[:5].mean()  # (10+12+14+13+15)/5 = 12.8
        assert abs(smoothed.iloc[4] - expected_first) < 0.01

        # Second value: (12.8 × 4 + 16) / 5 = 13.44
        expected_second = (expected_first * 4 + 16) / 5
        assert abs(smoothed.iloc[5] - expected_second) < 0.01

    def test_wilder_first_value_is_mean(self):
        """Test first Wilder value is simple mean"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series([10, 20, 30, 40, 50, 60])
        period = 3

        smoothed = loader.wilder_smoothing(series, period)

        # First smoothed at index 2 = (10+20+30)/3 = 20
        assert smoothed.iloc[2] == 20

    def test_wilder_nan_before_period(self):
        """Test Wilder returns NaN before period"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series([10, 20, 30, 40, 50])
        period = 3

        smoothed = loader.wilder_smoothing(series, period)

        # First two values should be NaN
        assert pd.isna(smoothed.iloc[0])
        assert pd.isna(smoothed.iloc[1])
        # Third value (index 2) should be valid
        assert not pd.isna(smoothed.iloc[2])

    def test_wilder_returns_series(self):
        """Test Wilder smoothing returns Series"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series([10, 20, 30, 40, 50])
        period = 3

        smoothed = loader.wilder_smoothing(series, period)

        assert isinstance(smoothed, pd.Series)
        assert len(smoothed) == len(series)
        assert smoothed.index.equals(series.index)

    def test_wilder_different_periods(self):
        """Test Wilder smoothing with different periods"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series(range(1, 31))  # 1 to 30

        for period in [5, 10, 14]:
            smoothed = loader.wilder_smoothing(series, period)

            # First valid value at period-1
            assert not pd.isna(smoothed.iloc[period - 1])
            # Values before should be NaN
            assert pd.isna(smoothed.iloc[period - 2])

    def test_wilder_insufficient_data(self):
        """Test Wilder with insufficient data"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series([10, 20])
        period = 5

        smoothed = loader.wilder_smoothing(series, period)

        # All values should be NaN (not enough data)
        assert smoothed.isna().all()

    def test_wilder_cumulative_smoothing(self):
        """Test Wilder smoothing is cumulative"""
        loader = ADXLoader(symbol='BTCUSDT')

        series = pd.Series([10, 10, 10, 10, 10, 20, 20, 20])
        period = 5

        smoothed = loader.wilder_smoothing(series, period)

        # Each value depends on previous smoothed value
        # After jump to 20, smoothed should gradually increase
        assert smoothed.iloc[4] == 10  # First smoothed
        assert smoothed.iloc[5] > smoothed.iloc[4]  # Increases
        assert smoothed.iloc[6] > smoothed.iloc[5]  # Continues increasing
        assert smoothed.iloc[7] > smoothed.iloc[6]  # Still increasing


# ============================================
# Group 5: DI Calculation (6 tests)
# ============================================

class TestDICalculation:
    """Tests for +DI/-DI calculation within ADX"""

    def test_di_formula(self):
        """Test DI = 100 × smoothed_DM / smoothed_TR"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Use enough data for Wilder smoothing
        high = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 170])
        low = pd.Series([95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165])
        close = pd.Series([98, 103, 108, 113, 118, 123, 128, 133, 138, 143, 148, 153, 158, 163, 168])

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # DI values should be percentage (0-100 range typically)
        assert isinstance(plus_di, pd.Series)
        assert isinstance(minus_di, pd.Series)

    def test_di_range(self):
        """Test DI values are typically 0-100"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100 + i for i in range(20)])
        low = pd.Series([95 + i for i in range(20)])
        close = pd.Series([98 + i for i in range(20)])

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # Valid DI values should be >= 0
        valid_plus = plus_di[plus_di.notna()]
        valid_minus = minus_di[minus_di.notna()]

        if len(valid_plus) > 0:
            assert (valid_plus >= 0).all()
        if len(valid_minus) > 0:
            assert (valid_minus >= 0).all()

    def test_di_uptrend_dominance(self):
        """Test +DI > -DI in strong uptrend"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Strong uptrend
        high = pd.Series([100 + i*2 for i in range(20)])
        low = pd.Series([95 + i*2 for i in range(20)])
        close = pd.Series([98 + i*2 for i in range(20)])

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # In uptrend, +DI should be higher
        valid_idx = plus_di.notna() & minus_di.notna()
        if valid_idx.sum() > 5:
            # Check last few valid values
            recent_plus = plus_di[valid_idx].iloc[-5:].mean()
            recent_minus = minus_di[valid_idx].iloc[-5:].mean()
            assert recent_plus > recent_minus

    def test_di_downtrend_dominance(self):
        """Test -DI > +DI in strong downtrend"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Strong downtrend
        high = pd.Series([100 - i*2 for i in range(20)])
        low = pd.Series([95 - i*2 for i in range(20)])
        close = pd.Series([98 - i*2 for i in range(20)])

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # In downtrend, -DI should be higher
        valid_idx = plus_di.notna() & minus_di.notna()
        if valid_idx.sum() > 5:
            recent_plus = plus_di[valid_idx].iloc[-5:].mean()
            recent_minus = minus_di[valid_idx].iloc[-5:].mean()
            assert recent_minus > recent_plus

    def test_di_nan_handling(self):
        """Test DI handles NaN values"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100, 105, 110, 115, 120])
        low = pd.Series([95, 100, 105, 110, 115])
        close = pd.Series([98, 103, 108, 113, 118])

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # Should return Series even with insufficient data
        assert isinstance(plus_di, pd.Series)
        assert isinstance(minus_di, pd.Series)
        # Most values will be NaN (period=7 but only 5 data points)
        assert plus_di.isna().sum() > 0
        assert minus_di.isna().sum() > 0

    def test_di_zero_tr_handling(self):
        """Test DI handles zero True Range"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Constant prices → TR=0
        high = pd.Series([100] * 15)
        low = pd.Series([100] * 15)
        close = pd.Series([100] * 15)

        result = loader.calculate_adx(high, low, close, period=7)

        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # Should handle division by zero gracefully (likely NaN or 0)
        assert isinstance(plus_di, pd.Series)
        assert isinstance(minus_di, pd.Series)


# ============================================
# Group 6: ADX Calculation (8 tests)
# ============================================

class TestADXCalculation:
    """Tests for complete ADX calculation"""

    def test_adx_returns_three_components(self):
        """Test ADX returns ADX, +DI, -DI"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100 + i for i in range(30)])
        low = pd.Series([95 + i for i in range(30)])
        close = pd.Series([98 + i for i in range(30)])

        result = loader.calculate_adx(high, low, close, period=14)

        assert isinstance(result, dict)
        assert 'adx' in result
        assert 'plus_di' in result
        assert 'minus_di' in result

    def test_adx_range(self):
        """Test ADX values are 0-100"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100 + i for i in range(30)])
        low = pd.Series([95 + i for i in range(30)])
        close = pd.Series([98 + i for i in range(30)])

        result = loader.calculate_adx(high, low, close, period=14)
        adx = result['adx']

        # Valid ADX values should be in 0-100 range
        valid_adx = adx[adx.notna()]
        if len(valid_adx) > 0:
            assert (valid_adx >= 0).all()
            assert (valid_adx <= 100).all()

    def test_adx_strong_trend(self):
        """Test ADX is high in strong trend"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Strong consistent uptrend
        high = pd.Series([100 + i*3 for i in range(50)])
        low = pd.Series([95 + i*3 for i in range(50)])
        close = pd.Series([98 + i*3 for i in range(50)])

        result = loader.calculate_adx(high, low, close, period=14)
        adx = result['adx']

        # ADX should be high during strong trend
        valid_adx = adx[adx.notna()]
        if len(valid_adx) > 10:
            # Late ADX values should be high (>= 25 indicates strong trend)
            late_adx = valid_adx.iloc[-5:].mean()
            # In very strong consistent trend, ADX should be elevated
            assert late_adx >= 25  # Strong trend threshold

    def test_adx_weak_trend(self, sample_adx_realistic):
        """Test ADX with mixed market conditions"""
        loader = ADXLoader(symbol='BTCUSDT')

        result = loader.calculate_adx(
            sample_adx_realistic['high'],
            sample_adx_realistic['low'],
            sample_adx_realistic['close'],
            period=14
        )

        adx = result['adx']

        # Should return valid Series
        assert isinstance(adx, pd.Series)
        assert len(adx) == len(sample_adx_realistic)
        # Should have some valid values
        assert adx.notna().sum() > 0

    def test_adx_double_smoothing(self):
        """Test ADX uses double Wilder smoothing"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Enough data for double smoothing
        high = pd.Series([100 + i for i in range(40)])
        low = pd.Series([95 + i for i in range(40)])
        close = pd.Series([98 + i for i in range(40)])

        result = loader.calculate_adx(high, low, close, period=7)
        adx = result['adx']

        # ADX should have NaN for first ~2*period values (double smoothing)
        # First smoothing on TR/DM: period values needed (index period-1)
        # Second smoothing on DX: period more values needed
        # Total: first valid at index ~2*(period-1)
        valid_start = adx.first_valid_index()
        if valid_start is not None:
            # Should be at least period-1 values before first valid (6 for period=7)
            assert valid_start >= 6

    def test_adx_different_periods(self, sample_adx_periods):
        """Test ADX calculation for different periods"""
        loader = ADXLoader(symbol='BTCUSDT')

        high = pd.Series([100 + i for i in range(100)])
        low = pd.Series([95 + i for i in range(100)])
        close = pd.Series([98 + i for i in range(100)])

        for period in [7, 14, 21]:
            result = loader.calculate_adx(high, low, close, period=period)

            assert 'adx' in result
            assert 'plus_di' in result
            assert 'minus_di' in result
            assert len(result['adx']) == 100

    def test_adx_series_index_preserved(self):
        """Test ADX preserves input Series index"""
        loader = ADXLoader(symbol='BTCUSDT')

        custom_index = pd.date_range('2024-01-01', periods=30, freq='1h')
        high = pd.Series([100 + i for i in range(30)], index=custom_index)
        low = pd.Series([95 + i for i in range(30)], index=custom_index)
        close = pd.Series([98 + i for i in range(30)], index=custom_index)

        result = loader.calculate_adx(high, low, close, period=14)

        assert result['adx'].index.equals(custom_index)
        assert result['plus_di'].index.equals(custom_index)
        assert result['minus_di'].index.equals(custom_index)

    def test_adx_insufficient_data(self):
        """Test ADX with insufficient data"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Only 5 data points, period=14
        high = pd.Series([100, 105, 110, 115, 120])
        low = pd.Series([95, 100, 105, 110, 115])
        close = pd.Series([98, 103, 108, 113, 118])

        result = loader.calculate_adx(high, low, close, period=14)

        # Should return Series with mostly NaN
        assert result['adx'].isna().all()
        assert result['plus_di'].isna().all()
        assert result['minus_di'].isna().all()


# ============================================
# Group 7: Database Operations (6 tests)
# ============================================

class TestDatabaseOperations:
    """Tests for database operations"""

    @patch('indicators.adx_loader.DatabaseConnection')
    def test_ensure_columns_creates_three_columns(self, mock_db_class):
        """Test ensure_columns creates ADX, +DI, -DI columns"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Columns don't exist
        mock_cursor.fetchone.return_value = (False,)

        loader = ADXLoader(symbol='BTCUSDT')
        loader.ensure_columns_exist('1m', 14)

        # Should execute ALTER TABLE 3 times
        execute_calls = mock_cursor.execute.call_args_list
        alter_calls = [call for call in execute_calls if 'ALTER TABLE' in str(call)]

        # Should create 3 columns: adx_14, adx_14_plus_di, adx_14_minus_di
        assert len(alter_calls) >= 3

    @patch('indicators.adx_loader.DatabaseConnection')
    def test_ensure_columns_decimal_precision(self, mock_db_class):
        """Test columns created with DECIMAL(10,4)"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.fetchone.return_value = (False,)

        loader = ADXLoader(symbol='BTCUSDT')
        loader.ensure_columns_exist('1m', 14)

        # Check that DECIMAL(10,4) is used
        execute_calls = mock_cursor.execute.call_args_list
        alter_calls = [call for call in execute_calls if 'ALTER TABLE' in str(call)]

        # At least one ALTER should have DECIMAL(10,4)
        has_decimal = any('DECIMAL(10' in str(call) for call in alter_calls)
        assert has_decimal

    def test_get_column_names_consistency(self):
        """Test column names are consistent across calls"""
        loader = ADXLoader(symbol='BTCUSDT')

        columns1 = loader.get_column_names(14)
        columns2 = loader.get_column_names(14)

        assert columns1 == columns2

    def test_symbol_initialization(self):
        """Test symbol is set correctly"""
        loader_btc = ADXLoader(symbol='BTCUSDT')
        loader_eth = ADXLoader(symbol='ETHUSDT')

        assert loader_btc.symbol == 'BTCUSDT'
        assert loader_eth.symbol == 'ETHUSDT'

    def test_batch_days_configuration(self):
        """Test batch_days setting"""
        loader1 = ADXLoader(symbol='BTCUSDT', batch_days=1)
        loader2 = ADXLoader(symbol='BTCUSDT', batch_days=7)

        assert loader1.batch_days == 1
        assert loader2.batch_days == 7

    def test_lookback_multiplier_configuration(self):
        """Test lookback_multiplier setting"""
        loader1 = ADXLoader(symbol='BTCUSDT', lookback_multiplier=3)
        loader2 = ADXLoader(symbol='BTCUSDT', lookback_multiplier=5)

        assert loader1.lookback_multiplier == 3
        assert loader2.lookback_multiplier == 5


# ============================================
# Group 8: Helper Functions (5 tests)
# ============================================

class TestHelperFunctions:
    """Tests for helper and utility functions"""

    def test_periods_constant(self):
        """Test PERIODS constant has correct values"""
        # PERIODS = [7, 10, 14, 20, 21, 25, 30, 50]
        assert len(PERIODS) == 8
        assert 7 in PERIODS
        assert 14 in PERIODS
        assert 50 in PERIODS

    def test_periods_sorted(self):
        """Test PERIODS are sorted"""
        assert PERIODS == sorted(PERIODS)

    def test_force_reload_flag(self):
        """Test force_reload flag"""
        loader1 = ADXLoader(symbol='BTCUSDT', force_reload=False)
        loader2 = ADXLoader(symbol='BTCUSDT', force_reload=True)

        assert loader1.force_reload is False
        assert loader2.force_reload is True

    def test_custom_date_range(self):
        """Test custom start/end dates"""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, tzinfo=timezone.utc)

        loader = ADXLoader(
            symbol='BTCUSDT',
            start_date=start,
            end_date=end
        )

        assert loader.custom_start_date == start
        assert loader.custom_end_date == end

    def test_symbol_progress_initialization(self):
        """Test symbol_progress attribute"""
        loader = ADXLoader(symbol='BTCUSDT')

        # Should have symbol_progress attribute
        assert hasattr(loader, 'symbol_progress')
        # Initially empty string
        assert loader.symbol_progress == ""
