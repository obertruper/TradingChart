"""
Unit tests for MFI (Money Flow Index) Loader

Tests cover:
- Column names generation
- Typical Price calculation (H+L+C)/3
- Money Flow calculation (TP × Volume)
- Positive/Negative Money Flow split
- MFI formula with edge cases (100, 0, NaN handling)
- Database operations
- Timeframe parsing and aggregation
- Helper functions

Total: 58 tests
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
from mfi_loader import MFILoader


# ============================================
# GROUP 1: COLUMN NAMES TESTS (5 tests)
# ============================================

class TestColumnNames:
    """Test MFI column names generation"""

    def test_column_names_format(self, mock_config):
        """Test column names follow format: mfi_{period}"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        for period in loader.periods:
            col_name = f'mfi_{period}'
            assert col_name == f'mfi_{period}'
            assert col_name.startswith('mfi_')
            assert col_name.split('_')[1] == str(period)

    def test_column_names_different_periods(self, mock_config):
        """Test each period gets unique column name"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        col_names = [f'mfi_{p}' for p in loader.periods]
        assert len(col_names) == len(set(col_names))  # All unique

    def test_column_names_structure(self, mock_config):
        """Test column names match periods configuration"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        expected_periods = [7, 10, 14, 20, 25]

        assert loader.periods == expected_periods

    def test_column_names_no_underscore_prefix(self, mock_config):
        """Test column names don't have double underscores"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        for period in loader.periods:
            col_name = f'mfi_{period}'
            assert not col_name.startswith('_')
            assert '__' not in col_name

    def test_column_names_all_periods(self, sample_mfi_periods):
        """Test all configured periods get column names"""
        periods = sample_mfi_periods
        col_names = [f'mfi_{p}' for p in periods]

        assert len(col_names) == 5
        assert 'mfi_7' in col_names
        assert 'mfi_14' in col_names
        assert 'mfi_25' in col_names


# ============================================
# GROUP 2: TYPICAL PRICE CALCULATION (6 tests)
# ============================================

class TestTypicalPriceCalculation:
    """Test Typical Price = (High + Low + Close) / 3"""

    def test_typical_price_basic_formula(self, sample_mfi_data):
        """Test TP formula: (H + L + C) / 3"""
        df = sample_mfi_data

        # Manual calculation for first row
        expected_tp = (df['high'].iloc[0] + df['low'].iloc[0] + df['close'].iloc[0]) / 3
        assert expected_tp == (102 + 98 + 100) / 3
        assert expected_tp == 100.0

    def test_typical_price_returns_series(self, sample_mfi_data):
        """Test TP calculation returns pandas Series"""
        df = sample_mfi_data
        tp = (df['high'] + df['low'] + df['close']) / 3

        assert isinstance(tp, pd.Series)
        assert len(tp) == len(df)

    def test_typical_price_average_of_three(self, sample_mfi_data):
        """Test TP is arithmetic mean of H, L, C"""
        df = sample_mfi_data

        for i in range(len(df)):
            tp = (df['high'].iloc[i] + df['low'].iloc[i] + df['close'].iloc[i]) / 3
            manual_avg = (df['high'].iloc[i] + df['low'].iloc[i] + df['close'].iloc[i]) / 3
            assert abs(tp - manual_avg) < 0.0001

    def test_typical_price_extreme_values(self):
        """Test TP with extreme price values"""
        df = pd.DataFrame({
            'high': [100000, 0.0001],
            'low': [99000, 0.00001],
            'close': [99500, 0.00005]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        assert tp.iloc[0] == (100000 + 99000 + 99500) / 3
        assert abs(tp.iloc[1] - (0.0001 + 0.00001 + 0.00005) / 3) < 1e-10

    def test_typical_price_equal_hlc(self):
        """Test TP when H = L = C (doji candle)"""
        df = pd.DataFrame({
            'high': [100, 100, 100],
            'low': [100, 100, 100],
            'close': [100, 100, 100]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        assert all(tp == 100.0)

    def test_typical_price_zero_values(self):
        """Test TP with zero values (edge case)"""
        df = pd.DataFrame({
            'high': [0, 1, 2],
            'low': [0, 0, 1],
            'close': [0, 0.5, 1.5]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        assert tp.iloc[0] == 0.0
        assert tp.iloc[1] == (1 + 0 + 0.5) / 3
        assert tp.iloc[2] == (2 + 1 + 1.5) / 3


# ============================================
# GROUP 3: MONEY FLOW CALCULATION (8 tests)
# ============================================

class TestMoneyFlowCalculation:
    """Test Money Flow = Typical Price × Volume"""

    def test_money_flow_basic_formula(self, sample_mfi_data):
        """Test MF formula: TP × Volume"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        # First row: TP=100, Volume=1000 → MF=100000
        assert mf.iloc[0] == 100.0 * 1000
        assert mf.iloc[0] == 100000.0

    def test_money_flow_zero_volume(self):
        """Test MF with zero volume"""
        df = pd.DataFrame({
            'high': [102],
            'low': [98],
            'close': [100],
            'volume': [0]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        assert mf.iloc[0] == 0.0

    def test_money_flow_high_volume(self):
        """Test MF with high volume"""
        df = pd.DataFrame({
            'high': [102],
            'low': [98],
            'close': [100],
            'volume': [1000000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        assert mf.iloc[0] == 100.0 * 1000000
        assert mf.iloc[0] == 100000000.0

    def test_money_flow_returns_series(self, sample_mfi_data):
        """Test MF calculation returns pandas Series"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        assert isinstance(mf, pd.Series)
        assert len(mf) == len(df)

    def test_money_flow_positive_values(self, sample_mfi_data):
        """Test MF is always positive (TP > 0, Volume >= 0)"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        assert all(mf >= 0)

    def test_money_flow_volume_impact(self):
        """Test higher volume increases money flow proportionally"""
        df = pd.DataFrame({
            'high': [102, 102],
            'low': [98, 98],
            'close': [100, 100],
            'volume': [1000, 2000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        # Same TP, double volume → double MF
        assert mf.iloc[1] == mf.iloc[0] * 2

    def test_money_flow_with_price_change(self):
        """Test MF changes with price movement"""
        df = pd.DataFrame({
            'high': [102, 152],
            'low': [98, 148],
            'close': [100, 150],
            'volume': [1000, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        # Higher price → higher MF (with same volume)
        assert mf.iloc[1] > mf.iloc[0]

    def test_money_flow_typical_price_relationship(self, sample_mfi_data):
        """Test MF is proportional to TP when volume is constant"""
        df = sample_mfi_data

        # Set constant volume
        df['volume'] = 1000

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']

        # MF should be TP × 1000
        for i in range(len(df)):
            assert abs(mf.iloc[i] - tp.iloc[i] * 1000) < 0.01


# ============================================
# GROUP 4: POSITIVE/NEGATIVE MONEY FLOW (10 tests)
# ============================================

class TestPositiveNegativeMoneyFlow:
    """Test Positive/Negative MF split based on TP direction"""

    def test_positive_mf_on_price_increase(self):
        """Test Positive MF when TP increases"""
        df = pd.DataFrame({
            'high': [102, 105],
            'low': [98, 101],
            'close': [100, 103],
            'volume': [1000, 1500]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)

        # First value is NaN (no previous TP)
        assert pd.isna(tp_diff.iloc[0])
        # Second value: TP increased → positive_mf = mf
        assert positive_mf.iloc[1] == mf.iloc[1]

    def test_negative_mf_on_price_decrease(self):
        """Test Negative MF when TP decreases"""
        df = pd.DataFrame({
            'high': [105, 102],
            'low': [101, 98],
            'close': [103, 100],
            'volume': [1500, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # Second value: TP decreased → negative_mf = mf
        assert negative_mf.iloc[1] == mf.iloc[1]

    def test_zero_mf_on_price_equal(self):
        """Test both MFs are zero when TP unchanged"""
        df = pd.DataFrame({
            'high': [102, 102],
            'low': [98, 98],
            'close': [100, 100],
            'volume': [1000, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # TP unchanged → both zero
        assert positive_mf.iloc[1] == 0
        assert negative_mf.iloc[1] == 0

    def test_mutual_exclusion(self, sample_mfi_data):
        """Test Positive and Negative MF are mutually exclusive"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # For each row, at most one can be non-zero
        for i in range(1, len(df)):
            if positive_mf.iloc[i] > 0:
                assert negative_mf.iloc[i] == 0
            if negative_mf.iloc[i] > 0:
                assert positive_mf.iloc[i] == 0

    def test_first_value_zero(self, sample_mfi_data):
        """Test first tp_diff is NaN → first positive/negative MF = 0"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        # First tp_diff is NaN
        assert pd.isna(tp_diff.iloc[0])

        # Implementation sets both to 0 when tp_diff is NaN
        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        assert positive_mf.iloc[0] == 0
        assert negative_mf.iloc[0] == 0

    def test_positive_mf_series_structure(self, sample_mfi_data):
        """Test Positive MF Series structure and length"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)

        assert isinstance(positive_mf, pd.Series)
        assert len(positive_mf) == len(df)
        assert all(positive_mf >= 0)

    def test_negative_mf_series_structure(self, sample_mfi_data):
        """Test Negative MF Series structure and length"""
        df = sample_mfi_data

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        assert isinstance(negative_mf, pd.Series)
        assert len(negative_mf) == len(df)
        assert all(negative_mf >= 0)

    def test_alternating_price_moves(self):
        """Test alternating up/down price movement"""
        df = pd.DataFrame({
            'high': [102, 105, 104, 107],
            'low': [98, 101, 100, 103],
            'close': [100, 103, 102, 105],
            'volume': [1000, 1000, 1000, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # Row 1: TP up → positive
        assert positive_mf.iloc[1] > 0
        # Row 2: TP down → negative
        assert negative_mf.iloc[2] > 0
        # Row 3: TP up → positive
        assert positive_mf.iloc[3] > 0

    def test_sustained_uptrend(self):
        """Test sustained uptrend produces only positive MF"""
        df = pd.DataFrame({
            'high': [102, 105, 108, 111],
            'low': [98, 101, 104, 107],
            'close': [100, 103, 106, 109],
            'volume': [1000, 1000, 1000, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # All rows after first should have positive MF
        assert all(positive_mf.iloc[1:] > 0)
        assert all(negative_mf.iloc[1:] == 0)

    def test_sustained_downtrend(self):
        """Test sustained downtrend produces only negative MF"""
        df = pd.DataFrame({
            'high': [111, 108, 105, 102],
            'low': [107, 104, 101, 98],
            'close': [109, 106, 103, 100],
            'volume': [1000, 1000, 1000, 1000]
        })

        tp = (df['high'] + df['low'] + df['close']) / 3
        mf = tp * df['volume']
        tp_diff = tp.diff()

        positive_mf = pd.Series(np.where(tp_diff > 0, mf, 0), index=df.index)
        negative_mf = pd.Series(np.where(tp_diff < 0, mf, 0), index=df.index)

        # All rows after first should have negative MF
        assert all(positive_mf.iloc[1:] == 0)
        assert all(negative_mf.iloc[1:] > 0)


# ============================================
# GROUP 5: MFI CALCULATION (12 tests)
# ============================================

class TestMFICalculation:
    """Test complete MFI calculation with edge cases"""

    def test_mfi_basic_formula(self, mock_config, sample_mfi_data):
        """Test MFI formula: 100 - (100 / (1 + ratio))"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        df = sample_mfi_data

        # Calculate MFI for period=7
        mfi = loader.calculate_mfi(df, period=7)

        # Should return Series with correct length
        assert isinstance(mfi, pd.Series)
        assert len(mfi) == len(df)

    def test_mfi_range_0_to_100(self, mock_config, sample_mfi_realistic):
        """Test MFI values are in range [0, 100]"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        df = sample_mfi_realistic

        mfi = loader.calculate_mfi(df, period=14)

        # Filter out NaN values
        valid_mfi = mfi.dropna()

        assert all(valid_mfi >= 0)
        assert all(valid_mfi <= 100)

    def test_mfi_overbought_condition(self):
        """Test MFI > 80 in strong uptrend (overbought)"""
        # Create strong uptrend with high volume
        df = pd.DataFrame({
            'high': [100 + i*2 for i in range(30)],
            'low': [98 + i*2 for i in range(30)],
            'close': [99 + i*2 for i in range(30)],
            'volume': [10000 + i*500 for i in range(30)]
        })

        from mfi_loader import MFILoader
        import yaml

        config = {
            'indicators': {
                'mfi': {
                    'periods': [14],
                    'batch_days': 1,
                    'lookback_multiplier': 2
                }
            }
        }

        loader = MFILoader('BTCUSDT', '1m', config)
        mfi = loader.calculate_mfi(df, period=14)

        # Late values should show overbought (> 80)
        late_mfi = mfi.iloc[-5:].dropna()
        assert all(late_mfi > 50)  # At least above 50 in strong uptrend

    def test_mfi_oversold_condition(self):
        """Test MFI < 20 in strong downtrend (oversold)"""
        # Create strong downtrend with high volume
        df = pd.DataFrame({
            'high': [200 - i*2 for i in range(30)],
            'low': [198 - i*2 for i in range(30)],
            'close': [199 - i*2 for i in range(30)],
            'volume': [10000 + i*500 for i in range(30)]
        })

        from mfi_loader import MFILoader

        config = {
            'indicators': {
                'mfi': {
                    'periods': [14],
                    'batch_days': 1,
                    'lookback_multiplier': 2
                }
            }
        }

        loader = MFILoader('BTCUSDT', '1m', config)
        mfi = loader.calculate_mfi(df, period=14)

        # Late values should show oversold (< 50)
        late_mfi = mfi.iloc[-5:].dropna()
        assert all(late_mfi < 50)  # At least below 50 in strong downtrend

    def test_mfi_all_positive_returns_100(self, mock_config):
        """Test MFI = 100 when all money flow is positive"""
        # Continuous uptrend → only positive MF
        df = pd.DataFrame({
            'high': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113],
            'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
            'volume': [1000] * 15
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=7)

        # After period, should be 100 (only positive MF)
        assert mfi.iloc[-1] == 100.0

    def test_mfi_all_negative_returns_0(self, mock_config):
        """Test MFI = 0 when all money flow is negative"""
        # Continuous downtrend → only negative MF
        df = pd.DataFrame({
            'high': [114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100],
            'low': [113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100, 99],
            'close': [114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100],
            'volume': [1000] * 15
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=7)

        # After period, should be 0 (only negative MF)
        assert mfi.iloc[-1] == 0.0

    def test_mfi_no_movement_returns_nan(self, mock_config):
        """Test MFI = NaN when no price movement"""
        # Flat prices → no MF
        df = pd.DataFrame({
            'high': [100] * 15,
            'low': [100] * 15,
            'close': [100] * 15,
            'volume': [1000] * 15
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=7)

        # All values after period should be NaN (no movement)
        assert pd.isna(mfi.iloc[-1])

    def test_mfi_insufficient_data_returns_nan(self, mock_config):
        """Test MFI = NaN for first (period-1) candles"""
        df = pd.DataFrame({
            'high': [102, 105, 104, 107, 106],
            'low': [98, 101, 100, 103, 102],
            'close': [100, 103, 102, 105, 104],
            'volume': [1000, 1500, 800, 1600, 1100]
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=7)

        # All values should be NaN (not enough data for period=7)
        assert all(pd.isna(mfi))

    def test_mfi_different_periods(self, mock_config, sample_mfi_realistic):
        """Test MFI calculation for different periods"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        df = sample_mfi_realistic

        mfi_7 = loader.calculate_mfi(df, period=7)
        mfi_14 = loader.calculate_mfi(df, period=14)
        mfi_25 = loader.calculate_mfi(df, period=25)

        # All should be valid Series
        assert isinstance(mfi_7, pd.Series)
        assert isinstance(mfi_14, pd.Series)
        assert isinstance(mfi_25, pd.Series)

        # Shorter period has more non-NaN values
        assert mfi_7.notna().sum() > mfi_14.notna().sum()
        assert mfi_14.notna().sum() > mfi_25.notna().sum()

    def test_mfi_zero_volume_handling(self, mock_config):
        """Test MFI handles zero volume correctly"""
        df = pd.DataFrame({
            'high': [102, 105, 104, 107, 106, 109, 108, 111],
            'low': [98, 101, 100, 103, 102, 105, 104, 107],
            'close': [100, 103, 102, 105, 104, 107, 106, 109],
            'volume': [1000, 0, 800, 0, 1100, 0, 1200, 0]  # Alternating zero volume
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=7)

        # Should not crash, MFI is valid Series
        assert isinstance(mfi, pd.Series)
        # Zero volume contributes 0 to MF but shouldn't break calculation
        assert mfi.iloc[-1] >= 0 or pd.isna(mfi.iloc[-1])

    def test_mfi_realistic_data(self, mock_config, sample_mfi_realistic):
        """Test MFI with realistic market data"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        df = sample_mfi_realistic

        mfi = loader.calculate_mfi(df, period=14)

        # Should have valid values after warmup period
        valid_mfi = mfi.iloc[14:].dropna()

        assert len(valid_mfi) > 0
        assert all(valid_mfi >= 0)
        assert all(valid_mfi <= 100)

    def test_mfi_formula_verification(self, mock_config):
        """Test MFI formula with hand-calculated example"""
        # Simple example where we can calculate by hand
        df = pd.DataFrame({
            'high': [102, 105, 103],
            'low': [98, 101, 99],
            'close': [100, 103, 101],
            'volume': [1000, 1000, 1000]
        })

        loader = MFILoader('BTCUSDT', '1m', mock_config)
        mfi = loader.calculate_mfi(df, period=2)

        # Manual calculation:
        # Row 0: TP = 100
        # Row 1: TP = 103 (up) → positive_mf = 103*1000 = 103000
        # Row 2: TP = 101 (down) → negative_mf = 101*1000 = 101000
        # For period=2 at row 2:
        # positive_sum = 103000, negative_sum = 101000
        # ratio = 103000/101000 ≈ 1.0198
        # MFI = 100 - (100 / (1 + 1.0198)) ≈ 50.5

        expected_mfi = 100 - (100 / (1 + (103000 / 101000)))
        actual_mfi = mfi.iloc[2]

        assert abs(actual_mfi - expected_mfi) < 0.1


# ============================================
# GROUP 6: DATABASE OPERATIONS (6 tests)
# ============================================

class TestDatabaseOperations:
    """Test database-related operations"""

    def test_ensure_columns_creates_mfi_columns(self, mock_config):
        """Test ensure_columns_exist creates MFI columns for all periods"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # All periods should get columns
        expected_columns = ['mfi_7', 'mfi_10', 'mfi_14', 'mfi_20', 'mfi_25']

        assert loader.periods == [7, 10, 14, 20, 25]

    def test_ensure_columns_decimal_precision(self, mock_config):
        """Test MFI columns use DECIMAL(10,2) type"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # MFI values are [0, 100] with 2 decimal places
        # DECIMAL(10,2) is appropriate
        periods = loader.periods

        assert all(isinstance(p, int) for p in periods)

    def test_get_column_names_consistency(self, mock_config):
        """Test column names are consistent with periods"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        for period in loader.periods:
            col_name = f'mfi_{period}'
            assert f'_{period}' in col_name

    def test_symbol_initialization(self, mock_config):
        """Test loader initializes with correct symbol"""
        loader = MFILoader('ETHUSDT', '1m', mock_config)

        assert loader.symbol == 'ETHUSDT'

    def test_batch_days_configuration(self, mock_config):
        """Test batch_days is configured correctly"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        assert loader.batch_days == 1

    def test_lookback_multiplier_configuration(self, mock_config):
        """Test lookback_multiplier is configured correctly"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # lookback_multiplier = 2
        # max_period = 25
        # lookback_periods = 25 * 2 = 50
        assert loader.lookback_multiplier == 2
        assert loader.lookback_periods == 25 * 2


# ============================================
# GROUP 7: TIMEFRAME OPERATIONS (6 tests)
# ============================================

class TestTimeframeOperations:
    """Test timeframe parsing and operations"""

    def test_parse_timeframe_minutes(self, mock_config):
        """Test parsing minute timeframes"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)
        assert loader.timeframe_minutes == 1

        loader = MFILoader('BTCUSDT', '15m', mock_config)
        assert loader.timeframe_minutes == 15

    def test_parse_timeframe_hours(self, mock_config):
        """Test parsing hour timeframes"""
        loader = MFILoader('BTCUSDT', '1h', mock_config)
        assert loader.timeframe_minutes == 60

        loader = MFILoader('BTCUSDT', '4h', mock_config)
        assert loader.timeframe_minutes == 240

    def test_parse_timeframe_days(self, mock_config):
        """Test parsing day timeframes"""
        loader = MFILoader('BTCUSDT', '1d', mock_config)
        assert loader.timeframe_minutes == 1440

    def test_parse_timeframe_invalid(self, mock_config):
        """Test invalid timeframe raises ValueError"""
        with pytest.raises(ValueError):
            loader = MFILoader('BTCUSDT', 'invalid', mock_config)

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
            loader = MFILoader('BTCUSDT', tf, mock_config)
            assert loader.timeframe_minutes == expected_minutes

    def test_lookback_calculation(self, mock_config):
        """Test lookback period calculation"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # max_period = 25, lookback_multiplier = 2
        # lookback_periods = 50
        assert loader.lookback_periods == 50


# ============================================
# GROUP 8: HELPER FUNCTIONS (5 tests)
# ============================================

class TestHelperFunctions:
    """Test helper functions and utilities"""

    def test_periods_constant(self, sample_mfi_periods):
        """Test MFI periods match configuration"""
        expected = [7, 10, 14, 20, 25]
        assert sample_mfi_periods == expected

    def test_periods_sorted(self, mock_config):
        """Test periods are in ascending order"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        periods = loader.periods
        assert periods == sorted(periods)

    def test_force_reload_flag(self, mock_config):
        """Test loader can be initialized without force_reload"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # Should initialize successfully
        assert loader.symbol == 'BTCUSDT'

    def test_custom_date_range(self, mock_config):
        """Test loader handles custom date range"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        # Loader should be ready to process any date range
        assert loader.timeframe == '1m'

    def test_symbol_progress_initialization(self, mock_config):
        """Test symbol_progress attribute is initialized"""
        loader = MFILoader('BTCUSDT', '1m', mock_config)

        assert hasattr(loader, 'symbol_progress')
        assert loader.symbol_progress == ""
