"""
Unit tests for Bollinger Bands Loader

Tests cover:
1. Column Name Generation (8 tests)
2. Bollinger Bands Calculation (18 tests)
3. SMA vs EMA Base (6 tests)
4. Squeeze Detection (6 tests)
5. Database Operations (10 tests)
6. Helper Functions (7 tests)

Total: 55 tests

Author: Claude Code
Created: 2025-10-27
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call
from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from indicators.bollinger_bands_loader import BollingerBandsLoader, BOLLINGER_CONFIGS


# ============================================
# Group 1: Column Name Generation (8 tests)
# ============================================

class TestColumnNameGeneration:
    """Tests for column name generation and std_dev formatting"""

    def test_format_std_dev_integer(self, mock_config):
        """Test formatting integer std_dev (2.0 -> '2_0')"""
        loader = BollingerBandsLoader()
        result = loader.format_std_dev(2.0)
        assert result == '2_0'

    def test_format_std_dev_golden_ratio(self, mock_config):
        """Test formatting golden ratio std_dev (1.618 -> '1_618')"""
        loader = BollingerBandsLoader()
        result = loader.format_std_dev(1.618)
        assert result == '1_618'

    def test_format_std_dev_wide_bands(self, mock_config):
        """Test formatting wide bands std_dev (3.0 -> '3_0')"""
        loader = BollingerBandsLoader()
        result = loader.format_std_dev(3.0)
        assert result == '3_0'

    def test_format_std_dev_tight_bands(self, mock_config):
        """Test formatting tight bands std_dev (1.0 -> '1_0')"""
        loader = BollingerBandsLoader()
        result = loader.format_std_dev(1.0)
        assert result == '1_0'

    def test_get_column_names_classic_sma(self, mock_config):
        """Test column names for classic SMA configuration (20, 2.0)"""
        loader = BollingerBandsLoader()
        columns = loader.get_column_names(period=20, std_dev=2.0, base='sma')

        assert columns['upper'] == 'bollinger_bands_sma_20_2_0_upper'
        assert columns['middle'] == 'bollinger_bands_sma_20_2_0_middle'
        assert columns['lower'] == 'bollinger_bands_sma_20_2_0_lower'
        assert columns['percent_b'] == 'bollinger_bands_sma_20_2_0_percent_b'
        assert columns['bandwidth'] == 'bollinger_bands_sma_20_2_0_bandwidth'
        assert columns['squeeze'] == 'bollinger_bands_sma_20_2_0_squeeze'

    def test_get_column_names_golden_sma(self, mock_config):
        """Test column names for golden ratio SMA (20, 1.618)"""
        loader = BollingerBandsLoader()
        columns = loader.get_column_names(period=20, std_dev=1.618, base='sma')

        assert columns['upper'] == 'bollinger_bands_sma_20_1_618_upper'
        assert columns['middle'] == 'bollinger_bands_sma_20_1_618_middle'

    def test_get_column_names_classic_ema(self, mock_config):
        """Test column names for classic EMA configuration (20, 2.0)"""
        loader = BollingerBandsLoader()
        columns = loader.get_column_names(period=20, std_dev=2.0, base='ema')

        assert columns['upper'] == 'bollinger_bands_ema_20_2_0_upper'
        assert columns['middle'] == 'bollinger_bands_ema_20_2_0_middle'
        assert columns['lower'] == 'bollinger_bands_ema_20_2_0_lower'

    def test_get_column_names_all_six_keys(self, mock_config):
        """Test that get_column_names returns all 6 required keys"""
        loader = BollingerBandsLoader()
        columns = loader.get_column_names(period=20, std_dev=2.0, base='sma')

        expected_keys = {'upper', 'middle', 'lower', 'percent_b', 'bandwidth', 'squeeze'}
        assert set(columns.keys()) == expected_keys


# ============================================
# Group 2: Bollinger Bands Calculation (18 tests)
# ============================================

class TestBollingerBandsCalculation:
    """Tests for Bollinger Bands calculation logic"""

    def test_calculate_bb_basic_structure(self, mock_config, sample_prices_for_bb):
        """Test that calculate_bollinger_bands returns all 6 components"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        assert 'upper' in result
        assert 'middle' in result
        assert 'lower' in result
        assert 'percent_b' in result
        assert 'bandwidth' in result
        assert 'squeeze' in result

    def test_calculate_bb_series_lengths(self, mock_config, sample_prices_for_bb):
        """Test that all returned series have same length as input"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        input_len = len(sample_prices_for_bb)
        assert len(result['upper']) == input_len
        assert len(result['middle']) == input_len
        assert len(result['lower']) == input_len
        assert len(result['percent_b']) == input_len
        assert len(result['bandwidth']) == input_len
        assert len(result['squeeze']) == input_len

    def test_calculate_bb_middle_equals_sma(self, mock_config, sample_prices_large):
        """Test that middle band equals SMA(period) when base='sma'"""
        loader = BollingerBandsLoader()
        period = 20

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')

        # Calculate expected SMA
        expected_sma = sample_prices_large.rolling(window=period).mean()

        # Compare middle band to SMA (skip NaN values)
        pd.testing.assert_series_equal(
            result['middle'].dropna(),
            expected_sma.dropna(),
            check_names=False
        )

    def test_calculate_bb_middle_equals_ema(self, mock_config, sample_prices_large):
        """Test that middle band equals EMA(period) when base='ema'"""
        loader = BollingerBandsLoader()
        period = 20

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='ema')

        # Calculate expected EMA
        expected_ema = sample_prices_large.ewm(span=period, adjust=False).mean()

        # Compare middle band to EMA
        pd.testing.assert_series_equal(
            result['middle'],
            expected_ema,
            check_names=False
        )

    def test_calculate_bb_upper_formula(self, mock_config, sample_prices_large):
        """Test upper band formula: Upper = Middle + k × σ"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 2.0

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Calculate expected upper band
        middle = sample_prices_large.rolling(window=period).mean()
        rolling_std = sample_prices_large.rolling(window=period).std()
        expected_upper = middle + (std_dev * rolling_std)

        # Compare (skip NaN)
        pd.testing.assert_series_equal(
            result['upper'].dropna(),
            expected_upper.dropna(),
            check_names=False
        )

    def test_calculate_bb_lower_formula(self, mock_config, sample_prices_large):
        """Test lower band formula: Lower = Middle - k × σ"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 2.0

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Calculate expected lower band
        middle = sample_prices_large.rolling(window=period).mean()
        rolling_std = sample_prices_large.rolling(window=period).std()
        expected_lower = middle - (std_dev * rolling_std)

        # Compare (skip NaN)
        pd.testing.assert_series_equal(
            result['lower'].dropna(),
            expected_lower.dropna(),
            check_names=False
        )

    def test_calculate_bb_percent_b_formula(self, mock_config, sample_prices_large):
        """Test %B formula: %B = (Close - Lower) / (Upper - Lower)"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 2.0

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Calculate expected %B
        band_range = result['upper'] - result['lower']
        expected_percent_b = (sample_prices_large - result['lower']) / band_range

        # Compare (skip NaN)
        pd.testing.assert_series_equal(
            result['percent_b'].dropna(),
            expected_percent_b.dropna(),
            check_names=False,
            rtol=1e-5
        )

    def test_calculate_bb_bandwidth_formula(self, mock_config, sample_prices_large):
        """Test Bandwidth formula: Bandwidth = (Upper - Lower) / Middle × 100"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 2.0

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Calculate expected bandwidth
        band_range = result['upper'] - result['lower']
        expected_bandwidth = (band_range / result['middle']) * 100

        # Compare (skip NaN)
        pd.testing.assert_series_equal(
            result['bandwidth'].dropna(),
            expected_bandwidth.dropna(),
            check_names=False,
            rtol=1e-5
        )

    def test_calculate_bb_bands_ordering(self, mock_config, sample_prices_large):
        """Test that Upper >= Middle >= Lower at all times"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_large, period=20, std_dev=2.0, base='sma')

        # Skip NaN values
        valid_idx = result['upper'].notna()

        assert (result['upper'][valid_idx] >= result['middle'][valid_idx]).all()
        assert (result['middle'][valid_idx] >= result['lower'][valid_idx]).all()

    def test_calculate_bb_percent_b_range(self, mock_config, sample_prices_large):
        """Test that %B typically stays around [0, 1] but can exceed"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_large, period=20, std_dev=2.0, base='sma')

        # %B = 0 when price at lower band
        # %B = 1 when price at upper band
        # %B can be < 0 or > 1 when price breaks bands
        percent_b_valid = result['percent_b'].dropna()

        # Most values should be between -0.5 and 1.5 for normal price action
        assert (percent_b_valid > -1.0).sum() > len(percent_b_valid) * 0.95
        assert (percent_b_valid < 2.0).sum() > len(percent_b_valid) * 0.95

    def test_calculate_bb_constant_prices(self, mock_config, sample_bb_constant_prices):
        """Test BB with constant prices (σ = 0, so Upper = Middle = Lower)"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_bb_constant_prices, period=10, std_dev=2.0, base='sma')

        # Skip first 9 NaN values
        valid_idx = result['upper'].notna()

        # When prices are constant, std = 0, so all bands are equal
        np.testing.assert_allclose(
            result['upper'][valid_idx],
            result['middle'][valid_idx],
            rtol=1e-10
        )
        np.testing.assert_allclose(
            result['middle'][valid_idx],
            result['lower'][valid_idx],
            rtol=1e-10
        )

    def test_calculate_bb_nan_at_start(self, mock_config, sample_prices_large):
        """Test that BB values are NaN for first (period - 1) candles"""
        loader = BollingerBandsLoader()
        period = 20

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')

        # First (period - 1) values should be NaN
        assert pd.isna(result['upper'].iloc[0:period-1]).all()
        assert pd.isna(result['middle'].iloc[0:period-1]).all()
        assert pd.isna(result['lower'].iloc[0:period-1]).all()

        # Value at index (period - 1) should be valid
        assert pd.notna(result['upper'].iloc[period-1])
        assert pd.notna(result['middle'].iloc[period-1])
        assert pd.notna(result['lower'].iloc[period-1])

    def test_calculate_bb_golden_ratio(self, mock_config, sample_prices_large):
        """Test BB with golden ratio std_dev (1.618)"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 1.618

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Bands should be narrower than std_dev=2.0
        result_wide = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')

        # Skip NaN
        valid_idx = result['upper'].notna()

        # Golden ratio bands should be inside standard bands
        assert (result['upper'][valid_idx] <= result_wide['upper'][valid_idx]).all()
        assert (result['lower'][valid_idx] >= result_wide['lower'][valid_idx]).all()

    def test_calculate_bb_wide_bands(self, mock_config, sample_prices_large):
        """Test BB with wide std_dev (3.0)"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 3.0

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Bands should be wider than std_dev=2.0
        result_normal = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')

        # Skip NaN
        valid_idx = result['upper'].notna()

        # Wide bands should be outside standard bands
        assert (result['upper'][valid_idx] >= result_normal['upper'][valid_idx]).all()
        assert (result['lower'][valid_idx] <= result_normal['lower'][valid_idx]).all()

    def test_calculate_bb_short_period(self, mock_config, sample_prices_large):
        """Test BB with short period (3)"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_large, period=3, std_dev=2.0, base='sma')

        # Should have valid values starting from index 2
        assert pd.notna(result['upper'].iloc[2])
        assert pd.notna(result['middle'].iloc[2])
        assert pd.notna(result['lower'].iloc[2])

    def test_calculate_bb_long_period(self, mock_config, sample_prices_large):
        """Test BB with long period (89)"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_large, period=89, std_dev=2.0, base='sma')

        # Should have valid values starting from index 88
        assert pd.isna(result['upper'].iloc[87])
        assert pd.notna(result['upper'].iloc[88])

    def test_calculate_bb_empty_series(self, mock_config):
        """Test BB calculation with empty price series"""
        loader = BollingerBandsLoader()
        empty_series = pd.Series([], dtype=float)

        result = loader.calculate_bollinger_bands(empty_series, period=20, std_dev=2.0, base='sma')

        assert len(result['upper']) == 0
        assert len(result['middle']) == 0
        assert len(result['lower']) == 0

    def test_calculate_bb_invalid_base(self, mock_config, sample_prices_large):
        """Test that invalid base raises ValueError"""
        loader = BollingerBandsLoader()

        with pytest.raises(ValueError, match="Unknown base"):
            loader.calculate_bollinger_bands(sample_prices_large, period=20, std_dev=2.0, base='invalid')


# ============================================
# Group 3: SMA vs EMA Base (6 tests)
# ============================================

class TestSMAvsEMABase:
    """Tests comparing SMA and EMA base calculations"""

    def test_sma_base_middle_band(self, mock_config, sample_prices_large):
        """Test that SMA base produces SMA middle band"""
        loader = BollingerBandsLoader()
        period = 20

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')
        expected = sample_prices_large.rolling(window=period).mean()

        pd.testing.assert_series_equal(
            result['middle'].dropna(),
            expected.dropna(),
            check_names=False
        )

    def test_ema_base_middle_band(self, mock_config, sample_prices_large):
        """Test that EMA base produces EMA middle band"""
        loader = BollingerBandsLoader()
        period = 20

        result = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='ema')
        expected = sample_prices_large.ewm(span=period, adjust=False).mean()

        pd.testing.assert_series_equal(
            result['middle'],
            expected,
            check_names=False
        )

    def test_std_always_from_close(self, mock_config, sample_prices_large):
        """Test that standard deviation is always calculated from close prices, not from middle band"""
        loader = BollingerBandsLoader()
        period = 20
        std_dev = 2.0

        # Calculate with SMA base
        result_sma = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='sma')

        # Calculate with EMA base
        result_ema = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=std_dev, base='ema')

        # Standard deviation component should be the same (from close prices)
        # Upper - Middle should have same std component
        rolling_std = sample_prices_large.rolling(window=period).std()
        expected_std_component = std_dev * rolling_std

        sma_std_component = result_sma['upper'] - result_sma['middle']
        ema_std_component = result_ema['upper'] - result_ema['middle']

        # Both should equal expected_std_component
        pd.testing.assert_series_equal(
            sma_std_component.dropna(),
            expected_std_component.dropna(),
            check_names=False
        )

        pd.testing.assert_series_equal(
            ema_std_component.dropna(),
            expected_std_component.dropna(),
            check_names=False
        )

    def test_ema_faster_reaction(self, mock_config):
        """Test that EMA base reacts faster to price changes than SMA"""
        loader = BollingerBandsLoader()

        # Create price series with sudden jump
        prices = pd.Series([100.0] * 20 + [110.0] * 20, dtype=float)
        period = 20

        result_sma = loader.calculate_bollinger_bands(prices, period=period, std_dev=2.0, base='sma')
        result_ema = loader.calculate_bollinger_bands(prices, period=period, std_dev=2.0, base='ema')

        # After the jump (index 25), EMA should be closer to new price (110) than SMA
        assert result_ema['middle'].iloc[25] > result_sma['middle'].iloc[25]

    def test_sma_ema_bandwidth_difference(self, mock_config, sample_prices_large):
        """Test that bandwidth differs between SMA and EMA bases"""
        loader = BollingerBandsLoader()
        period = 20

        result_sma = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')
        result_ema = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='ema')

        # Bandwidths should be different (because middle bands differ)
        # Though std component is same, division by different middle creates different bandwidth
        valid_idx = result_sma['bandwidth'].notna() & result_ema['bandwidth'].notna()

        # Should not be identical
        assert not np.allclose(
            result_sma['bandwidth'][valid_idx],
            result_ema['bandwidth'][valid_idx],
            rtol=1e-10
        )

    def test_base_affects_percent_b(self, mock_config, sample_prices_large):
        """Test that base (SMA/EMA) affects %B calculation"""
        loader = BollingerBandsLoader()
        period = 20

        result_sma = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='sma')
        result_ema = loader.calculate_bollinger_bands(sample_prices_large, period=period, std_dev=2.0, base='ema')

        # %B should differ because bands differ
        valid_idx = result_sma['percent_b'].notna() & result_ema['percent_b'].notna()

        # Should not be identical
        assert not np.allclose(
            result_sma['percent_b'][valid_idx],
            result_ema['percent_b'][valid_idx],
            rtol=1e-10
        )


# ============================================
# Group 4: Squeeze Detection (6 tests)
# ============================================

class TestSqueezeDetection:
    """Tests for squeeze detection logic"""

    def test_squeeze_flag_type(self, mock_config, sample_prices_for_bb):
        """Test that squeeze flag is boolean Series"""
        loader = BollingerBandsLoader()
        result = loader.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        assert result['squeeze'].dtype == bool

    def test_squeeze_threshold_5_percent(self, mock_config):
        """Test default squeeze threshold of 5%"""
        loader = BollingerBandsLoader()
        assert loader.squeeze_threshold == 5.0

    def test_squeeze_detection_low_volatility(self, mock_config, sample_bb_constant_prices):
        """Test squeeze detection with constant prices (bandwidth = 0)"""
        loader = BollingerBandsLoader()
        period = 10
        result = loader.calculate_bollinger_bands(sample_bb_constant_prices, period=period, std_dev=2.0, base='sma')

        # Constant prices -> bandwidth = 0 -> squeeze = True
        # But first (period-1) values are NaN, so only check valid bandwidth values
        valid_idx = result['bandwidth'].notna()
        assert result['squeeze'][valid_idx].all()

    def test_squeeze_detection_high_volatility(self, mock_config):
        """Test no squeeze with high volatility prices"""
        loader = BollingerBandsLoader()

        # Create prices with high volatility
        np.random.seed(42)
        prices = pd.Series([100 + np.random.uniform(-20, 20) for _ in range(30)], dtype=float)

        result = loader.calculate_bollinger_bands(prices, period=10, std_dev=2.0, base='sma')

        # High volatility -> bandwidth > 5% -> squeeze = False
        valid_idx = result['squeeze'].notna()
        # Most values should not be squeezed
        assert (~result['squeeze'][valid_idx]).sum() > len(result['squeeze'][valid_idx]) * 0.7

    def test_squeeze_formula(self, mock_config, sample_prices_for_bb):
        """Test squeeze formula: squeeze = (bandwidth < threshold)"""
        loader = BollingerBandsLoader(squeeze_threshold=5.0)
        result = loader.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        # Manually calculate expected squeeze
        expected_squeeze = result['bandwidth'] < 5.0

        # Compare
        valid_idx = result['squeeze'].notna()
        pd.testing.assert_series_equal(
            result['squeeze'][valid_idx],
            expected_squeeze[valid_idx],
            check_names=False
        )

    def test_squeeze_custom_threshold(self, mock_config, sample_prices_for_bb):
        """Test squeeze detection with custom threshold (10%)"""
        loader = BollingerBandsLoader(squeeze_threshold=10.0)
        result = loader.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        # More squeezes should be detected with higher threshold
        loader_default = BollingerBandsLoader(squeeze_threshold=5.0)
        result_default = loader_default.calculate_bollinger_bands(sample_prices_for_bb, period=20, std_dev=2.0, base='sma')

        valid_idx = result['squeeze'].notna()

        # Custom threshold should detect more squeezes
        assert result['squeeze'][valid_idx].sum() >= result_default['squeeze'][valid_idx].sum()


# ============================================
# Group 5: Database Operations (10 tests)
# ============================================

class TestDatabaseOperations:
    """Tests for database operations"""

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_ensure_columns_creates_upper_band(self, mock_db_class, mock_config, sample_bb_configs):
        """Test column creation for upper band"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Column doesn't exist
        mock_cursor.fetchone.return_value = [False]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        loader.ensure_columns_exist('1m', config)

        # Should have executed ALTER TABLE for upper column
        calls = mock_cursor.execute.call_args_list
        alter_calls = [call for call in calls if 'ALTER TABLE' in str(call)]

        assert len(alter_calls) > 0

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_ensure_columns_decimal_precision(self, mock_db_class, mock_config, sample_bb_configs):
        """Test that price columns use DECIMAL(20,8)"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Columns don't exist
        mock_cursor.fetchone.return_value = [False]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        loader.ensure_columns_exist('1m', config)

        # Check that ALTER TABLE uses DECIMAL(20,8) for upper/middle/lower
        calls = mock_cursor.execute.call_args_list
        alter_calls = [str(call) for call in calls if 'ALTER TABLE' in str(call)]

        # Should have calls with DECIMAL(20,8)
        decimal_calls = [call for call in alter_calls if 'DECIMAL(20,8)' in call]
        assert len(decimal_calls) >= 3  # upper, middle, lower

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_ensure_columns_percent_precision(self, mock_db_class, mock_config, sample_bb_configs):
        """Test that %B and bandwidth use DECIMAL(10,4)"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Columns don't exist
        mock_cursor.fetchone.return_value = [False]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        loader.ensure_columns_exist('1m', config)

        # Check that ALTER TABLE uses DECIMAL(10,4) for percent_b/bandwidth
        calls = mock_cursor.execute.call_args_list
        alter_calls = [str(call) for call in calls if 'ALTER TABLE' in str(call)]

        # Should have calls with DECIMAL(10,4)
        percent_calls = [call for call in alter_calls if 'DECIMAL(10,4)' in call]
        assert len(percent_calls) >= 2  # percent_b, bandwidth

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_ensure_columns_squeeze_boolean(self, mock_db_class, mock_config, sample_bb_configs):
        """Test that squeeze column uses BOOLEAN type"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Columns don't exist
        mock_cursor.fetchone.return_value = [False]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        loader.ensure_columns_exist('1m', config)

        # Check that ALTER TABLE uses BOOLEAN for squeeze
        calls = mock_cursor.execute.call_args_list
        alter_calls = [str(call) for call in calls if 'ALTER TABLE' in str(call)]

        # Should have call with BOOLEAN
        boolean_calls = [call for call in alter_calls if 'BOOLEAN' in call]
        assert len(boolean_calls) >= 1  # squeeze

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_ensure_columns_skips_existing(self, mock_db_class, mock_config, sample_bb_configs):
        """Test that existing columns are not recreated"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # All columns exist
        mock_cursor.fetchone.return_value = [True]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        loader.ensure_columns_exist('1m', config)

        # Should NOT have executed ALTER TABLE
        calls = mock_cursor.execute.call_args_list
        alter_calls = [call for call in calls if 'ALTER TABLE' in str(call)]

        assert len(alter_calls) == 0

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_get_last_processed_date_exists(self, mock_db_class, mock_config, sample_bb_configs):
        """Test getting last processed date when data exists"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        last_date = datetime(2024, 1, 15, 12, 0)
        mock_cursor.fetchone.return_value = [last_date]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        result = loader.get_last_processed_date('1m', config)

        assert result == last_date

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_get_last_processed_date_none(self, mock_db_class, mock_config, sample_bb_configs):
        """Test getting last processed date when no data exists"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.fetchone.return_value = [None]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]
        result = loader.get_last_processed_date('1m', config)

        assert result is None

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_get_data_range(self, mock_db_class, mock_config):
        """Test getting data range from candles table"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        min_date = datetime(2024, 1, 1, 0, 0)
        max_date = datetime(2024, 1, 31, 23, 59)
        mock_cursor.fetchone.return_value = [min_date, max_date]

        loader = BollingerBandsLoader()
        result_min, result_max = loader.get_data_range('1m')

        assert result_min == min_date
        assert result_max == max_date

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_table_name_formats(self, mock_db_class, mock_config, sample_bb_configs):
        """Test correct table name formatting"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn
        mock_cursor.fetchone.return_value = [True]

        loader = BollingerBandsLoader()
        config = sample_bb_configs[0]

        # Test 1m timeframe
        loader.ensure_columns_exist('1m', config)
        calls = mock_cursor.execute.call_args_list
        assert any('indicators_bybit_futures_1m' in str(call) for call in calls)

        # Test 15m timeframe
        mock_cursor.reset_mock()
        loader.ensure_columns_exist('15m', config)
        calls = mock_cursor.execute.call_args_list
        assert any('indicators_bybit_futures_15m' in str(call) for call in calls)

    @patch('indicators.bollinger_bands_loader.DatabaseConnection')
    def test_query_uses_symbol_filter(self, mock_db_class, mock_config, sample_bb_configs):
        """Test that queries filter by symbol"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn
        mock_cursor.fetchone.return_value = [None]

        loader = BollingerBandsLoader(symbol='ETHUSDT')
        config = sample_bb_configs[0]
        loader.get_last_processed_date('1m', config)

        # Check that query includes symbol parameter
        calls = mock_cursor.execute.call_args_list
        assert any('ETHUSDT' in str(call) for call in calls)


# ============================================
# Group 6: Helper Functions (7 tests)
# ============================================

class TestHelperFunctions:
    """Tests for helper and utility functions"""

    def test_aggregate_1m_to_1m_passthrough(self, mock_config):
        """Test that aggregating 1m to 1m returns same data"""
        loader = BollingerBandsLoader()

        timestamps = pd.date_range('2024-01-01 00:00', periods=10, freq='1min')
        df = pd.DataFrame({
            'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
        }, index=timestamps)

        result = loader.aggregate_1m_to_timeframe(df, '1m')

        pd.testing.assert_frame_equal(result, df)

    def test_aggregate_1m_to_15m(self, mock_config):
        """Test aggregating 1m candles to 15m"""
        loader = BollingerBandsLoader()

        # Create 30 minutes of data (2 complete 15m candles)
        timestamps = pd.date_range('2024-01-01 00:00', periods=30, freq='1min')
        df = pd.DataFrame({
            'close': list(range(100, 130))
        }, index=timestamps)

        result = loader.aggregate_1m_to_timeframe(df, '15m')

        # Should have 2 candles
        assert len(result) == 2

        # First 15m candle should have close = 114 (last of first 15 minutes)
        assert result['close'].iloc[0] == 114

        # Second 15m candle should have close = 129 (last of second 15 minutes)
        assert result['close'].iloc[1] == 129

    def test_aggregate_1m_to_1h(self, mock_config):
        """Test aggregating 1m candles to 1h"""
        loader = BollingerBandsLoader()

        # Create 120 minutes of data (2 complete 1h candles)
        timestamps = pd.date_range('2024-01-01 00:00', periods=120, freq='1min')
        df = pd.DataFrame({
            'close': list(range(100, 220))
        }, index=timestamps)

        result = loader.aggregate_1m_to_timeframe(df, '1h')

        # Should have 2 candles
        assert len(result) == 2

        # First 1h candle should have close = 159 (last of first 60 minutes)
        assert result['close'].iloc[0] == 159

        # Second 1h candle should have close = 219 (last of second 60 minutes)
        assert result['close'].iloc[1] == 219

    def test_aggregate_unsupported_timeframe(self, mock_config):
        """Test that unsupported timeframe raises ValueError"""
        loader = BollingerBandsLoader()

        timestamps = pd.date_range('2024-01-01 00:00', periods=10, freq='1min')
        df = pd.DataFrame({
            'close': list(range(100, 110))
        }, index=timestamps)

        with pytest.raises(ValueError, match="Unsupported timeframe"):
            loader.aggregate_1m_to_timeframe(df, '5m')

    def test_bollinger_configs_constant(self, mock_config):
        """Test that BOLLINGER_CONFIGS has expected structure"""
        assert isinstance(BOLLINGER_CONFIGS, list)
        assert len(BOLLINGER_CONFIGS) == 13  # 11 SMA + 2 EMA

        # Check first config structure
        config = BOLLINGER_CONFIGS[0]
        assert 'name' in config
        assert 'period' in config
        assert 'std_dev' in config
        assert 'base' in config
        assert 'description' in config

    def test_bollinger_configs_has_classic(self, mock_config):
        """Test that BOLLINGER_CONFIGS includes classic configuration"""
        classic = [c for c in BOLLINGER_CONFIGS if c['name'] == 'classic']
        assert len(classic) == 1
        assert classic[0]['period'] == 20
        assert classic[0]['std_dev'] == 2.0
        assert classic[0]['base'] == 'sma'

    def test_loader_initialization(self, mock_config):
        """Test BollingerBandsLoader initialization with defaults"""
        loader = BollingerBandsLoader()

        assert loader.symbol == 'BTCUSDT'
        assert loader.batch_days == 1
        assert loader.lookback_multiplier == 3
        assert loader.squeeze_threshold == 5.0


# ============================================
# Summary
# ============================================

"""
Test Coverage Summary:
- 8 tests for column name generation and formatting
- 18 tests for Bollinger Bands calculation formulas
- 6 tests for SMA vs EMA base comparison
- 6 tests for squeeze detection logic
- 10 tests for database operations
- 7 tests for helper functions

Total: 55 tests

All tests use mocking for database operations to ensure:
- No real database access during tests
- Fast execution
- Isolated unit testing
"""
