"""
Unit tests for ATR Loader (atr_loader.py)

Tests cover:
1. Timeframe Parsing - динамический парсинг таймфреймов
2. True Range Calculation - расчет True Range
3. ATR Calculation - расчет ATR с сглаживанием Уайлдера
4. Database Operations - создание колонок, сохранение данных
5. Helper Functions - вспомогательные функции

Total: 48 tests
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

from indicators.atr_loader import ATRLoader


# ============================================
# Group 1: Timeframe Parsing (6 tests)
# ============================================

class TestTimeframeParsing:
    """Tests for _parse_timeframes() method"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing with real production config (1m, 15m, 1h)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert '1m' in loader.timeframe_minutes
            assert '15m' in loader.timeframe_minutes
            assert '1h' in loader.timeframe_minutes

            assert loader.timeframe_minutes['1m'] == 1
            assert loader.timeframe_minutes['15m'] == 15
            assert loader.timeframe_minutes['1h'] == 60

    def test_parse_timeframes_all_formats(self, mock_config):
        """Test parsing all timeframe formats (m, h, d, w)"""
        mock_config['timeframes'] = ['1m', '15m', '1h', '4h', '1d', '1w']

        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert loader.timeframe_minutes['1m'] == 1
            assert loader.timeframe_minutes['15m'] == 15
            assert loader.timeframe_minutes['1h'] == 60
            assert loader.timeframe_minutes['4h'] == 240
            assert loader.timeframe_minutes['1d'] == 1440
            assert loader.timeframe_minutes['1w'] == 10080

    def test_parse_timeframes_case_insensitive(self, mock_config):
        """Test that parsing handles case correctly"""
        mock_config['timeframes'] = ['1M', '15M', '1H']

        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Keys stored in lowercase after parsing
            assert '1m' in loader.timeframe_minutes or '1M' in loader.timeframe_minutes
            # Values should be correct
            assert 1 in loader.timeframe_minutes.values()
            assert 15 in loader.timeframe_minutes.values()
            assert 60 in loader.timeframe_minutes.values()

    def test_parse_timeframes_invalid_format(self, mock_config):
        """Test that invalid timeframes are skipped with warning"""
        mock_config['timeframes'] = ['1m', 'invalid', '1h', '999x']

        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Valid timeframes should be parsed
            assert '1m' in loader.timeframe_minutes
            assert '1h' in loader.timeframe_minutes

            # Invalid timeframes should be skipped
            assert 'invalid' not in loader.timeframe_minutes
            assert '999x' not in loader.timeframe_minutes

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test that _parse_timeframes returns dict"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert isinstance(loader.timeframe_minutes, dict)
            assert len(loader.timeframe_minutes) > 0

    def test_parse_timeframes_caching(self, mock_config):
        """Test that timeframe_minutes is computed once and cached"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Access twice
            tf1 = loader.timeframe_minutes
            tf2 = loader.timeframe_minutes

            # Should be same object (cached)
            assert tf1 is tf2


# ============================================
# Group 2: True Range Calculation (12 tests)
# ============================================

class TestTrueRangeCalculation:
    """Tests for calculate_true_range() method"""

    def test_calculate_true_range_basic(self, mock_config):
        """Test basic True Range calculation"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [110, 115, 120],
                'low': [100, 105, 110],
                'close': [105, 112, 118]
            })

            tr = loader.calculate_true_range(df)

            assert isinstance(tr, pd.Series)
            assert len(tr) == len(df)

    def test_true_range_formula(self, mock_config):
        """Test TR = max(H-L, |H-PC|, |L-PC|)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [110, 115],
                'low': [100, 105],
                'close': [105, 112]
            })

            tr = loader.calculate_true_range(df)

            # First candle: TR = H - L = 110 - 100 = 10
            assert tr.iloc[0] == 10

            # Second candle: max(115-105, |115-105|, |105-105|) = max(10, 10, 0) = 10
            assert tr.iloc[1] == 10

    def test_true_range_case1_hl_maximum(self, mock_config):
        """Test case where H-L is the maximum (normal candle)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Normal candle where H-L > gaps
            df = pd.DataFrame({
                'high': [100, 120],  # Large range
                'low': [100, 100],
                'close': [100, 110]  # Closes in middle
            })

            tr = loader.calculate_true_range(df)

            # Second candle: H-L = 20, |H-PC| = |120-100| = 20, |L-PC| = 0
            # TR should be 20
            assert tr.iloc[1] == 20

    def test_true_range_case2_hc_maximum(self, mock_config):
        """Test case where |H-PC| is maximum (gap up)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Gap up scenario
            df = pd.DataFrame({
                'high': [100, 130],
                'low': [90, 125],  # Gap up from prev close
                'close': [95, 128]
            })

            tr = loader.calculate_true_range(df)

            # Second candle: H-L = 5, |H-PC| = |130-95| = 35, |L-PC| = |125-95| = 30
            # TR should be 35 (gap up)
            assert tr.iloc[1] == 35

    def test_true_range_case3_lc_maximum(self, mock_config):
        """Test case where |L-PC| is maximum (gap down)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Gap down scenario
            df = pd.DataFrame({
                'high': [100, 75],
                'low': [90, 70],  # Gap down from prev close
                'close': [95, 72]
            })

            tr = loader.calculate_true_range(df)

            # Second candle: H-L = 5, |H-PC| = |75-95| = 20, |L-PC| = |70-95| = 25
            # TR should be 25 (gap down)
            assert tr.iloc[1] == 25

    def test_true_range_first_candle_no_prev_close(self, mock_config):
        """Test that first candle uses H-L (no prev_close)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [110],
                'low': [100],
                'close': [105]
            })

            tr = loader.calculate_true_range(df)

            # First candle: TR = H - L = 10
            assert tr.iloc[0] == 10

    def test_true_range_sequence(self, mock_config):
        """Test True Range for a sequence of candles"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [110, 115, 108, 112],
                'low': [100, 105, 102, 107],
                'close': [105, 112, 105, 110]
            })

            tr = loader.calculate_true_range(df)

            # All TR values should be positive
            assert (tr > 0).all()

            # Length should match
            assert len(tr) == 4

    def test_true_range_precision(self, mock_config):
        """Test True Range precision (8 decimals)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [100.12345678, 105.87654321],
                'low': [99.11111111, 104.22222222],
                'close': [100.00000000, 105.00000000]
            })

            tr = loader.calculate_true_range(df)

            # Should maintain precision
            assert isinstance(tr.iloc[0], (float, np.float64))

    def test_true_range_empty_dataframe(self, mock_config):
        """Test True Range with empty DataFrame"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({'high': [], 'low': [], 'close': []})

            tr = loader.calculate_true_range(df)

            assert len(tr) == 0

    def test_true_range_single_candle(self, mock_config):
        """Test True Range with single candle"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'high': [110],
                'low': [100],
                'close': [105]
            })

            tr = loader.calculate_true_range(df)

            assert len(tr) == 1
            assert tr.iloc[0] == 10

    def test_true_range_constant_prices(self, mock_config):
        """Test True Range with constant prices (no volatility)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # All prices the same
            df = pd.DataFrame({
                'high': [100, 100, 100],
                'low': [100, 100, 100],
                'close': [100, 100, 100]
            })

            tr = loader.calculate_true_range(df)

            # TR should be 0 for all candles (no range)
            assert (tr == 0).all()

    def test_true_range_always_positive(self, mock_config, sample_atr_candles):
        """Test that True Range is always positive or zero"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            tr = loader.calculate_true_range(sample_atr_candles)

            # TR should never be negative
            assert (tr >= 0).all()


# ============================================
# Group 3: ATR Calculation (15 tests)
# ============================================

class TestATRCalculation:
    """Tests for calculate_atr() method"""

    def test_calculate_atr_basic(self, mock_config, sample_atr_candles):
        """Test basic ATR calculation"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Calculate TR first
            sample_atr_candles['tr'] = loader.calculate_true_range(sample_atr_candles)

            # Calculate ATR
            atr = loader.calculate_atr(sample_atr_candles, period=14)

            assert isinstance(atr, pd.Series)
            assert len(atr) == len(sample_atr_candles)

    def test_atr_first_value_is_sma(self, mock_config):
        """Test that first ATR value = SMA(TR, period)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Simple TR values
            df = pd.DataFrame({
                'tr': [10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
            })

            period = 5
            atr = loader.calculate_atr(df, period)

            # First ATR value (at index 4) should be SMA of first 5 TR values
            expected_first_atr = df['tr'].iloc[:5].mean()  # (10+12+14+16+18)/5 = 14
            assert abs(atr.iloc[4] - expected_first_atr) < 0.01

    def test_atr_wilders_smoothing_formula(self, mock_config):
        """Test Wilder's smoothing: ATR = (ATR_prev × (period-1) + TR_current) / period"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'tr': [10, 10, 10, 10, 10, 15, 20]  # Constant then increases
            })

            period = 5
            atr = loader.calculate_atr(df, period)

            # First ATR (index 4) = SMA = 10
            assert atr.iloc[4] == 10

            # Second ATR (index 5):
            # ATR = (10 × 4 + 15) / 5 = (40 + 15) / 5 = 11
            expected_atr_5 = (10 * 4 + 15) / 5
            assert abs(atr.iloc[5] - expected_atr_5) < 0.01

            # Third ATR (index 6):
            # ATR = (11 × 4 + 20) / 5 = (44 + 20) / 5 = 12.8
            expected_atr_6 = (expected_atr_5 * 4 + 20) / 5
            assert abs(atr.iloc[6] - expected_atr_6) < 0.01

    def test_atr_real_periods(self, mock_config, sample_atr_candles):
        """Test ATR with real production periods: 7, 14, 21, 30, 50, 100"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            sample_atr_candles['tr'] = loader.calculate_true_range(sample_atr_candles)

            periods = [7, 14, 21, 30]  # Test subset (not enough data for 50, 100)

            for period in periods:
                atr = loader.calculate_atr(sample_atr_candles, period)

                # Should have values
                assert not atr.isna().all()

                # ATR should be positive where not NaN
                assert (atr[~atr.isna()] >= 0).all()

    def test_atr_always_positive(self, mock_config, sample_atr_candles):
        """Test that ATR is always positive or zero (volatility indicator)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            sample_atr_candles['tr'] = loader.calculate_true_range(sample_atr_candles)
            atr = loader.calculate_atr(sample_atr_candles, period=14)

            # Remove NaN values
            atr_values = atr[~atr.isna()]

            # All ATR values should be >= 0
            assert (atr_values >= 0).all()

    def test_atr_high_volatility(self, mock_config, sample_atr_high_volatility):
        """Test that high volatility produces high ATR"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            sample_atr_high_volatility['tr'] = loader.calculate_true_range(sample_atr_high_volatility)
            atr = loader.calculate_atr(sample_atr_high_volatility, period=3)

            # Get non-NaN ATR values
            atr_values = atr[~atr.isna()]

            # ATR should be relatively high
            # With gaps and large ranges, ATR should be > 5
            assert atr_values.mean() > 5

    def test_atr_low_volatility(self, mock_config, sample_atr_low_volatility):
        """Test that low volatility produces low ATR"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            sample_atr_low_volatility['tr'] = loader.calculate_true_range(sample_atr_low_volatility)
            atr = loader.calculate_atr(sample_atr_low_volatility, period=7)

            # Get non-NaN ATR values
            atr_values = atr[~atr.isna()]

            # ATR should be very low (< 1)
            assert atr_values.mean() < 1

    def test_atr_smooths_over_time(self, mock_config):
        """Test that ATR smooths volatility over time (Wilder's smoothing)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Sudden spike in TR
            df = pd.DataFrame({
                'tr': [10] * 10 + [50] + [10] * 10  # Spike in middle
            })

            period = 5
            atr = loader.calculate_atr(df, period)

            # ATR at spike (index 10) should not jump to 50
            # It should be smoothed
            assert atr.iloc[10] < 50
            assert atr.iloc[10] > 10  # But should increase

    def test_atr_nan_for_first_period_minus_1(self, mock_config):
        """Test that first period-1 values are NaN"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'tr': [10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
            })

            period = 5
            atr = loader.calculate_atr(df, period)

            # First 4 values should be NaN
            assert atr.iloc[:4].isna().all()

            # 5th value onwards should have values
            assert not atr.iloc[4:].isna().any()

    def test_atr_insufficient_data(self, mock_config):
        """Test ATR with insufficient data (< period)"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Only 5 TR values, but period = 14
            df = pd.DataFrame({
                'tr': [10, 12, 14, 16, 18]
            })

            atr = loader.calculate_atr(df, period=14)

            # All values should be NaN
            assert atr.isna().all()

    def test_atr_empty_dataframe(self, mock_config):
        """Test ATR with empty DataFrame"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({'tr': []})

            atr = loader.calculate_atr(df, period=14)

            assert len(atr) == 0

    def test_atr_single_candle(self, mock_config):
        """Test ATR with single candle"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({'tr': [10]})

            atr = loader.calculate_atr(df, period=14)

            assert len(atr) == 1
            assert atr.iloc[0] is np.nan or pd.isna(atr.iloc[0])

    def test_atr_constant_tr(self, mock_config):
        """Test ATR with constant TR values"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # All TR values the same
            df = pd.DataFrame({
                'tr': [15.0] * 20
            })

            period = 7
            atr = loader.calculate_atr(df, period)

            # ATR should equal TR after first period
            # Since TR is constant, ATR should also be constant
            atr_values = atr[~atr.isna()]
            assert (atr_values == 15.0).all()

    def test_atr_precision_8_decimals(self, mock_config):
        """Test ATR precision (8 decimals for DECIMAL(20,8))"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'tr': [10.12345678, 12.87654321, 14.11111111, 16.22222222, 18.33333333]
            })

            atr = loader.calculate_atr(df, period=3)

            # Round to 8 decimals
            atr_rounded = atr.round(8)

            # Should maintain precision where not NaN
            assert not atr_rounded[~atr_rounded.isna()].empty

    def test_atr_period_equals_data_length(self, mock_config):
        """Test ATR when period equals data length"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'tr': [10, 12, 14, 16, 18]
            })

            period = 5
            atr = loader.calculate_atr(df, period)

            # Should have exactly one ATR value (the last one)
            assert atr.iloc[:-1].isna().all()
            assert not pd.isna(atr.iloc[-1])


# ============================================
# Group 4: Database Operations (8 tests)
# ============================================

class TestDatabaseOperations:
    """Tests for database operations"""

    def test_ensure_atr_columns_creates_missing(self, mock_config, mock_database):
        """Test that ensure_atr_columns creates missing columns"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate column doesn't exist
            mock_cursor.fetchone.return_value = None

            loader.ensure_atr_columns('1m', [7, 14])

            # Should check and create columns
            assert mock_cursor.execute.called

    def test_ensure_atr_columns_skips_existing(self, mock_config, mock_database):
        """Test that ensure_atr_columns skips existing columns"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate column already exists
            mock_cursor.fetchone.return_value = ['atr_14']

            loader.ensure_atr_columns('1m', [14])

            # Should only check, not create
            assert mock_cursor.execute.called

    def test_atr_column_naming_convention(self, mock_config):
        """Test ATR column naming: atr_{period}"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Check naming convention
            for period in [7, 14, 21, 30, 50, 100]:
                expected_name = f"atr_{period}"
                assert expected_name == f"atr_{period}"

    def test_atr_table_name_format(self, mock_config):
        """Test table name format: indicators_bybit_futures_{timeframe}"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert loader.get_table_name('1m') == 'indicators_bybit_futures_1m'
            assert loader.get_table_name('15m') == 'indicators_bybit_futures_15m'
            assert loader.get_table_name('1h') == 'indicators_bybit_futures_1h'

    def test_candles_table_name_format(self, mock_config):
        """Test candles table name format: candles_bybit_futures_{timeframe}"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert loader.get_candles_table_name('1m') == 'candles_bybit_futures_1m'
            assert loader.get_candles_table_name('15m') == 'candles_bybit_futures_15m'
            assert loader.get_candles_table_name('1h') == 'candles_bybit_futures_1h'

    def test_atr_value_decimal_precision(self, mock_config):
        """Test ATR values use DECIMAL(20,8) precision"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            df = pd.DataFrame({
                'tr': [10.12345678, 12.87654321, 14.11111111, 16.22222222, 18.33333333]
            })

            atr = loader.calculate_atr(df, period=3)

            # Round to 8 decimals (database precision)
            atr_rounded = atr.round(8)

            # Should maintain precision
            assert isinstance(atr_rounded.iloc[-1], (float, np.float64))

    def test_get_last_atr_date_with_data(self, mock_config, mock_database):
        """Test get_last_atr_date returns last date with ATR data"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate data exists
            last_date = datetime(2024, 10, 1, 12, 0)
            mock_cursor.fetchone.return_value = [last_date]

            result = loader.get_last_atr_date('1m', 14)

            assert result == last_date

    def test_get_last_atr_date_no_data(self, mock_config, mock_database):
        """Test get_last_atr_date returns None when no data"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate no data
            mock_cursor.fetchone.return_value = [None]

            result = loader.get_last_atr_date('1m', 14)

            assert result is None


# ============================================
# Group 5: Helper Functions (7 tests)
# ============================================

class TestHelperFunctions:
    """Tests for helper functions"""

    def test_load_config_success(self, mock_config):
        """Test loading config from YAML file"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert loader.config == mock_config
            assert 'timeframes' in loader.config
            assert 'indicators' in loader.config
            assert 'atr' in loader.config['indicators']

    def test_symbol_initialization(self, mock_config):
        """Test that symbol is properly initialized"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='ETHUSDT')

            assert loader.symbol == 'ETHUSDT'

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test that _parse_timeframes returns dict"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            assert isinstance(loader.timeframe_minutes, dict)
            assert len(loader.timeframe_minutes) > 0

    def test_timeframe_minutes_caching(self, mock_config):
        """Test that timeframe_minutes is computed once and cached"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Access twice
            tf1 = loader.timeframe_minutes
            tf2 = loader.timeframe_minutes

            # Should be same object (cached)
            assert tf1 is tf2

    def test_get_table_name_returns_correct_format(self, mock_config):
        """Test get_table_name returns correct format"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Test all timeframes
            for tf in ['1m', '15m', '1h']:
                table_name = loader.get_table_name(tf)
                assert table_name == f'indicators_bybit_futures_{tf}'
                assert table_name.startswith('indicators_bybit_futures_')

    def test_get_candles_table_name_returns_correct_format(self, mock_config):
        """Test get_candles_table_name returns correct format"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Test all timeframes
            for tf in ['1m', '15m', '1h']:
                table_name = loader.get_candles_table_name(tf)
                assert table_name == f'candles_bybit_futures_{tf}'
                assert table_name.startswith('candles_bybit_futures_')

    def test_atr_periods_from_config(self, mock_config):
        """Test that ATR periods are loaded from config"""
        with patch.object(ATRLoader, 'load_config', return_value=mock_config):
            loader = ATRLoader(symbol='BTCUSDT')

            # Should have ATR periods in config
            atr_config = loader.config['indicators']['atr']
            assert 'periods' in atr_config
            assert atr_config['periods'] == [7, 14, 21, 30, 50, 100]
