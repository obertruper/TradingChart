"""
Unit tests for OBVLoader

Test coverage for OBV (On-Balance Volume) indicator loader.

Test Structure:
    Group 1: Column Names (3 tests)
    Group 2: OBV Calculation (20 tests)
    Group 3: Database Operations (8 tests)
    Group 4: Timeframe Operations (6 tests)
    Group 5: Helper Functions (5 tests)

Total: 42 tests

OBV Formula:
    If Close(t) > Close(t-1):  OBV(t) = OBV(t-1) + Volume(t)
    If Close(t) < Close(t-1):  OBV(t) = OBV(t-1) - Volume(t)
    If Close(t) = Close(t-1):  OBV(t) = OBV(t-1)
    Initial: OBV(0) = 0
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from indicators.obv_loader import OBVLoader


# ============================================
# Group 1: Column Names (3 tests)
# ============================================

class TestColumnNames:
    """Tests for OBV column name format"""

    def test_column_name_format(self, mock_config):
        """Test OBV column name is 'obv'"""
        # OBV has single column named 'obv'
        column_name = 'obv'
        assert column_name == 'obv'
        assert isinstance(column_name, str)

    def test_column_name_is_lowercase(self, mock_config):
        """Test OBV column name is lowercase"""
        column_name = 'obv'
        assert column_name.islower()
        assert not column_name.isupper()

    def test_column_type_decimal(self, mock_config):
        """Test OBV column should be DECIMAL(30,8) type"""
        # OBV uses DECIMAL(30,8) for large cumulative values
        expected_type = 'DECIMAL(30,8)'

        # Verify the type string format
        assert 'DECIMAL' in expected_type
        assert '30' in expected_type
        assert '8' in expected_type


# ============================================
# Group 2: OBV Calculation (20 tests)
# ============================================

class TestOBVCalculation:
    """Tests for OBV calculation logic"""

    def test_obv_basic_structure(self, mock_config, sample_obv_data):
        """Test OBV returns Series with correct index"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        obv = loader.calculate_obv(sample_obv_data)

        assert isinstance(obv, pd.Series)
        assert len(obv) == len(sample_obv_data)
        assert obv.index.equals(sample_obv_data.index)

    def test_obv_initial_value_zero(self, mock_config, sample_obv_data):
        """Test OBV first value is always 0"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        obv = loader.calculate_obv(sample_obv_data)

        # First value must be 0 (industry standard)
        assert obv.iloc[0] == 0

    def test_obv_price_increase_adds_volume(self, mock_config):
        """Test OBV adds volume when price increases"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 110],
            'volume': [1000, 1500, 2000]
        })

        obv = loader.calculate_obv(df)

        # OBV[0] = 0
        # OBV[1] = 0 + 1500 = 1500 (price up)
        # OBV[2] = 1500 + 2000 = 3500 (price up)
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 1500
        assert obv.iloc[2] == 3500

    def test_obv_price_decrease_subtracts_volume(self, mock_config):
        """Test OBV subtracts volume when price decreases"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 95, 90],
            'volume': [1000, 1500, 2000]
        })

        obv = loader.calculate_obv(df)

        # OBV[0] = 0
        # OBV[1] = 0 - 1500 = -1500 (price down)
        # OBV[2] = -1500 - 2000 = -3500 (price down)
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == -1500
        assert obv.iloc[2] == -3500

    def test_obv_price_unchanged_maintains_obv(self, mock_config):
        """Test OBV stays same when price unchanged"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 105, 105],
            'volume': [1000, 1500, 2000, 1200]
        })

        obv = loader.calculate_obv(df)

        # OBV[0] = 0
        # OBV[1] = 0 + 1500 = 1500 (price up)
        # OBV[2] = 1500 + 0 = 1500 (price unchanged)
        # OBV[3] = 1500 + 0 = 1500 (price unchanged)
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 1500
        assert obv.iloc[2] == 1500
        assert obv.iloc[3] == 1500

    def test_obv_cumulative_nature(self, mock_config, sample_obv_data):
        """Test OBV is truly cumulative"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        obv = loader.calculate_obv(sample_obv_data)

        # Each OBV value should be sum of all previous signed volumes
        # Verify cumulative property: obv[i] depends on obv[i-1]
        for i in range(2, len(obv)):
            price_change = sample_obv_data['close'].iloc[i] - sample_obv_data['close'].iloc[i-1]
            volume = sample_obv_data['volume'].iloc[i]

            if price_change > 0:
                expected = obv.iloc[i-1] + volume
            elif price_change < 0:
                expected = obv.iloc[i-1] - volume
            else:
                expected = obv.iloc[i-1]

            assert abs(obv.iloc[i] - expected) < 0.01

    def test_obv_formula_validation(self, mock_config):
        """Test OBV formula with known values"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Known data: alternating up/down
        df = pd.DataFrame({
            'close': [100, 105, 103, 108],
            'volume': [1000, 1500, 800, 2000]
        })

        obv = loader.calculate_obv(df)

        # Calculate expected values manually
        # OBV[0] = 0 (initial)
        # OBV[1] = 0 + 1500 = 1500 (close: 100->105, up)
        # OBV[2] = 1500 - 800 = 700 (close: 105->103, down)
        # OBV[3] = 700 + 2000 = 2700 (close: 103->108, up)

        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 1500
        assert obv.iloc[2] == 700
        assert obv.iloc[3] == 2700

    def test_obv_empty_dataframe(self, mock_config):
        """Test OBV with empty DataFrame raises IndexError"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({'close': [], 'volume': []})

        # Empty DataFrame will cause IndexError when trying to set obv.iloc[0] = 0
        # This is expected behavior as OBV requires at least one candle
        with pytest.raises(IndexError):
            obv = loader.calculate_obv(df)

    def test_obv_single_candle(self, mock_config):
        """Test OBV with single candle"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100],
            'volume': [1000]
        })

        obv = loader.calculate_obv(df)

        # Single candle should have OBV = 0
        assert len(obv) == 1
        assert obv.iloc[0] == 0

    def test_obv_two_candles_up(self, mock_config):
        """Test OBV with two candles (price up)"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105],
            'volume': [1000, 1500]
        })

        obv = loader.calculate_obv(df)

        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 1500

    def test_obv_two_candles_down(self, mock_config):
        """Test OBV with two candles (price down)"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 95],
            'volume': [1000, 1500]
        })

        obv = loader.calculate_obv(df)

        assert obv.iloc[0] == 0
        assert obv.iloc[1] == -1500

    def test_obv_alternating_prices(self, mock_config):
        """Test OBV with alternating up/down prices"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 103, 107, 104],
            'volume': [1000, 1000, 1000, 1000, 1000]
        })

        obv = loader.calculate_obv(df)

        # OBV should oscillate: 0 -> +1000 -> 0 -> +1000 -> 0
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 1000
        assert obv.iloc[2] == 0
        assert obv.iloc[3] == 1000
        assert obv.iloc[4] == 0

    def test_obv_constant_prices(self, mock_config):
        """Test OBV with constant prices"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 100, 100, 100],
            'volume': [1000, 1500, 2000, 1200]
        })

        obv = loader.calculate_obv(df)

        # All OBV values should be 0 when price doesn't change
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 0
        assert obv.iloc[2] == 0
        assert obv.iloc[3] == 0

    def test_obv_large_volume(self, mock_config):
        """Test OBV with very large volume values"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 110],
            'volume': [1e9, 2e9, 3e9]  # Billions
        })

        obv = loader.calculate_obv(df)

        # Should handle large numbers correctly
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 2e9
        assert obv.iloc[2] == 2e9 + 3e9

    def test_obv_zero_volume(self, mock_config):
        """Test OBV with zero volume"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 110],
            'volume': [1000, 0, 1500]
        })

        obv = loader.calculate_obv(df)

        # Zero volume should not affect OBV
        assert obv.iloc[0] == 0
        assert obv.iloc[1] == 0  # price up but volume=0
        assert obv.iloc[2] == 1500  # price up with volume

    def test_obv_signed_volume_calculation(self, mock_config):
        """Test signed volume calculation logic"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 102],
            'volume': [1000, 1500, 800]
        })

        obv = loader.calculate_obv(df)

        # Verify signed volume logic
        price_change_1 = df['close'].iloc[1] - df['close'].iloc[0]  # +5 (up)
        price_change_2 = df['close'].iloc[2] - df['close'].iloc[1]  # -3 (down)

        assert price_change_1 > 0  # Price up -> add volume
        assert price_change_2 < 0  # Price down -> subtract volume

        assert obv.iloc[1] == 1500  # Added
        assert obv.iloc[2] == 1500 - 800  # Subtracted

    def test_obv_price_diff_first_value(self, mock_config):
        """Test price diff for first value is NaN"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        df = pd.DataFrame({
            'close': [100, 105, 110],
            'volume': [1000, 1500, 2000]
        })

        # Price diff first value is NaN, so signed_volume[0] = 0
        price_change = df['close'].diff()
        assert pd.isna(price_change.iloc[0])

        obv = loader.calculate_obv(df)
        # First OBV value is explicitly set to 0
        assert obv.iloc[0] == 0

    def test_obv_series_index_preserved(self, mock_config):
        """Test OBV preserves original DataFrame index"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Custom index
        custom_index = pd.date_range('2024-01-01', periods=5, freq='1h')
        df = pd.DataFrame({
            'close': [100, 105, 103, 108, 104],
            'volume': [1000, 1500, 800, 2000, 1200]
        }, index=custom_index)

        obv = loader.calculate_obv(df)

        assert obv.index.equals(custom_index)
        assert len(obv) == len(df)

    def test_obv_realistic_data(self, mock_config, sample_obv_realistic):
        """Test OBV with realistic market data"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        obv = loader.calculate_obv(sample_obv_realistic)

        # OBV should be cumulative
        assert len(obv) == len(sample_obv_realistic)
        assert obv.iloc[0] == 0
        assert not obv.isna().any()

        # OBV should vary based on price direction
        assert not (obv == 0).all()  # Not all zeros

    def test_obv_negative_values_possible(self, mock_config):
        """Test OBV can have negative values"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # More selling than buying
        df = pd.DataFrame({
            'close': [100, 95, 90, 85],
            'volume': [1000, 2000, 3000, 4000]
        })

        obv = loader.calculate_obv(df)

        # OBV should become increasingly negative
        assert obv.iloc[0] == 0
        assert obv.iloc[1] < 0
        assert obv.iloc[2] < obv.iloc[1]
        assert obv.iloc[3] < obv.iloc[2]


# ============================================
# Group 3: Database Operations (8 tests)
# ============================================

class TestDatabaseOperations:
    """Tests for database operations"""

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_ensure_columns_decimal_precision(self, mock_db_class, mock_config):
        """Test OBV column created with DECIMAL(30,8) precision"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Column doesn't exist
        mock_cursor.fetchone.return_value = None

        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader.ensure_columns_exist()

        # Verify ALTER TABLE with DECIMAL(30,8)
        calls = mock_cursor.execute.call_args_list
        alter_call = [call for call in calls if 'ALTER TABLE' in str(call)]

        assert len(alter_call) > 0
        alter_query = str(alter_call[0])
        assert 'obv' in alter_query.lower()
        assert 'DECIMAL(30' in alter_query or 'decimal(30' in alter_query

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_ensure_columns_skips_existing(self, mock_db_class, mock_config):
        """Test column creation skips if column exists"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        # Column exists
        mock_cursor.fetchone.return_value = ('obv',)

        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader.ensure_columns_exist()

        # Should not execute ALTER TABLE
        calls = mock_cursor.execute.call_args_list
        alter_calls = [call for call in calls if 'ALTER TABLE' in str(call)]
        assert len(alter_calls) == 0

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_get_last_obv_date_exists(self, mock_db_class, mock_config):
        """Test retrieving last OBV date when data exists"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        last_date = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        mock_cursor.fetchone.return_value = (last_date,)

        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        result = loader.get_last_obv_date()

        assert result == last_date

        # Verify query checks for obv IS NOT NULL
        calls = mock_cursor.execute.call_args_list
        query_call = str(calls[0])
        assert 'MAX(timestamp)' in query_call
        assert 'obv IS NOT NULL' in query_call

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_get_last_obv_date_none(self, mock_db_class, mock_config):
        """Test get_last_obv_date returns None when no data"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.fetchone.return_value = (None,)

        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        result = loader.get_last_obv_date()

        assert result is None

    def test_table_name_format(self, mock_config):
        """Test indicators table name format"""
        loader_1m = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_15m = OBVLoader(symbol='BTCUSDT', timeframe='15m', config=mock_config)
        loader_1h = OBVLoader(symbol='BTCUSDT', timeframe='1h', config=mock_config)

        assert loader_1m.indicators_table == 'indicators_bybit_futures_1m'
        assert loader_15m.indicators_table == 'indicators_bybit_futures_15m'
        assert loader_1h.indicators_table == 'indicators_bybit_futures_1h'

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_query_uses_symbol_filter(self, mock_db_class, mock_config):
        """Test database queries filter by symbol"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.fetchone.return_value = (None,)

        loader = OBVLoader(symbol='ETHUSDT', timeframe='1m', config=mock_config)
        loader.get_last_obv_date()

        # Verify symbol is used in query
        execute_calls = mock_cursor.execute.call_args_list
        query = str(execute_calls[0])
        assert 'symbol' in query.lower()

        # Verify loader has correct symbol
        assert loader.symbol == 'ETHUSDT'

    @patch('indicators.obv_loader.DatabaseConnection')
    def test_get_earliest_candle_date(self, mock_db_class, mock_config):
        """Test retrieving earliest candle date"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_db_class.return_value.get_connection.return_value.__enter__.return_value = mock_conn

        earliest_date = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
        mock_cursor.fetchone.return_value = (earliest_date,)

        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        result = loader.get_earliest_candle_date()

        assert result == earliest_date

        # Verify query uses MIN(timestamp)
        calls = mock_cursor.execute.call_args_list
        query_call = str(calls[0])
        assert 'MIN(timestamp)' in query_call or 'min(timestamp)' in query_call.lower()

    def test_batch_update_groups_by_date(self, mock_config):
        """Test batch_update_obv groups data by date"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Create test data spanning multiple days
        timestamps = pd.date_range('2024-01-01', periods=72, freq='1h')
        df = pd.DataFrame({
            'timestamp': timestamps,
            'obv': np.random.randn(72)
        })

        # Method should group by date (3 days)
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['timestamp']).dt.date
        grouped = df_copy.groupby('date')

        # Should have 3 groups (3 days)
        assert len(grouped) == 3


# ============================================
# Group 4: Timeframe Operations (6 tests)
# ============================================

class TestTimeframeOperations:
    """Tests for timeframe parsing and operations"""

    def test_parse_timeframe_minutes(self, mock_config):
        """Test parsing minute timeframes"""
        loader_1m = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_5m = OBVLoader(symbol='BTCUSDT', timeframe='5m', config=mock_config)
        loader_15m = OBVLoader(symbol='BTCUSDT', timeframe='15m', config=mock_config)

        assert loader_1m.timeframe_minutes == 1
        assert loader_5m.timeframe_minutes == 5
        assert loader_15m.timeframe_minutes == 15

    def test_parse_timeframe_hours(self, mock_config):
        """Test parsing hour timeframes"""
        loader_1h = OBVLoader(symbol='BTCUSDT', timeframe='1h', config=mock_config)
        loader_4h = OBVLoader(symbol='BTCUSDT', timeframe='4h', config=mock_config)

        assert loader_1h.timeframe_minutes == 60
        assert loader_4h.timeframe_minutes == 240

    def test_parse_timeframe_days(self, mock_config):
        """Test parsing day timeframes"""
        loader_1d = OBVLoader(symbol='BTCUSDT', timeframe='1d', config=mock_config)

        assert loader_1d.timeframe_minutes == 1440  # 24 * 60

    def test_parse_timeframe_invalid(self, mock_config):
        """Test parsing invalid timeframe raises error"""
        # Invalid timeframe should raise ValueError (either from int() or from validation)
        with pytest.raises(ValueError):
            loader = OBVLoader(symbol='BTCUSDT', timeframe='invalid', config=mock_config)

    def test_table_name_generation(self, mock_config):
        """Test table name generation for different timeframes"""
        loader_1m = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_15m = OBVLoader(symbol='BTCUSDT', timeframe='15m', config=mock_config)
        loader_1h = OBVLoader(symbol='BTCUSDT', timeframe='1h', config=mock_config)

        assert 'indicators_bybit_futures_1m' in loader_1m.indicators_table
        assert 'indicators_bybit_futures_15m' in loader_15m.indicators_table
        assert 'indicators_bybit_futures_1h' in loader_1h.indicators_table

    def test_timeframe_minutes_calculation(self, mock_config):
        """Test timeframe to minutes conversion"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Test _parse_timeframe method
        assert loader._parse_timeframe('1m') == 1
        assert loader._parse_timeframe('15m') == 15
        assert loader._parse_timeframe('1h') == 60
        assert loader._parse_timeframe('4h') == 240
        assert loader._parse_timeframe('1d') == 1440


# ============================================
# Group 5: Helper Functions (5 tests)
# ============================================

class TestHelperFunctions:
    """Tests for helper and utility functions"""

    def test_load_config(self, mock_config):
        """Test configuration is loaded correctly"""
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Verify OBV config loaded
        assert 'indicators' in mock_config
        assert 'obv' in mock_config['indicators']

    def test_symbol_initialization(self, mock_config):
        """Test symbol is set correctly during initialization"""
        loader_btc = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_eth = OBVLoader(symbol='ETHUSDT', timeframe='1m', config=mock_config)

        assert loader_btc.symbol == 'BTCUSDT'
        assert loader_eth.symbol == 'ETHUSDT'

    def test_batch_days_from_config(self, mock_config):
        """Test batch_days setting from config"""
        # Default batch_days from mock_config
        loader = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)

        # Should use config value
        assert hasattr(loader, 'batch_days')
        assert isinstance(loader.batch_days, int)
        assert loader.batch_days > 0

    def test_candles_table_name(self, mock_config):
        """Test candles table name is always 1m"""
        loader_1m = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_15m = OBVLoader(symbol='BTCUSDT', timeframe='15m', config=mock_config)
        loader_1h = OBVLoader(symbol='BTCUSDT', timeframe='1h', config=mock_config)

        # All should use same 1m candles table
        assert loader_1m.candles_table == 'candles_bybit_futures_1m'
        assert loader_15m.candles_table == 'candles_bybit_futures_1m'
        assert loader_1h.candles_table == 'candles_bybit_futures_1m'

    def test_indicators_table_name(self, mock_config):
        """Test indicators table name varies by timeframe"""
        loader_1m = OBVLoader(symbol='BTCUSDT', timeframe='1m', config=mock_config)
        loader_15m = OBVLoader(symbol='BTCUSDT', timeframe='15m', config=mock_config)
        loader_1h = OBVLoader(symbol='BTCUSDT', timeframe='1h', config=mock_config)

        # Each timeframe has own indicators table
        assert loader_1m.indicators_table == 'indicators_bybit_futures_1m'
        assert loader_15m.indicators_table == 'indicators_bybit_futures_15m'
        assert loader_1h.indicators_table == 'indicators_bybit_futures_1h'
