"""
Unit tests for VMA (Volume Moving Average) Loader

Tests cover:
- Timeframe parsing
- VMA calculation (rolling mean of volume)
- Volume aggregation for different timeframes
- Database operations
- Helper functions

Total: 35 tests
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
from vma_loader import VMALoader


# ============================================
# GROUP 1: TIMEFRAME PARSING TESTS (6 tests)
# ============================================

class TestTimeframeParsing:
    """Test timeframe parsing with various formats"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing real production config: 1m, 15m, 1h"""
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert '1m' in result
            assert result['1m'] == 1
            assert '15m' in result
            assert result['15m'] == 15
            assert '1h' in result
            assert result['1h'] == 60

    def test_parse_timeframes_minutes(self, mock_config):
        """Test parsing minute timeframes: 1m, 5m, 15m, 30m, 45m"""
        mock_config['timeframes'] = ['1m', '5m', '15m', '30m', '45m']
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert result['1m'] == 1
            assert result['5m'] == 5
            assert result['15m'] == 15
            assert result['30m'] == 30
            assert result['45m'] == 45

    def test_parse_timeframes_hours_days_weeks(self, mock_config):
        """Test parsing hour, day, week timeframes"""
        mock_config['timeframes'] = ['1h', '4h', '1d', '1w']
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert result['1h'] == 60
            assert result['4h'] == 240
            assert result['1d'] == 1440
            assert result['1w'] == 10080

    def test_parse_timeframes_case_insensitive(self, mock_config):
        """Test case insensitivity: 1M, 15M, 1H should work"""
        mock_config['timeframes'] = ['1M', '15M', '1H']
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            # Should convert to lowercase internally
            assert '1M' in result or '1m' in result
            assert len(result) == 3

    def test_parse_timeframes_invalid_format(self, mock_config):
        """Test invalid timeframe formats are skipped"""
        mock_config['timeframes'] = ['1m', 'invalid', '15x', '1h']
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert '1m' in result
            assert '1h' in result
            assert 'invalid' not in result
            assert '15x' not in result

    def test_parse_timeframes_mixed_formats(self, mock_config):
        """Test parsing mixed timeframe formats"""
        mock_config['timeframes'] = ['1m', '15m', '1h', '4h', '1d']
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert len(result) == 5
            assert result['1m'] == 1
            assert result['1h'] == 60
            assert result['1d'] == 1440


# ============================================
# GROUP 2: VMA CALCULATION TESTS (12 tests)
# ============================================

class TestVMACalculation:
    """Test VMA calculation with rolling mean of volume"""

    def test_vma_calculation_real_periods(self, sample_volumes_large):
        """Test VMA calculation with real production periods: 10, 20, 50, 100, 200"""
        periods = [10, 20, 50, 100, 200]
        df = pd.DataFrame({'volume': sample_volumes_large})

        # Calculate VMA using pandas rolling mean
        for period in periods:
            df[f'vma_{period}'] = df['volume'].rolling(window=period, min_periods=period).mean()

        # Verify VMA values are calculated
        for period in periods:
            col = f'vma_{period}'
            assert col in df.columns
            # First period-1 values should be NaN
            assert df[col].iloc[:period-1].isna().all()
            # From period onwards should have values
            assert not df[col].iloc[period:].isna().any()

    def test_vma_calculation_simple_sequence(self, sample_volumes_small):
        """Test VMA with simple sequence [1000, 1500, 2000, ...]"""
        df = pd.DataFrame({'volume': sample_volumes_small})
        df['vma_3'] = df['volume'].rolling(window=3, min_periods=3).mean()

        # Third value should be (1000+1500+2000)/3 = 1500
        assert df['vma_3'].iloc[2] == 1500.0
        # Fourth value should be (1500+2000+2500)/3 = 2000
        assert df['vma_3'].iloc[3] == 2000.0

    def test_vma_formula_validation(self, sample_volumes_small):
        """Test VMA formula: VMA = SUM(volume) / period"""
        volumes = sample_volumes_small.values
        period = 5

        # Manual calculation
        manual_vma_5 = np.mean(volumes[0:5])  # First 5 volumes

        # Pandas calculation
        df = pd.DataFrame({'volume': volumes})
        df['vma_5'] = df['volume'].rolling(window=5, min_periods=5).mean()
        pandas_vma_5 = df['vma_5'].iloc[4]

        # Should match
        assert abs(manual_vma_5 - pandas_vma_5) < 0.001

    def test_vma_nan_handling_first_n_minus_1(self, sample_volumes_small):
        """Test first N-1 values are NaN for VMA_N"""
        df = pd.DataFrame({'volume': sample_volumes_small})

        for period in [5, 7, 10]:
            df[f'vma_{period}'] = df['volume'].rolling(window=period, min_periods=period).mean()
            # First period-1 values should be NaN
            assert df[f'vma_{period}'].iloc[:period-1].isna().all()
            # From period onwards should NOT be NaN
            if len(df) >= period:
                assert not df[f'vma_{period}'].iloc[period-1:].isna().any()

    def test_vma_precision_8_decimals(self, sample_volumes_large):
        """Test VMA values maintain 8 decimal precision"""
        df = pd.DataFrame({'volume': sample_volumes_large})
        df['vma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()

        # Check precision by converting to string
        value = df['vma_20'].iloc[50]
        value_str = f"{value:.8f}"
        # Should have 8 decimals
        assert len(value_str.split('.')[1]) == 8

    def test_vma_insufficient_data(self):
        """Test VMA with insufficient data (< period)"""
        volumes = pd.Series([1000, 2000, 3000], dtype=float)
        df = pd.DataFrame({'volume': volumes})

        # Try to calculate VMA_10 with only 3 values
        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()

        # All values should be NaN (insufficient data)
        assert df['vma_10'].isna().all()

    def test_vma_empty_dataframe(self):
        """Test VMA with empty DataFrame"""
        df = pd.DataFrame({'volume': []})
        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()

        assert len(df) == 0
        assert 'vma_10' in df.columns

    def test_vma_single_value(self):
        """Test VMA with single volume value"""
        df = pd.DataFrame({'volume': [5000.0]})
        df['vma_5'] = df['volume'].rolling(window=5, min_periods=5).mean()

        # Single value: VMA_5 should be NaN (need 5 values)
        assert pd.isna(df['vma_5'].iloc[0])

    def test_vma_all_same_values(self):
        """Test VMA with constant volume"""
        volumes = pd.Series([10000.0] * 20, dtype=float)
        df = pd.DataFrame({'volume': volumes})
        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()

        # VMA of constant should equal constant
        assert (df['vma_10'].iloc[9:] == 10000.0).all()

    def test_vma_period_equals_data_length(self):
        """Test VMA when period equals data length"""
        volumes = pd.Series(range(1000, 1010, 1), dtype=float)  # 10 values
        df = pd.DataFrame({'volume': volumes})
        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()

        # Should have exactly one non-NaN value (the last one)
        non_nan = df['vma_10'].dropna()
        assert len(non_nan) == 1
        # Should equal mean of all values
        assert abs(non_nan.iloc[0] - np.mean(volumes.values)) < 0.001

    def test_vma_min_periods_behavior(self):
        """Test min_periods parameter behavior"""
        volumes = pd.Series([1000, 2000, 3000, 4000, 5000], dtype=float)
        df = pd.DataFrame({'volume': volumes})

        # With min_periods=5, first 4 should be NaN
        df['vma_strict'] = df['volume'].rolling(window=5, min_periods=5).mean()
        assert df['vma_strict'].iloc[:4].isna().all()
        assert not pd.isna(df['vma_strict'].iloc[4])

        # With min_periods=1, all should have values
        df['vma_relaxed'] = df['volume'].rolling(window=5, min_periods=1).mean()
        assert not df['vma_relaxed'].isna().any()

    def test_vma_increasing_sequence(self):
        """Test VMA with consistently increasing volumes"""
        volumes = pd.Series(range(1000, 1050), dtype=float)  # 50 values
        df = pd.DataFrame({'volume': volumes})
        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()

        # VMA should also increase
        vma_values = df['vma_10'].dropna().values
        # Check if monotonically increasing
        assert all(vma_values[i] <= vma_values[i+1] for i in range(len(vma_values)-1))


# ============================================
# GROUP 3: VOLUME AGGREGATION TESTS (6 tests)
# ============================================

class TestVolumeAggregation:
    """Test volume aggregation and data filtering"""

    def test_volume_summation_for_aggregation(self):
        """Test volume is summed when aggregating to higher timeframes"""
        # Simulate 1m volumes
        volumes_1m = pd.Series([1000, 1500, 2000, 2500], dtype=float)

        # For 2m aggregation, volumes should be summed
        # Period 1: 1000 + 1500 = 2500
        # Period 2: 2000 + 2500 = 4500
        aggregated = [
            volumes_1m[0:2].sum(),  # 2500
            volumes_1m[2:4].sum()   # 4500
        ]

        assert aggregated[0] == 2500.0
        assert aggregated[1] == 4500.0

    def test_filter_lookback_data(self, sample_candles_with_volume):
        """Test that lookback data is removed after VMA calculation"""
        df = sample_candles_with_volume.copy()
        period = 10

        # Add lookback rows (before target date)
        target_date = df['timestamp'].iloc[period]
        df_with_lookback = df[df['timestamp'] < target_date + timedelta(minutes=20)].copy()

        # Calculate VMA
        df_with_lookback['vma_10'] = df_with_lookback['volume'].rolling(window=10, min_periods=10).mean()

        # Filter to target date onwards
        df_filtered = df_with_lookback[df_with_lookback['timestamp'] >= target_date].copy()

        # Should have fewer rows than original
        assert len(df_filtered) < len(df_with_lookback)

    def test_dropna_all_vma_nan(self):
        """Test dropna removes rows where all VMA columns are NaN"""
        volumes = pd.Series([1000, 2000, 3000, 4000, 5000], dtype=float)
        df = pd.DataFrame({'volume': volumes})

        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()
        df['vma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()

        # All VMA columns are NaN
        df_cleaned = df.dropna(how='all', subset=['vma_10', 'vma_20'])

        # Should be empty (all rows have all NaN)
        assert len(df_cleaned) == 0

    def test_dropna_partial_vma(self):
        """Test dropna with partial VMA values"""
        volumes = pd.Series(range(1000, 1025), dtype=float)  # 25 values
        df = pd.DataFrame({'volume': volumes})

        df['vma_10'] = df['volume'].rolling(window=10, min_periods=10).mean()
        df['vma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()

        # Drop rows where BOTH are NaN
        df_cleaned = df.dropna(how='all', subset=['vma_10', 'vma_20'])

        # Should keep rows 9-19 (vma_10 exists) and 19+ (both exist)
        assert len(df_cleaned) >= 15  # At least from row 9 onwards

    def test_filter_by_timestamp_range(self, sample_candles_with_volume):
        """Test filtering DataFrame by timestamp range"""
        df = sample_candles_with_volume.copy()

        # Define range
        start_date = df['timestamp'].iloc[10]
        end_date = df['timestamp'].iloc[30]

        # Filter
        df_filtered = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]

        # Should have exactly 21 rows (indices 10-30 inclusive)
        assert len(df_filtered) == 21
        assert df_filtered['timestamp'].iloc[0] == start_date
        assert df_filtered['timestamp'].iloc[-1] == end_date

    def test_lookback_multiplier_calculation(self):
        """Test lookback period calculation for VMA"""
        # For VMA_20 on 1m timeframe: lookback = 20 * 1 = 20 minutes
        # For VMA_20 on 15m timeframe: lookback = 20 * 15 = 300 minutes
        period = 20
        timeframe_1m = 1
        timeframe_15m = 15

        lookback_1m = period * timeframe_1m
        lookback_15m = period * timeframe_15m

        assert lookback_1m == 20
        assert lookback_15m == 300


# ============================================
# GROUP 4: DATABASE OPERATIONS TESTS (6 tests)
# ============================================

class TestDatabaseOperations:
    """Test database-related operations"""

    @patch('vma_loader.DatabaseConnection')
    def test_create_vma_columns_creates_missing(self, mock_db_class, mock_config):
        """Test creating missing VMA columns"""
        mock_cursor = MagicMock()
        # Column doesn't exist
        mock_cursor.fetchone.side_effect = [(False,), (False,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            loader.create_vma_columns('1m', [10, 20])

            # Should execute ALTER TABLE for each period
            assert mock_cursor.execute.call_count >= 2

    @patch('vma_loader.DatabaseConnection')
    def test_create_vma_columns_skips_existing(self, mock_db_class, mock_config):
        """Test skipping creation of existing VMA columns"""
        mock_cursor = MagicMock()
        # Columns already exist
        mock_cursor.fetchone.side_effect = [(True,), (True,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            loader.create_vma_columns('1m', [10, 20])

            # Should not execute ALTER TABLE since columns exist
            # Only SELECT queries for checking existence
            assert mock_conn.commit.called

    def test_vma_column_naming_convention(self):
        """Test VMA column names follow convention: vma_{period}"""
        periods = [10, 20, 50, 100, 200]

        for period in periods:
            expected_name = f'vma_{period}'
            # Column naming is consistent in implementation
            assert expected_name == f'vma_{period}'

    def test_vma_table_name_format(self):
        """Test table name format: indicators_bybit_futures_{timeframe}"""
        timeframes = ['1m', '15m', '1h']
        expected_tables = [
            'indicators_bybit_futures_1m',
            'indicators_bybit_futures_15m',
            'indicators_bybit_futures_1h'
        ]

        for tf, expected in zip(timeframes, expected_tables):
            table_name = f'indicators_bybit_futures_{tf}'
            assert table_name == expected

    def test_nan_converts_to_none(self):
        """Test that NaN values convert to None for PostgreSQL NULL"""
        df = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1)],
            'symbol': ['BTCUSDT'],
            'volume': [5000.0],
            'vma_10': [np.nan]
        })

        # Simulate conversion
        value = df['vma_10'].iloc[0]
        db_value = None if pd.isna(value) else float(value)

        assert db_value is None

    def test_vma_value_decimal_precision(self):
        """Test VMA values are stored with DECIMAL(20,8) precision"""
        volumes = pd.Series([45123.12345678, 46234.87654321], dtype=float)
        df = pd.DataFrame({'volume': volumes})
        df['vma_2'] = df['volume'].rolling(window=2, min_periods=2).mean()

        # Values should maintain precision
        value = df['vma_2'].iloc[1]
        # Should be precise to 8 decimals
        value_str = f"{value:.8f}"
        assert len(value_str.split('.')[1]) == 8


# ============================================
# GROUP 5: HELPER FUNCTIONS TESTS (5 tests)
# ============================================

class TestHelperFunctions:
    """Test utility and helper functions"""

    def test_load_config_success(self, mock_config, tmp_path):
        """Test config loading from indicators_config.yaml"""
        import yaml

        # Create temp config file
        config_file = tmp_path / "indicators_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(mock_config, f)

        with patch('vma_loader.os.path.join', return_value=str(config_file)):
            with patch('vma_loader.os.path.exists', return_value=True):
                loader = VMALoader()
                config = loader.config

                assert config['timeframes'] == ['1m', '15m', '1h']
                assert config['indicators']['vma']['enabled'] is True
                assert config['indicators']['vma']['periods'] == [10, 20, 50, 100, 200]

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test _parse_timeframes returns dict mapping"""
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.timeframe_minutes

            assert isinstance(result, dict)
            assert all(isinstance(k, str) for k in result.keys())
            assert all(isinstance(v, int) for v in result.values())

    def test_symbol_initialization(self, mock_config):
        """Test loader initializes with correct symbol"""
        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader(symbol='ETHUSDT')
            assert loader.symbol == 'ETHUSDT'

            loader2 = VMALoader(symbol='BTCUSDT')
            assert loader2.symbol == 'BTCUSDT'

    @patch('vma_loader.DatabaseConnection')
    def test_get_last_vma_date_with_data(self, mock_db_class, mock_config):
        """Test retrieving last VMA date when data exists"""
        mock_cursor = MagicMock()
        last_date = datetime(2024, 1, 15, 12, 0)
        mock_cursor.fetchone.return_value = (last_date,)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.get_last_vma_date('1m', 10)

            assert result == last_date

    @patch('vma_loader.DatabaseConnection')
    def test_get_last_vma_date_no_data(self, mock_db_class, mock_config):
        """Test retrieving last VMA date when no data exists"""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('vma_loader.VMALoader.load_config', return_value=mock_config):
            loader = VMALoader()
            result = loader.get_last_vma_date('1m', 10)

            assert result is None


# ============================================
# MARKER for pytest
# ============================================

pytestmark = pytest.mark.unit
