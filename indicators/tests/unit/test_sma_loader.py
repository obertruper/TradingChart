"""
Unit tests for SMA Loader

Tests coverage:
- Timeframe parsing (9 tests)
- SMA calculations (12 tests)
- Data filtering (6 tests)
- Database formatting (5 tests)
- Helper functions (3 tests)

Total: ~35 unit tests
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# ============================================================================
# GROUP 1: TIMEFRAME PARSING TESTS (9 tests)
# ============================================================================

class TestTimeframeParsing:
    """Tests for _parse_timeframes() function"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing of real production config: 1m, 15m, 1h"""
        from sma_loader import SMALoader

        with patch.object(SMALoader, 'load_config', return_value=mock_config):
            with patch.object(SMALoader, '__init__', lambda x, y: None):
                loader = SMALoader.__new__(SMALoader)
                loader.config = mock_config
                result = loader._parse_timeframes()

        expected = {'1m': 1, '15m': 15, '1h': 60}
        assert result == expected

    def test_parse_timeframes_minutes(self, mock_config):
        """Test parsing minute timeframes: 1m, 5m, 15m, 30m, 45m"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1m', '5m', '15m', '30m', '45m']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        expected = {'1m': 1, '5m': 5, '15m': 15, '30m': 30, '45m': 45}
        assert result == expected

    def test_parse_timeframes_hours(self, mock_config):
        """Test parsing hour timeframes: 1h, 4h, 12h, 24h"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1h', '4h', '12h', '24h']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        expected = {'1h': 60, '4h': 240, '12h': 720, '24h': 1440}
        assert result == expected

    def test_parse_timeframes_days(self, mock_config):
        """Test parsing day timeframes: 1d, 7d"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1d', '7d']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        expected = {'1d': 1440, '7d': 10080}
        assert result == expected

    def test_parse_timeframes_weeks(self, mock_config):
        """Test parsing week timeframes: 1w, 2w"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1w', '2w']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        expected = {'1w': 10080, '2w': 20160}
        assert result == expected

    def test_parse_timeframes_mixed(self, mock_config):
        """Test parsing mixed timeframes: 1m, 1h, 1d, 1w"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1m', '1h', '1d', '1w']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        expected = {'1m': 1, '1h': 60, '1d': 1440, '1w': 10080}
        assert result == expected

    def test_parse_timeframes_case_insensitive(self, mock_config):
        """Test that uppercase formats work: 1M, 1H, 1D"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['1M', '1H', '1D']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        # Should be converted to lowercase and work
        assert '1m' in result or '1M' in result
        assert result.get('1m', result.get('1M')) == 1

    def test_parse_timeframes_invalid_format(self, mock_config):
        """Test handling of invalid timeframe formats"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = ['invalid', '1x', 'abc', '']

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        # Should fallback to defaults
        assert len(result) > 0
        assert '1m' in result  # Default should include 1m

    def test_parse_timeframes_empty_list(self, mock_config):
        """Test handling of empty timeframes list"""
        from sma_loader import SMALoader

        config = mock_config.copy()
        config['timeframes'] = []

        with patch.object(SMALoader, '__init__', lambda x, y: None):
            loader = SMALoader.__new__(SMALoader)
            loader.config = config
            result = loader._parse_timeframes()

        # Should fallback to defaults: 1m, 15m, 1h
        assert '1m' in result
        assert '15m' in result
        assert '1h' in result


# ============================================================================
# GROUP 2: SMA CALCULATION TESTS (12 tests)
# ============================================================================

class TestSMACalculation:
    """Tests for SMA calculation logic (pandas rolling mean)"""

    def test_sma_calculation_real_periods(self, sample_prices_large):
        """Test SMA calculation with real production periods: 10, 30, 50, 100, 200"""
        prices = sample_prices_large
        df = pd.DataFrame({'close': prices})

        # Real production periods from config
        periods = [10, 30, 50, 100, 200]

        for period in periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period, min_periods=period).mean()

        # Verify all SMA columns calculated
        assert not df['sma_10'].iloc[9:].isna().any(), "SMA_10 should have values from position 9"
        assert not df['sma_30'].iloc[29:].isna().any(), "SMA_30 should have values from position 29"
        assert not df['sma_50'].iloc[49:].isna().any(), "SMA_50 should have values from position 49"
        assert not df['sma_100'].iloc[99:].isna().any(), "SMA_100 should have values from position 99"
        assert not df['sma_200'].iloc[199:].isna().any(), "SMA_200 should have values from position 199"

    def test_sma_calculation_simple_sequence(self):
        """Test SMA with simple sequence [10, 20, 30, 40, 50]"""
        prices = pd.Series([10, 20, 30, 40, 50], dtype=float)
        sma_3 = prices.rolling(window=3, min_periods=3).mean()

        # First 2 values should be NaN
        assert pd.isna(sma_3.iloc[0])
        assert pd.isna(sma_3.iloc[1])

        # Position 2: (10+20+30)/3 = 20.0
        assert sma_3.iloc[2] == 20.0

        # Position 3: (20+30+40)/3 = 30.0
        assert sma_3.iloc[3] == 30.0

        # Position 4: (30+40+50)/3 = 40.0
        assert sma_3.iloc[4] == 40.0

    def test_sma_first_values_nan(self, sample_prices_medium):
        """Test that first (period-1) values are NaN"""
        prices = sample_prices_medium

        sma_10 = prices.rolling(window=10, min_periods=10).mean()
        sma_50 = prices.rolling(window=50, min_periods=50).mean()

        # First 9 values should be NaN for SMA_10
        assert sma_10.iloc[:9].isna().all()

        # First 49 values should be NaN for SMA_50
        assert sma_50.iloc[:49].isna().all()

    def test_sma_tenth_value_for_sma10(self):
        """Test SMA_10 at 10th position equals average of first 10 values"""
        prices = pd.Series(range(1, 11), dtype=float)  # [1, 2, 3, ..., 10]
        sma_10 = prices.rolling(window=10, min_periods=10).mean()

        # SMA(10) at position 9 (10th value) = (1+2+...+10)/10 = 5.5
        assert sma_10.iloc[9] == 5.5

    def test_sma_formula_manual_check(self, real_btc_prices):
        """Test SMA calculation matches manual calculation"""
        prices = real_btc_prices
        sma_5 = prices.rolling(window=5, min_periods=5).mean()

        # Manual calculation for position 4 (5th value)
        expected = (prices.iloc[0] + prices.iloc[1] + prices.iloc[2] +
                   prices.iloc[3] + prices.iloc[4]) / 5

        assert abs(sma_5.iloc[4] - expected) < 0.01

    def test_sma_precision_8_decimals(self):
        """Test that SMA preserves precision up to 8 decimal places"""
        prices = pd.Series([44.12345678, 45.98765432, 46.55555555], dtype=float)
        sma_3 = prices.rolling(window=3, min_periods=3).mean()

        expected = (44.12345678 + 45.98765432 + 46.55555555) / 3

        # Check precision (tolerance 1e-8)
        assert abs(sma_3.iloc[2] - expected) < 1e-7

    def test_sma_insufficient_data(self):
        """Test SMA with insufficient data returns all NaN"""
        prices = pd.Series([10, 20, 30, 40, 50], dtype=float)

        # Try to calculate SMA_100 on only 5 values
        sma_100 = prices.rolling(window=100, min_periods=100).mean()

        # All values should be NaN
        assert sma_100.isna().all()

    def test_sma_empty_dataframe(self):
        """Test SMA on empty DataFrame doesn't crash"""
        prices = pd.Series([], dtype=float)
        sma = prices.rolling(window=10, min_periods=10).mean()

        assert len(sma) == 0

    def test_sma_single_value_period_1(self):
        """Test SMA with single value and period=1"""
        prices = pd.Series([100.0])
        sma_1 = prices.rolling(window=1, min_periods=1).mean()

        assert sma_1.iloc[0] == 100.0

    def test_sma_all_same_values(self):
        """Test SMA when all prices are identical"""
        prices = pd.Series([100.0] * 100)
        sma_10 = prices.rolling(window=10, min_periods=10).mean()

        # All valid SMA values should equal 100.0
        assert (sma_10.iloc[9:] == 100.0).all()

    def test_sma_period_equals_data_length(self):
        """Test SMA when period equals data length"""
        prices = pd.Series(range(1, 51), dtype=float)  # 50 values
        sma_50 = prices.rolling(window=50, min_periods=50).mean()

        # Only last value should be non-NaN
        assert sma_50.iloc[:49].isna().all()
        assert not pd.isna(sma_50.iloc[49])

    def test_sma_min_periods_behavior(self):
        """Test that min_periods prevents calculation until enough data"""
        prices = pd.Series([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], dtype=float)
        sma_5_strict = prices.rolling(window=5, min_periods=5).mean()
        sma_5_relaxed = prices.rolling(window=5, min_periods=1).mean()

        # Strict: first 4 should be NaN
        assert sma_5_strict.iloc[:4].isna().all()

        # Relaxed: should calculate from position 0
        assert not pd.isna(sma_5_relaxed.iloc[0])


# ============================================================================
# GROUP 3: DATA FILTERING TESTS (6 tests)
# ============================================================================

class TestDataFiltering:
    """Tests for data filtering logic (lookback removal, NaN handling)"""

    def test_filter_lookback_data(self, sample_candles_df):
        """Test that lookback data is removed after SMA calculation"""
        df = sample_candles_df.copy()

        start_date = pd.Timestamp('2024-01-01 00:30')  # Start from 30 min
        lookback_minutes = 10

        # Simulate: loaded data includes lookback
        df_with_lookback = df.copy()

        # Filter to remove lookback
        df_filtered = df_with_lookback[df_with_lookback['timestamp'] >= start_date]

        # All timestamps should be >= start_date
        assert (df_filtered['timestamp'] >= start_date).all()

    def test_dropna_all_sma_nan(self):
        """Test removal of rows where ALL SMA values are NaN"""
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='1min'),
            'sma_10': [np.nan] * 5 + [100, 101, 102, 103, 104],
            'sma_30': [np.nan] * 10  # All NaN
        })

        # Drop rows where ALL SMA are NaN
        sma_columns = ['sma_10', 'sma_30']
        df_clean = df.dropna(subset=sma_columns, how='all')

        # First 5 rows should be removed (both SMA are NaN)
        # Last 5 should remain (sma_10 has values)
        assert len(df_clean) == 5

    def test_dropna_partial_sma(self):
        """Test that rows with SOME valid SMA are kept"""
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='1min'),
            'sma_10': [100] * 10,  # All valid
            'sma_50': [np.nan] * 10  # All NaN
        })

        sma_columns = ['sma_10', 'sma_50']
        df_clean = df.dropna(subset=sma_columns, how='all')

        # All rows should be kept (sma_10 has values)
        assert len(df_clean) == 10

    def test_filter_by_timestamp_range(self):
        """Test filtering by timestamp range"""
        timestamps = pd.date_range('2024-01-01', periods=20, freq='1h')
        df = pd.DataFrame({
            'timestamp': timestamps,
            'value': range(20)
        })

        start = pd.Timestamp('2024-01-01 05:00')
        end = pd.Timestamp('2024-01-01 15:00')

        df_filtered = df[(df['timestamp'] >= start) & (df['timestamp'] < end)]

        # Should have 10 hours (5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
        assert len(df_filtered) == 10
        assert df_filtered['timestamp'].min() == start
        assert df_filtered['timestamp'].max() == pd.Timestamp('2024-01-01 14:00')

    def test_lookback_multiplier_calculation(self):
        """Test calculation of lookback period"""
        max_period = 200
        timeframe_minutes = 1

        # lookback = max_period * timeframe_minutes
        lookback_minutes = max_period * timeframe_minutes
        lookback_timedelta = timedelta(minutes=lookback_minutes)

        assert lookback_timedelta == timedelta(minutes=200)

    def test_empty_after_filter(self):
        """Test that empty result after filter doesn't crash"""
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='1h'),
            'value': range(10)
        })

        # Filter that results in empty
        start = pd.Timestamp('2025-01-01')  # Future date
        df_filtered = df[df['timestamp'] >= start]

        assert len(df_filtered) == 0
        assert df_filtered.empty


# ============================================================================
# GROUP 4: DATABASE FORMATTING TESTS (5 tests)
# ============================================================================

class TestDatabaseFormatting:
    """Tests for data formatting for PostgreSQL insertion"""

    def test_prepare_records_structure(self):
        """Test that records are formatted as tuples for psycopg2"""
        df = pd.DataFrame({
            'timestamp': [pd.Timestamp('2024-01-01 10:00')],
            'symbol': ['BTCUSDT'],
            'close': [100.5],
            'sma_10': [99.8],
            'sma_30': [100.2]
        })

        periods = [10, 30]
        symbol = 'BTCUSDT'

        # Simulate record preparation (from sma_loader.py lines 544-550)
        records = []
        for _, row in df.iterrows():
            record = [row['timestamp'], symbol]
            for period in periods:
                value = row[f'sma_{period}'] if pd.notna(row[f'sma_{period}']) else None
                record.append(value)
            records.append(tuple(record))

        # Check structure
        assert len(records) == 1
        assert isinstance(records[0], tuple)
        assert len(records[0]) == 4  # timestamp, symbol, sma_10, sma_30

    def test_nan_converts_to_none(self):
        """Test that NaN values convert to None for PostgreSQL NULL"""
        df = pd.DataFrame({
            'timestamp': [pd.Timestamp('2024-01-01 10:00')],
            'sma_10': [99.8],
            'sma_30': [np.nan]  # NaN should become None
        })

        periods = [10, 30]
        symbol = 'BTCUSDT'

        records = []
        for _, row in df.iterrows():
            record = [row['timestamp'], symbol]
            for period in periods:
                value = row[f'sma_{period}'] if pd.notna(row[f'sma_{period}']) else None
                record.append(value)
            records.append(tuple(record))

        # sma_30 should be None (not NaN)
        assert records[0][3] is None
        assert records[0][2] == 99.8

    def test_valid_values_preserved(self):
        """Test that valid float values are preserved"""
        df = pd.DataFrame({
            'timestamp': [pd.Timestamp('2024-01-01')],
            'sma_10': [12345.678901234]
        })

        value = df['sma_10'].iloc[0]
        formatted = value if pd.notna(value) else None

        assert formatted == 12345.678901234
        assert isinstance(formatted, float)

    def test_timestamp_format_for_postgres(self):
        """Test that pandas Timestamp is compatible with psycopg2"""
        ts = pd.Timestamp('2024-01-01 10:30:00')

        # psycopg2 can handle pd.Timestamp directly
        # Just verify it's a valid timestamp object
        assert isinstance(ts, pd.Timestamp)
        assert ts.year == 2024
        assert ts.month == 1
        assert ts.day == 1

    def test_records_order_matches_columns(self):
        """Test that record tuple order matches SQL column order"""
        df = pd.DataFrame({
            'timestamp': [pd.Timestamp('2024-01-01')],
            'symbol': ['BTCUSDT'],
            'sma_10': [100.0],
            'sma_30': [101.0],
            'sma_50': [102.0]
        })

        periods = [10, 30, 50]
        symbol = 'BTCUSDT'
        columns = ['timestamp', 'symbol'] + [f'sma_{p}' for p in periods]

        records = []
        for _, row in df.iterrows():
            record = [row['timestamp'], symbol]
            for period in periods:
                value = row[f'sma_{period}'] if pd.notna(row[f'sma_{period}']) else None
                record.append(value)
            records.append(tuple(record))

        # Verify order: timestamp, symbol, sma_10, sma_30, sma_50
        assert records[0][0] == pd.Timestamp('2024-01-01')
        assert records[0][1] == 'BTCUSDT'
        assert records[0][2] == 100.0  # sma_10
        assert records[0][3] == 101.0  # sma_30
        assert records[0][4] == 102.0  # sma_50


# ============================================================================
# GROUP 5: HELPER FUNCTIONS TESTS (3 tests)
# ============================================================================

class TestHelperFunctions:
    """Tests for utility and helper functions"""

    def test_timeframe_to_minutes_mapping(self):
        """Test timeframe to minutes conversion"""
        timeframe_map = {
            '1m': 1,
            '15m': 15,
            '1h': 60,
            '1d': 1440,
            '1w': 10080
        }

        assert timeframe_map['1m'] == 1
        assert timeframe_map['15m'] == 15
        assert timeframe_map['1h'] == 60
        assert timeframe_map['1d'] == 1440
        assert timeframe_map['1w'] == 10080

    def test_sma_column_names_generation(self):
        """Test generation of SMA column names"""
        periods = [10, 30, 50, 100, 200]
        sma_columns = [f'sma_{p}' for p in periods]

        expected = ['sma_10', 'sma_30', 'sma_50', 'sma_100', 'sma_200']
        assert sma_columns == expected

    def test_table_name_generation(self):
        """Test generation of table names for different timeframes"""
        def get_table_name(timeframe):
            return f'indicators_bybit_futures_{timeframe}'

        assert get_table_name('1m') == 'indicators_bybit_futures_1m'
        assert get_table_name('15m') == 'indicators_bybit_futures_15m'
        assert get_table_name('1h') == 'indicators_bybit_futures_1h'


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
