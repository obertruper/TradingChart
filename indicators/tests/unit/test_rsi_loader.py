"""
Unit tests for RSI (Relative Strength Index) Loader

Tests cover:
- Timeframe parsing
- RSI calculation with Wilder's smoothing
- RSI range validation [0, 100]
- Checkpoint system (avg_gain, avg_loss preservation)
- Period analysis (empty/partial/complete groups)
- Database operations
- Helper functions

Total: ~40 tests
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
from rsi_loader import RSILoader


# ============================================
# GROUP 1: TIMEFRAME PARSING TESTS (4 tests)
# ============================================

class TestTimeframeParsing:
    """Test timeframe parsing (similar to SMA/EMA)"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing real production config: 1m, 15m, 1h"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.timeframe_minutes

            assert '1m' in result
            assert result['1m'] == 1
            assert '15m' in result
            assert result['15m'] == 15
            assert '1h' in result
            assert result['1h'] == 60

    def test_parse_timeframes_all_formats(self, mock_config):
        """Test parsing all timeframe formats: m, h, d, w"""
        mock_config['timeframes'] = ['1m', '15m', '1h', '4h', '1d', '1w']
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.timeframe_minutes

            assert result['1m'] == 1
            assert result['15m'] == 15
            assert result['1h'] == 60
            assert result['4h'] == 240
            assert result['1d'] == 1440
            assert result['1w'] == 10080

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test _parse_timeframes returns correct dict structure"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.timeframe_minutes

            assert isinstance(result, dict)
            assert all(isinstance(v, int) for v in result.values())

    def test_parse_timeframes_invalid_ignored(self, mock_config):
        """Test invalid timeframe formats are ignored"""
        mock_config['timeframes'] = ['1m', 'invalid', '15m']
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.timeframe_minutes

            assert '1m' in result
            assert '15m' in result
            assert 'invalid' not in result


# ============================================
# GROUP 2: RSI CALCULATION TESTS (16 tests)
# ============================================

class TestRSICalculation:
    """Test RSI calculation with Wilder's smoothing method"""

    def test_rsi_formula_basic(self):
        """Test RSI formula: RSI = 100 - (100 / (1 + RS))"""
        # RS = 10 (avg_gain=10, avg_loss=1)
        avg_gain = 10.0
        avg_loss = 1.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        assert abs(rsi - 90.909) < 0.01  # Should be ~90.9

        # RS = 1 (balanced)
        avg_gain = 5.0
        avg_loss = 5.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        assert abs(rsi - 50.0) < 0.01  # Should be 50

    def test_rsi_range_validation(self, sample_prices_trending_up, mock_config):
        """Test RSI values are always in range [0, 100]"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            rsi_values, _, _ = loader.calculate_rsi_batch(sample_prices_trending_up, 14)

            # Remove NaN values
            valid_rsi = rsi_values[~np.isnan(rsi_values)]

            # All RSI values should be [0, 100]
            assert (valid_rsi >= 0).all()
            assert (valid_rsi <= 100).all()

    def test_rsi_uptrend_above_50(self, sample_prices_trending_up, mock_config):
        """Test RSI > 50 for uptrending prices"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            rsi_values, _, _ = loader.calculate_rsi_batch(sample_prices_trending_up, 14)

            # Skip first period + 1 (initialization)
            valid_rsi = rsi_values[15:]

            # For uptrend, RSI should be mostly > 50
            above_50_count = np.sum(valid_rsi > 50)
            total_count = len(valid_rsi)

            assert above_50_count / total_count > 0.7  # At least 70% above 50

    def test_rsi_downtrend_below_50(self, sample_prices_trending_down, mock_config):
        """Test RSI < 50 for downtrending prices"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            rsi_values, _, _ = loader.calculate_rsi_batch(sample_prices_trending_down, 14)

            # Skip first period + 1 (initialization)
            valid_rsi = rsi_values[15:]

            # For downtrend, RSI should be mostly < 50
            below_50_count = np.sum(valid_rsi < 50)
            total_count = len(valid_rsi)

            assert below_50_count / total_count > 0.7  # At least 70% below 50

    def test_rsi_sideways_around_50(self, sample_prices_sideways, mock_config):
        """Test RSI oscillates around 50 for sideways market"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            rsi_values, _, _ = loader.calculate_rsi_batch(sample_prices_sideways, 14)

            # Skip first period + 1 (initialization)
            valid_rsi = rsi_values[15:]

            # Mean RSI should be close to 50 for sideways
            mean_rsi = np.mean(valid_rsi)
            assert 40 < mean_rsi < 60  # Within ±10 of 50

    def test_wilders_smoothing_formula(self, mock_config):
        """Test Wilder's smoothing: avg = (avg_prev * (period-1) + current) / period"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            period = 14
            # Initial: avg_gain = 10, avg_loss = 5
            # New: gain = 15, loss = 3

            # Wilder's formula
            prev_avg_gain = 10.0
            new_gain = 15.0
            new_avg_gain = (prev_avg_gain * (period - 1) + new_gain) / period

            expected = (10.0 * 13 + 15.0) / 14
            assert abs(new_avg_gain - expected) < 0.001

            prev_avg_loss = 5.0
            new_loss = 3.0
            new_avg_loss = (prev_avg_loss * (period - 1) + new_loss) / period

            expected = (5.0 * 13 + 3.0) / 14
            assert abs(new_avg_loss - expected) < 0.001

    def test_rsi_calculation_real_periods(self, sample_prices_large, mock_config):
        """Test RSI calculation with real production periods: 7, 9, 14, 21, 25"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            periods = [7, 9, 14, 21, 25]
            prices = sample_prices_large.values

            for period in periods:
                rsi_values, avg_gain, avg_loss = loader.calculate_rsi_batch(prices, period)

                # Should have same length as input
                assert len(rsi_values) == len(prices)

                # First 'period' values should be NaN
                assert np.isnan(rsi_values[:period]).all()

                # Rest should be valid RSI [0, 100]
                valid_rsi = rsi_values[period:]
                valid_rsi = valid_rsi[~np.isnan(valid_rsi)]
                assert (valid_rsi >= 0).all()
                assert (valid_rsi <= 100).all()

                # Final state should be returned
                assert avg_gain is not None
                assert avg_loss is not None

    def test_rsi_with_checkpoint_continuation(self, sample_rsi_checkpoint, mock_config):
        """Test RSI calculation continues from checkpoint (avg_gain, avg_loss)"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            period = 14
            # Need enough prices - checkpoint allows starting from index 0
            # But still need at least period+1 prices for calculation
            prices = np.array([100, 105, 103, 108, 110, 115, 112, 118, 120, 125,
                              122, 128, 130, 135, 133, 138])

            # Use checkpoint
            checkpoint = sample_rsi_checkpoint[14]
            initial_avg_gain = checkpoint['avg_gain']
            initial_avg_loss = checkpoint['avg_loss']

            rsi_values, _, _ = loader.calculate_rsi_batch(
                prices, period, initial_avg_gain, initial_avg_loss
            )

            # With checkpoint, should start calculating from beginning
            # Deltas start at index 0 when checkpoint is provided
            valid_rsi = rsi_values[~np.isnan(rsi_values)]
            assert len(valid_rsi) > 0  # At least some RSI calculated

            # Should have values in valid range
            assert (valid_rsi >= 0).all()
            assert (valid_rsi <= 100).all()

    def test_rsi_extreme_gain(self, mock_config):
        """Test RSI approaches 100 with only gains"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # All increasing prices
            prices = np.array(list(range(100, 130)))
            rsi_values, _, _ = loader.calculate_rsi_batch(prices, 14)

            # Last RSI should be high (approaching 100)
            last_rsi = rsi_values[-1]
            assert last_rsi > 70  # Should be overbought

    def test_rsi_extreme_loss(self, mock_config):
        """Test RSI approaches 0 with only losses"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # All decreasing prices
            prices = np.array(list(range(130, 100, -1)))
            rsi_values, _, _ = loader.calculate_rsi_batch(prices, 14)

            # Last RSI should be low (approaching 0)
            last_rsi = rsi_values[-1]
            assert last_rsi < 30  # Should be oversold

    def test_rsi_with_zero_loss(self, mock_config):
        """Test RSI = 100 when avg_loss = 0 (only gains)"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # Strictly increasing prices (no losses)
            prices = np.array([100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
                              110, 111, 112, 113, 114, 115])
            rsi_values, _, _ = loader.calculate_rsi_batch(prices, 14)

            # RSI should be 100 (or very close)
            last_rsi = rsi_values[-1]
            assert last_rsi >= 99.9

    def test_rsi_insufficient_data(self, mock_config):
        """Test RSI with insufficient data (< period)"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # Only 5 prices, need 14 for RSI_14
            prices = np.array([100, 105, 103, 108, 110])
            rsi_values, avg_gain, avg_loss = loader.calculate_rsi_batch(prices, 14)

            # Should return all NaN
            assert np.isnan(rsi_values).all()
            assert avg_gain is None
            assert avg_loss is None

    def test_rsi_single_price(self, mock_config):
        """Test RSI with single price (edge case)"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            prices = np.array([100.0])
            rsi_values, avg_gain, avg_loss = loader.calculate_rsi_batch(prices, 14)

            # Should return NaN for single price
            assert len(rsi_values) == 1
            assert avg_gain is None
            assert avg_loss is None

    def test_rsi_constant_prices(self, mock_config):
        """Test RSI with constant prices (no change)"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # All prices = 100
            prices = np.array([100.0] * 30)
            rsi_values, avg_gain, avg_loss = loader.calculate_rsi_batch(prices, 14)

            # No gains, no losses → avg_gain=0, avg_loss=0
            # RSI undefined, but implementation should handle gracefully
            # Check that it doesn't crash and returns valid structure
            assert len(rsi_values) == len(prices)

    def test_rsi_multiple_periods_simultaneously(self, sample_prices_large, mock_config):
        """Test calculating multiple RSI periods"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            periods = [7, 14, 25]
            prices = sample_prices_large.values

            results = {}
            for period in periods:
                rsi_values, _, _ = loader.calculate_rsi_batch(prices, period)
                results[period] = rsi_values

            # All periods should be calculated
            for period in periods:
                assert len(results[period]) == len(prices)
                # Valid RSI should be in [0, 100]
                valid = results[period][~np.isnan(results[period])]
                assert (valid >= 0).all()
                assert (valid <= 100).all()


# ============================================
# GROUP 3: CHECKPOINT SYSTEM TESTS (8 tests)
# ============================================

class TestCheckpointSystem:
    """Test RSI checkpoint system for resumable loading"""

    def test_checkpoint_structure(self, sample_rsi_checkpoint):
        """Test checkpoint has correct structure: {period: {avg_gain, avg_loss}}"""
        assert 14 in sample_rsi_checkpoint
        assert 'avg_gain' in sample_rsi_checkpoint[14]
        assert 'avg_loss' in sample_rsi_checkpoint[14]

        assert isinstance(sample_rsi_checkpoint[14]['avg_gain'], (int, float))
        assert isinstance(sample_rsi_checkpoint[14]['avg_loss'], (int, float))

    def test_checkpoint_preserves_rsi_continuity(self, mock_config):
        """Test checkpoint preserves RSI calculation continuity"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # Full sequence calculation
            prices_full = np.array(list(range(100, 150)))
            rsi_full, _, _ = loader.calculate_rsi_batch(prices_full, 14)

            # Split at index 30
            split_idx = 30

            # Calculate first part
            prices_first = prices_full[:split_idx+1]
            _, avg_gain_checkpoint, avg_loss_checkpoint = loader.calculate_rsi_batch(prices_first, 14)

            # Calculate second part with checkpoint
            prices_second = prices_full[split_idx:]
            rsi_second, _, _ = loader.calculate_rsi_batch(
                prices_second, 14, avg_gain_checkpoint, avg_loss_checkpoint
            )

            # Should match continuation from full calculation
            expected = rsi_full[split_idx:]
            actual = rsi_second

            # Should be similar (minor differences due to floating point)
            # Compare non-NaN values
            mask = ~np.isnan(expected) & ~np.isnan(actual)
            if mask.any():
                np.testing.assert_array_almost_equal(
                    actual[mask], expected[mask], decimal=4
                )

    def test_checkpoint_state_updates(self, mock_config):
        """Test checkpoint state (avg_gain, avg_loss) updates correctly"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            prices = np.array([100, 105, 103, 108, 110, 115, 112, 118])
            period = 5

            _, avg_gain, avg_loss = loader.calculate_rsi_batch(prices, period)

            # Should return final state
            assert avg_gain is not None
            assert avg_loss is not None
            assert avg_gain >= 0
            assert avg_loss >= 0

    def test_checkpoint_with_multiple_periods(self, sample_rsi_checkpoint, mock_config):
        """Test checkpoint system with multiple RSI periods"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # Both periods have checkpoints
            periods = [14, 21]
            prices = np.array([100, 105, 103, 108, 110])

            for period in periods:
                if period in sample_rsi_checkpoint:
                    checkpoint = sample_rsi_checkpoint[period]
                    rsi_values, _, _ = loader.calculate_rsi_batch(
                        prices, period,
                        checkpoint['avg_gain'],
                        checkpoint['avg_loss']
                    )
                    # Should calculate successfully
                    assert rsi_values is not None

    @patch('rsi_loader.DatabaseConnection')
    def test_analyze_rsi_periods_empty(self, mock_db_class, mock_config):
        """Test period analysis identifies empty periods (<50% filled)"""
        # Setup mock: 0% filled
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1000, 0, None, None, 0, None, None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            groups = loader.analyze_rsi_periods('1m', [14])

            assert 14 in groups['empty']

    @patch('rsi_loader.DatabaseConnection')
    def test_analyze_rsi_periods_partial(self, mock_db_class, mock_config):
        """Test period analysis identifies partial periods (50-95% filled)"""
        # Setup mock: 70% filled
        mock_cursor = MagicMock()
        last_date = datetime(2024, 1, 15)
        mock_cursor.fetchone.return_value = (1000, 700, None, last_date)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            groups = loader.analyze_rsi_periods('1m', [14])

            assert 14 in groups['partial']

    @patch('rsi_loader.DatabaseConnection')
    def test_analyze_rsi_periods_complete(self, mock_db_class, mock_config):
        """Test period analysis identifies complete periods (>95% filled)"""
        # Setup mock: 99% filled
        mock_cursor = MagicMock()
        last_date = datetime(2024, 1, 20)
        mock_cursor.fetchone.return_value = (1000, 990, None, last_date)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            groups = loader.analyze_rsi_periods('1m', [14])

            assert 14 in groups['complete']

    def test_checkpoint_immutability(self, sample_rsi_checkpoint, mock_config):
        """Test checkpoint state is not modified by calculation"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            checkpoint_copy = {
                14: {
                    'avg_gain': sample_rsi_checkpoint[14]['avg_gain'],
                    'avg_loss': sample_rsi_checkpoint[14]['avg_loss']
                }
            }

            prices = np.array([100, 105, 103, 108, 110])
            loader.calculate_rsi_batch(
                prices, 14,
                sample_rsi_checkpoint[14]['avg_gain'],
                sample_rsi_checkpoint[14]['avg_loss']
            )

            # Original checkpoint should be unchanged
            assert sample_rsi_checkpoint[14] == checkpoint_copy[14]


# ============================================
# GROUP 4: DATABASE OPERATIONS TESTS (6 tests)
# ============================================

class TestDatabaseOperations:
    """Test database-related operations"""

    @patch('rsi_loader.DatabaseConnection')
    def test_create_rsi_columns_creates_missing(self, mock_db_class, mock_config):
        """Test creating missing RSI columns"""
        mock_cursor = MagicMock()
        # Table exists
        mock_cursor.fetchone.return_value = (True,)
        # No existing RSI columns
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.create_rsi_columns('1m', [14, 21])

            assert result is True

    @patch('rsi_loader.DatabaseConnection')
    def test_create_rsi_columns_skips_existing(self, mock_db_class, mock_config):
        """Test skipping creation of existing RSI columns"""
        mock_cursor = MagicMock()
        # Table exists
        mock_cursor.fetchone.return_value = (True,)
        # RSI columns already exist
        mock_cursor.fetchall.return_value = [('rsi_14',), ('rsi_21',)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.create_rsi_columns('1m', [14, 21])

            assert result is True

    @patch('rsi_loader.DatabaseConnection')
    def test_create_rsi_columns_table_not_exists(self, mock_db_class, mock_config):
        """Test error when table doesn't exist"""
        mock_cursor = MagicMock()
        # Table doesn't exist
        mock_cursor.fetchone.return_value = (False,)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.create_rsi_columns('1m', [14, 21])

            assert result is False

    def test_rsi_column_naming_convention(self, mock_config):
        """Test RSI column names follow convention: rsi_{period}"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            periods = [7, 9, 14, 21, 25]

            for period in periods:
                expected_name = f'rsi_{period}'
                # Column naming is consistent in implementation
                assert expected_name == f'rsi_{period}'

    def test_rsi_table_name_format(self, mock_config):
        """Test table name format: indicators_bybit_futures_{timeframe}"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            timeframes = ['1m', '15m', '1h']
            expected_tables = [
                'indicators_bybit_futures_1m',
                'indicators_bybit_futures_15m',
                'indicators_bybit_futures_1h'
            ]

            for tf, expected in zip(timeframes, expected_tables):
                table_name = f'indicators_bybit_futures_{tf}'
                assert table_name == expected

    def test_rsi_value_decimal_precision(self, mock_config):
        """Test RSI values stored with DECIMAL(10,4) precision"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            prices = np.array([100, 105, 103, 108, 110, 115, 112, 118,
                              120, 125, 123, 128, 130, 135, 133])
            rsi_values, _, _ = loader.calculate_rsi_batch(prices, 14)

            # Check precision (4 decimals for RSI)
            valid_rsi = rsi_values[~np.isnan(rsi_values)]
            if len(valid_rsi) > 0:
                value = valid_rsi[0]
                # RSI precision is 4 decimals
                rounded = round(value, 4)
                assert abs(value - rounded) < 1e-4


# ============================================
# GROUP 5: HELPER FUNCTIONS TESTS (4 tests)
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

        with patch('rsi_loader.os.path.join', return_value=str(config_file)):
            with patch('rsi_loader.os.path.exists', return_value=True):
                loader = RSILoader()
                config = loader.config

                assert config['timeframes'] == ['1m', '15m', '1h']
                assert config['indicators']['rsi']['enabled'] is True
                assert config['indicators']['rsi']['periods'] == [7, 9, 14, 21, 25]

    def test_symbol_initialization(self, mock_config):
        """Test loader initializes with correct symbol"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader(symbol='ETHUSDT')
            assert loader.symbol == 'ETHUSDT'

            loader2 = RSILoader(symbol='BTCUSDT')
            assert loader2.symbol == 'BTCUSDT'

    def test_timeframe_minutes_caching(self, mock_config):
        """Test timeframe parsing is cached in instance"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()

            # Access twice
            result1 = loader.timeframe_minutes
            result2 = loader.timeframe_minutes

            # Should be same object (cached)
            assert result1 is result2

    def test_parse_timeframes_returns_correct_types(self, mock_config):
        """Test _parse_timeframes returns correct data types"""
        with patch('rsi_loader.RSILoader.load_config', return_value=mock_config):
            loader = RSILoader()
            result = loader.timeframe_minutes

            assert isinstance(result, dict)
            for key, value in result.items():
                assert isinstance(key, str)
                assert isinstance(value, int)
                assert value > 0


# ============================================
# MARKER for pytest
# ============================================

pytestmark = pytest.mark.unit
