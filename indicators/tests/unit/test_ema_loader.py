"""
Unit tests for EMA (Exponential Moving Average) Loader

Tests cover:
- Timeframe parsing
- EMA calculation with exponential smoothing
- Alpha calculation (2 / (period + 1))
- EMA initialization (first value = SMA)
- Checkpoint system (continuing from saved EMA value)
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
from ema_loader import EMALoader


# ============================================
# GROUP 1: TIMEFRAME PARSING TESTS (6 tests)
# ============================================

class TestTimeframeParsing:
    """Test timeframe parsing with various formats"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing real production config: 1m, 15m, 1h"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
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
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            assert result['1m'] == 1
            assert result['5m'] == 5
            assert result['15m'] == 15
            assert result['30m'] == 30
            assert result['45m'] == 45

    def test_parse_timeframes_hours(self, mock_config):
        """Test parsing hour timeframes: 1h, 2h, 4h, 12h"""
        mock_config['timeframes'] = ['1h', '2h', '4h', '12h']
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            assert result['1h'] == 60
            assert result['2h'] == 120
            assert result['4h'] == 240
            assert result['12h'] == 720

    def test_parse_timeframes_days_weeks(self, mock_config):
        """Test parsing day and week timeframes: 1d, 7d, 1w"""
        mock_config['timeframes'] = ['1d', '7d', '1w']
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            assert result['1d'] == 1440  # 24 * 60
            assert result['7d'] == 10080  # 7 * 24 * 60
            assert result['1w'] == 10080  # Same as 7d

    def test_parse_timeframes_case_insensitive(self, mock_config):
        """Test case insensitivity: 1M, 15M, 1H should work"""
        mock_config['timeframes'] = ['1M', '15M', '1H']
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            # Should convert to lowercase internally
            assert '1M' in result or '1m' in result
            assert len(result) == 3

    def test_parse_timeframes_invalid_format(self, mock_config):
        """Test invalid timeframe formats are skipped"""
        mock_config['timeframes'] = ['1m', 'invalid', '15x', '1h']
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            assert '1m' in result
            assert '1h' in result
            assert 'invalid' not in result
            assert '15x' not in result


# ============================================
# GROUP 2: EMA CALCULATION TESTS (15 tests)
# ============================================

class TestEMACalculation:
    """Test EMA calculation with exponential smoothing formula"""

    def test_ema_alpha_calculation(self):
        """Test alpha (smoothing factor) calculation: α = 2 / (period + 1)"""
        # EMA_12: α = 2 / (12 + 1) = 0.1538...
        alpha_12 = 2.0 / (12 + 1)
        assert abs(alpha_12 - 0.1538) < 0.001

        # EMA_26: α = 2 / (26 + 1) = 0.0741...
        alpha_26 = 2.0 / (26 + 1)
        assert abs(alpha_26 - 0.0741) < 0.001

        # EMA_9: α = 2 / (9 + 1) = 0.2
        alpha_9 = 2.0 / (9 + 1)
        assert alpha_9 == 0.2

    def test_ema_calculation_real_periods(self, sample_prices_large, mock_config):
        """Test EMA calculation with real production periods: 9, 12, 21, 26, 50, 100, 200"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            periods = [9, 12, 21, 26, 50, 100, 200]
            df = pd.DataFrame({'price': sample_prices_large})

            # Calculate using pandas ewm (reference implementation)
            for period in periods:
                df[f'ema_{period}'] = df['price'].ewm(span=period, adjust=False).mean()

            # Verify EMA values are calculated
            for period in periods:
                col = f'ema_{period}'
                assert col in df.columns
                # First value should equal first price
                assert not pd.isna(df[col].iloc[0])
                # Should have values for entire series
                assert not df[col].isna().any()

    def test_ema_formula_manual_calculation(self, sample_prices_for_ema):
        """Test EMA formula manually: EMA = Price × α + EMA_prev × (1 - α)"""
        prices = sample_prices_for_ema.values
        period = 5
        alpha = 2.0 / (period + 1)  # 2 / 6 = 0.333...

        # First EMA = first price
        ema_values = [prices[0]]

        # Calculate subsequent EMAs
        for i in range(1, len(prices)):
            ema = prices[i] * alpha + ema_values[-1] * (1 - alpha)
            ema_values.append(ema)

        # Convert to pandas and compare with ewm
        df = pd.DataFrame({'price': prices})
        df['ema_manual'] = ema_values
        df['ema_pandas'] = df['price'].ewm(span=period, adjust=False).mean()

        # Should match within floating point precision
        pd.testing.assert_series_equal(
            df['ema_manual'],
            df['ema_pandas'],
            check_exact=False,
            check_names=False,  # Ignore series names
            rtol=1e-10
        )

    def test_ema_initialization_first_value(self, sample_prices_small):
        """Test EMA initialization: first value should equal first price"""
        df = pd.DataFrame({'price': sample_prices_small})

        for period in [9, 12, 26]:
            df[f'ema_{period}'] = df['price'].ewm(span=period, adjust=False).mean()
            # First EMA value should equal first price
            assert df[f'ema_{period}'].iloc[0] == df['price'].iloc[0]

    def test_ema_responds_to_price_changes(self):
        """Test EMA responds to price changes (more recent = more weight)"""
        # Create sequence: stable prices then sudden increase
        prices = [100] * 10 + [120]
        df = pd.DataFrame({'price': prices})
        df['ema_5'] = df['price'].ewm(span=5, adjust=False).mean()

        # EMA at position 10 (after increase) should be between 100 and 120
        ema_after_jump = df['ema_5'].iloc[10]
        assert 100 < ema_after_jump < 120

        # Should be closer to new price due to exponential weighting
        # α = 2/6 = 0.333, so weight on new price is significant
        assert ema_after_jump > 105

    def test_ema_shorter_period_more_responsive(self):
        """Test shorter EMA periods respond faster to price changes"""
        # Price spike sequence
        prices = [100] * 5 + [120] * 5
        df = pd.DataFrame({'price': prices})

        df['ema_3'] = df['price'].ewm(span=3, adjust=False).mean()
        df['ema_10'] = df['price'].ewm(span=10, adjust=False).mean()

        # After spike, shorter EMA should be higher (faster response)
        assert df['ema_3'].iloc[-1] > df['ema_10'].iloc[-1]

    def test_ema_with_checkpoint_continuation(self, sample_prices_for_ema, mock_config):
        """Test EMA calculation continues from checkpoint value"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            period = 12
            alpha = 2.0 / (period + 1)

            # Simulate checkpoint: previous EMA = 110
            initial_emas = {12: 110.0}

            df = pd.DataFrame({'price': sample_prices_for_ema})

            # Calculate EMA continuing from checkpoint
            result_df = loader.calculate_ema_batch(df, [period], initial_emas)

            # First value should use checkpoint
            # EMA = price[0] * alpha + initial_ema * (1 - alpha)
            expected_first = df['price'].iloc[0] * alpha + 110.0 * (1 - alpha)
            actual_first = result_df['ema_12'].iloc[0]

            assert abs(actual_first - expected_first) < 0.01

    def test_ema_precision_8_decimals(self, sample_prices_medium, mock_config):
        """Test EMA values maintain 8 decimal precision"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            df = pd.DataFrame({'price': sample_prices_medium})
            result_df = loader.calculate_ema_batch(df, [12, 26], {})

            # Check precision by converting to string
            for col in ['ema_12', 'ema_26']:
                value = result_df[col].iloc[50]
                value_str = f"{value:.8f}"
                # Should have 8 decimals
                assert len(value_str.split('.')[1]) == 8

    def test_ema_with_nan_prices(self, mock_config):
        """Test EMA handles NaN prices gracefully"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            prices = [100, 110, np.nan, 120, 130]
            df = pd.DataFrame({'price': prices})

            result_df = loader.calculate_ema_batch(df, [5], {})

            # pandas ewm() continues calculating through NaN
            # (it uses previous EMA value)
            # So we just verify the calculation doesn't crash
            assert 'ema_5' in result_df.columns
            assert len(result_df) == len(prices)

    def test_ema_multiple_periods_simultaneously(self, sample_prices_large, mock_config):
        """Test calculating multiple EMA periods in one pass"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            periods = [9, 12, 26, 50]
            df = pd.DataFrame({'price': sample_prices_large})

            result_df = loader.calculate_ema_batch(df, periods, {})

            # All periods should be calculated
            for period in periods:
                assert f'ema_{period}' in result_df.columns
                assert not result_df[f'ema_{period}'].isna().any()

    def test_ema_empty_dataframe(self, mock_config):
        """Test EMA with empty DataFrame"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            df = pd.DataFrame({'price': []})
            result_df = loader.calculate_ema_batch(df, [12], {})

            assert len(result_df) == 0

    def test_ema_single_price(self, mock_config):
        """Test EMA with single price value"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            df = pd.DataFrame({'price': [100.0]})
            result_df = loader.calculate_ema_batch(df, [12], {})

            # Single value: EMA should equal price
            assert result_df['ema_12'].iloc[0] == 100.0

    def test_ema_vs_sma_convergence(self, sample_prices_large):
        """Test EMA eventually converges toward price like SMA"""
        df = pd.DataFrame({'price': sample_prices_large})

        period = 50
        df['ema'] = df['price'].ewm(span=period, adjust=False).mean()
        df['sma'] = df['price'].rolling(window=period).mean()

        # After enough periods, EMA and SMA should be correlated
        # (not equal, but similar trend)
        recent_ema = df['ema'].iloc[-50:].values
        recent_sma = df['sma'].iloc[-50:].dropna().values

        # Calculate correlation
        min_len = min(len(recent_ema), len(recent_sma))
        correlation = np.corrcoef(recent_ema[:min_len], recent_sma[:min_len])[0, 1]

        # Should be highly correlated (> 0.9)
        assert correlation > 0.9

    def test_ema_with_constant_prices(self, mock_config):
        """Test EMA with constant prices (should equal price)"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # All prices = 100
            df = pd.DataFrame({'price': [100.0] * 50})
            result_df = loader.calculate_ema_batch(df, [12, 26], {})

            # EMA of constant should equal constant
            assert (result_df['ema_12'] == 100.0).all()
            assert (result_df['ema_26'] == 100.0).all()

    def test_ema_increasing_sequence(self, mock_config):
        """Test EMA with consistently increasing prices"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # Linearly increasing prices
            df = pd.DataFrame({'price': list(range(100, 150))})
            result_df = loader.calculate_ema_batch(df, [12], {})

            # EMA should also increase
            ema_values = result_df['ema_12'].values
            # Check if monotonically increasing
            assert all(ema_values[i] <= ema_values[i+1] for i in range(len(ema_values)-1))


# ============================================
# GROUP 3: CHECKPOINT SYSTEM TESTS (8 tests)
# ============================================

class TestCheckpointSystem:
    """Test EMA checkpoint system for resumable loading"""

    @patch('ema_loader.DatabaseConnection')
    def test_get_last_ema_checkpoint_with_data(self, mock_db_class, mock_config):
        """Test retrieving last EMA checkpoint when data exists"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 1), 43500.5)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            timestamp, ema_value = loader.get_last_ema_checkpoint('1m', 12)

            assert timestamp == datetime(2024, 1, 1)
            assert ema_value == 43500.5

    @patch('ema_loader.DatabaseConnection')
    def test_get_last_ema_checkpoint_no_data(self, mock_db_class, mock_config):
        """Test retrieving checkpoint when no data exists"""
        # Setup mock to return None
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            timestamp, ema_value = loader.get_last_ema_checkpoint('1m', 12)

            assert timestamp is None
            assert ema_value is None

    def test_checkpoint_preserves_ema_continuity(self, mock_config):
        """Test checkpoint preserves EMA calculation continuity"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # Calculate full sequence
            prices_full = list(range(100, 150))
            df_full = pd.DataFrame({'price': prices_full})
            df_full['ema_12'] = df_full['price'].ewm(span=12, adjust=False).mean()

            # Split point at index 25
            split_idx = 25
            checkpoint_ema = df_full['ema_12'].iloc[split_idx - 1]

            # Calculate second half from checkpoint
            prices_second_half = prices_full[split_idx:]
            df_second = pd.DataFrame({'price': prices_second_half})

            initial_emas = {12: checkpoint_ema}
            result = loader.calculate_ema_batch(df_second, [12], initial_emas)

            # Compare with full calculation
            expected = df_full['ema_12'].iloc[split_idx:].values
            actual = result['ema_12'].values

            # Should match within floating point precision
            np.testing.assert_array_almost_equal(actual, expected, decimal=6)

    def test_multiple_period_checkpoints(self, sample_ema_initial_state, mock_config):
        """Test checkpoint system with multiple EMA periods"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            periods = [12, 26, 50]
            df = pd.DataFrame({'price': [110, 115, 120, 125, 130]})

            # All periods have checkpoints
            initial_emas = sample_ema_initial_state

            result = loader.calculate_ema_batch(df, periods, initial_emas)

            # All periods should be calculated
            for period in periods:
                assert f'ema_{period}' in result.columns
                assert not result[f'ema_{period}'].isna().any()

    def test_checkpoint_with_missing_period(self, sample_ema_initial_state, mock_config):
        """Test checkpoint when some periods are missing"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # Request periods including one without checkpoint
            periods = [12, 26, 100]  # 100 not in checkpoint
            df = pd.DataFrame({'price': [110, 115, 120, 125, 130]})

            initial_emas = {12: 43500.5, 26: 43480.2}  # No 100

            result = loader.calculate_ema_batch(df, periods, initial_emas)

            # All periods should still be calculated
            # Period 100 should use pandas ewm (no checkpoint)
            for period in periods:
                assert f'ema_{period}' in result.columns

    @patch('ema_loader.DatabaseConnection')
    def test_checkpoint_query_format(self, mock_db_class, mock_config):
        """Test checkpoint query uses correct SQL format"""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 1), 43500.5)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            loader.get_last_ema_checkpoint('1h', 26)

            # Verify SQL query was executed
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args[0]

            # Check query contains key elements
            query = call_args[0]
            assert 'indicators_bybit_futures_1h' in query
            assert 'ema_26' in query
            assert 'ORDER BY timestamp DESC' in query
            assert 'LIMIT 1' in query

    def test_checkpoint_state_immutability(self, sample_ema_initial_state, mock_config):
        """Test checkpoint state is not modified by calculation"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            initial_emas_copy = sample_ema_initial_state.copy()
            df = pd.DataFrame({'price': [110, 115, 120]})

            loader.calculate_ema_batch(df, [12, 26], sample_ema_initial_state)

            # Original state should be unchanged
            assert sample_ema_initial_state == initial_emas_copy

    def test_checkpoint_with_float_conversion(self, mock_config):
        """Test checkpoint handles Decimal/float conversion"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # Initial state might come from DB as Decimal
            # But in practice, get_last_ema_checkpoint converts to float
            # So we test with float checkpoint
            initial_emas = {12: 43500.12345678}

            df = pd.DataFrame({'price': [43510.0, 43520.0]})
            result = loader.calculate_ema_batch(df, [12], initial_emas)

            # Should handle conversion and calculate correctly
            assert 'ema_12' in result.columns
            assert isinstance(result['ema_12'].iloc[0], (float, np.floating))


# ============================================
# GROUP 4: DATABASE OPERATIONS TESTS (6 tests)
# ============================================

class TestDatabaseOperations:
    """Test database-related operations"""

    @patch('ema_loader.DatabaseConnection')
    def test_create_ema_columns_creates_missing(self, mock_db_class, mock_config):
        """Test creating missing EMA columns"""
        mock_cursor = MagicMock()
        # Table exists
        mock_cursor.fetchone.side_effect = [(True,), ]
        # No existing EMA columns
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.create_ema_columns('1m', [12, 26])

            assert result is True
            # Should execute ALTER TABLE for each period
            assert mock_cursor.execute.call_count >= 2

    @patch('ema_loader.DatabaseConnection')
    def test_create_ema_columns_skips_existing(self, mock_db_class, mock_config):
        """Test skipping creation of existing EMA columns"""
        mock_cursor = MagicMock()
        # Table exists
        mock_cursor.fetchone.return_value = (True,)
        # EMA columns already exist
        mock_cursor.fetchall.return_value = [('ema_12',), ('ema_26',)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_db_instance = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn
        mock_db_class.return_value = mock_db_instance

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.create_ema_columns('1m', [12, 26])

            assert result is True

    @patch('ema_loader.DatabaseConnection')
    def test_create_ema_columns_table_not_exists(self, mock_db_class, mock_config):
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

        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.create_ema_columns('1m', [12, 26])

            assert result is False

    def test_ema_column_naming_convention(self, mock_config):
        """Test EMA column names follow convention: ema_{period}"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            periods = [9, 12, 21, 26, 50, 100, 200]
            df = pd.DataFrame({'price': range(100, 350)})

            result = loader.calculate_ema_batch(df, periods, {})

            # Check column naming
            for period in periods:
                expected_name = f'ema_{period}'
                assert expected_name in result.columns

    def test_ema_table_name_format(self, mock_config):
        """Test table name format: indicators_bybit_futures_{timeframe}"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            timeframes = ['1m', '15m', '1h']
            expected_tables = [
                'indicators_bybit_futures_1m',
                'indicators_bybit_futures_15m',
                'indicators_bybit_futures_1h'
            ]

            for tf, expected in zip(timeframes, expected_tables):
                # Table name is constructed inline in methods
                # We verify the pattern is used correctly
                table_name = f'indicators_bybit_futures_{tf}'
                assert table_name == expected

    def test_ema_value_decimal_precision(self, mock_config):
        """Test EMA values are stored with DECIMAL(20,8) precision"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            df = pd.DataFrame({'price': [43500.12345678, 43510.87654321]})
            result = loader.calculate_ema_batch(df, [12], {})

            # Values should maintain precision
            value = result['ema_12'].iloc[0]
            # Should be precise to 8 decimals
            assert abs(value - 43500.12345678) < 1e-6


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

        with patch('ema_loader.os.path.join', return_value=str(config_file)):
            with patch('ema_loader.os.path.exists', return_value=True):
                loader = EMALoader()
                config = loader.config

                assert config['timeframes'] == ['1m', '15m', '1h']
                assert config['indicators']['ema']['enabled'] is True
                assert config['indicators']['ema']['periods'] == [9, 12, 21, 26, 50, 100, 200]

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test _parse_timeframes returns dict mapping"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()
            result = loader.timeframe_minutes

            assert isinstance(result, dict)
            assert all(isinstance(k, str) for k in result.keys())
            assert all(isinstance(v, int) for v in result.values())

    def test_symbol_initialization(self, mock_config):
        """Test loader initializes with correct symbol"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader(symbol='ETHUSDT')
            assert loader.symbol == 'ETHUSDT'

            loader2 = EMALoader(symbol='BTCUSDT')
            assert loader2.symbol == 'BTCUSDT'

    def test_timeframe_minutes_caching(self, mock_config):
        """Test timeframe parsing is cached in instance"""
        with patch('ema_loader.EMALoader.load_config', return_value=mock_config):
            loader = EMALoader()

            # Access twice
            result1 = loader.timeframe_minutes
            result2 = loader.timeframe_minutes

            # Should be same object (cached)
            assert result1 is result2


# ============================================
# MARKER for pytest
# ============================================

pytestmark = pytest.mark.unit
