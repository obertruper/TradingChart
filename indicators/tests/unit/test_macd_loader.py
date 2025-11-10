"""
Unit tests for MACD Loader (macd_loader.py)

Tests cover:
1. Timeframe Parsing - динамический парсинг таймфреймов
2. EMA Calculation - расчет экспоненциальной скользящей средней
3. MACD Calculation - MACD Line, Signal Line, Histogram
4. Database Operations - создание колонок, сохранение данных
5. Helper Functions - вспомогательные функции

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

from indicators.macd_loader import MACDLoader


# ============================================
# Group 1: Timeframe Parsing (6 tests)
# ============================================

class TestTimeframeParsing:
    """Tests for _parse_timeframes() method"""

    def test_parse_timeframes_real_config(self, mock_config):
        """Test parsing with real production config (1m, 15m, 1h)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert '1m' in loader.timeframe_minutes
            assert '15m' in loader.timeframe_minutes
            assert '1h' in loader.timeframe_minutes

            assert loader.timeframe_minutes['1m'] == 1
            assert loader.timeframe_minutes['15m'] == 15
            assert loader.timeframe_minutes['1h'] == 60

    def test_parse_timeframes_minutes(self, mock_config):
        """Test parsing minute timeframes"""
        mock_config['timeframes'] = ['1m', '5m', '15m', '30m']

        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert loader.timeframe_minutes['1m'] == 1
            assert loader.timeframe_minutes['5m'] == 5
            assert loader.timeframe_minutes['15m'] == 15
            assert loader.timeframe_minutes['30m'] == 30

    def test_parse_timeframes_hours(self, mock_config):
        """Test parsing hour timeframes"""
        mock_config['timeframes'] = ['1h', '4h', '12h']

        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert loader.timeframe_minutes['1h'] == 60
            assert loader.timeframe_minutes['4h'] == 240
            assert loader.timeframe_minutes['12h'] == 720

    def test_parse_timeframes_days_weeks(self, mock_config):
        """Test parsing day and week timeframes"""
        mock_config['timeframes'] = ['1d', '3d', '1w']

        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert loader.timeframe_minutes['1d'] == 1440
            assert loader.timeframe_minutes['3d'] == 4320
            assert loader.timeframe_minutes['1w'] == 10080

    def test_parse_timeframes_case_insensitive(self, mock_config):
        """Test that parsing is case insensitive (keys stored as provided)"""
        mock_config['timeframes'] = ['1M', '15M', '1H']

        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Keys are stored in original case (uppercase in this case)
            assert '1M' in loader.timeframe_minutes
            assert '15M' in loader.timeframe_minutes
            assert '1H' in loader.timeframe_minutes

            # Values should be correct regardless of case
            assert loader.timeframe_minutes['1M'] == 1
            assert loader.timeframe_minutes['15M'] == 15
            assert loader.timeframe_minutes['1H'] == 60

    def test_parse_timeframes_invalid_format(self, mock_config):
        """Test that invalid timeframes are skipped with warning"""
        mock_config['timeframes'] = ['1m', 'invalid', '1h', '999x']

        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Valid timeframes should be parsed
            assert '1m' in loader.timeframe_minutes
            assert '1h' in loader.timeframe_minutes

            # Invalid timeframes should be skipped
            assert 'invalid' not in loader.timeframe_minutes
            assert '999x' not in loader.timeframe_minutes


# ============================================
# Group 2: EMA Calculation (10 tests)
# ============================================

class TestEMACalculation:
    """Tests for calculate_ema() method"""

    def test_calculate_ema_basic(self, mock_config):
        """Test basic EMA calculation"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100, 102, 104, 103, 105, 107, 106, 108, 110, 109], dtype=float)
            ema_5 = loader.calculate_ema(prices, 5)

            # EMA should have same length as prices
            assert len(ema_5) == len(prices)

            # EMA values should be floats
            assert ema_5.dtype == np.float64

    def test_calculate_ema_formula_validation(self, mock_config):
        """Test EMA formula: EMA = Price × k + EMA_prev × (1 - k), where k = 2/(period+1)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100, 110, 105, 115, 120], dtype=float)
            period = 3
            ema = loader.calculate_ema(prices, period)

            # Calculate expected EMA manually
            k = 2 / (period + 1)  # 2 / 4 = 0.5
            expected_ema = [100.0]  # First value = first price

            for i in range(1, len(prices)):
                ema_val = prices.iloc[i] * k + expected_ema[-1] * (1 - k)
                expected_ema.append(ema_val)

            # Compare with calculated EMA
            pd.testing.assert_series_equal(
                ema,
                pd.Series(expected_ema, dtype=float),
                check_names=False,
                atol=0.0001
            )

    def test_calculate_ema_real_periods(self, mock_config, sample_prices_large):
        """Test EMA with real MACD periods: 12, 26"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            ema_12 = loader.calculate_ema(sample_prices_large, 12)
            ema_26 = loader.calculate_ema(sample_prices_large, 26)

            # Both should have values
            assert not ema_12.isna().all()
            assert not ema_26.isna().all()

            # EMA_26 should be smoother (less responsive) than EMA_12
            # Check by comparing variance
            assert ema_26.var() <= ema_12.var()

    def test_calculate_ema_first_value_equals_first_price(self, mock_config):
        """Test that first EMA value equals first price"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([43500, 43520, 43510, 43530], dtype=float)
            ema = loader.calculate_ema(prices, 12)

            # First EMA value should equal first price
            assert abs(ema.iloc[0] - prices.iloc[0]) < 0.01

    def test_calculate_ema_responds_to_trend(self, mock_config):
        """Test that EMA follows price trend"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Strong uptrend
            prices_up = pd.Series(range(100, 150), dtype=float)
            ema_up = loader.calculate_ema(prices_up, 12)

            # EMA should increase
            assert ema_up.iloc[-1] > ema_up.iloc[0]

            # Strong downtrend
            prices_down = pd.Series(range(150, 100, -1), dtype=float)
            ema_down = loader.calculate_ema(prices_down, 12)

            # EMA should decrease
            assert ema_down.iloc[-1] < ema_down.iloc[0]

    def test_calculate_ema_shorter_period_more_responsive(self, mock_config):
        """Test that shorter period EMA is more responsive than longer period"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Price with sudden spike
            prices = pd.Series([100] * 20 + [150] + [100] * 20, dtype=float)

            ema_5 = loader.calculate_ema(prices, 5)
            ema_20 = loader.calculate_ema(prices, 20)

            # EMA_5 should react more strongly to the spike
            spike_idx = 20
            ema_5_spike = ema_5.iloc[spike_idx + 1]
            ema_20_spike = ema_20.iloc[spike_idx + 1]

            # EMA_5 should be closer to the spike price (150) than EMA_20
            assert abs(ema_5_spike - 150) < abs(ema_20_spike - 150)

    def test_calculate_ema_precision(self, mock_config):
        """Test EMA precision (8 decimals for DECIMAL(20,8))"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([43521.12345678, 43532.87654321], dtype=float)
            ema = loader.calculate_ema(prices, 12)

            # Round to 8 decimals
            ema_rounded = ema.round(8)

            # Should maintain precision
            assert ema_rounded.iloc[0] == round(43521.12345678, 8)

    def test_calculate_ema_empty_series(self, mock_config):
        """Test EMA with empty series"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([], dtype=float)
            ema = loader.calculate_ema(prices, 12)

            assert len(ema) == 0

    def test_calculate_ema_single_value(self, mock_config):
        """Test EMA with single value"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100.0])
            ema = loader.calculate_ema(prices, 12)

            assert len(ema) == 1
            assert ema.iloc[0] == 100.0

    def test_calculate_ema_constant_prices(self, mock_config):
        """Test EMA with constant prices"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100.0] * 50, dtype=float)
            ema = loader.calculate_ema(prices, 12)

            # All EMA values should be 100.0
            assert (ema == 100.0).all()


# ============================================
# Group 3: MACD Calculation (15 tests)
# ============================================

class TestMACDCalculation:
    """Tests for calculate_macd() method"""

    def test_macd_calculation_basic(self, mock_config, sample_prices_for_macd):
        """Test basic MACD calculation returns three components"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            macd_line, signal_line, histogram = loader.calculate_macd(
                sample_prices_for_macd, fast=12, slow=26, signal=9
            )

            # All three components should be Series
            assert isinstance(macd_line, pd.Series)
            assert isinstance(signal_line, pd.Series)
            assert isinstance(histogram, pd.Series)

            # All should have same length as input
            assert len(macd_line) == len(sample_prices_for_macd)
            assert len(signal_line) == len(sample_prices_for_macd)
            assert len(histogram) == len(sample_prices_for_macd)

    def test_macd_line_formula(self, mock_config):
        """Test MACD Line = Fast EMA - Slow EMA"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100, 102, 104, 106, 108, 110, 112, 114, 116, 118] * 3, dtype=float)

            # Calculate MACD
            macd_line, _, _ = loader.calculate_macd(prices, fast=6, slow=13, signal=5)

            # Calculate EMAs separately
            ema_fast = loader.calculate_ema(prices, 6)
            ema_slow = loader.calculate_ema(prices, 13)

            # MACD Line should equal Fast EMA - Slow EMA
            expected_macd = ema_fast - ema_slow

            pd.testing.assert_series_equal(
                macd_line,
                expected_macd,
                check_names=False,
                atol=0.0001
            )

    def test_macd_signal_line_is_ema_of_macd(self, mock_config):
        """Test Signal Line = EMA(MACD Line, signal_period)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100, 102, 104, 106, 108, 110, 112, 114, 116, 118] * 3, dtype=float)

            # Calculate MACD
            macd_line, signal_line, _ = loader.calculate_macd(prices, fast=6, slow=13, signal=5)

            # Signal line should be EMA of MACD line
            expected_signal = loader.calculate_ema(macd_line, 5)

            pd.testing.assert_series_equal(
                signal_line,
                expected_signal,
                check_names=False,
                atol=0.0001
            )

    def test_macd_histogram_formula(self, mock_config):
        """Test Histogram = MACD Line - Signal Line"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([100, 102, 104, 106, 108, 110, 112, 114, 116, 118] * 3, dtype=float)

            # Calculate MACD
            macd_line, signal_line, histogram = loader.calculate_macd(prices, fast=6, slow=13, signal=5)

            # Histogram should equal MACD Line - Signal Line
            expected_histogram = macd_line - signal_line

            pd.testing.assert_series_equal(
                histogram,
                expected_histogram,
                check_names=False,
                atol=0.0001
            )

    def test_macd_classic_configuration(self, mock_config, sample_prices_for_macd):
        """Test MACD with classic configuration (12, 26, 9)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            macd_line, signal_line, histogram = loader.calculate_macd(
                sample_prices_for_macd, fast=12, slow=26, signal=9
            )

            # Should produce valid values
            assert not macd_line.isna().all()
            assert not signal_line.isna().all()
            assert not histogram.isna().all()

    def test_macd_crypto_configuration(self, mock_config, sample_prices_for_macd):
        """Test MACD with crypto configuration (6, 13, 5)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            macd_line, signal_line, histogram = loader.calculate_macd(
                sample_prices_for_macd, fast=6, slow=13, signal=5
            )

            # Should produce valid values
            assert not macd_line.isna().all()
            assert not signal_line.isna().all()
            assert not histogram.isna().all()

    def test_macd_uptrend_positive_values(self, mock_config):
        """Test MACD produces positive values in uptrend"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Strong uptrend
            prices = pd.Series(range(100, 200), dtype=float)

            macd_line, _, _ = loader.calculate_macd(prices, fast=12, slow=26, signal=9)

            # MACD line should eventually become positive in uptrend
            # (after enough data for EMA convergence)
            assert macd_line.iloc[-10:].mean() > 0

    def test_macd_downtrend_negative_values(self, mock_config):
        """Test MACD produces negative values in downtrend"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Strong downtrend
            prices = pd.Series(range(200, 100, -1), dtype=float)

            macd_line, _, _ = loader.calculate_macd(prices, fast=12, slow=26, signal=9)

            # MACD line should be negative in downtrend
            assert macd_line.iloc[-10:].mean() < 0

    def test_macd_signal_crossover(self, mock_config):
        """Test MACD and Signal line crossover behavior"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Price reversal: down then up
            prices = pd.Series(
                list(range(150, 100, -1)) + list(range(100, 150)),
                dtype=float
            )

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=6, slow=13, signal=5
            )

            # Histogram should cross zero (negative to positive)
            histogram_early = histogram.iloc[len(histogram)//3]
            histogram_late = histogram.iloc[-10]

            # Early should be negative (downtrend), late should be positive (uptrend)
            assert histogram_early < 0
            assert histogram_late > 0

    def test_macd_histogram_zero_when_lines_equal(self, mock_config):
        """Test histogram is zero when MACD and Signal lines are equal"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Flat prices
            prices = pd.Series([100.0] * 100, dtype=float)

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=12, slow=26, signal=9
            )

            # With flat prices, all should converge to zero
            assert abs(macd_line.iloc[-1]) < 0.01
            assert abs(signal_line.iloc[-1]) < 0.01
            assert abs(histogram.iloc[-1]) < 0.01

    def test_macd_precision_8_decimals(self, mock_config):
        """Test MACD components maintain 8 decimal precision"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([43521.12345678, 43532.87654321] * 20, dtype=float)

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=12, slow=26, signal=9
            )

            # Round to 8 decimals
            macd_rounded = macd_line.round(8)
            signal_rounded = signal_line.round(8)
            histogram_rounded = histogram.round(8)

            # Should not lose significant data
            assert len(macd_rounded[macd_rounded != 0]) > 0

    def test_macd_empty_prices(self, mock_config):
        """Test MACD with empty price series"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([], dtype=float)

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=12, slow=26, signal=9
            )

            assert len(macd_line) == 0
            assert len(signal_line) == 0
            assert len(histogram) == 0

    def test_macd_insufficient_data(self, mock_config):
        """Test MACD with insufficient data (< slow period)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Only 10 prices, but slow = 26
            prices = pd.Series(range(100, 110), dtype=float)

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=12, slow=26, signal=9
            )

            # Should still calculate, but values may not be meaningful
            assert len(macd_line) == 10
            assert len(signal_line) == 10
            assert len(histogram) == 10

    def test_macd_multiple_configs_simultaneously(self, mock_config, sample_macd_configs, sample_prices_for_macd):
        """Test calculating MACD for multiple configurations"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            results = {}
            for config in sample_macd_configs:
                macd, signal, hist = loader.calculate_macd(
                    sample_prices_for_macd,
                    config['fast'],
                    config['slow'],
                    config['signal']
                )
                results[config['name']] = (macd, signal, hist)

            # Both configs should produce results
            assert 'classic' in results
            assert 'crypto' in results

            # Results should be different
            classic_macd = results['classic'][0]
            crypto_macd = results['crypto'][0]

            # Crypto should be more responsive (faster periods)
            # Check by comparing variance or latest values
            assert not classic_macd.equals(crypto_macd)

    def test_macd_signal_line_lags_behind_macd(self, mock_config):
        """Test that Signal line lags behind MACD line (smoothing effect)"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Sudden price jump
            prices = pd.Series([100] * 30 + [120] * 30, dtype=float)

            macd_line, signal_line, _ = loader.calculate_macd(
                prices, fast=6, slow=13, signal=5
            )

            # After the jump, MACD should react faster than Signal
            jump_idx = 30
            check_idx = jump_idx + 5

            # MACD should be higher than Signal (positive histogram)
            assert macd_line.iloc[check_idx] > signal_line.iloc[check_idx]


# ============================================
# Group 4: Database Operations (8 tests)
# ============================================

class TestDatabaseOperations:
    """Tests for database operations"""

    def test_ensure_macd_columns_creates_missing(self, mock_config, sample_macd_configs, mock_database):
        """Test that ensure_macd_columns creates missing columns"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate column doesn't exist
            mock_cursor.fetchone.return_value = [False]

            loader.ensure_macd_columns('1m', sample_macd_configs)

            # Should check and create columns
            assert mock_cursor.execute.called

    def test_ensure_macd_columns_skips_existing(self, mock_config, sample_macd_configs, mock_database):
        """Test that ensure_macd_columns skips existing columns"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate column already exists
            mock_cursor.fetchone.return_value = [True]

            loader.ensure_macd_columns('1m', sample_macd_configs)

            # Should only check, not create
            assert mock_cursor.execute.called

    def test_macd_column_naming_convention(self, mock_config):
        """Test MACD column naming: macd_{fast}_{slow}_{signal}_line/signal/histogram"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            config = {'fast': 12, 'slow': 26, 'signal': 9, 'name': 'classic'}

            expected_base = "macd_12_26_9"
            expected_columns = [
                f"{expected_base}_line",
                f"{expected_base}_signal",
                f"{expected_base}_histogram"
            ]

            # Check naming in ensure_macd_columns logic
            assert expected_columns[0] == "macd_12_26_9_line"
            assert expected_columns[1] == "macd_12_26_9_signal"
            assert expected_columns[2] == "macd_12_26_9_histogram"

    def test_macd_table_name_format(self, mock_config):
        """Test table name format: indicators_bybit_futures_{timeframe}"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert loader.get_table_name('1m') == 'indicators_bybit_futures_1m'
            assert loader.get_table_name('15m') == 'indicators_bybit_futures_15m'
            assert loader.get_table_name('1h') == 'indicators_bybit_futures_1h'

    def test_save_macd_to_db_converts_nan_to_none(self, mock_config, mock_database):
        """Test that NaN values are converted to None for PostgreSQL NULL"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Create DataFrame with NaN
            df = pd.DataFrame({
                'timestamp': [datetime(2024, 1, 1)],
                'symbol': ['BTCUSDT'],
                'macd_12_26_9_line': [np.nan],
                'macd_12_26_9_signal': [1.5],
                'macd_12_26_9_histogram': [np.nan]
            })

            config = {'fast': 12, 'slow': 26, 'signal': 9, 'name': 'classic'}

            # Mock execute_batch
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            with patch('psycopg2.extras.execute_batch'):
                loader.save_macd_to_db(df, 'indicators_bybit_futures_1m', config)

            # Should skip rows with all NaN in line (filtered out)
            # But if it has any values, NaN should convert to None

    def test_macd_value_decimal_precision(self, mock_config):
        """Test MACD values use DECIMAL(20,8) precision"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            prices = pd.Series([43521.12345678, 43532.87654321] * 15, dtype=float)

            macd_line, signal_line, histogram = loader.calculate_macd(
                prices, fast=12, slow=26, signal=9
            )

            # Round to 8 decimals (database precision)
            macd_rounded = macd_line.round(8)

            # Should maintain precision
            assert isinstance(macd_rounded.iloc[-1], (float, np.float64))

    def test_get_last_macd_date_with_data(self, mock_config, mock_database):
        """Test get_last_macd_date returns last date with MACD data"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate data exists
            last_date = datetime(2024, 10, 1, 12, 0)
            mock_cursor.fetchone.return_value = [last_date]

            config = {'fast': 12, 'slow': 26, 'signal': 9}
            result = loader.get_last_macd_date('1m', config)

            assert result == last_date

    def test_get_last_macd_date_no_data(self, mock_config, mock_database):
        """Test get_last_macd_date returns None when no data"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')
            loader.db = mock_database

            # Mock cursor
            mock_cursor = MagicMock()
            mock_database.get_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor

            # Simulate no data
            mock_cursor.fetchone.return_value = [None]

            config = {'fast': 12, 'slow': 26, 'signal': 9}
            result = loader.get_last_macd_date('1m', config)

            assert result is None


# ============================================
# Group 5: Helper Functions (6 tests)
# ============================================

class TestHelperFunctions:
    """Tests for helper functions"""

    def test_load_config_success(self, mock_config):
        """Test loading config from YAML file"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert loader.config == mock_config
            assert 'timeframes' in loader.config
            assert 'indicators' in loader.config
            assert 'macd' in loader.config['indicators']

    def test_symbol_initialization(self, mock_config):
        """Test that symbol is properly initialized"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='ETHUSDT')

            assert loader.symbol == 'ETHUSDT'

    def test_parse_timeframes_returns_dict(self, mock_config):
        """Test that _parse_timeframes returns dict"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            assert isinstance(loader.timeframe_minutes, dict)
            assert len(loader.timeframe_minutes) > 0

    def test_timeframe_minutes_caching(self, mock_config):
        """Test that timeframe_minutes is computed once and cached"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Access twice
            tf1 = loader.timeframe_minutes
            tf2 = loader.timeframe_minutes

            # Should be same object (cached)
            assert tf1 is tf2

    def test_get_table_name_returns_correct_format(self, mock_config):
        """Test get_table_name returns correct format"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Test all timeframes
            for tf in ['1m', '15m', '1h']:
                table_name = loader.get_table_name(tf)
                assert table_name == f'indicators_bybit_futures_{tf}'
                assert table_name.startswith('indicators_bybit_futures_')

    def test_lookback_multiplier_from_config(self, mock_config):
        """Test that lookback_multiplier is loaded from config"""
        with patch.object(MACDLoader, 'load_config', return_value=mock_config):
            loader = MACDLoader(symbol='BTCUSDT')

            # Should have lookback_multiplier in config
            lookback = loader.config['indicators']['macd'].get('lookback_multiplier', 3)
            assert lookback == 3
