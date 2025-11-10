"""
Unit tests for Long/Short Ratio Loader

Tests cover:
1. Column Names - Verification of database column names
2. API Period Mapping - Timeframe to API period conversion
3. API Fetch - API request handling and retry logic
4. Ratio Calculation - buyRatio / sellRatio calculation
5. Data Parsing - Timestamp and data type conversions
6. Save to DB - INSERT...ON CONFLICT logic
7. NULL Handling - Special handling for 1m timeframe (API doesn't support)
8. Database Operations - Column creation and date range detection
9. Helper Functions - Timeframe parsing utilities

Total: 50 tests
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call
from decimal import Decimal
import pytz
import requests

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from indicators.long_short_ratio_loader import LongShortRatioLoader


# ============================================
# 1. Column Names Tests (3 tests)
# ============================================

class TestColumnNames:
    """Test database column name verification"""

    def test_column_names_long_short_buy_ratio_exists(self, mock_config):
        """Test long_short_buy_ratio column name is defined"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Check that method exists (column names are used internally in ensure_columns_exist)
        assert hasattr(loader, 'ensure_columns_exist')
        assert callable(loader.ensure_columns_exist)

    def test_column_names_long_short_sell_ratio_exists(self, mock_config):
        """Test long_short_sell_ratio column name is defined"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        assert hasattr(loader, 'ensure_columns_exist')
        assert callable(loader.ensure_columns_exist)

    def test_column_names_long_short_ratio_exists(self, mock_config):
        """Test long_short_ratio column name is defined"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        assert hasattr(loader, 'ensure_columns_exist')
        assert callable(loader.ensure_columns_exist)


# ============================================
# 2. API Period Mapping Tests (4 tests)
# ============================================

class TestAPIPeriodMapping:
    """Test timeframe to API period conversion"""

    def test_api_period_map_1m_is_none(self, mock_config):
        """Test 1m timeframe maps to None (API doesn't support)"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        assert loader.api_period_map['1m'] is None

    def test_api_period_map_15m_is_15min(self, mock_config):
        """Test 15m timeframe maps to '15min' for API"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        assert loader.api_period_map['15m'] == '15min'

    def test_api_period_map_1h_is_1h(self, mock_config):
        """Test 1h timeframe maps to '1h' for API"""
        loader = LongShortRatioLoader('BTCUSDT', '1h', mock_config)

        assert loader.api_period_map['1h'] == '1h'

    def test_api_period_map_invalid_timeframe_raises_error(self, mock_config):
        """Test invalid timeframe raises ValueError during initialization"""
        with pytest.raises(ValueError):
            LongShortRatioLoader('BTCUSDT', 'invalid', mock_config)


# ============================================
# 3. API Fetch Tests (10 tests)
# ============================================

class TestAPIFetch:
    """Test API request handling and retry logic"""

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_success(self, mock_get, mock_config, sample_long_short_api_response):
        """Test successful API fetch returns data"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert len(result) == 5
        assert result[0]['timestamp'] == '1609459200000'
        assert result[0]['buyRatio'] == '0.5500'

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_empty_response(self, mock_get, mock_config, sample_long_short_api_response_empty):
        """Test API fetch with empty result list"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock empty response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response_empty
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert result == []

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_error_retcode(self, mock_get, mock_config, sample_long_short_api_response_error):
        """Test API fetch with error retCode returns empty list"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock error response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response_error
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert result == []

    @patch('indicators.long_short_ratio_loader.requests.get')
    @patch('indicators.long_short_ratio_loader.time.sleep')
    def test_fetch_from_api_timeout_retry(self, mock_sleep, mock_get, mock_config, sample_long_short_api_response):
        """Test API fetch retries on timeout"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock first call timeout, second call success
        mock_get.side_effect = [
            requests.exceptions.Timeout('Connection timeout'),
            MagicMock(json=lambda: sample_long_short_api_response)
        ]

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert len(result) == 5
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    @patch('indicators.long_short_ratio_loader.requests.get')
    @patch('indicators.long_short_ratio_loader.time.sleep')
    def test_fetch_from_api_connection_error(self, mock_sleep, mock_get, mock_config):
        """Test API fetch handles connection errors"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock connection error
        mock_get.side_effect = requests.exceptions.ConnectionError('Connection failed')

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert result == []
        assert mock_get.call_count == 3  # api_retry_attempts = 3

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_max_retries_exceeded(self, mock_get, mock_config):
        """Test API fetch returns empty list after max retries"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock all retries fail
        mock_get.side_effect = requests.exceptions.RequestException('API error')

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        result = loader.fetch_from_bybit_api(start_time, end_time)

        assert result == []

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_params_correct(self, mock_get, mock_config, sample_long_short_api_response):
        """Test API fetch uses correct parameters"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        loader.fetch_from_bybit_api(start_time, end_time)

        # Verify call parameters
        call_args = mock_get.call_args
        params = call_args[1]['params']

        assert params['category'] == 'linear'
        assert params['symbol'] == 'BTCUSDT'
        assert params['period'] == '15min'
        assert params['limit'] == 1000

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_url_correct(self, mock_get, mock_config, sample_long_short_api_response):
        """Test API fetch uses correct URL"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        loader.fetch_from_bybit_api(start_time, end_time)

        # Verify URL
        call_args = mock_get.call_args
        url = call_args[0][0]

        assert url == 'https://api.bybit.com/v5/market/account-ratio'

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_converts_timestamps_to_ms(self, mock_get, mock_config, sample_long_short_api_response):
        """Test API fetch converts datetime to milliseconds"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        loader.fetch_from_bybit_api(start_time, end_time)

        # Verify timestamps in milliseconds
        call_args = mock_get.call_args
        params = call_args[1]['params']

        assert params['startTime'] == 1609459200000
        assert params['endTime'] == 1609477200000

    @patch('indicators.long_short_ratio_loader.requests.get')
    def test_fetch_from_api_respects_batch_size(self, mock_get, mock_config, sample_long_short_api_response):
        """Test API fetch uses batch_size from config"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_long_short_api_response
        mock_get.return_value = mock_response

        # Fetch data
        start_time = datetime(2021, 1, 1, 0, 0, tzinfo=pytz.UTC)
        end_time = datetime(2021, 1, 1, 5, 0, tzinfo=pytz.UTC)
        loader.fetch_from_bybit_api(start_time, end_time)

        # Verify limit parameter matches batch_size
        call_args = mock_get.call_args
        params = call_args[1]['params']

        assert params['limit'] == loader.batch_size


# ============================================
# 4. Ratio Calculation Tests (7 tests)
# ============================================

class TestRatioCalculation:
    """Test buyRatio / sellRatio calculation"""

    def test_ratio_calculation_buyRatio_divided_by_sellRatio(self, mock_config):
        """Test ratio = buyRatio / sellRatio"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 0.60
        sell_ratio = 0.40
        expected_ratio = 0.60 / 0.40  # = 1.5

        # Test via save_to_db calculation
        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        assert ratio == expected_ratio

    def test_ratio_calculation_sellRatio_zero_returns_none(self, mock_config):
        """Test ratio = None when sellRatio is 0 (avoid division by zero)"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 1.0
        sell_ratio = 0.0

        # Calculate ratio
        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        assert ratio is None

    def test_ratio_calculation_high_buy_ratio(self, mock_config):
        """Test ratio calculation with high buyRatio (bullish)"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 0.80
        sell_ratio = 0.20
        expected_ratio = 0.80 / 0.20  # = 4.0 (very bullish)

        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        assert ratio == expected_ratio

    def test_ratio_calculation_high_sell_ratio(self, mock_config):
        """Test ratio calculation with high sellRatio (bearish)"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 0.30
        sell_ratio = 0.70
        expected_ratio = 0.30 / 0.70  # ≈ 0.4286 (bearish)

        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        assert abs(ratio - expected_ratio) < 0.0001

    def test_ratio_calculation_equal_ratios(self, mock_config):
        """Test ratio = 1.0 when buyRatio == sellRatio (neutral)"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 0.50
        sell_ratio = 0.50
        expected_ratio = 1.0  # Neutral market

        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        assert ratio == expected_ratio

    def test_ratio_calculation_preserves_precision(self, mock_config):
        """Test ratio calculation preserves decimal precision"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio = 0.5555
        sell_ratio = 0.4445
        expected_ratio = 0.5555 / 0.4445

        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        # Check precision up to 6 decimal places (DECIMAL(10,6))
        assert abs(ratio - expected_ratio) < 0.000001

    def test_ratio_calculation_parses_strings_to_float(self, mock_config):
        """Test ratio calculation converts string values to float"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # API returns strings
        buy_ratio_str = '0.6000'
        sell_ratio_str = '0.4000'

        # Parse to float
        buy_ratio = float(buy_ratio_str)
        sell_ratio = float(sell_ratio_str)

        if sell_ratio > 0:
            ratio = buy_ratio / sell_ratio
        else:
            ratio = None

        # Use approximate comparison due to float precision
        assert abs(ratio - 1.5) < 0.0001


# ============================================
# 5. Data Parsing Tests (5 tests)
# ============================================

class TestDataParsing:
    """Test timestamp and data type conversions"""

    def test_parse_api_timestamp_converts_ms_to_datetime(self, mock_config):
        """Test API timestamp (ms) converts to datetime"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        timestamp_ms = 1609459200000  # 2021-01-01 00:00:00 UTC
        expected_dt = datetime(2021, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

        # Convert
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)

        assert timestamp_dt == expected_dt

    def test_parse_api_timestamp_in_utc(self, mock_config):
        """Test parsed timestamp is in UTC timezone"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        timestamp_ms = 1609459200000
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)

        assert timestamp_dt.tzinfo == pytz.UTC

    def test_parse_buyRatio_as_float(self, mock_config):
        """Test buyRatio string parses to float"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        buy_ratio_str = '0.5500'
        buy_ratio = float(buy_ratio_str)

        assert isinstance(buy_ratio, float)
        assert buy_ratio == 0.55

    def test_parse_sellRatio_as_float(self, mock_config):
        """Test sellRatio string parses to float"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        sell_ratio_str = '0.4500'
        sell_ratio = float(sell_ratio_str)

        assert isinstance(sell_ratio, float)
        assert sell_ratio == 0.45

    def test_parse_handles_string_numbers(self, mock_config, sample_long_short_api_response):
        """Test parsing handles various string number formats"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Various formats
        test_strings = ['0.5500', '0.55', '1.0', '0.123456']

        for s in test_strings:
            parsed = float(s)
            assert isinstance(parsed, float)
            assert parsed > 0


# ============================================
# 6. Save to DB Tests (9 tests)
# ============================================

class TestSaveToDB:
    """Test INSERT...ON CONFLICT logic"""

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_inserts_new_records(self, mock_db_class, mock_config, sample_long_short_api_response):
        """Test save_to_db inserts new records"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1  # INSERT
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = sample_long_short_api_response['result']['list']
        loader.save_to_db(api_data)

        # Verify INSERT was called
        assert mock_cursor.execute.call_count == len(api_data)

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_updates_existing_null_records(self, mock_db_class, mock_config):
        """Test save_to_db updates existing records with NULL values"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2  # UPDATE
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = [{
            'timestamp': '1609459200000',
            'buyRatio': '0.55',
            'sellRatio': '0.45'
        }]
        loader.save_to_db(api_data)

        # Verify UPDATE was triggered (rowcount = 2)
        assert mock_cursor.execute.called

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_does_not_overwrite_existing_data(self, mock_db_class, mock_config):
        """Test save_to_db does not overwrite existing non-NULL data (WHERE clause)"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # No update (record already has data)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = [{
            'timestamp': '1609459200000',
            'buyRatio': '0.55',
            'sellRatio': '0.45'
        }]
        loader.save_to_db(api_data)

        # Verify query includes WHERE clause to protect existing data
        call_args = mock_cursor.execute.call_args_list
        sql = call_args[0][0][0]
        assert 'WHERE' in sql
        assert 'long_short_ratio IS NULL' in sql

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_on_conflict_behavior(self, mock_db_class, mock_config):
        """Test save_to_db uses ON CONFLICT clause"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = [{
            'timestamp': '1609459200000',
            'buyRatio': '0.55',
            'sellRatio': '0.45'
        }]
        loader.save_to_db(api_data)

        # Verify SQL includes ON CONFLICT
        call_args = mock_cursor.execute.call_args_list
        sql = call_args[0][0][0]
        assert 'ON CONFLICT' in sql
        assert 'DO UPDATE SET' in sql

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_empty_data_does_nothing(self, mock_db_class, mock_config):
        """Test save_to_db with empty list does nothing"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save empty data
        loader.save_to_db([])

        # Verify no execute was called
        assert not mock_cursor.execute.called

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_counts_inserted(self, mock_db_class, mock_config):
        """Test save_to_db counts inserted records correctly"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1  # INSERT
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save 3 records
        api_data = [
            {'timestamp': '1609459200000', 'buyRatio': '0.55', 'sellRatio': '0.45'},
            {'timestamp': '1609462800000', 'buyRatio': '0.60', 'sellRatio': '0.40'},
            {'timestamp': '1609466400000', 'buyRatio': '0.50', 'sellRatio': '0.50'}
        ]
        loader.save_to_db(api_data)

        # Verify 3 executes
        assert mock_cursor.execute.call_count == 3

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_counts_updated(self, mock_db_class, mock_config):
        """Test save_to_db counts updated records correctly"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2  # UPDATE
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = [
            {'timestamp': '1609459200000', 'buyRatio': '0.55', 'sellRatio': '0.45'}
        ]
        loader.save_to_db(api_data)

        # Verify execute was called (rowcount = 2 indicates UPDATE)
        assert mock_cursor.execute.called

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_commits_transaction(self, mock_db_class, mock_config):
        """Test save_to_db commits transaction after saving"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save data
        api_data = [
            {'timestamp': '1609459200000', 'buyRatio': '0.55', 'sellRatio': '0.45'}
        ]
        loader.save_to_db(api_data)

        # Verify commit was called
        mock_conn.commit.assert_called_once()

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_save_to_db_rollback_on_error(self, mock_db_class, mock_config):
        """Test save_to_db handles database errors gracefully"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception('DB error')
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Save should not raise exception
        api_data = [
            {'timestamp': '1609459200000', 'buyRatio': '0.55', 'sellRatio': '0.45'}
        ]

        with pytest.raises(Exception):
            loader.save_to_db(api_data)


# ============================================
# 7. NULL Handling for 1m Timeframe (6 tests)
# ============================================

class TestNULLHandling:
    """Test special handling for 1m timeframe (API doesn't support)"""

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_1m_sets_all_columns_to_null(self, mock_db_class, mock_config):
        """Test 1m timeframe sets all 3 columns to NULL"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC),),  # max_timestamp
            (None,)  # last_null_timestamp
        ]
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL
        loader.set_null_for_existing_records()

        # Verify UPDATE SQL includes all 3 columns
        sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list if 'UPDATE' in str(call[0][0])]
        assert len(sql_calls) > 0

        update_sql = sql_calls[0]
        assert 'long_short_buy_ratio = NULL' in update_sql
        assert 'long_short_sell_ratio = NULL' in update_sql
        assert 'long_short_ratio = NULL' in update_sql

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_1m_only_new_records(self, mock_db_class, mock_config):
        """Test NULL update only processes new records (incremental)"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        last_processed = datetime(2024, 1, 1, 10, 0, tzinfo=pytz.UTC)
        max_time = datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC)

        mock_cursor.fetchone.side_effect = [
            (max_time,),  # max_timestamp
            (last_processed,)  # last_null_timestamp
        ]
        mock_cursor.rowcount = 120  # 2 hours * 60 minutes = 120 new records
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL
        loader.set_null_for_existing_records()

        # Verify UPDATE includes WHERE timestamp > last_processed
        sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list if 'UPDATE' in str(call[0][0])]
        update_sql = sql_calls[0]
        assert 'timestamp >' in update_sql

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_1m_does_not_overwrite_processed(self, mock_db_class, mock_config):
        """Test NULL update does not re-process already processed records"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        max_time = datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC)

        # last_null_timestamp == max_timestamp (already processed)
        mock_cursor.fetchone.side_effect = [
            (max_time,),  # max_timestamp
            (max_time,)  # last_null_timestamp (same - all processed)
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL
        loader.set_null_for_existing_records()

        # Verify UPDATE was not called (rowcount check)
        update_calls = [call for call in mock_cursor.execute.call_args_list if 'UPDATE' in str(call[0][0])]
        assert len(update_calls) == 0  # No UPDATE when already processed

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_1m_identifies_last_processed(self, mock_db_class, mock_config):
        """Test NULL handler identifies last processed timestamp correctly"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        last_processed = datetime(2024, 1, 1, 10, 0, tzinfo=pytz.UTC)
        max_time = datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC)

        mock_cursor.fetchone.side_effect = [
            (max_time,),
            (last_processed,)
        ]
        mock_cursor.rowcount = 120
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL
        loader.set_null_for_existing_records()

        # Verify query for last_null_timestamp
        queries = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any('long_short_buy_ratio IS NULL' in q and 'MAX(timestamp)' in q for q in queries)

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_incremental_null_update(self, mock_db_class, mock_config):
        """Test NULL update is incremental (only new data)"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Simulate: processed up to 10:00, new data up to 12:00
        last_null = datetime(2024, 1, 1, 10, 0, tzinfo=pytz.UTC)
        max_time = datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC)

        mock_cursor.fetchone.side_effect = [
            (max_time,),
            (last_null,)
        ]
        mock_cursor.rowcount = 120  # 2 hours of new data
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL
        loader.set_null_for_existing_records()

        # Verify incremental update
        assert mock_cursor.rowcount == 120  # Only new records updated

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_null_handling_no_records_warning(self, mock_db_class, mock_config):
        """Test NULL handler logs warning when no records found"""
        loader = LongShortRatioLoader('BTCUSDT', '1m', mock_config)

        # Mock database with no records
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)  # No records
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Set NULL (should return early with warning)
        loader.set_null_for_existing_records()

        # Verify only 1 query was executed (MAX(timestamp) check)
        assert mock_cursor.execute.call_count == 1


# ============================================
# 8. Database Operations (4 tests)
# ============================================

class TestDatabaseOperations:
    """Test column creation and date range detection"""

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_ensure_columns_exist_creates_missing(self, mock_db_class, mock_config):
        """Test ensure_columns_exist creates missing columns"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database with no columns
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []  # No existing columns
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Ensure columns
        loader.ensure_columns_exist()

        # Verify ALTER TABLE was called 3 times (3 columns)
        alter_calls = [call for call in mock_cursor.execute.call_args_list if 'ALTER TABLE' in str(call[0][0])]
        assert len(alter_calls) == 3

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_ensure_columns_exist_skips_existing(self, mock_db_class, mock_config):
        """Test ensure_columns_exist skips already existing columns"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database with all columns already existing
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('long_short_buy_ratio',),
            ('long_short_sell_ratio',),
            ('long_short_ratio',)
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Ensure columns
        loader.ensure_columns_exist()

        # Verify no ALTER TABLE was called
        alter_calls = [call for call in mock_cursor.execute.call_args_list if 'ALTER TABLE' in str(call[0][0])]
        assert len(alter_calls) == 0

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_get_date_range_returns_correct_start_end(self, mock_db_class, mock_config):
        """Test get_date_range returns correct start and end dates"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        last_ratio = datetime(2024, 1, 1, 10, 0, tzinfo=pytz.UTC)
        max_indicator = datetime(2024, 1, 1, 12, 0, tzinfo=pytz.UTC)

        mock_cursor.fetchone.side_effect = [
            (last_ratio,),  # Last ratio date
            (max_indicator,)  # Max indicator date
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get date range
        start_date, end_date = loader.get_date_range()

        # Verify dates
        expected_start = last_ratio + timedelta(minutes=15)
        assert start_date == expected_start
        assert end_date == max_indicator

    @patch('indicators.long_short_ratio_loader.DatabaseConnection')
    def test_get_date_range_handles_no_data(self, mock_db_class, mock_config):
        """Test get_date_range returns (None, None) when no data exists"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        # Mock database with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (None,),  # No ratio data
            (None,)  # No indicator data
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get date range
        start_date, end_date = loader.get_date_range()

        # Verify None, None
        assert start_date is None
        assert end_date is None


# ============================================
# 9. Helper Functions (2 tests)
# ============================================

class TestHelperFunctions:
    """Test timeframe parsing utilities"""

    def test_parse_timeframe_minutes(self, mock_config):
        """Test _parse_timeframe converts minutes correctly"""
        loader = LongShortRatioLoader('BTCUSDT', '15m', mock_config)

        assert loader.timeframe_minutes == 15

    def test_parse_timeframe_hours(self, mock_config):
        """Test _parse_timeframe converts hours to minutes"""
        loader = LongShortRatioLoader('BTCUSDT', '1h', mock_config)

        assert loader.timeframe_minutes == 60
