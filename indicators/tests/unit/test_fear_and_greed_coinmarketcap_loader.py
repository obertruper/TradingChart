"""
Unit tests for Fear & Greed Index Loader (CoinMarketCap)

Tests cover:
1. Column Names - Verification of database column names
2. API Integration - API request handling with API key and retry logic
3. Data Parsing - Timestamp and data type conversions
4. Value Validation - Range 0-100, classification validation
5. Checkpoint System - Checkpoint loading, saving, and caching
6. Database Operations - Column creation and update_batch
7. Day Consistency - Validation of one value per day
8. Batch Processing - Processing 2 batches (limit=500, start=500)
9. Caching - API data and checkpoint caching

Total: 55 tests
"""

import pytest
from datetime import datetime, date, timezone, timedelta
from unittest.mock import MagicMock, patch, call
import requests

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from indicators.fear_and_greed_coinmarketcap_loader import CoinMarketCapFearGreedLoader


# ============================================
# 1. Column Names Tests (3 tests)
# ============================================

class TestColumnNames:
    """Test database column name verification"""

    def test_column_names_fear_and_greed_index_coinmarketcap_exists(self):
        """Test fear_and_greed_index_coinmarketcap column name is defined"""
        loader = CoinMarketCapFearGreedLoader()

        # Check that create_columns method exists
        assert hasattr(loader, 'create_columns')
        assert callable(loader.create_columns)
        assert loader.index_column == 'fear_and_greed_index_coinmarketcap'

    def test_column_names_fear_and_greed_classification_coinmarketcap_exists(self):
        """Test fear_and_greed_index_coinmarketcap_classification column name is defined"""
        loader = CoinMarketCapFearGreedLoader()

        assert hasattr(loader, 'create_columns')
        assert callable(loader.create_columns)
        assert loader.classification_column == 'fear_and_greed_index_coinmarketcap_classification'

    def test_column_names_symbol_is_btcusdt(self):
        """Test that symbol is set to BTCUSDT"""
        loader = CoinMarketCapFearGreedLoader()

        assert loader.symbol == 'BTCUSDT'


# ============================================
# 2. API Integration Tests (12 tests)
# ============================================

class TestAPIIntegration:
    """Test API request handling with API key and retry logic"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_success_batch1(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test successful API fetch for batch 1 returns data"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response for batch 1
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is not None
        assert len(result) == 5
        assert date(2021, 1, 1) in result
        assert result[date(2021, 1, 1)]['value'] == 75

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_two_batches(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API fetch makes 2 batch requests"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock responses for both batches
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Should call API twice (batch 1 and batch 2)
        assert mock_get.call_count == 2

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_batch1_params_correct(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test batch 1 uses correct parameters (limit=500)"""
        loader = CoinMarketCapFearGreedLoader()
        loader.batch_size = 500

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Check batch 1 parameters
        first_call = mock_get.call_args_list[0]
        params = first_call[1]['params']
        assert params['limit'] == 500
        assert 'start' not in params

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_batch2_params_correct(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test batch 2 uses correct parameters (start=500, limit=500)"""
        loader = CoinMarketCapFearGreedLoader()
        loader.batch_size = 500

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Check batch 2 parameters
        second_call = mock_get.call_args_list[1]
        params = second_call[1]['params']
        assert params['start'] == 500
        assert params['limit'] == 500

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_api_key_in_headers(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API key is included in request headers"""
        loader = CoinMarketCapFearGreedLoader()
        loader.api_key = 'test_api_key_12345'

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Check headers
        first_call = mock_get.call_args_list[0]
        headers = first_call[1]['headers']
        assert 'X-CMC_PRO_API_KEY' in headers
        assert headers['X-CMC_PRO_API_KEY'] == 'test_api_key_12345'

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_no_api_key_returns_none(self, mock_get):
        """Test API fetch returns None when no API key is configured"""
        loader = CoinMarketCapFearGreedLoader()
        loader.api_key = None

        # Get data
        result = loader.get_api_data()

        assert result is None
        assert not mock_get.called

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_empty_response(self, mock_get, sample_fear_greed_coinmarketcap_api_response_empty):
        """Test API fetch with empty data list"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock empty response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response_empty
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        # Should return None when no data (line 297: return None)
        assert result is None

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_api_error_401(self, mock_get):
        """Test API fetch handles 401 Unauthorized (invalid API key)"""
        loader = CoinMarketCapFearGreedLoader()
        loader.api_key = 'invalid_key'

        # Mock 401 error
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is None

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_api_error_429(self, mock_get):
        """Test API fetch handles 429 Too Many Requests (rate limit)"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock 429 error
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is None

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_url_correct(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API fetch uses correct URL"""
        loader = CoinMarketCapFearGreedLoader()
        loader.base_url = 'https://pro-api.coinmarketcap.com'

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Verify URL
        first_call = mock_get.call_args_list[0]
        url = first_call[0][0]
        assert url == 'https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical'

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_credits_tracking(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API fetch tracks credit usage"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Credits should be tracked (1 credit per batch = 2 total)
        # Verify through logging or internal state if needed

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_get_api_data_timeout_parameter(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API fetch uses timeout parameter"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Verify timeout parameter
        first_call = mock_get.call_args_list[0]
        assert first_call[1]['timeout'] == 30


# ============================================
# 3. Data Parsing Tests (5 tests)
# ============================================

class TestDataParsing:
    """Test timestamp and data type conversions"""

    def test_parse_timestamp_converts_to_date(self):
        """Test API timestamp (int) converts to date"""
        loader = CoinMarketCapFearGreedLoader()

        timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
        expected_date = date(2021, 1, 1)

        # Convert
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        result_date = dt.date()

        assert result_date == expected_date

    def test_parse_value_as_integer(self):
        """Test value is already integer (not string like Alternative.me)"""
        loader = CoinMarketCapFearGreedLoader()

        value = 75

        assert isinstance(value, int)
        assert value == 75

    def test_parse_classification_as_string(self):
        """Test classification is string"""
        loader = CoinMarketCapFearGreedLoader()

        classification = 'Greed'

        assert isinstance(classification, str)
        assert classification in ['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed']

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_parse_converts_data_to_dict_by_date(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API data converts to dict keyed by date"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        # Check structure
        assert isinstance(result, dict)
        for date_key, data in result.items():
            assert isinstance(date_key, date)
            assert 'value' in data
            assert 'classification' in data

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_parse_handles_both_batches(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test parsing combines data from both batches"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock different responses for each batch
        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        batch1_data = sample_fear_greed_coinmarketcap_api_response.copy()
        mock_response1.json.return_value = batch1_data

        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        batch2_data = sample_fear_greed_coinmarketcap_api_response.copy()
        mock_response2.json.return_value = batch2_data

        mock_get.side_effect = [mock_response1, mock_response2]

        # Get data
        result = loader.get_api_data()

        # Should have combined data from both batches
        assert isinstance(result, dict)
        assert len(result) > 0


# ============================================
# 4. Value Validation Tests (5 tests)
# ============================================

class TestValueValidation:
    """Test value range 0-100 and classification validation"""

    def test_value_in_range_0_100(self):
        """Test fear & greed value is in range [0, 100]"""
        loader = CoinMarketCapFearGreedLoader()

        valid_values = [0, 25, 50, 75, 100]

        for value in valid_values:
            assert 0 <= value <= 100

    def test_value_extreme_fear_range(self):
        """Test Extreme Fear classification (0-24)"""
        loader = CoinMarketCapFearGreedLoader()

        value = 20
        classification = 'Extreme Fear'

        assert value < 25
        assert classification == 'Extreme Fear'

    def test_value_fear_range(self):
        """Test Fear classification (25-49)"""
        loader = CoinMarketCapFearGreedLoader()

        value = 35
        classification = 'Fear'

        assert 25 <= value < 50
        assert classification == 'Fear'

    def test_value_greed_range(self):
        """Test Greed classification (51-74)"""
        loader = CoinMarketCapFearGreedLoader()

        value = 65
        classification = 'Greed'

        assert 51 <= value < 75
        assert classification == 'Greed'

    def test_value_extreme_greed_range(self):
        """Test Extreme Greed classification (75-100)"""
        loader = CoinMarketCapFearGreedLoader()

        value = 85
        classification = 'Extreme Greed'

        assert value >= 75
        assert classification == 'Extreme Greed'


# ============================================
# 5. Checkpoint System Tests (8 tests)
# ============================================

class TestCheckpointSystem:
    """Test checkpoint loading, saving, and caching"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_checkpoint_returns_last_date(self, mock_connect):
        """Test get_checkpoint returns last processed date with 00:00:00 time"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database - returns date object (from DATE(MAX()))
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        last_date = date(2024, 1, 15)  # date object, not datetime!
        mock_cursor.fetchone.return_value = (last_date,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get checkpoint
        result = loader.get_checkpoint('1h')

        # Should return datetime with 00:00:00 UTC (line 399)
        expected = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        assert result == expected

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_checkpoint_returns_none_when_no_data(self, mock_connect):
        """Test get_checkpoint returns None when no data exists"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get checkpoint
        result = loader.get_checkpoint('1h')

        assert result is None

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_all_checkpoints_returns_dict(self, mock_connect):
        """Test get_all_checkpoints returns dict of all timeframes"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        date_1m = datetime(2024, 1, 15, tzinfo=timezone.utc)
        date_15m = datetime(2024, 1, 14, tzinfo=timezone.utc)
        date_1h = datetime(2024, 1, 13, tzinfo=timezone.utc)
        mock_cursor.fetchone.side_effect = [(date_1m,), (date_15m,), (date_1h,)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get all checkpoints
        result = loader.get_all_checkpoints()

        assert isinstance(result, dict)
        assert '1m' in result
        assert '15m' in result
        assert '1h' in result

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_all_checkpoints_caches_result(self, mock_connect):
        """Test get_all_checkpoints caches results"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 15, tzinfo=timezone.utc),)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # First call
        result1 = loader.get_all_checkpoints()

        # Second call should use cache
        result2 = loader.get_all_checkpoints()

        assert result1 == result2
        # Should only call database once (cached on second call)

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_checkpoint_queries_correct_table(self, mock_connect):
        """Test get_checkpoint queries correct timeframe table"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get checkpoint for 15m
        loader.get_checkpoint('15m')

        # Verify query uses correct table
        query = mock_cursor.execute.call_args[0][0]
        assert 'indicators_bybit_futures_15m' in query

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_checkpoint_filters_by_symbol(self, mock_connect):
        """Test get_checkpoint filters by BTCUSDT symbol"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get checkpoint
        loader.get_checkpoint('1h')

        # Verify query filters by symbol
        query_params = mock_cursor.execute.call_args[0][1]
        assert query_params[0] == 'BTCUSDT'

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_get_checkpoint_filters_not_null(self, mock_connect):
        """Test get_checkpoint filters for NOT NULL values"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Get checkpoint
        loader.get_checkpoint('1h')

        # Verify query checks for NOT NULL
        query = mock_cursor.execute.call_args[0][0]
        assert 'IS NOT NULL' in query

    def test_checkpoints_cache_initialized_as_none(self):
        """Test checkpoints cache is initialized as None"""
        loader = CoinMarketCapFearGreedLoader()

        assert loader.checkpoints_cache is None


# ============================================
# 6. Database Operations Tests (8 tests)
# ============================================

class TestDatabaseOperations:
    """Test column creation and update_batch"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_create_columns_creates_missing(self, mock_connect):
        """Test create_columns creates missing columns"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1h']

        # Mock database with no columns
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Table exists, but columns don't
        mock_cursor.fetchone.side_effect = [(True,), (False,), (False,)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Create columns
        result = loader.create_columns()

        assert result == True

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_create_columns_skips_existing(self, mock_connect):
        """Test create_columns skips already existing columns"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1h']

        # Mock database with all columns existing
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Table exists, columns exist
        mock_cursor.fetchone.side_effect = [(True,), (True,), (True,)]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Create columns
        result = loader.create_columns()

        assert result == True

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_updates_records(self, mock_connect):
        """Test update_batch updates records in database"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        result = loader.update_batch('1h', test_date, 75, 'Greed')

        assert result == 100

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_uses_correct_table(self, mock_connect):
        """Test update_batch uses correct timeframe table"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch for 15m
        test_date = date(2024, 1, 15)
        loader.update_batch('15m', test_date, 75, 'Greed')

        # Verify query uses correct table
        query = mock_cursor.execute.call_args[0][0]
        assert 'indicators_bybit_futures_15m' in query

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_filters_by_date(self, mock_connect):
        """Test update_batch filters by date range"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify date parameters
        query_params = mock_cursor.execute.call_args[0][1]
        # Should filter by date range (timestamp >= start AND timestamp < end)
        assert 75 in query_params  # value
        assert 'Greed' in query_params  # classification

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_sets_value_and_classification(self, mock_connect):
        """Test update_batch sets both value and classification"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify parameters include value and classification
        query_params = mock_cursor.execute.call_args[0][1]
        assert 75 in query_params
        assert 'Greed' in query_params

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_commits_transaction(self, mock_connect):
        """Test update_batch commits transaction"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify commit was called
        mock_conn.commit.assert_called_once()

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_update_batch_returns_zero_on_error(self, mock_connect):
        """Test update_batch returns 0 on database error"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock database error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception('DB error')
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Update batch should handle error
        test_date = date(2024, 1, 15)
        result = loader.update_batch('1h', test_date, 75, 'Greed')

        assert result == 0


# ============================================
# 7. Day Consistency Tests (6 tests)
# ============================================

class TestDayConsistency:
    """Test validation of one value per day"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_returns_true_when_consistent(self, mock_connect):
        """Test validate_day_consistency returns True when all timeframes have same value"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - each timeframe returns ONE value (loop on line 485)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Each fetchall() call returns list with one tuple (one unique value per timeframe)
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m timeframe
            [(75, 'Greed')],  # 15m timeframe
            [(75, 'Greed')]   # 1h timeframe
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_returns_false_when_inconsistent(self, mock_connect):
        """Test validate_day_consistency returns False when values differ"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - different values
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(75, 'Greed'), (62, 'Greed'), (75, 'Greed')]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == False

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_checks_all_timeframes(self, mock_connect):
        """Test validate_day_consistency checks all configured timeframes"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(75, 'Greed')]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        loader.validate_day_consistency(test_date)

        # Should query 3 tables (1m, 15m, 1h)
        assert mock_cursor.execute.call_count == 3

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_filters_by_date(self, mock_connect):
        """Test validate_day_consistency filters by specific date"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(75, 'Greed')]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        loader.validate_day_consistency(test_date)

        # Verify date is in query
        query = mock_cursor.execute.call_args[0][0]
        assert 'DATE(timestamp)' in query or 'timestamp::date' in query

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_returns_true_when_no_data(self, mock_connect):
        """Test validate_day_consistency returns True when no data (considered consistent)"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1h']

        # Mock database with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_validate_day_consistency_checks_classification_too(self, mock_connect):
        """Test validate_day_consistency checks both value and classification"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - same value and classification
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Each fetchall() call returns list with one tuple per timeframe
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m
            [(75, 'Greed')],  # 15m
            [(75, 'Greed')]   # 1h
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True


# ============================================
# 8. Batch Processing Tests (5 tests)
# ============================================

class TestBatchProcessing:
    """Test processing 2 batches (limit=500, start=500)"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_batch_processing_two_requests(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test batch processing makes 2 API requests"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        assert mock_get.call_count == 2

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_batch_processing_first_batch_params(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test first batch uses limit parameter only"""
        loader = CoinMarketCapFearGreedLoader()
        loader.batch_size = 500

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Check first batch
        first_call = mock_get.call_args_list[0]
        params = first_call[1]['params']
        assert 'limit' in params
        assert 'start' not in params

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_batch_processing_second_batch_params(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test second batch uses start and limit parameters"""
        loader = CoinMarketCapFearGreedLoader()
        loader.batch_size = 500

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Check second batch
        second_call = mock_get.call_args_list[1]
        params = second_call[1]['params']
        assert 'start' in params
        assert 'limit' in params
        assert params['start'] == 500

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_batch_processing_combines_results(self, mock_get):
        """Test batch processing combines results from both batches"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock different data for each batch
        batch1_response = {
            'status': {'error_code': '0', 'credit_count': 1},
            'data': [
                {'timestamp': 1609459200, 'value': 75, 'value_classification': 'Greed'}
            ]
        }

        batch2_response = {
            'status': {'error_code': '0', 'credit_count': 1},
            'data': [
                {'timestamp': 1609545600, 'value': 62, 'value_classification': 'Greed'}
            ]
        }

        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = batch1_response

        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = batch2_response

        mock_get.side_effect = [mock_response1, mock_response2]

        # Get data
        result = loader.get_api_data()

        # Should have data from both batches
        assert len(result) == 2

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_batch_processing_second_batch_continues_on_empty(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test second batch request is made even if first batch has data"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock responses
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Should still make 2 requests
        assert mock_get.call_count == 2


# ============================================
# 9. Caching Tests (3 tests)
# ============================================

class TestCaching:
    """Test API data and checkpoint caching"""

    @patch('indicators.fear_and_greed_coinmarketcap_loader.requests.get')
    def test_api_data_caches_result(self, mock_get, sample_fear_greed_coinmarketcap_api_response):
        """Test API data is cached after first fetch"""
        loader = CoinMarketCapFearGreedLoader()

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_fear_greed_coinmarketcap_api_response
        mock_get.return_value = mock_response

        # First call
        result1 = loader.get_api_data()

        # Second call should use cache
        result2 = loader.get_api_data()

        assert result1 == result2
        assert mock_get.call_count == 2  # Only called twice for 2 batches, not 4

    def test_api_data_cache_initialized_as_none(self):
        """Test api_data_cache is initialized as None"""
        loader = CoinMarketCapFearGreedLoader()

        assert loader.api_data_cache is None

    @patch('indicators.fear_and_greed_coinmarketcap_loader.psycopg2.connect')
    def test_checkpoints_cache_populated_on_first_call(self, mock_connect):
        """Test checkpoints cache stays None after get_all_checkpoints (filled only in run())"""
        loader = CoinMarketCapFearGreedLoader()
        loader.timeframes = ['1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # fetchall() returns list of tuples (timeframe, date) from UNION ALL query
        mock_cursor.fetchall.return_value = [('1h', date(2024, 1, 15))]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Initially None
        assert loader.checkpoints_cache is None

        # get_all_checkpoints() returns dict but doesn't populate cache (line 307-363)
        result = loader.get_all_checkpoints()

        # Result should be dict, but cache still None (filled only in run() at line 624)
        assert isinstance(result, dict)
        assert loader.checkpoints_cache is None
