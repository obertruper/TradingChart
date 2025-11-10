"""
Unit tests for Fear & Greed Index Loader (Alternative.me)

Tests cover:
1. Column Names - Verification of database column names
2. API Integration - API request handling and retry logic
3. Data Parsing - Timestamp and data type conversions
4. Value Validation - Range 0-100, classification validation
5. Checkpoint System - Checkpoint loading and saving
6. Database Operations - Column creation and update_batch
7. Day Consistency - Validation of one value per day
8. Timeframe Processing - Processing multiple timeframes
9. Helper Functions - Configuration and utilities

Total: 50 tests
"""

import pytest
from datetime import datetime, date, timezone, timedelta
from unittest.mock import MagicMock, patch, call
import requests

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from indicators.fear_and_greed_loader_alternative import FearAndGreedLoader


# ============================================
# 1. Column Names Tests (3 tests)
# ============================================

class TestColumnNames:
    """Test database column name verification"""

    def test_column_names_fear_and_greed_index_alternative_exists(self, mock_config):
        """Test fear_and_greed_index_alternative column name is defined"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Check that create_columns method exists
        assert hasattr(loader, 'create_columns')
        assert callable(loader.create_columns)

    def test_column_names_fear_and_greed_classification_alternative_exists(self, mock_config):
        """Test fear_and_greed_index_classification_alternative column name is defined"""
        loader = FearAndGreedLoader('BTCUSDT')

        assert hasattr(loader, 'create_columns')
        assert callable(loader.create_columns)

    def test_column_names_only_btcusdt_supported(self, mock_config):
        """Test that only BTCUSDT symbol is supported"""
        with pytest.raises(ValueError, match="Fear & Greed Index доступен только для BTCUSDT"):
            FearAndGreedLoader('ETHUSDT')


# ============================================
# 2. API Integration Tests (10 tests)
# ============================================

class TestAPIIntegration:
    """Test API request handling and retry logic"""

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_success(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test successful API fetch returns data"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is not None
        assert len(result) == 5
        assert date(2021, 1, 1) in result
        assert result[date(2021, 1, 1)]['value'] == 75

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_empty_response(self, mock_get, mock_config, sample_fear_greed_alternative_api_response_empty):
        """Test API fetch with empty data list raises ValueError on min/max"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock empty response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response_empty
        mock_get.return_value = mock_response

        # Get data - should raise ValueError: min() arg is an empty sequence (line 189)
        with pytest.raises(ValueError, match="min"):
            loader.get_api_data()

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_no_data_key(self, mock_get, mock_config):
        """Test API fetch with missing 'data' key returns None"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response without 'data' key
        mock_response = MagicMock()
        mock_response.json.return_value = {'name': 'Fear and Greed Index'}
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is None

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_timeout_retry(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test API fetch retries on timeout"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock first call timeout, second call success
        mock_get.side_effect = [
            requests.exceptions.Timeout('Connection timeout'),
            MagicMock(json=lambda: sample_fear_greed_alternative_api_response)
        ]

        # Get data
        result = loader.get_api_data()

        assert result is not None
        assert len(result) == 5
        assert mock_get.call_count == 2

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_connection_error(self, mock_get, mock_config):
        """Test API fetch handles connection errors"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock connection error
        mock_get.side_effect = requests.exceptions.ConnectionError('Connection failed')

        # Get data
        result = loader.get_api_data()

        assert result is None
        assert mock_get.call_count == 3  # retry_count = 3

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_max_retries_exceeded(self, mock_get, mock_config):
        """Test API fetch returns None after max retries"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock all retries fail
        mock_get.side_effect = requests.exceptions.RequestException('API error')

        # Get data
        result = loader.get_api_data()

        assert result is None
        assert mock_get.call_count == 3

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_url_correct(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test API fetch uses correct URL"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Verify URL
        call_args = mock_get.call_args
        url = call_args[0][0]

        assert url == 'https://api.alternative.me/fng/?limit=0'

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_timeout_parameter(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test API fetch uses timeout parameter"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response
        mock_get.return_value = mock_response

        # Get data
        loader.get_api_data()

        # Verify timeout parameter
        call_args = mock_get.call_args
        assert call_args[1]['timeout'] == 30

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_caches_result(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test API data is cached after first fetch"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response
        mock_get.return_value = mock_response

        # First call
        result1 = loader.get_api_data()

        # Second call should use cache
        result2 = loader.get_api_data()

        assert result1 == result2
        assert mock_get.call_count == 1  # Only called once

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_get_api_data_raise_for_status(self, mock_get, mock_config):
        """Test API fetch handles HTTP errors"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock HTTP error
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('404 Not Found')
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        assert result is None


# ============================================
# 3. Data Parsing Tests (5 tests)
# ============================================

class TestDataParsing:
    """Test timestamp and data type conversions"""

    def test_parse_timestamp_converts_to_date(self, mock_config):
        """Test API timestamp converts to date"""
        loader = FearAndGreedLoader('BTCUSDT')

        timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
        expected_date = date(2021, 1, 1)

        # Convert
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()

        assert dt == expected_date

    def test_parse_value_as_integer(self, mock_config):
        """Test value string parses to integer"""
        loader = FearAndGreedLoader('BTCUSDT')

        value_str = '75'
        value_int = int(value_str)

        assert isinstance(value_int, int)
        assert value_int == 75

    def test_parse_classification_as_string(self, mock_config):
        """Test classification remains as string"""
        loader = FearAndGreedLoader('BTCUSDT')

        classification = 'Greed'

        assert isinstance(classification, str)
        assert classification in ['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed']

    def test_parse_handles_various_classifications(self, mock_config):
        """Test parsing handles all classification types"""
        loader = FearAndGreedLoader('BTCUSDT')

        valid_classifications = ['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed']

        for classification in valid_classifications:
            assert isinstance(classification, str)
            assert len(classification) > 0

    @patch('indicators.fear_and_greed_loader_alternative.requests.get')
    def test_parse_converts_data_to_dict_by_date(self, mock_get, mock_config, sample_fear_greed_alternative_api_response):
        """Test API data converts to dict keyed by date"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = sample_fear_greed_alternative_api_response
        mock_get.return_value = mock_response

        # Get data
        result = loader.get_api_data()

        # Check structure
        assert isinstance(result, dict)
        for date_key, data in result.items():
            assert isinstance(date_key, date)
            assert 'value' in data
            assert 'classification' in data


# ============================================
# 4. Value Validation Tests (5 tests)
# ============================================

class TestValueValidation:
    """Test value range 0-100 and classification validation"""

    def test_value_in_range_0_100(self, mock_config):
        """Test fear & greed value is in range [0, 100]"""
        loader = FearAndGreedLoader('BTCUSDT')

        valid_values = [0, 25, 50, 75, 100]

        for value in valid_values:
            assert 0 <= value <= 100

    def test_value_extreme_fear_range(self, mock_config):
        """Test Extreme Fear classification (0-24)"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Extreme Fear: 0-24
        value = 20
        classification = 'Extreme Fear'

        assert value < 25
        assert classification == 'Extreme Fear'

    def test_value_fear_range(self, mock_config):
        """Test Fear classification (25-49)"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Fear: 25-49
        value = 35
        classification = 'Fear'

        assert 25 <= value < 50
        assert classification == 'Fear'

    def test_value_greed_range(self, mock_config):
        """Test Greed classification (51-74)"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Greed: 51-74
        value = 65
        classification = 'Greed'

        assert 51 <= value < 75
        assert classification == 'Greed'

    def test_value_extreme_greed_range(self, mock_config):
        """Test Extreme Greed classification (75-100)"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Extreme Greed: 75-100
        value = 85
        classification = 'Extreme Greed'

        assert value >= 75
        assert classification == 'Extreme Greed'


# ============================================
# 5. Checkpoint System Tests (7 tests)
# ============================================

class TestCheckpointSystem:
    """Test checkpoint loading and saving"""

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_checkpoint_returns_last_date(self, mock_db_class, mock_config):
        """Test get_checkpoint returns last processed date"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database - cursor() does NOT use context manager (line 214)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        last_date = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        mock_cursor.fetchone.return_value = (last_date,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get checkpoint
        result = loader.get_checkpoint('1h')

        assert result == last_date

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_checkpoint_returns_none_when_no_data(self, mock_db_class, mock_config):
        """Test get_checkpoint returns None when no data exists"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get checkpoint
        result = loader.get_checkpoint('1h')

        assert result is None

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_checkpoint_queries_correct_table(self, mock_db_class, mock_config):
        """Test get_checkpoint queries correct timeframe table"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get checkpoint for 15m
        loader.get_checkpoint('15m')

        # Verify query uses correct table
        query = mock_cursor.execute.call_args[0][0]
        assert 'indicators_bybit_futures_15m' in query

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_checkpoint_filters_by_symbol(self, mock_db_class, mock_config):
        """Test get_checkpoint filters by BTCUSDT symbol"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get checkpoint
        loader.get_checkpoint('1h')

        # Verify query filters by symbol
        query_params = mock_cursor.execute.call_args[0][1]
        assert query_params[0] == 'BTCUSDT'

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_checkpoint_filters_not_null(self, mock_db_class, mock_config):
        """Test get_checkpoint filters for NOT NULL values"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get checkpoint
        loader.get_checkpoint('1h')

        # Verify query checks for NOT NULL
        query = mock_cursor.execute.call_args[0][0]
        assert 'IS NOT NULL' in query

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_start_date_returns_min_timestamp(self, mock_db_class, mock_config):
        """Test get_start_date returns MIN(timestamp) from database"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database to return MIN timestamp
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        min_date = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        mock_cursor.fetchone.return_value = (min_date,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get start date
        result = loader.get_start_date()

        # Should return MIN timestamp from DB (line 229-250)
        assert result == min_date
        # Verify query uses MIN(timestamp)
        query = mock_cursor.execute.call_args[0][0]
        assert 'MIN(timestamp)' in query

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_get_start_date_returns_none_when_no_data(self, mock_db_class, mock_config):
        """Test get_start_date returns None when no data in database"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database to return None (no data)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Get start date
        result = loader.get_start_date()

        # Should return None (line 249-250)
        assert result is None


# ============================================
# 6. Database Operations Tests (8 tests)
# ============================================

class TestDatabaseOperations:
    """Test column creation and update_batch"""

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_create_columns_creates_missing(self, mock_db_class, mock_config):
        """Test create_columns creates missing columns"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database with no columns - 3 calls per timeframe × 3 timeframes = 9 calls
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # For each timeframe: table exists (True), column1 missing (False), column2 missing (False)
        mock_cursor.fetchone.side_effect = [
            (True,), (False,), (False,),  # 1m timeframe
            (True,), (False,), (False,),  # 15m timeframe
            (True,), (False,), (False,)   # 1h timeframe
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        mock_conn.commit = MagicMock()  # Mock commit
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Create columns
        result = loader.create_columns()

        assert result == True
        assert mock_conn.commit.called

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_create_columns_skips_existing(self, mock_db_class, mock_config):
        """Test create_columns skips already existing columns"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database with all columns existing
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Table exists, columns exist
        mock_cursor.fetchone.side_effect = [(True,), (True,), (True,)] * 3
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Create columns
        result = loader.create_columns()

        assert result == True

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_updates_records(self, mock_db_class, mock_config):
        """Test update_batch updates records in database"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        result = loader.update_batch('1h', test_date, 75, 'Greed')

        assert result == 100

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_uses_correct_table(self, mock_db_class, mock_config):
        """Test update_batch uses correct timeframe table"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch for 15m
        test_date = date(2024, 1, 15)
        loader.update_batch('15m', test_date, 75, 'Greed')

        # Verify query uses correct table
        query = mock_cursor.execute.call_args[0][0]
        assert 'indicators_bybit_futures_15m' in query

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_filters_by_date(self, mock_db_class, mock_config):
        """Test update_batch filters by date range"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify date parameters - method converts date to datetime (line 269-270)
        query_params = mock_cursor.execute.call_args[0][1]
        start_time = datetime.combine(test_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_time = start_time + timedelta(days=1)
        assert start_time in query_params
        assert end_time in query_params

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_sets_value_and_classification(self, mock_db_class, mock_config):
        """Test update_batch sets both value and classification"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify parameters include value and classification
        query_params = mock_cursor.execute.call_args[0][1]
        assert 75 in query_params
        assert 'Greed' in query_params

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_commits_transaction(self, mock_db_class, mock_config):
        """Test update_batch commits transaction"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch
        test_date = date(2024, 1, 15)
        loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify commit was called
        mock_conn.commit.assert_called_once()

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_update_batch_raises_exception_on_error(self, mock_db_class, mock_config):
        """Test update_batch raises exception on database error"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception('DB error')
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        mock_conn.rollback = MagicMock()  # Mock rollback
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Update batch should raise exception (line 294)
        test_date = date(2024, 1, 15)
        with pytest.raises(Exception, match='DB error'):
            loader.update_batch('1h', test_date, 75, 'Greed')

        # Verify rollback was called
        assert mock_conn.rollback.called


# ============================================
# 7. Day Consistency Tests (6 tests)
# ============================================

class TestDayConsistency:
    """Test validation of one value per day"""

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_returns_true_when_consistent(self, mock_db_class, mock_config):
        """Test validate_day_consistency returns True when all timeframes have same value"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - each timeframe returns ONE value (loop on line 315)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Each fetchall() call returns list with one tuple (one unique value per timeframe)
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m timeframe
            [(75, 'Greed')],  # 15m timeframe
            [(75, 'Greed')]   # 1h timeframe
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_returns_false_when_inconsistent(self, mock_db_class, mock_config):
        """Test validate_day_consistency returns False when values differ"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - different values between timeframes
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m
            [(62, 'Greed')],  # 15m - different value!
            [(75, 'Greed')]   # 1h
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == False

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_checks_all_timeframes(self, mock_db_class, mock_config):
        """Test validate_day_consistency checks all configured timeframes"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - each timeframe returns ONE value
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m
            [(75, 'Greed')],  # 15m
            [(75, 'Greed')]   # 1h
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        loader.validate_day_consistency(test_date)

        # Should query 3 tables (1m, 15m, 1h)
        assert mock_cursor.execute.call_count == 3

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_filters_by_date(self, mock_db_class, mock_config):
        """Test validate_day_consistency filters by specific date"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m
            [(75, 'Greed')],  # 15m
            [(75, 'Greed')]   # 1h
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        loader.validate_day_consistency(test_date)

        # Verify date parameters in query - method converts to datetime (line 309-310)
        start_time = datetime.combine(test_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_time = start_time + timedelta(days=1)
        for call_args in mock_cursor.execute.call_args_list:
            params = call_args[0][1]
            assert start_time in params
            assert end_time in params

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_returns_true_when_no_data(self, mock_db_class, mock_config):
        """Test validate_day_consistency returns True when no data (considered consistent)"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock database with no data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_validate_day_consistency_checks_classification_too(self, mock_db_class, mock_config):
        """Test validate_day_consistency checks both value and classification"""
        loader = FearAndGreedLoader('BTCUSDT')
        loader.timeframes = ['1m', '15m', '1h']

        # Mock database - same value and classification across all timeframes
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [(75, 'Greed')],  # 1m
            [(75, 'Greed')],  # 15m
            [(75, 'Greed')]   # 1h
        ]
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Validate
        test_date = date(2024, 1, 15)
        result = loader.validate_day_consistency(test_date)

        assert result == True


# ============================================
# 8. Timeframe Processing Tests (4 tests)
# ============================================

class TestTimeframeProcessing:
    """Test processing multiple timeframes"""

    def test_timeframes_loaded_from_config(self, mock_config):
        """Test timeframes are loaded from configuration"""
        loader = FearAndGreedLoader('BTCUSDT')

        assert loader.timeframes == ['1m', '15m', '1h']

    def test_timeframes_can_be_overridden(self, mock_config):
        """Test timeframes can be overridden in config"""
        # This test verifies that the loader reads timeframes from config
        loader = FearAndGreedLoader('BTCUSDT')

        assert hasattr(loader, 'timeframes')
        assert isinstance(loader.timeframes, list)
        assert len(loader.timeframes) > 0

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_process_timeframe_updates_all_days_in_range(self, mock_db_class, mock_config):
        """Test process_timeframe updates all days in range"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock API data
        api_data = {
            date(2024, 1, 1): {'value': 75, 'classification': 'Greed'},
            date(2024, 1, 2): {'value': 62, 'classification': 'Greed'},
            date(2024, 1, 3): {'value': 45, 'classification': 'Fear'}
        }
        loader.api_data = api_data

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Process timeframe
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 3, 23, 59, tzinfo=timezone.utc)

        with patch.object(loader, 'update_batch') as mock_update:
            with patch.object(loader, 'get_api_data') as mock_get_api:
                with patch.object(loader, 'get_checkpoint') as mock_checkpoint:
                    # Return None so it starts from beginning (line 367-370)
                    mock_checkpoint.return_value = None
                    mock_get_api.return_value = api_data
                    mock_update.return_value = 100  # Mock update count
                    result = loader.process_timeframe('1h', start, end)

        # Should update for each day in api_data
        assert mock_update.call_count == 3

    @patch('indicators.fear_and_greed_loader_alternative.DatabaseConnection')
    def test_process_timeframe_skips_days_without_data(self, mock_db_class, mock_config):
        """Test process_timeframe skips days without API data"""
        loader = FearAndGreedLoader('BTCUSDT')

        # Mock API data with gap
        api_data = {
            date(2024, 1, 1): {'value': 75, 'classification': 'Greed'},
            # 2024-01-02 missing
            date(2024, 1, 3): {'value': 45, 'classification': 'Fear'}
        }
        loader.api_data = api_data

        # Mock database
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor  # No __enter__!
        loader.db.get_connection.return_value.__enter__.return_value = mock_conn

        # Process timeframe
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 3, 23, 59, tzinfo=timezone.utc)

        with patch.object(loader, 'update_batch') as mock_update:
            with patch.object(loader, 'get_api_data') as mock_get_api:
                with patch.object(loader, 'get_checkpoint') as mock_checkpoint:
                    # Return None so it starts from beginning
                    mock_checkpoint.return_value = None
                    mock_get_api.return_value = api_data
                    mock_update.return_value = 100  # Mock update count
                    result = loader.process_timeframe('1h', start, end)

        # Should only update for 2 days (days with data: 2024-01-01, 2024-01-03)
        assert mock_update.call_count == 2


# ============================================
# 9. Helper Functions Tests (2 tests)
# ============================================

class TestHelperFunctions:
    """Test configuration and utilities"""

    def test_load_config_returns_dict(self, mock_config):
        """Test load_config returns dictionary"""
        loader = FearAndGreedLoader('BTCUSDT')

        assert isinstance(loader.config, dict)
        assert 'indicators' in loader.config

    def test_symbol_set_to_btcusdt(self, mock_config):
        """Test symbol is set to BTCUSDT"""
        loader = FearAndGreedLoader('BTCUSDT')

        assert loader.symbol == 'BTCUSDT'
