# Unit Tests for Indicators Module

## Overview

Unit tests for indicators loaders: `sma_loader.py`, `ema_loader.py`, `rsi_loader.py`, `vma_loader.py`, `macd_loader.py`, `atr_loader.py`, `bollinger_bands_loader.py`, `stochastic_williams_loader.py`, `obv_loader.py`, `adx_loader.py`, `mfi_loader.py`, `vwap_loader.py`, `long_short_ratio_loader.py`, `fear_and_greed_alternative_loader.py`, `fear_and_greed_coinmarketcap_loader.py`.

**Total: 751 tests** (36 SMA + 40 EMA + 38 RSI + 36 VMA + 46 MACD + 49 ATR + 56 BB + 69 Stochastic/Williams + 43 OBV + 56 ADX + 59 MFI + 65 VWAP + 51 Long/Short Ratio + 51 Fear&Greed Alternative + 56 Fear&Greed CoinMarketCap)

**Не покрыты тестами (8 загрузчиков):** ichimoku, hv, supertrend, orderbook_bybit, orderbook_binance, options_dvol, options_dvol_indicators, options_aggregated

## Structure

```
tests/
├── __init__.py                         # Package init
├── conftest.py                         # Shared pytest fixtures (50 fixtures)
├── README.md                           # This file
└── unit/                               # Unit tests
    ├── __init__.py
    ├── test_sma_loader.py              # 36 tests for SMA Loader
    ├── test_ema_loader.py              # 40 tests for EMA Loader
    ├── test_rsi_loader.py              # 38 tests for RSI Loader
    ├── test_vma_loader.py              # 36 tests for VMA Loader
    ├── test_macd_loader.py             # 46 tests for MACD Loader
    ├── test_atr_loader.py              # 49 tests for ATR Loader
    ├── test_bollinger_bands_loader.py  # 56 tests for Bollinger Bands Loader
    ├── test_stochastic_williams_loader.py  # 69 tests for Stochastic & Williams %R
    ├── test_obv_loader.py              # 43 tests for OBV Loader
    ├── test_adx_loader.py              # 56 tests for ADX Loader
    ├── test_mfi_loader.py              # 59 tests for MFI Loader
    ├── test_vwap_loader.py             # 65 tests for VWAP Loader
    ├── test_long_short_ratio_loader.py # 51 tests for Long/Short Ratio Loader
    ├── test_fear_and_greed_alternative_loader.py  # 51 tests for Fear & Greed Alternative.me
    └── test_fear_and_greed_coinmarketcap_loader.py # 56 tests for Fear & Greed CoinMarketCap
```

## Running Tests

### ⚡ Quick Start (from project root)

**Using Makefile (RECOMMENDED):**
```bash
# From TradingChart/ directory
make test              # All tests
make test-unit         # Unit tests only
make test-sma          # SMA tests only
make test-ema          # EMA tests only
make test-rsi          # RSI tests only
make test-vma          # VMA tests only
make test-macd         # MACD tests only
make test-atr          # ATR tests only
make test-bb           # Bollinger Bands tests only
make test-stochastic   # Stochastic & Williams %R tests only
make test-obv          # OBV tests only
make test-adx          # ADX tests only
make test-mfi          # MFI tests only
make test-vwap         # VWAP tests only
make test-long-short-ratio  # Long/Short Ratio tests only
make test-fear-greed-alternative  # Fear & Greed Alternative.me tests only
make test-fear-greed-coinmarketcap  # Fear & Greed CoinMarketCap tests only
make coverage          # With coverage report
make test-quick        # Quick run
make help              # Show all commands
```

**Using pytest directly:**
```bash
# From TradingChart/ directory
pytest -v                         # All tests (auto-discovery with pytest.ini)
pytest indicators/tests/unit/ -v  # Unit tests only
pytest -k sma -v                  # Only SMA tests
```

---

### 📍 From indicators/ directory

```bash
cd indicators
pytest tests/unit/ -v
```

### Specific test file
```bash
# From indicators/
pytest tests/unit/test_sma_loader.py -v

# From project root TradingChart/
pytest indicators/tests/unit/test_sma_loader.py -v
# or with Makefile:
make test-sma
```

### Specific test class or function
```bash
# Run all timeframe parsing tests
pytest tests/unit/test_sma_loader.py::TestTimeframeParsing -v

# Run single test
pytest tests/unit/test_sma_loader.py::TestSMACalculation::test_sma_calculation_real_periods -v

# From project root:
pytest indicators/tests/unit/test_sma_loader.py::TestSMACalculation::test_sma_calculation_real_periods -v
```

### With coverage report
```bash
# From indicators/
pytest tests/unit/ --cov=. --cov-report=html

# From project root TradingChart/
make coverage
# or:
pytest indicators/tests/unit/ --cov=indicators --cov-report=html --cov-report=term-missing
```

### Quick run (no verbose)
```bash
pytest tests/unit/ -q
# or from root:
make test-quick
```

## Test Coverage

### test_sma_loader.py (36 tests)
Tests for Simple Moving Average loader

**1. Timeframe Parsing (9 tests)**
- Real production config (1m, 15m, 1h)
- Minutes, hours, days, weeks
- Mixed formats
- Case insensitivity
- Invalid formats handling
- Empty list fallback

**2. SMA Calculation (12 tests)**
- Real production periods [10, 30, 50, 100, 200]
- Simple sequences
- NaN handling (first N-1 values)
- Formula validation
- Precision (8 decimals)
- Edge cases (empty, single value, all same)

**3. Data Filtering (6 tests)**
- Lookback data removal
- dropna with 'how=all'
- Partial SMA values
- Timestamp range filtering
- Lookback multiplier calculation
- Empty result handling

**4. Database Formatting (5 tests)**
- Record structure (tuples for psycopg2)
- NaN → None conversion for PostgreSQL NULL
- Float value preservation
- Timestamp format compatibility
- Column order validation

**5. Helper Functions (3 tests)**
- Timeframe → minutes mapping
- SMA column name generation
- Table name generation

### test_ema_loader.py (40 tests)
Tests for Exponential Moving Average loader

**1. Timeframe Parsing (6 tests)**
- Real production config (1m, 15m, 1h)
- All timeframe formats (m, h, d, w)
- Case insensitivity
- Invalid format handling

**2. EMA Calculation (15 tests)**
- Alpha calculation: α = 2 / (period + 1)
- Real production periods [9, 12, 21, 26, 50, 100, 200]
- EMA formula: EMA = Price × α + EMA_prev × (1 - α)
- EMA initialization (first value = first price)
- Response to price changes
- Shorter period = faster response
- Checkpoint continuation
- Precision (8 decimals)
- NaN handling
- Multiple periods simultaneously
- Edge cases (empty, single, constant)
- Convergence comparison with SMA

**3. Checkpoint System (8 tests)**
- Retrieving last EMA checkpoint
- No data handling
- Continuity preservation
- Multiple period checkpoints
- Missing period handling
- Query format validation
- State immutability
- Decimal/float conversion

**4. Database Operations (6 tests)**
- Creating missing columns
- Skipping existing columns
- Table existence validation
- Column naming convention (ema_{period})
- Table name format
- Decimal precision (20,8)

**5. Helper Functions (4 tests)**
- Config loading
- Timeframe parsing returns dict
- Symbol initialization
- Timeframe minutes caching

### test_rsi_loader.py (38 tests)
Tests for Relative Strength Index loader with single-pass Wilder's smoothing

**1. Timeframe Parsing (4 tests)**
- Real production config (1m, 15m, 1h)
- All timeframe formats
- Invalid format handling
- Dict structure validation

**2. RSI Calculation (16 tests)**
- RSI formula: RSI = 100 - (100 / (1 + RS))
- Range validation [0, 100]
- Uptrend detection (RSI > 50)
- Downtrend detection (RSI < 50)
- Sideways market (RSI ≈ 50)
- Wilder's smoothing: avg = (avg × (period-1) + current) / period
- Real production periods [7, 9, 14, 21, 25]
- Checkpoint continuation (avg_gain, avg_loss)
- Extreme values (RSI → 100 with gains, RSI → 0 with losses)
- Zero loss handling (RSI = 100)
- Insufficient data handling
- Edge cases (single price, constant prices)
- Multiple periods simultaneously

**3. Checkpoint System (8 tests)**
- Checkpoint structure (avg_gain, avg_loss)
- Continuity preservation
- State updates
- Multiple period checkpoints
- Period analysis (empty/partial/complete groups)
- State immutability

**4. Database Operations (6 tests)**
- Creating missing columns
- Skipping existing columns
- Table existence validation
- Column naming convention (rsi_{period})
- Table name format
- Decimal precision (10,4)

**5. Helper Functions (4 tests)**
- Config loading
- Symbol initialization
- Timeframe minutes caching
- Type validation

### test_vma_loader.py (36 tests)
Tests for Volume Moving Average loader

**1. Timeframe Parsing (6 tests)**
- Real production config (1m, 15m, 1h)
- All timeframe formats (m, h, d, w)
- Case insensitivity
- Invalid format handling
- Mixed formats

**2. VMA Calculation (12 tests)**
- Real production periods [10, 20, 50, 100, 200]
- VMA formula: VMA = rolling mean of volume
- Simple sequence validation
- Formula validation: VMA = SUM(volume) / period
- NaN handling (first N-1 values)
- Precision (8 decimals, DECIMAL(20,8))
- Edge cases:
  - Insufficient data (< period)
  - Empty DataFrame
  - Single value
  - Constant volumes
  - Period equals data length
  - min_periods behavior
  - Increasing sequence

**3. Volume Aggregation (6 tests)**
- Volume summation for higher timeframes
- Lookback data removal
- dropna behaviors (all NaN, partial NaN)
- Timestamp range filtering
- Lookback multiplier calculation

**4. Database Operations (6 tests)**
- Creating missing columns
- Skipping existing columns
- Column naming convention (vma_{period})
- Table name format
- NaN → None conversion for PostgreSQL
- Decimal precision (20,8)

**5. Helper Functions (5 tests)**
- Config loading
- Timeframe parsing returns dict
- Symbol initialization
- get_last_vma_date (with data / no data)

### test_macd_loader.py (46 tests)
Tests for MACD (Moving Average Convergence Divergence) loader

**1. Timeframe Parsing (6 tests)**
- Real production config (1m, 15m, 1h)
- All timeframe formats (m, h, d, w)
- Case insensitivity
- Invalid format handling

**2. EMA Calculation (10 tests)**
- Basic EMA calculation
- Formula validation: EMA = Price × k + EMA_prev × (1 - k), k = 2/(period+1)
- Real MACD periods (12, 26)
- First value equals first price
- Response to trend
- Shorter period more responsive
- Precision (8 decimals)
- Edge cases (empty, single, constant prices)

**3. MACD Calculation (15 tests)**
- Basic MACD returns three components (line, signal, histogram)
- MACD Line formula: Fast EMA - Slow EMA
- Signal Line formula: EMA(MACD Line, signal_period)
- Histogram formula: MACD Line - Signal Line
- Classic configuration (12, 26, 9)
- Crypto configuration (6, 13, 5)
- Uptrend produces positive values
- Downtrend produces negative values
- Signal line crossover detection
- Histogram zero when lines equal
- Precision (8 decimals, DECIMAL(20,8))
- Edge cases (empty, insufficient data)
- Multiple configs simultaneously
- Signal line lags behind MACD (smoothing effect)

**4. Database Operations (8 tests)**
- Creating missing columns
- Skipping existing columns
- Column naming convention (macd_{fast}_{slow}_{signal}_line/signal/histogram)
- Table name format
- NaN → None conversion for PostgreSQL
- Decimal precision (20,8)
- get_last_macd_date (with data / no data)

**5. Helper Functions (6 tests)**
- Config loading
- Symbol initialization
- Timeframe parsing returns dict
- Timeframe minutes caching
- Table name format validation
- Lookback multiplier from config

### test_atr_loader.py (49 tests)
Tests for ATR (Average True Range) loader

**1. Timeframe Parsing (6 tests)**
- Real production config (1m, 15m, 1h)
- All timeframe formats (m, h, d, w)
- Case insensitivity
- Invalid format handling
- Returns dict
- Caching

**2. True Range Calculation (12 tests)**
- Basic TR calculation
- TR formula: TR = max(H-L, |H-PC|, |L-PC|)
- Case 1: H-L maximum (normal candle)
- Case 2: |H-PC| maximum (gap up)
- Case 3: |L-PC| maximum (gap down)
- First candle without prev_close (TR = H-L)
- Sequence of candles
- Precision (8 decimals)
- Edge cases (empty, single, constant prices)
- TR always positive or zero

**3. ATR Calculation (15 tests)**
- Basic ATR calculation
- First value = SMA(TR, period)
- Wilder's smoothing: ATR = (ATR_prev × (period-1) + TR_current) / period
- Real production periods [7, 14, 21, 30, 50, 100]
- ATR always positive (volatility indicator)
- High volatility → high ATR
- Low volatility → low ATR
- ATR smooths over time (Wilder's smoothing effect)
- NaN for first period-1 values
- Insufficient data (< period)
- Edge cases (empty, single, constant TR)
- Precision (8 decimals, DECIMAL(20,8))
- Period equals data length

**4. Database Operations (8 tests)**
- Creating missing columns
- Skipping existing columns
- Column naming convention (atr_{period})
- Table name format (indicators_bybit_futures_{tf})
- Candles table name format (candles_bybit_futures_{tf})
- Decimal precision (20,8)
- get_last_atr_date (with data / no data)

**5. Helper Functions (7 tests)**
- Config loading
- Symbol initialization
- Timeframe parsing returns dict
- Timeframe minutes caching
- Table names (indicators + candles)
- ATR periods from config

### test_bollinger_bands_loader.py (56 tests)
Tests for Bollinger Bands (BB) volatility indicator

**1. Column Name Generation (8 tests)**
- std_dev formatting: 2.0 → "2_0", 1.618 → "1_618"
- Column name generation for all 6 components
- Classic SMA configuration (20, 2.0)
- Golden ratio SMA (20, 1.618)
- Classic EMA configuration (20, 2.0)
- All six keys validation (upper, middle, lower, percent_b, bandwidth, squeeze)

**2. Bollinger Bands Calculation (18 tests)**
- Basic structure (all 6 components returned)
- Series lengths match input
- Middle band equals SMA when base='sma'
- Middle band equals EMA when base='ema'
- Upper band formula: Upper = Middle + k × σ
- Lower band formula: Lower = Middle - k × σ
- %B formula: %B = (Close - Lower) / (Upper - Lower)
- Bandwidth formula: Bandwidth = (Upper - Lower) / Middle × 100
- Band ordering: Upper ≥ Middle ≥ Lower
- %B typical range [0, 1] but can exceed
- Constant prices (σ = 0, so Upper = Middle = Lower)
- NaN at start (first period-1 values)
- Golden ratio std_dev (1.618)
- Wide bands std_dev (3.0)
- Short period (3)
- Long period (89)
- Empty series handling
- Invalid base raises ValueError

**3. SMA vs EMA Base (6 tests)**
- SMA base produces SMA middle band
- EMA base produces EMA middle band
- Standard deviation always from close prices (not from middle band)
- EMA faster reaction to price changes
- Bandwidth differs between SMA and EMA bases
- Base affects %B calculation

**4. Squeeze Detection (6 tests)**
- Squeeze flag is boolean Series
- Default squeeze threshold = 5%
- Low volatility detection (bandwidth < 5%)
- High volatility (no squeeze)
- Squeeze formula: squeeze = (bandwidth < threshold)
- Custom threshold (10%)

**5. Database Operations (10 tests)**
- Creating columns for upper band
- Price columns use DECIMAL(20,8)
- Percent columns use DECIMAL(10,4)
- Squeeze column uses BOOLEAN
- Skipping existing columns
- Getting last processed date (exists / none)
- Getting data range from candles
- Table name formats (indicators_bybit_futures_{tf})
- Query uses symbol filter

**6. Helper Functions (7 tests)**
- Aggregating 1m to 1m (passthrough)
- Aggregating 1m to 15m
- Aggregating 1m to 1h
- Unsupported timeframe raises ValueError
- BOLLINGER_CONFIGS structure (13 configs: 11 SMA + 2 EMA)
- Classic configuration exists (20, 2.0)
- Loader initialization defaults

### test_fear_and_greed_alternative_loader.py (51 tests)
Tests for Fear & Greed Index loader from Alternative.me API

**1. Column Names (3 tests)**
- Index column: fear_and_greed_index_alternative
- Classification column: fear_and_greed_index_classification_alternative
- Both columns follow naming convention

**2. API Integration (10 tests)**
- Successful API response parsing
- Public API (no authentication)
- API endpoint format (limit=0 for all data)
- Response structure validation (name, data array)
- Data array contains dictionaries with value, value_classification, timestamp
- Empty response handling
- API error handling
- HTTP request error handling
- Invalid response structure handling
- Timeout handling

**3. Data Parsing (5 tests)**
- Converting string timestamps to datetime
- Converting string values to integers
- Extracting value_classification strings
- Handling missing fields in response
- Data validation before database insertion

**4. Value Validation (5 tests)**
- Range validation [0, 100]
- Classification validation (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)
- Value-classification consistency
- Invalid value rejection
- Boundary value handling

**5. Checkpoint System (7 tests)**
- Getting last processed date
- No data handling
- Incremental loading (only new records)
- Checkpoint date filtering
- State management
- Resume from last checkpoint
- Date comparison logic

**6. Database Operations (8 tests)**
- Creating missing columns (index, classification)
- Skipping existing columns
- Table existence validation
- Column naming convention
- Index column: DECIMAL(10,4)
- Classification column: VARCHAR(50)
- Update batch execution
- Conflict handling (ON CONFLICT UPDATE)

**7. Day Consistency (6 tests)**
- One value per day across all timeframes
- Same index value in 1m, 15m, 1h tables
- Same classification in all tables
- Day-level grouping
- Timestamp normalization to start of day
- Multi-timeframe consistency validation

**8. Timeframe Processing (4 tests)**
- Processing 1m timeframe
- Processing 15m timeframe
- Processing 1h timeframe
- All three timeframes in sequence

**9. Helper Functions (2 tests)**
- Config loading
- Symbol validation (BTCUSDT only)

### test_fear_and_greed_coinmarketcap_loader.py (56 tests)
Tests for Fear & Greed Index loader from CoinMarketCap API

**1. Column Names (3 tests)**
- Index column: fear_and_greed_index_coinmarketcap
- Classification column: fear_and_greed_index_classification_coinmarketcap
- Both columns follow naming convention

**2. API Integration (12 tests)**
- Successful API response parsing
- API key authentication (X-CMC_PRO_API_KEY header)
- API endpoint format with API key
- Response structure validation (status, data array)
- Status code validation (error_code = 0)
- Credit count tracking
- Empty response handling
- API error handling (error_code != 0)
- HTTP request error handling
- Invalid response structure handling
- Timeout handling
- Missing API key handling

**3. Data Parsing (5 tests)**
- Converting integer timestamps to datetime
- Extracting integer values directly
- Extracting value_classification strings
- Handling missing fields in response
- Data validation before database insertion

**4. Value Validation (5 tests)**
- Range validation [0, 100]
- Classification validation (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)
- Value-classification consistency
- Invalid value rejection
- Boundary value handling

**5. Checkpoint System (8 tests)**
- Getting last processed date
- No data handling
- Incremental loading (only new records)
- Checkpoint date filtering
- State management
- Resume from last checkpoint
- Date comparison logic
- Batch continuation between checkpoints

**6. Database Operations (8 tests)**
- Creating missing columns (index, classification)
- Skipping existing columns
- Table existence validation
- Column naming convention
- Index column: DECIMAL(10,4)
- Classification column: VARCHAR(50)
- Update batch execution
- Conflict handling (ON CONFLICT UPDATE)

**7. Day Consistency (6 tests)**
- One value per day across all timeframes
- Same index value in 1m, 15m, 1h tables
- Same classification in all tables
- Day-level grouping
- Timestamp normalization to start of day
- Multi-timeframe consistency validation

**8. Batch Processing (5 tests)**
- Two-batch API calls (batch 1: limit=500, batch 2: start=500, limit=500)
- Batch 1 URL format (limit=500)
- Batch 2 URL format (start=500, limit=500)
- Combining results from both batches
- Deduplication across batches

**9. Caching (3 tests)**
- API response caching between timeframes
- Checkpoint caching
- Cache invalidation on new data

## Test Results

### All tests (751 total)
```bash
# Run all unit tests
make test-unit

============================= test session starts ==============================
751 tests collected

============================ 751 passed =======================================
```

## Dependencies

Required packages (already in requirements.txt):
- pytest >= 7.0.0
- pytest-cov >= 4.0.0
- pytest-mock >= 3.10.0

## Fixtures (conftest.py)

Reusable fixtures for all tests (50 total):

**General fixtures (8):**
- `mock_config` - Production config mock (SMA, EMA, RSI, VMA, MACD, ATR, BB)
- `sample_prices_small` - 10 prices for quick tests
- `sample_prices_medium` - 100 prices
- `sample_prices_large` - 250 prices (for SMA_200, EMA_200)
- `sample_candles_df` - DataFrame with timestamp, symbol, close
- `mock_database` - Mocked DatabaseConnection
- `real_btc_prices` - Real BTC prices for validation
- `sample_timeframes` - Timeframe test data

**EMA-specific fixtures (2):**
- `sample_ema_initial_state` - Initial EMA state for checkpoint testing
- `sample_prices_for_ema` - Specific price sequence for EMA

**RSI-specific fixtures (4):**
- `sample_rsi_checkpoint` - RSI checkpoint state (avg_gain, avg_loss)
- `sample_prices_trending_up` - Uptrend prices (RSI > 50)
- `sample_prices_trending_down` - Downtrend prices (RSI < 50)
- `sample_prices_sideways` - Sideways market (RSI ≈ 50)

**VMA-specific fixtures (3):**
- `sample_volumes_small` - 10 volume values for quick tests
- `sample_volumes_large` - 250 volume values (for VMA_200)
- `sample_candles_with_volume` - DataFrame with volume data

**MACD-specific fixtures (3):**
- `sample_macd_configs` - MACD configurations (classic, crypto)
- `sample_prices_for_macd` - Price sequence with upward trend
- `sample_ema_values` - Pre-calculated EMA values for testing

**ATR-specific fixtures (3):**
- `sample_atr_candles` - DataFrame with high, low, close for basic tests
- `sample_atr_high_volatility` - Candles with large gaps (high ATR)
- `sample_atr_low_volatility` - Candles with tight ranges (low ATR)

**Bollinger Bands-specific fixtures (3):**
- `sample_bb_configs` - BB configurations (classic, golden, classic_ema)
- `sample_prices_for_bb` - Price sequence with varying volatility (squeeze testing)
- `sample_bb_constant_prices` - Constant prices (edge case: σ = 0)

**Long/Short Ratio-specific fixtures (3):**
- `sample_long_short_api_response` - Successful API response with 5 records (buyRatio, sellRatio)
- `sample_long_short_api_response_empty` - Empty API response (no data for period)
- `sample_long_short_api_response_error` - API error response (retCode != 0)

**Fear & Greed Alternative.me-specific fixtures (3):**
- `sample_fear_greed_alternative_api_response` - Successful API response with 5 records
- `sample_fear_greed_alternative_api_response_empty` - Empty API response
- `sample_fear_greed_alternative_api_response_error` - API error response

**Fear & Greed CoinMarketCap-specific fixtures (3):**
- `sample_fear_greed_coinmarketcap_api_response` - Successful API response with 5 records (batch 1)
- `sample_fear_greed_coinmarketcap_api_response_empty` - Empty API response
- `sample_fear_greed_coinmarketcap_api_response_error` - API error response with error_code

## Adding More Tests

To add tests for other loaders (ATR, ADX, VMA, etc.):

1. Create new test file: `tests/unit/test_{loader}_loader.py`
2. Add loader-specific fixtures to conftest.py if needed
3. Follow same structure as existing tests (5 groups: Parsing, Calculation, Checkpoint, Database, Helpers)
4. Update Makefile with new test target
5. Update README with test documentation
6. Run: `pytest tests/unit/test_{loader}_loader.py -v`

**Example for ATR loader:**
```bash
# Create test file
touch indicators/tests/unit/test_atr_loader.py

# Add fixture for ATR to conftest.py
# - sample_candles_with_high_low (for ATR calculation)
# - sample_atr_checkpoint (previous ATR value)

# Add Makefile target
test-atr:
	pytest indicators/tests/unit/test_atr_loader.py -v

# Run tests
make test-atr
```

## CI/CD Integration

For GitHub Actions / GitLab CI:

```yaml
- name: Run unit tests
  run: |
    cd indicators
    pytest tests/unit/ -v --cov=. --cov-report=xml
```

## Configuration Files

### pytest.ini (project root)
Allows running tests from anywhere in the project. Configures:
- Test discovery paths
- Markers for test classification
- Output formatting
- Ignored directories

### Makefile (project root)
Convenient commands for running tests:
- `make test` - All tests
- `make test-unit` - Unit tests only
- `make test-sma` - SMA tests only
- `make coverage` - Tests with coverage
- `make clean` - Clean cache
- `make help` - Show all commands

## Notes

- **Tests are isolated** (no real DB access)
- **Uses mocks** for DatabaseConnection
- **Fast execution**: ~0.2s per loader (SMA, EMA, RSI, VMA)
- **Real production config** values used in tests
- **Timeframes**: 1m, 15m, 1h, 4h, 1d (all loaders)
- **Periods**:
  - SMA: 10, 30, 50, 100, 200
  - EMA: 9, 12, 21, 26, 50, 100, 200
  - RSI: 7, 9, 14, 21, 25
  - VMA: 10, 20, 50, 100, 200
  - MACD: 8 configurations (classic 12/26/9, crypto 6/13/5, aggressive 5/35/5, balanced 8/17/9, scalping 5/13/3, swing 10/21/9, longterm 21/55/13, ultralong 50/200/9)
  - ATR: 7, 14, 21, 30, 50, 100
- **Run from anywhere**: project root or indicators/ directory
- **pytest.ini** enables automatic test discovery
- **Incremental loading**: EMA, VMA test resumable loading
- **Formula validation**: All calculations verified against known formulas
  - SMA: rolling mean of price
  - EMA: exponential smoothing with α = 2/(period+1)
  - RSI: Wilder's smoothing with range [0, 100]
  - VMA: rolling mean of volume
  - MACD: Line = EMA(fast) - EMA(slow), Signal = EMA(Line, signal_period), Histogram = Line - Signal
  - ATR: TR = max(H-L, |H-PC|, |L-PC|), ATR = Wilder's smoothing (first = SMA, then (ATR_prev×(N-1) + TR)/N)
