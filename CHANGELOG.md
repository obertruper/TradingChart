# CHANGELOG

## [2025-10-23] - Enhanced Console Output and Performance Tracking

### ✨ Improvements

#### All Indicator Loaders (RSI, SMA, EMA, VMA, ATR, OBV)
- **Performance tracking**: Added automatic display of total execution time at the end of processing
  - Format: `⏱️  Total time: Xm Ys` (minutes and seconds)
  - Helps users track processing efficiency and estimate future runs
  - Added to: `rsi_loader.py`, `sma_loader.py`, `ema_loader.py`, `vma_loader.py`, `atr_loader.py`, `obv_loader.py`

#### RSI Loader Console Output
- **Cleaner output**: Removed redundant configuration file loading messages
- **Visual separators**: Added clear dividers (`=====`) between trading pairs for better readability
- **Optimized analysis display**: Improved formatting of RSI analysis messages
  - Before: `🔍 Анализ существующих данных RSI (оптимизированный):`
  - After: `🔍 BTCUSDT [1/10]. Анализ данных RSI [7, 9, 14, 21, 25]:`
- **Removed decorative lines**: Eliminated unnecessary `====` separators throughout the output
- **Removed empty log lines**: Cleaned up blank logger.info() calls

#### OBV Loader Console Output
- **Progress display**: Added trading pair progress counter to OBV calculation message
  - Before: `🔄 [XLMUSDT] [1m] OBV: Кумулятивный расчёт от начала истории до текущей даты`
  - After: `🔄 [XLMUSDT] [8/10] [1m] OBV: Кумулятивный расчёт от начала истории до текущей даты`
- Shows which symbol is being processed out of total symbols for better progress tracking

#### Documentation Updates
- Updated `indicators/README.md` with new "Отслеживание времени обработки" feature
- Updated `CLAUDE.md` Recent Improvements section with console output enhancements
- Added performance tracking documentation

### 📊 User Experience
These changes make it easier to:
- Track processing time for capacity planning
- Visually distinguish between different trading pairs during multi-symbol processing
- Quickly identify which symbol and periods are currently being analyzed
- Have cleaner, more professional console output

## [2025-10-17] - ADX (Average Directional Index) Indicator Implementation

### 🚀 New Features

#### ADX Indicator Loader
- **adx_loader.py**: Comprehensive ADX calculator with Wilder's smoothing method
- 8 periods total: 7, 10, 14, 20, 21, 25, 30, 50
- 24 columns in database: 8 periods × 3 components (adx, +DI, -DI)
- Multi-timeframe support: 1m, 15m, 1h with automatic aggregation from base 1m data
- Sequential period processing for better interrupt/resume capability
- Batch processing with 1-day batches for precise progress control

#### ADX Periods
**8 periods**: 7 (scalping), 10 (short-term swing), 14 (classic Wilder's original), 20 (medium-term), 21 (Fibonacci), 25 (balanced), 30 (monthly), 50 (long-term)

#### Technical Implementation
- **ADX Line**: Measures trend strength (0-100), does NOT show direction
- **+DI (Plus Directional Indicator)**: Bullish directional movement
- **-DI (Minus Directional Indicator)**: Bearish directional movement
- **Double Wilder Smoothing**: TR/+DM/-DM → smoothing → +DI/-DI → DX → smoothing → ADX
- True Range calculation: TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
- Directional Movement: +DM/-DM based on high/low changes
- Independent calculation (doesn't depend on other loaders)
- Lookback period: period × 4 for accuracy with double smoothing
- Processing order: shortest to longest (7 → 50)

#### ADX Interpretation
- **0-25**: Weak/absent trend (sideways movement) - avoid trend strategies
- **25-50**: Strong trend - good for trend-following strategies
- **50-75**: Very strong trend - excellent trending conditions
- **75-100**: Extremely strong trend - possible exhaustion, be cautious
- **+DI > -DI**: Bullish trend (upward)
- **-DI > +DI**: Bearish trend (downward)
- Difference between +DI/-DI shows direction strength

#### Type Conversion & Database Management
- **Decimal → float**: Automatic conversion of PostgreSQL Decimal types for pandas operations
- **numpy → Python native**: Convert np.float64 to float before DB insertion
- Direct use of `self.db.get_connection()` context manager
- Automatic column creation with proper data types (DECIMAL(10,4) for all components)
- Column existence check before data queries

#### Verification Tools
- **check_adx_status.py**: ADX-specific status checker
- Displays fill statistics per period and timeframe
- Shows latest ADX values with interpretation (trend strength and direction)
- Gap detection for last 30 days
- Trend analysis: ADX strength + +DI/-DI direction
- Example values for verification with TradingView/Bybit

### 📝 Documentation Updates

#### README.md
- Added ADX to file structure documentation
- Added usage examples with command-line options
- Updated database schema with 24 ADX columns
- Added ADX to implemented indicators list with all technical details

#### INDICATORS_REFERENCE.md
- Added comprehensive ADX documentation (~650 lines)
- Detailed explanation of all 3 components (ADX, +DI, -DI)
- All 8 periods with use cases
- Complete formulas and calculation steps
- 5 trading strategies with SQL examples
- Combinations with other indicators (SMA/EMA, RSI, MACD, Bollinger Bands, ATR)
- Limitations, best practices, and interesting facts
- Technical implementation details

#### File Naming
- Loader: `adx_loader.py`
- Status checker: `check_adx_status.py`
- Log files: `adx_*.log`

### 🗄️ Database Schema

#### New Columns in indicators_bybit_futures_{1m,15m,1h}:
```sql
-- Period 7
adx_7             DECIMAL(10,4)  -- ADX line
adx_7_plus_di     DECIMAL(10,4)  -- +DI line
adx_7_minus_di    DECIMAL(10,4)  -- -DI line

-- Period 10
adx_10            DECIMAL(10,4)
adx_10_plus_di    DECIMAL(10,4)
adx_10_minus_di   DECIMAL(10,4)

-- Period 14 (classic Wilder's original)
adx_14            DECIMAL(10,4)
adx_14_plus_di    DECIMAL(10,4)
adx_14_minus_di   DECIMAL(10,4)

-- Period 20
adx_20            DECIMAL(10,4)
adx_20_plus_di    DECIMAL(10,4)
adx_20_minus_di   DECIMAL(10,4)

-- Period 21 (Fibonacci)
adx_21            DECIMAL(10,4)
adx_21_plus_di    DECIMAL(10,4)
adx_21_minus_di   DECIMAL(10,4)

-- Period 25
adx_25            DECIMAL(10,4)
adx_25_plus_di    DECIMAL(10,4)
adx_25_minus_di   DECIMAL(10,4)

-- Period 30
adx_30            DECIMAL(10,4)
adx_30_plus_di    DECIMAL(10,4)
adx_30_minus_di   DECIMAL(10,4)

-- Period 50
adx_50            DECIMAL(10,4)
adx_50_plus_di    DECIMAL(10,4)
adx_50_minus_di   DECIMAL(10,4)
```

### 📈 Usage Examples

```bash
# Load ADX for all timeframes (1m, 15m, 1h)
cd indicators
python3 adx_loader.py

# Load ADX for specific timeframe
python3 adx_loader.py --timeframe 1m

# Load specific period only
python3 adx_loader.py --period 14

# Use larger batches for faster processing
python3 adx_loader.py --batch-days 3

# Check ADX status in database
python3 check_adx_status.py

# Show values for TradingView comparison
python3 check_adx_status.py --comparison
```

### 🔧 Technical Improvements
- Consistent naming convention across all files
- Proper error handling with detailed traceback
- Progress bars with day-based tracking
- Checkpoint system for resumable loading
- Applied lessons learned from Bollinger Bands implementation

### 📦 Files Changed
- **Added**: indicators/adx_loader.py (~660 lines)
- **Added**: indicators/check_adx_status.py (~350 lines)
- **Modified**: indicators/indicators_config.yaml (+7 lines for ADX section)
- **Modified**: check_indicators_in_db_save_excel.py (+18 lines for 24 ADX columns)
- **Modified**: indicators/README.md (+70 lines)
- **Modified**: indicators/INDICATORS_REFERENCE.md (+650 lines)
- **Modified**: CHANGELOG.md (this file)

---

## [2025-10-16] - Bollinger Bands: Fix pandas FutureWarning

### 🔧 Technical Improvements

#### Fixed pandas resample deprecation warnings
- **Problem**: `FutureWarning: 'H' is deprecated and will be removed in a future version, please use 'h' instead`
- **Cause**: Using deprecated uppercase timeframe aliases in pandas resample ('H' for hour, 'T' for minute)
- **Solution**: Updated to new lowercase format in `bollinger_bands_loader.py`:
  - `'1H'` → `'1h'` (hourly timeframe)
  - `'15T'` → `'15min'` (15-minute timeframe)
- **Impact**: Eliminates FutureWarning messages in console output
- **Compatibility**: Ensures compatibility with future pandas versions (2.x+)

---

## [2025-10-16] - Bollinger Bands Indicator Implementation

### 🚀 New Features

#### Bollinger Bands Indicator Loader
- **bollinger_bands_loader.py**: Comprehensive Bollinger Bands calculator with multiple configurations
- 13 configurations total: 11 SMA-based + 2 EMA-based variants
- 78 columns in database: 13 configs × 6 components (upper, middle, lower, %B, bandwidth, squeeze)
- Multi-timeframe support: 1m, 15m, 1h with automatic aggregation from base 1m data
- Sequential configuration processing for better interrupt/resume capability
- Batch processing with 1-day batches for precise progress control

#### BB Configurations
**SMA-based (11)**: ultrafast (3,2.0), scalping (5,2.0), short (10,1.5), intraday (14,2.0), tight (20,1.0), golden (20,1.618), classic (20,2.0), wide (20,3.0), fibonacci (21,2.0), fibonacci_medium (34,2.0), fibonacci_long (89,2.0)

**EMA-based (2)**: classic_ema (20,2.0), golden_ema (20,1.618)

#### Technical Implementation
- **Middle Band**: SMA or EMA of close prices
- **Upper/Lower Bands**: Middle ± (k × σ), where σ = standard deviation
- **%B (Percent B)**: (Close - Lower) / (Upper - Lower) - position within bands (0.0-1.0)
- **Bandwidth**: (Upper - Lower) / Middle × 100 - width in percentage
- **Squeeze**: Boolean flag when Bandwidth < 5% (low volatility, potential breakout)
- Independent SMA/EMA calculation (doesn't depend on sma_loader/ema_loader)
- Lookback period: period × 3 for accuracy at batch boundaries
- Processing order: shortest to longest (3 → 89)

#### Type Conversion & Database Management
- **Decimal → float**: Automatic conversion of PostgreSQL Decimal types for pandas operations
- **numpy → Python native**: Convert np.float64/np.bool_ to float/bool before DB insertion
- Direct use of `self.db.get_connection()` context manager
- Automatic column creation with proper data types (DECIMAL for numbers, BOOLEAN for squeeze)
- Column existence check before data queries

#### Verification Tools
- **check_bollinger_status.py**: BB-specific status checker
- Displays fill statistics per configuration and timeframe
- Shows latest BB values with interpretation (overbought/oversold based on %B)
- Gap detection for last 30 days
- Squeeze events detection (low volatility periods)
- Example values for verification with TradingView/Bybit

### 🐛 Bug Fixes

#### Fixed Context Manager Issue
- **Problem**: `'_GeneratorContextManager' object has no attribute 'close'`
- **Cause**: Double-wrapping of context manager (custom wrapper over DatabaseConnection's context manager)
- **Solution**: Removed custom `get_connection()` method, using `self.db.get_connection()` directly
- **Impact**: Fixed all 4 locations in the code

#### Fixed Column Creation Order
- **Problem**: `column "bollinger_bands_*" does not exist`
- **Cause**: Calling `get_last_processed_date()` before `ensure_columns_exist()`
- **Solution**: Added `ensure_columns_exist()` call before checking last date in config loop
- **Impact**: Columns now created automatically before first use

#### Fixed Decimal Type Incompatibility
- **Problem**: `unsupported operand type(s) for -: 'decimal.Decimal' and 'float'`
- **Cause**: PostgreSQL returns Decimal types, pandas operations expect float
- **Solution**: Added `close_prices.astype(float)` at start of `calculate_bollinger_bands()`
- **Impact**: All pandas calculations work correctly with DB data

#### Fixed numpy Type SQL Error
- **Problem**: `schema "np" does not exist` when inserting data
- **Cause**: numpy types (np.float64) passed directly to SQL query
- **Solution**: Explicit conversion to Python native types using `float()` and `bool()`
- **Impact**: All UPDATE queries execute successfully

### 📝 Documentation Updates

#### README.md
- Added Bollinger Bands to implemented indicators list
- Added usage examples with command-line options
- Added "Known Issues and Solutions" section with all 4 fixed bugs
- Updated file structure with bollinger_bands_loader.py and check_bollinger_status.py
- Added log file naming: bollinger_bands_*.log

#### INDICATORS_REFERENCE.md
- Added comprehensive BB documentation (~720 lines)
- Detailed explanation of all 6 components
- All 13 configurations with use cases
- Type conversion and DB management details
- 5 trading strategies with SQL examples
- Best practices and common pitfalls
- Technical implementation details

#### File Renaming
- Renamed: `bollinger_loader.py` → `bollinger_bands_loader.py`
- Updated all references in documentation
- Updated log file names: `bollinger_loader_*` → `bollinger_bands_*`

### 🔧 Technical Improvements
- Removed unused import: `from contextlib import contextmanager`
- Consistent naming convention across all files
- Proper error handling with detailed traceback
- Progress bars with day-based tracking

---

## [2025-10-16] - ATR (Average True Range) Indicator Implementation

### 🚀 New Features

#### ATR Indicator Loader
- **atr_loader.py**: Comprehensive ATR indicator calculator with Wilder smoothing
- Multi-period support: ATR_7, ATR_14, ATR_21, ATR_30, ATR_50, ATR_100
- Multi-timeframe support: 1m, 15m, 1h with automatic aggregation from base 1m data
- Sequential period processing for better interrupt/resume capability
- Batch processing with 1-day batches for precise progress control

#### Technical Implementation
- **True Range Calculation**: TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
- **Wilder Smoothing**: ATR = (ATR_prev × (period-1) + TR_current) / period
- Smart lookback period (period × 2 × timeframe_minutes) for calculation accuracy
- Dynamic timeframe aggregation: MAX(high), MIN(low), LAST(close)
- Timezone-aware datetime operations (UTC)

#### Verification Tools
- **check_atr_status.py**: ATR-specific status checker
- Displays fill statistics per period and timeframe
- Shows latest ATR values (top 10 records)
- Gap detection for last 30 days
- Comparison with source candle data (High, Low, Close)

### 🔧 Improvements

#### Progress Bar Enhancements
- Integrated tqdm progress bars with real-time status updates
- Shows total records processed and latest timestamp
- Clean single-line updates (no multi-line output)
- Format: `📊 ATR_{period} {TF}: XX%|████| N/total [elapsed<remaining] всего: XXX, последняя: YYYY-MM-DD HH:MM`

#### Database Fixes
- Fixed "relation does not exist" error for 15m and 1h timeframes
- `get_data_range()` now always reads from base `candles_bybit_futures_1m` table
- Proper handling of timezone-aware vs timezone-naive datetime comparisons
- Added timezone import and UTC-aware datetime operations

#### Error Handling
- Retry logic with exponential backoff (3 attempts, 2^attempt seconds delay)
- Context manager pattern for all database connections
- NULL value handling for insufficient data periods
- Graceful skipping of incomplete current periods

### 📊 Documentation Updates

#### INDICATORS_REFERENCE.md (+315 lines)
- Comprehensive ATR documentation with full history
- Detailed calculation formulas and examples
- All 6 periods with specific use cases:
  - ATR_7: Short-term volatility (scalping, day trading)
  - ATR_14: Standard period (J. Welles Wilder's original)
  - ATR_21: Medium-term volatility (swing trading)
  - ATR_30: Monthly volatility (position trading)
  - ATR_50: Long-term trend analysis
  - ATR_100: Ultra long-term volatility baseline
- 5 trading strategies with SQL examples
- Integration with other indicators (MA, RSI, Bollinger Bands, VMA)

#### README.md Updates
- Added ATR to file structure documentation
- Updated configuration examples with ATR section
- Added ATR loading instructions with command-line options
- Updated database schema with 6 ATR columns
- Added ATR to implemented indicators list

#### check_indicators_in_db_save_excel.py
- Added ATR column mapping for Excel export
- Now supports all 6 ATR periods in verification reports

### 🗄️ Database Schema

#### New Columns in indicators_bybit_futures_{1m,15m,1h}:
```sql
atr_7     DECIMAL(20,8)  -- 7-period ATR
atr_14    DECIMAL(20,8)  -- 14-period ATR (standard)
atr_21    DECIMAL(20,8)  -- 21-period ATR
atr_30    DECIMAL(20,8)  -- 30-period ATR
atr_50    DECIMAL(20,8)  -- 50-period ATR
atr_100   DECIMAL(20,8)  -- 100-period ATR
```

### 📈 Usage Examples

```bash
# Load ATR for all timeframes (1m, 15m, 1h)
cd indicators
python3 atr_loader.py

# Load ATR for specific timeframe
python3 atr_loader.py --timeframe 1m

# Use larger batches for faster processing
python3 atr_loader.py --batch-days 7

# Check ATR status in database
python3 check_atr_status.py
```

### 🐛 Bug Fixes
- Fixed AttributeError: '_GeneratorContextManager' object has no attribute 'cursor'
- Fixed TypeError: can't compare offset-naive and offset-aware datetimes
- Fixed psycopg2.errors.UndefinedTable for non-existent 15m/1h candle tables
- Removed logger.info() calls inside tqdm loops to prevent multi-line output

### 📦 Files Changed
- **Added**: indicators/atr_loader.py (~660 lines)
- **Added**: indicators/check_atr_status.py (~350 lines)
- **Modified**: indicators/INDICATORS_REFERENCE.md (+315 lines)
- **Modified**: indicators/README.md (+70 lines)
- **Modified**: check_indicators_in_db_save_excel.py (+1 line)

---

## [2025-09-16] - Major Update: Multi-Symbol Support & Enhanced Monitoring

### 🚀 New Features

#### Multi-Symbol Support
- **data_loader_futures.py** now processes multiple symbols in a single run
- **monitor.py** monitors and updates multiple symbols sequentially
- Automatic handling of different launch dates for each symbol
- Configured symbols: BTCUSDT, ETHUSDT, XRPUSDT, SOLUSDT, ADAUSDT

#### Enhanced Progress Tracking
- Progress bars now show exact candle count (e.g., `252/252`) instead of uncertain count (`252/?`)
- Real-time display of latest loaded timestamp during data collection
- Automatic adjustment when API returns more data than expected
- Clean progress bar management between symbols

#### Database Analysis Tool
- New `check_db_symbols.py` script for comprehensive database analysis
- Shows data completeness percentage for each symbol
- Identifies gaps in historical data
- Displays daily statistics and year-by-year distribution

### 🔧 Improvements

#### monitor.py Enhancements
- Fixed multi-symbol processing logic
- Improved logging for all symbols, even when up-to-date
- Compact mode now shows symbol status clearly
- Added configuration debug output at startup
- Better cleanup of progress bars between symbols

#### Configuration Updates
- `data_collector_config.yaml` supports multiple symbols with comments on launch dates
- `monitor_config.yaml` includes 5 default symbols for monitoring
- Smart detection of symbol launch dates from symbols_launch_dates.json

#### API Integration
- API keys now optional for public data collection (OHLCV candles)
- Improved error handling when API keys not provided
- Maintained compatibility with authenticated endpoints

### 📊 Data Collected

As of 2025-09-16:
- **BTCUSDT**: 2,881,405 candles (100% complete from 2020-03-25)
- **ETHUSDT**: 2,370,722 candles (100% complete from 2021-03-15)
- **XRPUSDT**: Ready for collection (launch: 2021-05-13)
- **SOLUSDT**: Ready for collection (launch: 2021-10-15)
- **ADAUSDT**: Ready for collection (launch: 2021-01-13)

### 🔒 Security Improvements
- All passwords removed from documentation
- Environment variables via .env file
- Secure credential management
- Three-tier database user permission system

### 📝 Documentation Updates
- Updated CLAUDE.md with multi-symbol capabilities
- Enhanced VPS_DEPLOYMENT.md with new features
- Added symbol launch dates to configuration comments
- Created comprehensive changelog

### 🐛 Bug Fixes
- Fixed progress bar showing '?' for total count
- Resolved timestamp type conversion errors
- Fixed duplicate progress bar display issues
- Corrected multi-symbol processing in check-once mode

### 🔄 Files Modified

#### Core Scripts
- `monitor.py` - Multi-symbol support, enhanced progress bars
- `data_loader_futures.py` - Optional API keys, multi-symbol processing
- `check_db_symbols.py` - New database analysis tool

#### Configuration Files
- `data_collector_config.yaml` - Multiple symbols with launch dates
- `monitor_config.yaml` - 5 symbols configured by default

#### Documentation
- `CLAUDE.md` - Updated with recent improvements
- `VPS_DEPLOYMENT.md` - Enhanced deployment instructions
- `CHANGELOG.md` - This file (new)

### 📋 Migration Notes

To use the new multi-symbol features:

1. Update configuration files with desired symbols
2. Copy updated `monitor.py` to VPS
3. Restart monitoring daemon
4. Run `data_loader_futures.py` for new symbols

### 🎯 Next Steps

Potential future improvements:
- Parallel symbol processing for faster updates
- Web dashboard for monitoring status
- Automated alerting for data gaps
- Integration with trading strategies
- Performance optimization for large datasets