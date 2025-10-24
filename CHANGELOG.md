# CHANGELOG

## [2025-10-24] - Unified Progress Bar Format Across All Loaders

### 🐛 Bug Fixes

#### KeyboardInterrupt Handling in All Indicator Loaders (12 files)
Added proper KeyboardInterrupt (Ctrl+C) handling to all indicator loaders for graceful script termination.

**Problem:**
- 11 indicator loaders had NO KeyboardInterrupt handling - script would crash with ugly traceback when pressing Ctrl+C
- 1 indicator loader (bollinger_bands) had INCORRECT handling - used `break` instead of `sys.exit(0)`, only exiting timeframe loop
- Users could not gracefully stop long-running indicator calculations
- Progress bars (tqdm) remained in inconsistent states after interrupt
- Database connections might not close properly

**Solution:**
- Added `try-except KeyboardInterrupt` blocks with `sys.exit(0)` to all 12 indicator loaders
- Now all loaders match correct behavior from rsi_loader.py and ema_loader.py (already had proper handling)
- Clean exit with informative message: "⚠️ Прервано пользователем. Можно продолжить позже с этого места."
- Proper cleanup and graceful termination

**Files changed (12 total):**

1. `indicators/bollinger_bands_loader.py` - FIXED incorrect `break` → `sys.exit(0)` (line 634)
2. `indicators/sma_loader.py` - ADDED KeyboardInterrupt handling (lines 685-705)
3. `indicators/atr_loader.py` - ADDED KeyboardInterrupt handling (lines 684-696)
4. `indicators/adx_loader.py` - ADDED KeyboardInterrupt handling (lines 766-787)
5. `indicators/vma_loader.py` - ADDED KeyboardInterrupt handling (lines 668-687)
6. `indicators/mfi_loader.py` - ADDED KeyboardInterrupt handling (lines 604-606)
7. `indicators/vwap_loader.py` - ADDED KeyboardInterrupt handling (lines 665-667)
8. `indicators/macd_loader.py` - ADDED KeyboardInterrupt handling (lines 706-718)
9. `indicators/long_short_ratio_loader.py` - ADDED KeyboardInterrupt handling (lines 641-643)
10. `indicators/stochastic_williams_loader.py` - ADDED KeyboardInterrupt handling (lines 1204-1211)
11. `indicators/obv_loader.py` - ADDED KeyboardInterrupt handling (lines 556-558)
12. `indicators/fear_and_greed_coinmarketcap_loader.py` - ADDED KeyboardInterrupt handling (lines 570-572)

**Code pattern used:**
```python
try:
    # Loader logic here
    loader.run(...)
    logger.info(f"✅ Символ {symbol} обработан")
except KeyboardInterrupt:
    logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
    sys.exit(0)
except Exception as e:
    logger.error(f"❌ Критическая ошибка для символа {symbol}: {e}")
    import traceback
    traceback.print_exc()
    continue
```

---

### ✨ Improvements

#### Progress Bar Standardization (12 Indicator Loaders)
Unified all progress bars to consistent format: `SYMBOL [x/y] INDICATOR TIMEFRAME`

**Changes applied:**
- **Removed emojis 📊**: Cleaned up progress bars for better readability and cleaner terminal output
- **Fixed spacing issues**: Corrected missing spaces between `symbol_progress` and indicator names (MACD, OBV)
- **Standardized order**: All progress bars now follow `INDICATOR TIMEFRAME` format (some had `TIMEFRAME INDICATOR`)
- **Added missing `symbol_progress`**: 3 loaders (ADX, Bollinger Bands, VMA) now properly show `[x/y]` counter

**Updated loaders (12 files):**

1. **sma_loader.py**
   - Removed: `📊` emoji from progress bar
   - Format: `BTCUSDT [1/10] SMA[10,30,50,100,200] 1M`

2. **ema_loader.py**
   - Removed: `📊` emoji
   - Changed: `EMA {periods}` → `EMA[{periods_str}]` for consistency with SMA/RSI
   - Format: `BTCUSDT [1/10] EMA[9,12,21,26,50,100,200] 1M`

3. **rsi_loader.py**
   - Removed: `📊` emoji
   - Changed: `RSI {periods}` → `RSI[{periods_str}]` for consistency
   - Format: `BTCUSDT [1/10] RSI[7,9,14,21,25] 1M - Загрузка`

4. **atr_loader.py**
   - Changed order: `{timeframe} ATR-{period}` → `ATR-{period} {timeframe}`
   - Format: `BTCUSDT [1/10] ATR-14 1M`

5. **mfi_loader.py**
   - Changed order: `{timeframe} MFI-{period}` → `MFI-{period} {timeframe}`
   - Format: `BTCUSDT [1/10] MFI-14 1M`

6. **vwap_loader.py**
   - Changed order for both daily and rolling VWAP
   - Daily: `{timeframe} Daily VWAP` → `VWAP-daily {timeframe}`
   - Rolling: `{timeframe} VWAP-{period}` → `VWAP-{period} {timeframe}`
   - Format: `BTCUSDT [1/10] VWAP-daily 1M`, `BTCUSDT [1/10] VWAP-20 1M`

7. **long_short_ratio_loader.py**
   - Changed: `{timeframe} Long/Short Ratio` → `LONG-SHORT {timeframe}`
   - Format: `BTCUSDT [1/10] LONG-SHORT 1M`

8. **macd_loader.py**
   - Added: `symbol_progress` attribute in `__init__` and main()
   - Removed: `📊` emoji
   - Fixed: Missing space between `symbol_progress` and `MACD`
   - Changed: `MACD {name} ({params})` → `MACD-{name}({params})` (compact format)
   - Format: `BTCUSDT [1/10] MACD-classic(12,26,9) 1M`

9. **obv_loader.py**
   - Removed: `📊` emoji
   - Fixed: Missing space between `symbol_progress` and timeframe
   - Added: `OBV` label for clarity
   - Format: `BTCUSDT [1/10] OBV 1M - Обновление БД`

10. **adx_loader.py**
    - Added: `symbol_progress` attribute in `__init__` and main()
    - Changed: `ADX_{period} {timeframe}` → `{symbol} {symbol_progress} ADX-{period} {timeframe}`
    - Format: `BTCUSDT [1/10] ADX-14 1M`

11. **bollinger_bands_loader.py**
    - Added: `symbol_progress` attribute in `__init__` and main()
    - Changed: `BB {name} ({period}, {std_dev}) {base} | {timeframe}` → `{symbol} {symbol_progress} BB-{name}({period},{std_dev},{base}) {timeframe}`
    - Format: `BTCUSDT [1/10] BB-classic(20,2.0,SMA) 1M`

12. **vma_loader.py**
    - Added: `symbol_progress` attribute in `__init__` and main()
    - Removed: `📊` emoji
    - Changed: `VMA_{period} {timeframe}` → `{symbol} {symbol_progress} VMA-{period} {timeframe}`
    - Format: `BTCUSDT [1/10] VMA-20 1M`

**Not modified:**
- `stochastic_williams_loader.py` - Already in correct format ✅
- `fear_and_greed_*.py` - Kept as-is (different logic for global metrics)

#### Before vs After Examples

**Before (inconsistent formats):**
```
📊 BTCUSDT [1/10] SMA[10,30,50,100,200] 1M
📊 BTCUSDT [1/10] EMA [9,12,21,26,50,100,200] 1M
📊 BTCUSDT [1/10] RSI [7,9,14,21,25] 1M - Загрузка
BTCUSDT [1/10] 1M ATR-14
ADX_21 1m:  75%|████████████...
BB classic (20, 2.0) SMA | 1m:  35%|████...
📊 VMA_20 1M
📊 BTCUSDT [1/10]MACD classic (12, 26, 9) 1M    ← missing space
📊 BTCUSDT [1/10]1M - Обновление БД              ← missing space
```

**After (unified format):**
```
BTCUSDT [1/10] SMA[10,30,50,100,200] 1M
BTCUSDT [1/10] EMA[9,12,21,26,50,100,200] 1M
BTCUSDT [1/10] RSI[7,9,14,21,25] 1M - Загрузка
BTCUSDT [1/10] ATR-14 1M
BTCUSDT [1/10] ADX-14 1M
BTCUSDT [1/10] BB-classic(20,2.0,SMA) 1M
BTCUSDT [1/10] VMA-20 1M
BTCUSDT [1/10] MACD-classic(12,26,9) 1M
BTCUSDT [1/10] OBV 1M - Обновление БД
BTCUSDT [1/10] VWAP-daily 1M
BTCUSDT [1/10] VWAP-20 1M
BTCUSDT [1/10] MFI-14 1M
BTCUSDT [1/10] STOCH[scalping,classic,swing] 1M
BTCUSDT [1/10] WILLIAMS[6,10,14,20,30] 1M
BTCUSDT [1/10] LONG-SHORT 1M
```

### 📊 User Experience

These changes make it easier to:
- **Track multi-symbol processing**: Clear `[x/y]` counter shows which symbol is being processed
- **Visual consistency**: All progress bars follow same format regardless of indicator type
- **Cleaner terminal output**: Removed unnecessary emojis that cluttered the display
- **Better readability**: Fixed spacing issues that made progress bars harder to read
- **Monitor parallel runs**: Consistent format helps when viewing logs from multiple loaders
- **Quick status checks**: Instantly see symbol, progress, indicator, and timeframe at a glance

### 🔧 Technical Details

**Pattern used:**
```python
# Initialize in __init__
self.symbol_progress = ""  # Will be set from main()

# Set in main() loop
for idx, symbol in enumerate(symbols, 1):
    loader = IndicatorLoader(symbol=symbol)
    loader.symbol_progress = f"[{idx}/{total_symbols}] "

# Use in progress bar
desc=f"{self.symbol} {self.symbol_progress} INDICATOR-{param} {timeframe.upper()}"
```

**Naming conventions:**
- Dashes for single values: `ATR-14`, `VMA-20`, `MFI-14`
- Square brackets for lists: `SMA[10,30,50]`, `RSI[7,9,14]`
- Parentheses for configs: `MACD-classic(12,26,9)`, `BB-golden(20,1.618,SMA)`

### 📦 Files Changed

- **Modified (12 files)**:
  - `indicators/sma_loader.py` (1 line changed)
  - `indicators/ema_loader.py` (3 lines changed)
  - `indicators/rsi_loader.py` (3 lines changed)
  - `indicators/atr_loader.py` (1 line changed)
  - `indicators/mfi_loader.py` (1 line changed)
  - `indicators/vwap_loader.py` (2 lines changed)
  - `indicators/long_short_ratio_loader.py` (1 line changed)
  - `indicators/macd_loader.py` (2 lines changed)
  - `indicators/obv_loader.py` (2 lines changed)
  - `indicators/adx_loader.py` (3 lines changed)
  - `indicators/bollinger_bands_loader.py` (3 lines changed)
  - `indicators/vma_loader.py` (3 lines changed)

- **Not modified (2 files)**:
  - `indicators/stochastic_williams_loader.py` (already correct)
  - `indicators/fear_and_greed_*.py` (different format by design)

---

## [2025-10-23] - Enhanced Console Output and Performance Tracking

### ✨ Improvements

#### All Indicator Loaders (RSI, SMA, EMA, VMA, ATR, OBV, MACD, Long/Short Ratio)
- **Performance tracking**: Added automatic display of total execution time at the end of processing
  - Format: `⏱️  Total time: Xm Ys` (minutes and seconds)
  - Helps users track processing efficiency and estimate future runs
  - Added to: `rsi_loader.py`, `sma_loader.py`, `ema_loader.py`, `vma_loader.py`, `atr_loader.py`, `obv_loader.py`, `macd_loader.py`, `long_short_ratio_loader.py`

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

#### MACD Loader Console Output
- **Enhanced progress bar**: Added symbol, progress counter, and configuration parameters to MACD progress bar
  - Before: `📊 MACD classic 1M:  19%|███...`
  - After: `📊 ETHUSDT [2/10] MACD classic (12, 26, 9) 1M:  19%|███...`
- Now displays: symbol name, trading pair progress [x/y], configuration name, and parameters (fast, slow, signal)
- Makes it clear which symbol, configuration, and parameters are currently being processed

#### Long/Short Ratio Loader - Critical Bug Fix
- **Fixed data not saving to database** (CRITICAL FIX)
  - Problem: Used UPDATE instead of INSERT - data only saved if records already existed
  - Solution: Changed to INSERT...ON CONFLICT DO UPDATE pattern
  - Now creates new records if they don't exist, updates existing ones if they do
  - Works independently without requiring other indicators to be loaded first
- **Added performance tracking**: Total time display at completion
- **Improved logging**: Shows inserted vs updated record counts

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

### 🐛 Bug Fixes
- **Long/Short Ratio Loader**: Fixed critical bug preventing data from saving to database
  - The loader now works independently and doesn't require other indicators to be loaded first
  - Data is properly saved using INSERT...ON CONFLICT pattern instead of UPDATE-only approach
- **check_indicators_db.py**: Fixed incorrect detection of Long/Short Ratio data
  - Problem: Script only checked 1m timeframe where Long/Short Ratio is NULL by design
  - Solution: Added INDICATOR_TIMEFRAMES mapping to check appropriate timeframes (15m, 1h for Long/Short)
  - Now correctly shows ✓ for all 10 trading pairs with Long/Short Ratio data
  - Multi-timeframe detection allows proper verification of indicators that don't support 1m period

### 🤖 Automation

#### Orchestrator - Automatic Sequential Indicator Loading
- **Created start_all_loaders.py**: Automated orchestrator for sequential loading of all indicators
  - Reads configuration from `indicators_config.yaml` (new `orchestrator.loaders` section)
  - Executes loaders sequentially in the order defined in config file
  - Smart handling of stochastic + williams_r (both indicators in one file)
  - Stops execution on first error for easy debugging
  - Comprehensive logging: console + file (`indicators/logs/start_all_loaders_YYYYMMDD_HHMMSS.log`)
  - Execution statistics: time per loader + total time
  - Color-coded console output for better readability

- **Added orchestrator configuration** to `indicators_config.yaml`:
  - Section `orchestrator.loaders` with true/false flags for each indicator
  - Currently enabled: sma, ema, rsi, vma, atr, obv (data already loaded)
  - Temporarily disabled: macd, bollinger_bands, adx, stochastic, williams_r, vwap, mfi, long_short_ratio, fear_and_greed (long loading times)
  - Easy to enable/disable loaders for incremental data loading

- **Usage**:
  ```bash
  cd indicators
  python3 start_all_loaders.py
  ```

- **Benefits**:
  - No need to run each loader manually
  - Consistent execution order
  - Easy progress tracking
  - Perfect for cron jobs (automated daily updates)
  - Detailed logs for troubleshooting

### 🔍 Research & Analysis

#### CoinMarketCap Global Metrics API - Market Capitalization Data
- **Researched and validated** CoinMarketCap API for obtaining global cryptocurrency market metrics
- **Status**: Ready for future implementation

**Available Endpoint** (Free Plan):
```
GET https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest
```

**Available Metrics**:

1. **Market Capitalization**:
   - `total_market_cap` - Total crypto market cap (currently $3.68T)
   - `altcoin_market_cap` - Altcoins market cap (currently $1.50T)
   - `defi_market_cap` - DeFi market cap (currently $95.21B)
   - `stablecoin_market_cap` - Stablecoins market cap (currently $284.08B)

2. **Trading Volumes (24h)**:
   - `total_volume_24h` - Total 24h volume (currently $181.85B)
   - `altcoin_volume_24h` - Altcoins 24h volume (currently $111.99B)
   - `defi_volume_24h` - DeFi 24h volume (currently $21.72B)
   - `stablecoin_volume_24h` - Stablecoins 24h volume (currently $181.30B)
   - `derivatives_volume_24h` - Derivatives 24h volume (currently $1.55T)

3. **Dominance Metrics**:
   - `btc_dominance` - Bitcoin market dominance % (currently 59.20%)
   - `eth_dominance` - Ethereum market dominance % (currently 12.63%)
   - `btc_dominance_24h_percentage_change` - BTC dominance change
   - `eth_dominance_24h_percentage_change` - ETH dominance change

4. **Market Statistics**:
   - `active_cryptocurrencies` - Number of active cryptocurrencies (9,441)
   - `total_cryptocurrencies` - Total cryptocurrencies (36,363)
   - `active_market_pairs` - Active trading pairs (114,993)
   - `active_exchanges` - Active exchanges (876)

5. **Change Metrics**:
   - `total_market_cap_yesterday_percentage_change` - Market cap 24h change %
   - `total_volume_24h_yesterday_percentage_change` - Volume 24h change %

**Update Frequency**:
- **Official documentation**: Every 5 minutes
- **Actual testing**: Every 1 minute (last_updated timestamp changes every minute)
- **Recommendation**: Query every 1 hour (optimal for 1h timeframe)

**API Limits** (Free Plan):
- 10,000 calls/month
- **Hourly updates**: 24 calls/day = 720 calls/month (7.2% of limit) ✅ Recommended
- **15-min updates**: 96 calls/day = 2,880 calls/month (28.8% of limit) ⚠️ Acceptable
- **5-min updates**: 288 calls/day = 8,640 calls/month (86.4% of limit) ⚠️ High usage

**Limitations**:
- ❌ Historical endpoint (`/v1/global-metrics/quotes/historical`) requires paid plan ($79+/month)
- ✅ Solution: Build own historical database by saving latest data periodically

**Future Implementation**:
- Create `market_cap_global_loader.py` to fetch and store global metrics
- Store in `indicators_bybit_futures_1h` table (or 15m for more detail)
- Run via cron every hour for automatic updates
- Duplicate values to 1m table (metrics don't change significantly minute-to-minute)

**API Key**: Already configured in `indicators_config.yaml` (coinmarketcap_fear_and_greed.api_key)

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