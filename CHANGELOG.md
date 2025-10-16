# CHANGELOG

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