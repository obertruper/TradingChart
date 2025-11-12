# Mathematical Data Validation Suite for Technical Indicators

This directory contains comprehensive validation scripts that verify the **mathematical correctness** and **data completeness** of technical indicators stored in the database.

## Purpose

Unlike status check scripts (`check_*_status.py`) that only verify data **presence** (NULL vs NOT NULL), these validation scripts:

1. **Recalculate** indicators from source candle data
2. **Compare** calculated values with stored values
3. **Detect**:
   - Mathematical calculation errors
   - Data corruption
   - Missing data points
   - Warm-up period violations
   - Inconsistencies between timeframes

## Why This Matters

Technical indicators must be mathematically correct for trading strategies to work. Even small calculation errors can lead to:
- False trading signals
- Incorrect backtesting results
- Financial losses in production

This validation suite ensures data integrity by verifying every single indicator value against the expected calculation.

---

## Available Validators

### 1. SMA Validator (`check_sma_data.py`)

Validates Simple Moving Average (SMA) indicators across all timeframes and periods.

**What it checks:**
- ✅ Mathematical correctness (recalculates SMA using `pandas.rolling().mean()`)
- ✅ Warm-up period (first N-1 candles should be NULL, where N = period)
- ✅ Data completeness (no unexpected NULL values after warm-up)
- ✅ All 5 SMA periods: 10, 30, 50, 100, 200
- ✅ All 3 timeframes: 1m, 15m, 1h
- ✅ All configured symbols (17 futures pairs)

**Tolerance:**
- Allows up to ±0.00000001 difference for floating-point rounding errors
- Flags anything beyond this tolerance as calculation error

---

### 2. EMA Validator (`check_ema_data.py`)

Validates Exponential Moving Average (EMA) indicators across all timeframes and periods.

**What it checks:**
- ✅ Mathematical correctness (recalculates EMA using `pandas.ewm().mean()`)
- ✅ Warm-up period (first N-1 candles should be NULL, where N = period)
- ✅ Data completeness (no unexpected NULL values after warm-up)
- ✅ All 7 EMA periods: 9, 12, 21, 26, 50, 100, 200
- ✅ All 3 timeframes: 1m, 15m, 1h
- ✅ All configured symbols (17 futures pairs)

**Formula:**
- Multiplier = 2 / (Period + 1)
- EMA = (Close - Previous EMA) × Multiplier + Previous EMA
- First EMA value = SMA(period) for initialization

**Tolerance:**
- Allows up to ±0.00000001 difference for floating-point rounding errors
- Flags anything beyond this tolerance as calculation error

---

### 3. RSI Validator (`check_rsi_data.py`)

Validates Relative Strength Index (RSI) indicators across all timeframes and periods using Wilder smoothing method.

**What it checks:**
- ✅ Mathematical correctness (recalculates RSI using Wilder smoothing: `avg = (avg * (period-1) + new_value) / period`)
- ✅ Warm-up period (10x lookback for 99.996% convergence accuracy)
- ✅ Data completeness (no unexpected NULL values after warm-up)
- ✅ All 5 RSI periods: 7, 9, 14, 21, 25
- ✅ All 3 timeframes: 1m, 15m, 1h
- ✅ All configured symbols (17 futures pairs)

**Formula:**
- Gains/Losses calculated from price changes
- First avg_gain/avg_loss = SMA(period) for initialization
- Subsequent: `avg_gain = (avg_gain * (period-1) + current_gain) / period` (Wilder smoothing)
- RS = avg_gain / avg_loss
- RSI = 100 - (100 / (1 + RS))

**Tolerance:**
- Allows up to ±0.5 difference (RSI oscillates 0-100)
- Flags anything beyond this tolerance as calculation error

**Special Notes:**
- Uses 10x lookback multiplier (vs 2x in old version) for full Wilder smoothing convergence
- Checkpoint system validation: verifies stored avg_gain/avg_loss states
- Critical for momentum trading strategies

---

## Usage

### Basic Usage

```bash
cd indicators/tests/check_full_data

# SMA validation (all symbols, timeframes, periods)
python3 check_sma_data.py

# EMA validation (all symbols, timeframes, periods)
python3 check_ema_data.py

# RSI validation (all symbols, timeframes, periods)
python3 check_rsi_data.py
```

**Warning:**
- SMA validation: ~255 combinations (17 symbols × 3 timeframes × 5 periods) - **15-30 minutes**
- EMA validation: ~357 combinations (17 symbols × 3 timeframes × 7 periods) - **20-40 minutes**
- RSI validation: ~255 combinations (17 symbols × 3 timeframes × 5 periods) - **15-30 minutes**

### Filtered Validation

**SMA Examples:**
```bash
# Single symbol (faster for debugging)
python3 check_sma_data.py --symbol BTCUSDT

# Single timeframe (recommended for quick checks)
python3 check_sma_data.py --timeframe 1h

# Single period
python3 check_sma_data.py --period 50

# Last 30 days only (much faster)
python3 check_sma_data.py --days 30

# Combination filters
python3 check_sma_data.py --symbol BTCUSDT --timeframe 1h --days 7

# Verbose output (shows details for each validation)
python3 check_sma_data.py --symbol ETHUSDT --verbose
```

**EMA Examples:**
```bash
# Single symbol
python3 check_ema_data.py --symbol BTCUSDT

# Single timeframe
python3 check_ema_data.py --timeframe 1h

# Single period
python3 check_ema_data.py --period 21

# Last 30 days only
python3 check_ema_data.py --days 30

# Combination filters
python3 check_ema_data.py --symbol BTCUSDT --timeframe 1h --days 7

# Verbose output
python3 check_ema_data.py --symbol ETHUSDT --verbose
```

**RSI Examples:**
```bash
# Single symbol
python3 check_rsi_data.py --symbol ETHUSDT

# Single timeframe
python3 check_rsi_data.py --timeframe 1h

# Single period
python3 check_rsi_data.py --period 14

# Last 7 days only (recommended after recalculation)
python3 check_rsi_data.py --days 7

# Combination filters
python3 check_rsi_data.py --symbol ETHUSDT --timeframe 1h --days 7

# Verbose output
python3 check_rsi_data.py --symbol ETHUSDT --verbose
```

### Common Workflows

**Quick Health Check (Last Week):**
```bash
python3 check_sma_data.py --days 7
python3 check_ema_data.py --days 7
python3 check_rsi_data.py --days 7
```

**Single Symbol Deep Dive:**
```bash
python3 check_sma_data.py --symbol BTCUSDT --verbose
python3 check_ema_data.py --symbol BTCUSDT --verbose
python3 check_rsi_data.py --symbol ETHUSDT --verbose
```

**Production Validation (Hourly Data Only):**
```bash
python3 check_sma_data.py --timeframe 1h
python3 check_ema_data.py --timeframe 1h
python3 check_rsi_data.py --timeframe 1h
```

**Debug Specific Period:**
```bash
python3 check_sma_data.py --period 200 --days 30 --verbose
python3 check_ema_data.py --period 200 --days 30 --verbose
python3 check_rsi_data.py --period 14 --days 30 --verbose
```

---

## Output Format

### Progress Bar

```
Validation Progress: 100%|████████████████████| 255/255 [12:34<00:00, 20.32 check/s] ✅ BTCUSDT 1h SMA_200
```

### Summary Report

```
════════════════════════════════════════════════════════════════════════════════
  VALIDATION REPORT
════════════════════════════════════════════════════════════════════════════════

✅ Passed: 253/255 (99.2%)
❌ Failed: 2/255 (0.8%)

════════════════════════════════════════════════════════════════════════════════
  FAILED VALIDATIONS
════════════════════════════════════════════════════════════════════════════════

❌ ETHUSDT - 1h - SMA_30
   Total Records: 41,636
   Valid: 41,634
   Mismatches: 2
   NULL Issues: 0
   Errors (2 total, showing 2):
      └─ Calculation mismatch at 2024-05-15 12:00:00: Expected 43250.12345678, Found 43250.12345680, Δ = 0.0000000200
      └─ Missing stored value at 2024-06-20 09:00:00: Expected 44100.50000000, Found NULL

════════════════════════════════════════════════════════════════════════════════
  SUMMARY STATISTICS
════════════════════════════════════════════════════════════════════════════════
Total Checks: 255
Passed Checks: 253 (99.2%)
Failed Checks: 2 (0.8%)
Total Records Validated: 10,617,450
Error Records: 2,340
Error Rate: 0.0220%
════════════════════════════════════════════════════════════════════════════════

Total execution time: 0:12:34.567890
```

---

## Exit Codes

- **0**: All validations passed ✅
- **1**: One or more validations failed ❌

Useful for automation and CI/CD pipelines:

```bash
python3 check_sma_data.py --days 1 || echo "ALERT: SMA validation failed!"
```

---

## Understanding Results

### Validation States

| State | Description | Action Required |
|-------|-------------|-----------------|
| ✅ **Passed** | All values match calculated values within tolerance | None |
| ❌ **Failed** | Calculation mismatches or missing data detected | Investigate and re-run loader |

### Error Types

#### 1. Calculation Mismatch
```
Calculation mismatch at 2024-05-15 12:00:00:
Expected 43250.12345678, Found 43250.12345680, Δ = 0.0000000200
```
**Cause:** Stored SMA value differs from recalculated value beyond tolerance
**Fix:** Re-run `sma_loader.py` for affected symbol/timeframe

#### 2. Missing Stored Value
```
Missing stored value at 2024-06-20 09:00:00:
Expected 44100.50000000, Found NULL
```
**Cause:** Candle data exists but SMA was not calculated
**Fix:** Re-run `sma_loader.py` with appropriate date range

#### 3. Warm-up Period Violation
```
Warm-up period violation at 2020-01-01 00:05:00:
SMA should be NULL (within first 49 candles)
```
**Cause:** SMA_50 has a value before 50th candle
**Fix:** Delete incorrect early values, re-run loader

#### 4. Missing Data
```
Missing data at 2024-07-10 14:00:00:
Both stored and calculated SMA are NULL
```
**Cause:** Missing candle data in source table
**Fix:** Run `data_loader_futures.py` to fill candle gaps first

---

## Automation

### Daily Validation Cron Job

Add to crontab for automated daily validation:

```bash
# Run daily at 2 AM, check last 7 days
0 2 * * * cd /path/to/TradingChart/indicators/tests/check_full_data && python3 check_sma_data.py --days 7 >> /var/log/sma_validation.log 2>&1
```

### Email Alerts on Failure

```bash
#!/bin/bash
# validate_and_alert.sh

cd /path/to/TradingChart/indicators/tests/check_full_data

python3 check_sma_data.py --days 1

if [ $? -ne 0 ]; then
    echo "SMA validation failed! Check logs for details." | mail -s "ALERT: SMA Validation Failed" admin@example.com
fi
```

---

## Performance

### Validation Speed

| Scope | Estimated Time |
|-------|---------------|
| Single symbol, 1h, last 7 days | 2-5 seconds |
| Single symbol, all timeframes, full history | 30-60 seconds |
| All symbols, 1h, last 30 days | 2-3 minutes |
| Full validation (all symbols, timeframes, periods) | 15-30 minutes |

### Memory Usage

- **Batch processing**: Validates 30 days at a time to limit memory usage
- **Peak memory**: ~500 MB for large validations
- **Recommended**: Run on machine with at least 2 GB free RAM

---

## Troubleshooting

### "No data found for SYMBOL timeframe"

**Cause:** Symbol has no candle data in that timeframe
**Fix:** Verify symbol exists in database, run data collector if needed

### "Database connection failed"

**Cause:** Incorrect database credentials or VPS unreachable
**Fix:** Check `indicators_config.yaml` and network connectivity

### High Error Rate (>1%)

**Cause:** Indicator loader has bugs or incomplete runs
**Fix:**
1. Check `indicators/logs/` for loader errors
2. Re-run `sma_loader.py` for affected symbols
3. If persists, investigate loader logic

### Validation Too Slow

**Cause:** Validating full history across all symbols
**Fix:**
- Use `--days 30` for recent data only
- Use `--timeframe 1h` to skip minute data
- Validate single symbols with `--symbol BTCUSDT`

---

## Architecture

### Validation Flow

```
1. Load Config (indicators_config.yaml)
   ↓
2. Connect to Database (PostgreSQL)
   ↓
3. For each (symbol, timeframe, period):
   ↓
   a. Fetch candle data (close prices)
   b. Fetch stored SMA values
   ↓
   c. Calculate SMA locally (pandas.rolling().mean())
   ↓
   d. Compare calculated vs stored (tolerance: ±0.00000001)
   ↓
   e. Check warm-up period (first N-1 should be NULL)
   ↓
   f. Report errors
   ↓
4. Generate Summary Report
```

### Data Flow

```
Database Tables:
  candles_bybit_futures_1m/15m/1h  →  [close prices]
  indicators_bybit_futures_1m/15m/1h  →  [stored SMA values]
                                      ↓
                              SMA Validator
                                      ↓
                              [recalculate]
                                      ↓
                              [compare]
                                      ↓
                              Validation Report
```

---

## Extending the Suite

### Adding New Indicator Validators

To create validators for other indicators (RSI, EMA, MACD, etc.), follow this template:

```python
# check_rsi_data.py

class RSIValidator:
    def __init__(self, config_path=None):
        # Load config, connect to DB
        pass

    def calculate_rsi(self, df, period):
        # Implement RSI calculation
        # Use same formula as rsi_loader.py
        pass

    def validate_period(self, symbol, timeframe, period):
        # 1. Fetch candle data + stored RSI
        # 2. Calculate RSI locally
        # 3. Compare with tolerance
        # 4. Report errors
        pass

    def validate_all(self, symbols, timeframes, periods):
        # Loop through all combinations
        # Use tqdm progress bar
        pass

    def print_report(self, results):
        # Format and print results
        pass
```

---

## Best Practices

1. **Run validation after every loader execution**
   ```bash
   # After SMA loader
   python3 sma_loader.py && python3 check_sma_data.py --days 1

   # After EMA loader
   python3 ema_loader.py && python3 check_ema_data.py --days 1

   # After RSI loader
   python3 rsi_loader.py && python3 check_rsi_data.py --days 1
   ```

2. **Validate recent data frequently (daily)**
   ```bash
   python3 check_sma_data.py --days 7
   python3 check_ema_data.py --days 7
   python3 check_rsi_data.py --days 7
   ```

3. **Full validation periodically (weekly/monthly)**
   ```bash
   python3 check_sma_data.py
   python3 check_ema_data.py
   python3 check_rsi_data.py
   ```

4. **Test new loaders with validation**
   ```bash
   # After modifying sma_loader.py
   python3 sma_loader.py --symbol BTCUSDT --timeframe 1h
   python3 check_sma_data.py --symbol BTCUSDT --timeframe 1h --verbose

   # After modifying ema_loader.py
   python3 ema_loader.py --symbol BTCUSDT --timeframe 1h
   python3 check_ema_data.py --symbol BTCUSDT --timeframe 1h --verbose

   # After modifying rsi_loader.py
   python3 rsi_loader.py --symbol ETHUSDT --timeframe 1h --force-reload
   python3 check_rsi_data.py --symbol ETHUSDT --timeframe 1h --verbose
   ```

5. **Use filters for faster debugging**
   ```bash
   # Don't validate everything when debugging
   python3 check_sma_data.py --symbol BTCUSDT --days 1 --verbose
   python3 check_ema_data.py --symbol BTCUSDT --days 1 --verbose
   python3 check_rsi_data.py --symbol ETHUSDT --days 1 --verbose
   ```

---

## Related Scripts

| Script | Purpose | Speed |
|--------|---------|-------|
| `check_sma_data.py` | **SMA mathematical validation** | Slow (recalculates) |
| `check_ema_data.py` | **EMA mathematical validation** | Slow (recalculates) |
| `check_rsi_data.py` | **RSI mathematical validation** | Slow (recalculates) |
| `../../check_indicators_status.py` | Quick status overview (all indicators) | Fast (counts only) |
| `../../check_atr_status.py` | ATR-specific status check | Fast (no recalc) |
| `../../sma_loader.py` | Load SMA data into database | Medium |
| `../../ema_loader.py` | Load EMA data into database | Medium |
| `../../rsi_loader.py` | Load RSI data into database | Medium |

---

## Support

For issues or questions:
1. Check script output for specific error messages
2. Review loader logs in `indicators/logs/`
3. Verify database connectivity and credentials
4. Run with `--verbose` flag for detailed diagnostics

---

## Changelog

### Version 1.4.0 (2025-11-11) - RSI Validator Created
- **Created RSI Mathematical Validation** (`check_rsi_data.py`)
  - Validates all 5 RSI periods (7, 9, 14, 21, 25) across all timeframes
  - Uses Wilder smoothing method: `avg = (avg * (period-1) + new_value) / period`
  - 10x lookback multiplier for 99.996% convergence accuracy (vs 2x = 86.6%)
  - Tolerance: ±0.5 (RSI oscillates 0-100)
  - Validates checkpoint system: avg_gain/avg_loss states
- **RSI Loader Bug Fix** (same timestamp offset bug as EMA/SMA)
  - Fixed SQL aggregation: period START → period END
  - Increased lookback: 2x → 10x for Wilder smoothing convergence
  - Added checkpoint file system with 7-day validation
  - Added --force-reload flag for full recalculation
  - Expected validation results: 99%+ accuracy after full historical recalculation
- **Critical**: All RSI data in aggregated timeframes (15m, 1h) requires recalculation with --force-reload

### Version 1.3.0 (2025-11-10) - SMA Validator Timestamp Offset Bug Fix
- **Fixed Timestamp Offset Bug in SMA Validator** (same as EMA)
  - SQL aggregation formula used period START instead of END for timestamps
  - BEFORE: timestamp `15:00` contained data from `15:00-15:59` (FUTURE data!)
  - AFTER: timestamp `15:00` contains data from `14:00-14:59` (CORRECT)
  - Fixed SQL in `check_sma_data.py` lines 336-358
  - Applied same fix as EMA validator
- **Validation Results**: 99.998% accuracy (246,750/246,755 correct)
  - Even BETTER accuracy than EMA (99.99%)!
  - Only 5 errors on latest candle (2025-11-10 17:00) - race condition during real-time update
  - All historical data 100% accurate
- **Note**: SMA uses 1x lookback (optimal), unlike EMA which needs 5x

### Version 1.2.0 (2025-11-10) - EMA Validator Critical Bug Fix
- **Fixed Timestamp Offset Bug in EMA Validator** (ROOT CAUSE #1)
  - EMA validation was showing 90-100% error rate after loader recalculation
  - SQL aggregation formula used period START instead of END for timestamps
  - BEFORE: timestamp `15:00` contained data from `15:00-15:59` (FUTURE data!)
  - AFTER: timestamp `15:00` contains data from `14:00-14:59` (CORRECT)
  - Fixed SQL in `check_ema_data.py` lines 346-360
  - Result: Errors reduced from 100-400 points → <0.2 points (99.5% improvement!)
- **Increased Lookback Multiplier** (ROOT CAUSE #2)
  - Changed `lookback_multiplier` from 3x to 5x (line 502)
  - 3x covers 95% of EMA weights, 5x covers 99% of EMA weights
  - Error reduction: 1.99 points → 0.01 points
- **Validation Results**: 99.99% accuracy for short periods (EMA 9-26), long periods within industry standards
- **Full Analysis**: See `indicators/tools/EMA_ROOT_CAUSE_REPORT.md`

### Version 1.1.0 (2025-01-10)
- Added EMA mathematical validation (`check_ema_data.py`)
- Support for 7 EMA periods (9, 12, 21, 26, 50, 100, 200)
- Exponential weighted moving average calculation using `pandas.ewm()`

### Version 1.0.0 (2025-01-07)
- Initial release
- SMA mathematical validation (`check_sma_data.py`)
- Support for all 17 symbols, 3 timeframes, 5 periods
- Warm-up period validation
- CLI interface with filters
- Comprehensive error reporting
