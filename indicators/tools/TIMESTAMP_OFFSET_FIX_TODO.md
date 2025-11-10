# TIMESTAMP OFFSET BUG - TODO: Apply Fix to Other Loaders

**Status:** ✅ Fixed in EMA loader, ⚠️ Needs to be applied to other loaders

**Date:** 2025-11-10

---

## Background

The timestamp offset bug was discovered in EMA loader validation. The SQL aggregation formula was using period START instead of END for timestamps, causing all aggregated timeframes (15m, 1h) to use data from the WRONG hour.

**Example:**
- **BEFORE (WRONG):** timestamp `15:00` contained data from `15:00-15:59`
- **AFTER (CORRECT):** timestamp `15:00` contains data from `14:00-14:59`

This bug affects **ALL indicators** that use aggregation from 1m to 15m/1h timeframes.

---

## ✅ Already Fixed

### 1. EMA Loader (`indicators/ema_loader.py`)
- ✅ Fixed SQL aggregation formula (line 393)
- ✅ Increased lookback_multiplier from 3x to 5x (line 555)
- ✅ Added adjusted_overlap_start for aggregation (lines 364-369)
- ✅ Removed first batch special case (lines 571-576)
- ✅ Validation: 99.99% accuracy achieved
- ✅ Data recalculated with `--force-reload`

### 2. EMA Validator (`tests/check_full_data/check_ema_data.py`)
- ✅ Fixed SQL aggregation formula (lines 346-360)
- ✅ Increased lookback_multiplier to 5x (line 502)

---

## ⚠️ TODO: Apply Same Fix to These Loaders

### Priority 1: Most-Used Indicators (Apply First)

#### 1. SMA Loader (`indicators/sma_loader.py`)
**Status:** ✅ FIXED (2025-11-10)
**Impact:** HIGH - SMA is fundamental indicator
**Validation Results:**
- 99.998% accuracy (246,750/246,755 correct)
- Better accuracy than EMA!
- Only 5 errors on latest candle (17:00) - race condition during real-time update
**Fixed:**
- SQL aggregation formula (lines 318-343)
- adjusted_overlap_start for aggregation (lines 523-529)
- Validator: `check_sma_data.py` (lines 336-358)

#### 2. RSI Loader (`indicators/rsi_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** HIGH - RSI is critical for momentum trading
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

#### 3. MACD Loader (`indicators/macd_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** HIGH - MACD is widely used
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

#### 4. Bollinger Bands Loader (`indicators/bollinger_bands_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** HIGH - Volatility indicator
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

---

### Priority 2: Volatility & Trend Indicators

#### 5. ATR Loader (`indicators/atr_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** MEDIUM - Volatility analysis
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

#### 6. ADX Loader (`indicators/adx_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** MEDIUM - Trend strength
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

---

### Priority 3: Volume Indicators

#### 7. VWAP Loader (`indicators/vwap_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** MEDIUM - Institutional trading
**Lines to check:**
- SQL aggregation formula (if used)
- Daily reset logic
**Estimated Time:** 15-20 minutes

#### 8. MFI Loader (`indicators/mfi_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** MEDIUM - Volume-weighted momentum
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 15-20 minutes

#### 9. VMA Loader (`indicators/vma_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** LOW - Volume analysis
**Lines to check:**
- SQL aggregation formula
**Estimated Time:** 15-20 minutes

#### 10. OBV Loader (`indicators/obv_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** LOW - Cumulative volume
**Lines to check:**
- SQL aggregation formula (if used)
**Estimated Time:** 15-20 minutes

---

### Priority 4: Momentum Indicators

#### 11. Stochastic & Williams Loader (`indicators/stochastic_williams_loader.py`)
**Status:** ⬜ NOT FIXED
**Impact:** MEDIUM - Oscillator indicators
**Lines to check:**
- SQL aggregation formula
- lookback_multiplier (if applicable)
**Estimated Time:** 20-25 minutes

---

## SQL Fix Pattern

### BEFORE (WRONG):
```sql
date_trunc('hour', timestamp) +
INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start
```

### AFTER (CORRECT):
```sql
date_trunc('hour', timestamp) + INTERVAL '{minutes} minutes' as period_end
```

### For 15-minute aggregation:
```sql
-- BEFORE (WRONG):
date_trunc('hour', timestamp) +
INTERVAL '15 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 15) as period_start

-- AFTER (CORRECT):
date_trunc('hour', timestamp) +
INTERVAL '15 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 15 + 1) as period_end
```

**Important:** The formula for 15m is different from 1h! Check carefully.

---

## Lookback Multiplier Optimization

For indicators using exponential moving averages (EMA, MACD, etc.):
- **Current (insufficient):** `lookback_multiplier = 3` (95% weights)
- **Optimal:** `lookback_multiplier = 5` (99% weights)

This reduces calculation errors from ~2 points to ~0.01 points.

---

## Adjusted Overlap Start Pattern

For loaders that aggregate from 1m to 15m/1h:

```python
if timeframe != '1m':
    minutes = self.timeframe_minutes[timeframe]
    # Вычитаем один период таймфрейма для загрузки достаточных 1m свечей
    adjusted_overlap_start = overlap_start - timedelta(minutes=minutes)
else:
    adjusted_overlap_start = overlap_start
```

**Why needed:**
- For 1h candle at 15:00 (containing data 14:00-14:59), need to load 1m candles from 14:00
- Without adjustment, would filter `timestamp >= 15:00` and miss necessary data

---

## Testing Process

After applying fix to each loader:

1. **Test on small dataset:**
   ```bash
   python3 {loader_name}.py --symbol BTCUSDT --timeframe 1h --start-date 2025-11-03 --force-reload
   ```

2. **Run validation (if validator exists):**
   ```bash
   python3 tests/check_full_data/check_{indicator}_data.py --symbol BTCUSDT --timeframe 1h --days 7
   ```

3. **Full recalculation:**
   ```bash
   python3 {loader_name}.py --timeframe 15m --force-reload
   python3 {loader_name}.py --timeframe 1h --force-reload
   ```

4. **Verify with status check:**
   ```bash
   python3 check_indicators_status.py
   ```

---

## Estimated Total Time

- **Priority 1 (4 loaders):** 1-1.5 hours
- **Priority 2 (2 loaders):** 30-40 minutes
- **Priority 3 (4 loaders):** 1-1.5 hours
- **Priority 4 (1 loader):** 20-25 minutes

**Total:** 3-4 hours to fix all loaders

**Data Recalculation:** 10-30 hours for full database recalculation with `--force-reload`

---

## Validation

After all fixes applied:
- Run full validation suite
- Compare with TradingView values for spot checks
- Verify 99%+ accuracy across all indicators

---

## Documentation Updates

After fixes applied, update:
- ✅ `CHANGELOG.md` - Add entry for each fixed loader
- ✅ `CLAUDE.md` - Update Recent Improvements section
- ⬜ Individual loader docstrings - Note the fix date
- ⬜ `indicators/tools/EMA_ROOT_CAUSE_REPORT.md` - Update "Impact on Other Indicators" section

---

## Notes

- **1m timeframe NOT affected** - No aggregation used, timestamps are correct
- **Only 15m and 1h timeframes need recalculation**
- **Can be done in batches** - Fix Priority 1 loaders first, then proceed to others
- **Production impact** - Plan for 10-30 hours of database recalculation time

---

## Progress Tracking

**Overall Status:** 2/11 loaders fixed (18.2%)

| Loader | Status | Priority | Date Fixed | Notes |
|--------|--------|----------|------------|-------|
| ema_loader.py | ✅ | 1 | 2025-11-10 | 99.99% accuracy |
| sma_loader.py | ✅ | 1 | 2025-11-10 | 99.998% accuracy |
| rsi_loader.py | ⬜ | 1 | - | - |
| macd_loader.py | ⬜ | 1 | - | - |
| bollinger_bands_loader.py | ⬜ | 1 | - | - |
| atr_loader.py | ⬜ | 2 | - | - |
| adx_loader.py | ⬜ | 2 | - | - |
| vwap_loader.py | ⬜ | 3 | - | - |
| mfi_loader.py | ⬜ | 3 | - | - |
| vma_loader.py | ⬜ | 3 | - | - |
| obv_loader.py | ⬜ | 3 | - | - |
| stochastic_williams_loader.py | ⬜ | 4 | - | - |

---

## References

- **Full Analysis:** `indicators/tools/EMA_ROOT_CAUSE_REPORT.md`
- **Changelog:** `CHANGELOG.md` (2025-11-10 entry)
- **Project Docs:** `CLAUDE.md` (Recent Improvements section)
