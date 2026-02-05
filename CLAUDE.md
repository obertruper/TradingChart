# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a cryptocurrency trading data collection system focused on collecting historical and real-time candle data from Bybit exchange. The system supports both **futures (linear/perpetual)** and **spot** markets with separate data collection pipelines:
- `data_collectors/bybit/futures/` - Futures market data collection
- `data_collectors/bybit/spot/` - Spot market data collection

Both systems collect 1-minute OHLCV candles and store them in PostgreSQL hosted on a VPS (82.25.115.144) with daily batching for safe interruption recovery.

## Key Commands

### Installation & Setup
```bash
# Install dependencies
pip install -r requirements.txt
pip install python-dotenv  # For environment variable support

# Set up environment variables
cp .env.example .env
# Edit .env file with your database passwords

# Copy and configure settings
cp data_collectors/data_collector_config.example.yaml data_collectors/data_collector_config.yaml
cp data_collectors/monitor_config.example.yaml data_collectors/monitor_config.yaml

# Validate configuration
cd data_collectors/bybit/futures
python3 config_validator.py
```

### Run Historical Data Collection
```bash
cd data_collectors/bybit/futures
python3 data_loader_futures.py
```

### Check Database for Collected Data
```bash
cd data_collectors/bybit/futures
python3 check_data.py
```

### Run Historical Data Collection (Spot Market)
```bash
cd data_collectors/bybit/spot
python3 data_loader_spot.py

# Check collected spot data in database
python3 check_data_spot.py
```

### Real-time Monitoring (Spot Market)
```bash
cd data_collectors/bybit/spot

# Single gap check for specific symbol
python3 monitor_spot.py --check-once --symbol BTCUSDT

# Check all configured symbols once
python3 monitor_spot.py --check-once

# Run continuous monitoring daemon (checks every minute)
python3 monitor_spot.py --daemon

# With verbose output for debugging
python3 monitor_spot.py --check-once --verbose

# Quiet mode for production (minimal output)
python3 monitor_spot.py --daemon --quiet
```

**Note:** Spot monitor uses `monitor_config_spot.yaml` for configuration. Ensure all 16 symbols are configured before running daemon mode.

### Real-time Monitoring (Futures Market - monitor.py)
```bash
cd data_collectors/bybit/futures

# Single gap check for specific symbol
python3 monitor.py --check-once --symbol BTCUSDT

# Check all configured symbols once
python3 monitor.py --check-once

# Run continuous monitoring daemon (checks every minute)
python3 monitor.py --daemon

# With verbose output for debugging
python3 monitor.py --check-once --verbose

# Quiet mode for production (minimal output)
python3 monitor.py --daemon --quiet

# Using monitor manager script (alternative)
./monitor_manager.sh start    # Start daemon
./monitor_manager.sh stop     # Stop daemon
./monitor_manager.sh status   # Check status
./monitor_manager.sh logs     # View logs
```

### Testing & Development
```bash
# Run tests (when available)
pytest

# Code formatting
black data_collectors/

# Linting
flake8 data_collectors/
```

### Indicators Module
```bash
cd indicators

# ORCHESTRATOR - Automatic sequential loading of ALL indicators
python3 start_all_loaders.py
# Loads all enabled indicators sequentially from indicators_config.yaml
# Configuration: orchestrator.loaders section (true/false for each indicator)
# Logs: indicators/logs/start_all_loaders_YYYYMMDD_HHMMSS.log (real-time)
# Perfect for manual runs and cron jobs

# Individual loader commands (if you need to run specific indicators):

# Load SMA indicators for all timeframes
python3 sma_loader.py
python3 sma_loader.py --symbol BTCUSDT  # Specific symbol only
python3 sma_loader.py --timeframe 1h  # Specific timeframe only
python3 sma_loader.py --check-nulls  # Check and fill NULL values in middle of data
python3 sma_loader.py --check-nulls --symbol ETHUSDT  # Check NULLs for specific symbol
# Note: SMA is non-cumulative, can fill NULLs with local lookback (fast)
# 5 periods: SMA 10, 30, 50, 100, 200
# --check-nulls: Finds NULL after unavoidable boundary (first 200 records), fills with recalculation

# Load EMA indicators with checkpoint support
python3 ema_loader.py
python3 ema_loader.py --symbol BTCUSDT  # Specific symbol only
python3 ema_loader.py --timeframe 1h  # Specific timeframe only
python3 ema_loader.py --force-reload  # Full recalculation from history start
python3 ema_loader.py --check-nulls  # Check and fill NULL values (FULL recalc from start!)
python3 ema_loader.py --check-nulls --symbol ETHUSDT  # Check NULLs for specific symbol
# Note: EMA is cumulative (each value depends on previous), NULL fix requires full recalculation
# 7 periods: EMA 9, 12, 21, 26, 50, 100, 200
# --check-nulls: Full recalculation from data start for 100% accuracy (EMA chain integrity)

# Load RSI indicators with single-pass calculation (like validator)
python3 rsi_loader.py
python3 rsi_loader.py --symbol ETHUSDT --timeframe 1h  # Specific symbol and timeframe
python3 rsi_loader.py --start-date 2024-01-01 --force-reload  # From specific date
python3 rsi_loader.py --batch-days 14  # Larger batches for faster DB writes
python3 rsi_loader.py --check-nulls  # Check and fill NULL values (FULL recalc from start!)
python3 rsi_loader.py --check-nulls --symbol BTCUSDT  # Check NULLs for specific symbol
# Architecture: Single-pass calculation (load all → calculate → write in batches)
# Accuracy: 100% mathematical correctness (validated against independent calculation)
# Performance: 3.9x faster than old checkpoint-based approach
# Lookback: 10x multiplier for Wilder smoothing (99.996% convergence)
# --check-nulls: RSI uses Wilder smoothing (cumulative), requires full recalc for 100% accuracy
# 5 periods: RSI 7, 9, 14, 21, 25
# Batch writes: Database writes only, calculation is single-pass

# Load VMA (Volume Moving Average) indicators
python3 vma_loader.py
python3 vma_loader.py --symbol BTCUSDT  # Specific symbol only
python3 vma_loader.py --timeframe 1h  # Specific timeframe only
python3 vma_loader.py --force-reload  # Full recalculation from history start
# Note: Volume Moving Average for volume analysis
# 5 periods: VMA 10, 20, 50, 100, 200
# Incremental loading: only new data written (unless --force-reload specified)

# Load ATR (Average True Range) and NATR (Normalized ATR) indicators
python3 atr_loader.py
python3 atr_loader.py --symbol BTCUSDT  # Specific symbol only
python3 atr_loader.py --timeframe 1m  # Specific timeframe only
python3 atr_loader.py --force-reload  # Full recalculation ATR + NATR from history start
python3 atr_loader.py --backfill-natr  # Fast backfill NATR using existing ATR data
# Note: Uses OBV pattern - loads ALL data, calculates full Wilder chain, writes in batches
# ATR: Wilder smoothing - ATR = (ATR_prev × (period - 1) + TR) / period
# NATR: Normalized ATR - NATR = ATR / Close × 100 (volatility as percentage)
# Calculation time: ~5-10 seconds for 5 years, DB write: ~30-35 minutes per period
# 6 periods: ATR/NATR 7, 14, 21, 30, 50, 100
# --force-reload: Recalculates and overwrites ALL ATR + NATR data
# --backfill-natr: Quick NATR fill using existing ATR + Close prices (no ATR recalculation)

# Load ADX (Average Directional Index) indicators
python3 adx_loader.py
python3 adx_loader.py --period 14  # Specific period only
python3 adx_loader.py --timeframe 1h  # Specific timeframe only
python3 adx_loader.py --batch-days 3  # Larger batches for faster processing

# Load Long/Short Ratio (market sentiment from Bybit API)
python3 long_short_ratio_loader.py
python3 long_short_ratio_loader.py --symbol BTCUSDT  # Specific symbol only
python3 long_short_ratio_loader.py --timeframe 15m  # Specific timeframe only
python3 long_short_ratio_loader.py --force-reload  # Full reload from 2020-07-20 (fills gaps)
python3 long_short_ratio_loader.py --symbol BTCUSDT --force-reload  # Force reload for specific symbol
# Note: 1m timeframe sets NULL (API doesn't support), 15m and 1h load real data from API
# Historical data available from July 20, 2020
# Incremental loading: repeating runs only process new records
# --force-reload: Starts from 2020-07-20 and fills all historical gaps

# Load Open Interest (total open futures contracts from Bybit API)
python3 open_interest_loader.py
python3 open_interest_loader.py --symbol BTCUSDT  # Specific symbol only
python3 open_interest_loader.py --timeframe 1h  # Specific timeframe only
python3 open_interest_loader.py --force-reload  # Full reload from October 2023
python3 open_interest_loader.py --symbol BTCUSDT --force-reload  # Force reload for specific symbol
# Note: 1m timeframe sets NULL (API minimum interval is 5min), 15m and 1h load real data
# Historical data available from October 2023 (~27 months)
# Incremental loading: repeating runs only process new records
# Columns: open_interest (in base currency - BTC, ETH, etc.)
# Future: Bybit provides Open Interest via WebSocket for real-time tick updates

# Load Funding Rate (perpetual futures funding fee from Bybit API)
python3 funding_fee_loader.py
python3 funding_fee_loader.py --symbol BTCUSDT  # Specific symbol only
python3 funding_fee_loader.py --timeframe 1h  # Specific timeframe only
python3 funding_fee_loader.py --force-reload  # Full reload from March 2020
python3 funding_fee_loader.py --symbol BTCUSDT --force-reload  # Force reload for specific symbol
# Note: Funding Rate updates every 8 hours (00:00, 08:00, 16:00 UTC)
# Historical data: BTCUSDT from 2020-03-25 (~5.8 years), others from 2020-2021
# Backward-fill logic: stores FUTURE funding rate (what will be applied next)
# Columns: funding_rate_next (rate), funding_time_next (when it applies)
# API limit: 200 records per request

# Load Premium Index (futures-spot price difference from Bybit API)
python3 premium_index_loader.py
python3 premium_index_loader.py --symbol BTCUSDT  # Specific symbol only
python3 premium_index_loader.py --timeframe 1h  # Specific timeframe only
python3 premium_index_loader.py --force-reload  # Full reload from earliest API date
# Note: Premium Index = (Futures Price - Spot Price) / Spot Price
# Leading indicator for Funding Rate (reacts instantly vs 8h average)
# Historical data: BTCUSDT from 2020-03-25, others from 2020-2021 (see indicators_config.yaml)
# Column: premium_index (close value, DECIMAL(20,10))
# API endpoint: /v5/market/premium-index-price-kline (limit: 1000 records/request)
# Architecture: Daily batching from OLDEST to NEWEST (like data_loader_futures)
#   - Processes one day at a time, commits after each day
#   - If interrupted, next run continues from last saved date
#   - Uses UPDATE only (does NOT create orphan rows)
# Gap detection: Automatically finds and fills NULL values after main load
# Coverage: 99.99% for BTCUSDT (verified), minimal gaps from API outages (212 records)
# Interpretation: Positive = bullish (longs dominate), Negative = bearish (shorts dominate)

# Load OBV (On-Balance Volume) - cumulative volume indicator
python3 obv_loader.py
python3 obv_loader.py --symbol BTCUSDT  # Specific symbol only
python3 obv_loader.py --timeframe 1m  # Specific timeframe only
python3 obv_loader.py --batch-days 7  # Larger batches for faster DB writes
# Note: Full recalculation from history start (cumulative indicator)
# Calculation time: ~1-2 minutes for 5 years, DB write: ~28-38 minutes
# Incremental writes: only new data written to DB

# Load MACD (Moving Average Convergence Divergence)
python3 macd_loader.py
python3 macd_loader.py --symbol BTCUSDT  # Specific symbol only
python3 macd_loader.py --timeframe 1h  # Specific timeframe only
python3 macd_loader.py --batch-days 3  # Larger batches for faster processing
# Note: 8 configurations (classic, crypto, aggressive, balanced, scalping, swing, longterm, ultralong)
# Each configuration creates 3 columns: line, signal, histogram

# Load Bollinger Bands (volatility indicator)
python3 bollinger_bands_loader.py
python3 bollinger_bands_loader.py --symbol BTCUSDT  # Specific symbol only
python3 bollinger_bands_loader.py --timeframe 1h  # Specific timeframe only
python3 bollinger_bands_loader.py --batch-days 3  # Larger batches for faster processing
# Note: 13 configurations (11 SMA-based + 2 EMA-based)
# Each configuration creates 6 columns: upper, middle, lower, %B, bandwidth, squeeze
# Optimized: processes all 13 configs simultaneously with single data load (~3-4x faster)
# Safe division: handles zero bandwidth/middle band gracefully (NaN → NULL in DB)

# Load VWAP (Volume Weighted Average Price)
python3 vwap_loader.py
python3 vwap_loader.py --symbol BTCUSDT  # Specific symbol only
python3 vwap_loader.py --timeframe 1m  # Specific timeframe only
python3 vwap_loader.py --batch-days 7  # Larger batches for faster processing
# Note: 16 variants (1 daily + 15 rolling periods)
# Daily VWAP resets at 00:00 UTC, rolling VWAP uses fixed windows

# Load MFI (Money Flow Index) - volume-weighted RSI
python3 mfi_loader.py
python3 mfi_loader.py --symbol BTCUSDT  # Specific symbol only
python3 mfi_loader.py --timeframe 1h  # Specific timeframe only
python3 mfi_loader.py --batch-days 3  # Larger batches for faster processing
# Note: 5 periods (7, 10, 14, 20, 25) - similar to RSI but with volume

# Load Stochastic Oscillator and Williams %R (momentum indicators)
python3 stochastic_williams_loader.py
python3 stochastic_williams_loader.py --symbol BTCUSDT  # Specific symbol only
python3 stochastic_williams_loader.py --timeframe 1h  # Specific timeframe only
python3 stochastic_williams_loader.py --batch-days 3  # Larger batches for faster processing
# Note: Stochastic - 8 configurations (%K and %D lines)
# Williams %R - 5 periods (6, 10, 14, 20, 30)

# Load Ichimoku Cloud (Japanese trend and momentum indicator)
python3 ichimoku_loader.py
python3 ichimoku_loader.py --symbol BTCUSDT  # Specific symbol only
python3 ichimoku_loader.py --timeframe 1h  # Specific timeframe only
python3 ichimoku_loader.py --force-reload  # Full reload (all data)
# Note: 2 configurations: Crypto (9/26/52), Long (20/60/120)
# Each creates 8 columns: tenkan, kijun, senkou_a, senkou_b, chikou, cloud_thick, price_cloud, tk_cross
# Total: 16 columns per timeframe
# Senkou Span stored as "effective" values (cloud that applies to current timestamp)
# Incremental loading: only NULL records updated (ignores natural NULLs at data start)

# Load HV (Historical Volatility) - statistical volatility measure
python3 hv_loader.py
python3 hv_loader.py --symbol BTCUSDT  # Specific symbol only
python3 hv_loader.py --timeframe 1h  # Specific timeframe only
python3 hv_loader.py --force-reload  # Full reload (all data)
# Note: HV = StdDev(log returns) × √(periods_per_year) × 100%
# 5 periods: HV 7, 14, 30, 60, 90 (in % annualized)
# 3 derived: hv_ratio_7_30, hv_percentile_7d, hv_percentile_90d
# Total: 8 columns per timeframe
# Current timeframes: 1m, 15m, 1h, 4h, 1d
# Annualization: 1m=725×, 15m=187×, 1h=93.6×, 4h=46.8×, 1d=19.1×
# Percentile uses 7 days and 90 days lookback windows
# Incremental loading: only NULL records updated (unless --force-reload)

# Load SuperTrend (trend indicator based on ATR)
python3 supertrend_loader.py
python3 supertrend_loader.py --symbol BTCUSDT  # Specific symbol only
python3 supertrend_loader.py --timeframe 1h  # Specific timeframe only
python3 supertrend_loader.py --force-reload  # Full reload (all data)
# Note: SuperTrend = dynamic support/resistance based on ATR
# 5 configurations: (7,1.5), (10,2.0), (10,3.0), (14,2.5), (20,3.0)
# Column naming: supertrend_p{period}_m{multiplier×10} (e.g., supertrend_p10_m20)
# 4 columns per config: value, dir (1/-1), upper, lower
# Total: 20 columns per timeframe (5 configs × 4 columns)
# Consensus Score: sum of all 5 directions (-5 to +5)
# Incremental loading: only NULL records updated (unless --force-reload)

# Load Fear and Greed Index (market sentiment from Alternative.me API)
python3 fear_and_greed_loader_alternative.py
python3 fear_and_greed_loader_alternative.py --timeframe 1h  # Specific timeframe only
python3 fear_and_greed_loader_alternative.py --force-reload  # Full reload (all dates)
# Note: Daily sentiment index (0-100), applied to BTCUSDT symbol
# Historical data available from 2018
# Incremental loading: only NULL records are updated (unless --force-reload)

# Load Fear and Greed Index (market sentiment from CoinMarketCap API)
python3 fear_and_greed_coinmarketcap_loader.py
python3 fear_and_greed_coinmarketcap_loader.py --timeframe 1h  # Specific timeframe only
python3 fear_and_greed_coinmarketcap_loader.py --force-reload  # Full reload (all dates)
# Note: Global crypto market metrics (market cap, dominance, volumes)
# Historical data: ~2.7 years (API limitation)
# Incremental loading: only NULL records are updated (unless --force-reload)

# Check indicators status in database
python3 check_indicators_status.py
python3 check_atr_status.py  # ATR-specific status
python3 check_adx_status.py  # ADX-specific status with trend interpretation
python3 check_adx_status.py --comparison  # Show values for TradingView comparison

# Validate indicator calculations (mathematical correctness)
cd tests/check_full_data
python3 check_rsi_data.py --symbol ETHUSDT --timeframe 1h --days 7  # RSI validation
python3 check_ema_data.py --symbol BTCUSDT --timeframe 1h --days 30  # EMA validation
python3 check_atr_data.py --symbol ETHUSDT --timeframe 1h  # ATR validation (all periods)

# View indicator reference documentation
cat INDICATORS_REFERENCE.md
```

## Architecture

### Core Components

#### Data Collection

**Futures Market (data_collectors/bybit/futures/):**
- **data_loader_futures.py**: Production-ready bulk historical data collector with daily batching (1440 candles/day), smart chunking (1000 candles per API request), UTC timezone handling, and progress tracking. Supports multiple symbols with immediate DB writes after each day
- **monitor.py**: Real-time data monitor with gap detection, automatic filling, and continuous updates. Production-ready with checkpoint support and multi-symbol monitoring
- **database.py**: PostgreSQL connection management with bulk insertion, deduplication, and connection pooling
- **time_utils.py**: UTC timezone utilities with future date limiting for proper timestamp handling
- **config_validator.py**: Configuration validation, template generation, and auto-correction
- **check_data.py**: Database statistics and data verification tool
- **check_db_symbols.py**: Comprehensive database analysis tool for data completeness and gap detection
- **monitor_manager.sh**: Shell script for daemon process management

**Spot Market (data_collectors/bybit/spot/):**
- **data_loader_spot.py**: Production-ready spot data collector with identical architecture to futures - daily batching, smart collection, and multi-symbol support. Fully autonomous with no dependencies on futures directory
- **monitor_spot.py**: Real-time monitor for spot market data (same functionality as futures monitor)
- **database.py**: Database operations for spot market (candles_bybit_spot_1m table)
- **time_utils.py**: Identical time utilities as futures (real file, not symlink)
- **config_validator.py**: Configuration validation for spot market
- **check_data_spot.py**: Data verification tool for spot market

#### Indicators System
- **Loader Architecture**: All indicator loaders follow a common pattern (though not yet unified via base class):
  1. **Initialization**: Load config from indicators_config.yaml, connect to database
  2. **Column Creation**: `ensure_columns()` - Add indicator columns if they don't exist (ALTER TABLE)
  3. **Date Range**: `get_date_range()` - Determine what data needs processing
  4. **Calculation**: Calculate indicators using pandas on batches of candles
  5. **Database Write**: Batch UPDATE to save calculated values (requires existing rows in indicators table)
  6. **Progress Tracking**: tqdm progress bars showing real-time processing status
  7. **Timeframe Aggregation**: Automatically aggregate from 1m → 15m, 1h

- **Standard Implementation Pattern** (REQUIRED for new loaders):
  - **UPDATE only**: Use `UPDATE ... SET ... WHERE symbol = %s AND timestamp = %s` (not INSERT)
  - **Chronological order**: Process from oldest to newest (min_date → max_date)
  - **Daily batching**: Process 1 day at a time, commit after each day
  - **`--force-reload` flag**: When set, starts from min_date and rewrites all data
  - **Incremental by default**: Without --force-reload, continues from last processed date

- **Common Patterns**:
  - **Lookback Multiplier**: Each indicator uses `lookback_multiplier` (2x-5x) to load extra historical data for warm-up periods
  - **Batch Processing**: Process data in configurable batch sizes (1-14 days) for memory efficiency
  - **Multi-Period Calculation**: Most indicators calculate multiple periods in parallel (e.g., RSI 7/9/14/21/25)
  - **Multi-Configuration Support**: Complex indicators (MACD, Bollinger Bands, Stochastic, Ichimoku) support multiple configurations
  - **OBV Pattern**: For pseudo-cumulative indicators (ATR, OBV) - load ALL data from history start, calculate full chain maintaining continuity (e.g., Wilder smoothing), write in daily batches for progress visibility

- **Indicator Categories**:
  - **Trend**: SMA, EMA, MACD, ADX, Ichimoku Cloud
  - **Momentum**: RSI, Stochastic, Williams %R, MFI
  - **Volatility**: ATR, Bollinger Bands
  - **Volume**: VMA, OBV, VWAP, MFI
  - **Sentiment**: Long/Short Ratio, Fear & Greed Index
  - **Market Data**: Open Interest, Funding Rate, Premium Index

- **Known Technical Debt**:
  - ~1000-1500 lines of duplicated code across loaders (logging setup, aggregation, timeframe parsing)
  - No base class - each loader is standalone
  - Inconsistent checkpoint/resume approaches
  - OBV and ATR recalculate entire history each run (by design for accuracy - maintains cumulative/Wilder chains)
  - Sequential-only orchestrator (no parallel execution)

- **Best Practices Implemented**:
  - **ATR Loader (2025-11)**: Refactored to OBV pattern - maintains Wilder smoothing chain across full history for mathematical accuracy (99.99% validation accuracy vs 66% with batch approach)

### Database Schema

#### Database Size Overview (as of 2025-12-17)

| Database | Size | Description |
|----------|------|-------------|
| trading_data | **137 GB** | Main database with candles and indicators |
| trading_operations | 8.3 MB | Trading signals and positions |

#### Table Sizes (trading_data)

| Table | Total Size | Data | Indexes | Rows | Columns |
|-------|------------|------|---------|------|---------|
| indicators_bybit_futures_1m | **114 GB** | 108 GB | 5.3 GB | 25.6M | 261 |
| indicators_bybit_futures_15m | 7.7 GB | 7.3 GB | 313 MB | 1.7M | 261 |
| candles_bybit_futures_1m | 6.7 GB | 3.3 GB | 3.4 GB | 38M | 8 |
| candles_bybit_spot_1m | 5.5 GB | 2.7 GB | 2.8 GB | 31M | 8 |
| backtest_ml | 2.9 GB | 2.4 GB | 503 MB | 2M | 80 |
| indicators_bybit_futures_1h | 2 GB | 1.9 GB | 82 MB | 437K | 261 |
| indicators_bybit_futures_4h | 32 KB | - | - | 0 | 261 |
| indicators_bybit_futures_1d | 32 KB | - | - | 0 | 261 |
| eda | 48 KB | 0 | 48 KB | 0 | 22 |

**Storage per row:**
- indicators tables: ~4.7 KB/row (261 columns of numeric data)
- candles tables: ~90 bytes/row (8 columns)
- backtest_ml: ~1.3 KB/row (80 columns)

#### Candles Tables
- **Database**: PostgreSQL on VPS (82.25.115.144)
- **Database Name**: `trading_data`
- **Tables**:
  - `candles_bybit_futures_1m` - Futures/perpetual contracts data
  - `candles_bybit_spot_1m` - Spot market data
- **User Architecture**:
  - `trading_admin`: Full admin privileges (migrations, schema changes)
  - `trading_writer`: Data collection user (INSERT, UPDATE, SELECT, DELETE)
  - `trading_reader`: Read-only access for analysis (SELECT only)
- **Columns** (identical for both futures and spot):
  - timestamp (TIMESTAMPTZ): Candle timestamp in UTC
  - symbol (VARCHAR(20)): Trading pair (e.g., BTCUSDT)
  - open, high, low, close (DECIMAL(20,8)): Price data
  - volume, turnover (DECIMAL(20,8)): Trading metrics
- **Indexes** (identical for both futures and spot):
  - Primary key on (timestamp, symbol)
  - idx_symbol_timestamp on (symbol, timestamp)
  - idx_timestamp on (timestamp)
- **Storage**: ~150 bytes per candle
- **Data Availability**:
  - Futures: Varies by symbol (check symbols_launch_dates.json)
  - Spot: Most pairs from 2021-07-05 12:00:00 UTC (verified for BTCUSDT, ETHUSDT), BNBUSDT from 2022-03

#### Indicators Tables
- **Tables**: `indicators_bybit_futures_1m`, `indicators_bybit_futures_15m`, `indicators_bybit_futures_1h`, `indicators_bybit_futures_4h`, `indicators_bybit_futures_1d`
- **Architecture**: One table per timeframe, indicators stored as columns (wide table format)
- **Primary Key**: (timestamp, symbol) - matches candles tables
- **Timeframe Aggregation**:
  - 1m: Base data from candles_bybit_futures_1m
  - 15m: Aggregated from 1m (15 candles per period)
  - 1h: Aggregated from 1m (60 candles per period)
  - 4h: Fixed intervals 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
  - 1d: Daily aggregation at 00:00 UTC
- **Indicator Columns** (261 total columns):
  - **Moving Averages**: sma_10, sma_30, sma_50, sma_100, sma_200 (5 columns)
  - **EMA**: ema_9, ema_12, ema_21, ema_26, ema_50, ema_100, ema_200 (7 columns)
  - **RSI**: rsi_7, rsi_9, rsi_14, rsi_21, rsi_25 (5 columns)
  - **VMA**: vma_10, vma_20, vma_50, vma_100, vma_200 (5 columns)
  - **ATR**: atr_7, atr_14, atr_21, atr_30, atr_50, atr_100 (6 columns)
  - **ADX**: adx_{period}, adx_{period}_plus_di, adx_{period}_minus_di for 8 periods (24 columns)
  - **MACD**: 8 configurations × 3 values (line, signal, histogram) = 24 columns
  - **Bollinger Bands**: 13 configurations × 6 values (upper, middle, lower, %b, bandwidth, squeeze) = 78 columns
  - **VWAP**: vwap_daily + 15 rolling periods = 16 columns
  - **MFI**: mfi_7, mfi_10, mfi_14, mfi_20, mfi_25 (5 columns)
  - **Stochastic**: 8 configurations × 2 values (%K, %D) = 16 columns
  - **Williams %R**: williams_r_6, williams_r_10, williams_r_14, williams_r_20, williams_r_30 (5 columns)
  - **OBV**: obv (1 column)
  - **Long/Short Ratio**: long_short_buy_ratio, long_short_sell_ratio, long_short_ratio (3 columns, NULL for 1m)
  - **Open Interest**: open_interest (1 column, NULL for 1m)
  - **Funding Rate**: funding_rate_next, funding_time_next (2 columns)
  - **Premium Index**: premium_index (1 column) - futures-spot price difference
  - **Ichimoku Cloud**: 2 configs × 8 values (tenkan, kijun, senkou_a, senkou_b, chikou, cloud_thick, price_cloud, tk_cross) = 16 columns
  - **Fear & Greed**: fear_and_greed_value, fear_and_greed_classification (2 columns)
  - **Market Metrics**: Various CoinMarketCap global metrics columns
- **Data Flow**: 1m candles → calculate indicators → aggregate to 15m/1h/4h/1d with indicator recalculation
- **Update Strategy**: INSERT...ON CONFLICT DO UPDATE for idempotent loading
- **Note**: 4h and 1d tables created 2026-02-05, pending initial data load

#### Backtest Tables
- **Table**: `backtest_ml`
- **Purpose**: Storage for ML backtesting results
- **Created**: 2025-12-08 to 2025-12-10
- **Size**: 2.9 GB (2,015,988 rows)
- **Strategy**: `ML_Backtest_v1`
- **Columns** (80 total):
  - Trade info: symbol, entry_timestamp, exit_timestamp, direction, timeframe, strategy_name
  - Prices: entry_price, exit_price, tp_price, sl_price, candle OHLCV
  - Performance: profit_usd, profit_percent, net_profit_usd, drawdown_percent
  - Risk metrics: tp_percent, sl_percent, risk_reward_ratio, leverage
  - Time metrics: duration_minutes, bars_in_trade, mirror_duration_minutes
  - APR calculations: apr_net, apr_gross, return_per_dollar_hour/month
  - Indicators snapshot: ema_9/12/21/26/50/100/200, rsi_7/9/14/21/25, mfi_7/10/14/20/25, atr_7/14/21/30/50/100
  - Multi-timeframe ATR: atr_5m, atr_15m, atr_1h, atr_6h, atr_12h, atr_24h
  - Price changes: price_change_5m/15m/1h/6h/12h/24h
- **Note**: Created by former team member for ML strategy backtesting

#### Other Tables
- **Table**: `eda` - Empty table (schema only, 0 rows) for exploratory data analysis
- **Table**: `trading_signals`, `open_positions`, `trade_history` - In `trading_operations` database for live trading

### Configuration Files
- **data_collector_config.yaml**: Futures market configuration
  - Database connection:
    - Host: 82.25.115.144 (VPS)
    - Port: 5432
    - Database: trading_data
    - Table: candles_bybit_futures_1m
    - User: trading_writer (for data collection)
    - Password: Secure password stored in config
  - Collection settings (start_date, end_date, symbols)
  - API settings (category: linear, rate_limit, max_retries, timeout)
  - Batch settings (batch_size: 1000 candles per API request, daily batching for DB writes)

- **data_collector_config_spot.yaml**: Spot market configuration
  - Database connection: Same as futures but different table
    - Table: candles_bybit_spot_1m
  - Collection settings:
    - start_date: 2021-07-05 12:00:00 (verified earliest spot data)
    - Symbols: 16 trading pairs (all futures pairs except XMRUSDT which is not available on spot)
  - API settings (category: spot)
  - Daily batching: Process 1 day (1440 candles) at a time with immediate DB write

- **monitor_config.yaml**: Monitor configuration (futures)
  - Symbols to monitor (default: BTCUSDT, ETHUSDT, XRPUSDT, SOLUSDT, ADAUSDT)
  - Check interval: 1 minute for real-time updates
  - Gap detection thresholds
  - Checkpoint and state management
  - Logging settings with compact mode support

- **monitor_config_spot.yaml**: Monitor configuration (spot)
  - Symbols to monitor: All 16 spot trading pairs
  - Same settings as futures monitor
  - Deployed on VPS in daemon mode for continuous updates
  - Real-time gap detection and filling

### API Integration
- **Exchange**: Bybit v5 API
- **Categories Supported**:
  - **linear**: Futures/perpetual contracts (data_collectors/bybit/futures/)
  - **spot**: Spot market (data_collectors/bybit/spot/)
- **Interval**: 1-minute candles (1m)
- **Rate Limit**: 100 requests/minute (conservative, actual limit is higher)
- **Max Candles per Request**: 1000 (Bybit API limit)
- **Retry Logic**: Exponential backoff with max 5 retries
- **Daily Batching**: Both futures and spot collectors process 1 day (1440 candles) at a time with immediate DB write for safe interruption recovery

## Project Structure

```
TradingChart/
├── api/
│   └── bybit/
│       └── bybit_api_client.py         # Bybit API wrapper
├── data_collectors/
│   ├── bybit/
│   │   ├── futures/
│   │   │   ├── data_loader_futures.py  # Futures historical collector with daily batching
│   │   │   ├── monitor.py             # Real-time futures data monitor
│   │   │   ├── database.py            # Database operations
│   │   │   ├── time_utils.py          # Timezone utilities with future date limiting
│   │   │   ├── config_validator.py    # Config validation
│   │   │   ├── check_data.py          # Data verification
│   │   │   └── monitor_manager.sh     # Process management
│   │   └── spot/
│   │       ├── data_loader_spot.py    # Spot historical collector with daily batching
│   │       ├── monitor_spot.py        # Real-time spot data monitor
│   │       ├── database.py            # Database operations (spot table)
│   │       ├── time_utils.py          # Timezone utilities (real file, not symlink)
│   │       ├── config_validator.py    # Config validation (spot)
│   │       └── check_data_spot.py     # Data verification (spot)
│   ├── data_collector_config.yaml     # Futures config
│   ├── data_collector_config_spot.yaml # Spot config
│   └── monitor_config.yaml            # Monitor config
├── indicators/
│   ├── start_all_loaders.py           # Orchestrator for automatic sequential loading
│   ├── sma_loader.py                  # SMA indicator calculator
│   ├── ema_loader.py                  # EMA indicator calculator with checkpoint
│   ├── rsi_loader.py                  # RSI indicator calculator with batch processing
│   ├── vma_loader.py                  # VMA indicator calculator (volume analysis)
│   ├── atr_loader.py                  # ATR indicator calculator with Wilder smoothing
│   ├── adx_loader.py                  # ADX indicator calculator with double smoothing
│   ├── macd_loader.py                 # MACD indicator calculator (8 configurations)
│   ├── obv_loader.py                  # OBV cumulative volume indicator
│   ├── bollinger_bands_loader.py      # Bollinger Bands (13 configurations)
│   ├── vwap_loader.py                 # VWAP daily and rolling (16 variants)
│   ├── mfi_loader.py                  # MFI volume-weighted momentum (5 periods)
│   ├── stochastic_williams_loader.py  # Stochastic & Williams %R oscillators
│   ├── ichimoku_loader.py             # Ichimoku Cloud (2 configs × 8 columns)
│   ├── long_short_ratio_loader.py     # Long/Short Ratio from Bybit API
│   ├── open_interest_loader.py        # Open Interest from Bybit API (Oct 2023+)
│   ├── funding_fee_loader.py         # Funding Rate from Bybit API (Mar 2020+)
│   ├── premium_index_loader.py       # Premium Index from Bybit API (Mar 2020+)
│   ├── fear_and_greed_loader.py       # Fear & Greed from Alternative.me API
│   ├── fear_and_greed_coinmarketcap_loader.py  # Market metrics from CoinMarketCap API
│   ├── database.py                    # Database operations for indicators
│   ├── indicators_config.yaml         # Indicators configuration
│   ├── check_indicators_status.py     # Check indicators in DB
│   ├── check_atr_status.py            # Check ATR-specific status
│   ├── check_adx_status.py            # Check ADX-specific status
│   └── INDICATORS_REFERENCE.md        # Technical indicators documentation
├── bybit_futures_check_start_date_by_symbol/
│   ├── check_symbols_launch.py        # Symbol launch date checker
│   └── symbols_launch_dates.json      # Symbol metadata
└── requirements.txt                   # Python dependencies
```

## VPS Infrastructure

### Server Specifications
- **Host**: 82.25.115.144 (srv1010160)
- **Disk**: 193 GB total
- **RAM**: 16 GB (7.9 GB tmpfs)
- **OS**: Ubuntu with Docker

### Disk Usage (as of 2025-12-17)
| Component | Size | Notes |
|-----------|------|-------|
| PostgreSQL | 138 GB | Main data storage |
| Docker | ~3 GB | 5 active containers |
| /root projects | 2.3 GB | TradingCharts, backtester, etc. |
| System | ~5 GB | /usr, /boot, etc. |
| **Free** | **47 GB** | After cleanup |

### Docker Containers (Active)
| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| backtester_flower | backtester_01-flower | 5555 | Celery monitoring |
| backtester_streamlit | backtester_01-streamlit | 8501 | Web UI |
| backtester_celery | backtester_01-celery_worker | - | Task worker |
| backtester_redis | redis:7-alpine | 6379 | Message broker |
| metabase | metabase/metabase | 3000 | BI dashboard |

### Cron Jobs (VPS)
```bash
# Indicators orchestrator - runs daily at 01:00
0 1 * * * /bin/bash -c 'cd /root/TradingCharts/indicators && /root/TradingCharts/venv/bin/python3 start_all_loaders.py >> /root/TradingCharts/indicators/logs/cron/start_all_loaders_$(date +\%Y\%m\%d_\%H\%M).log 2>&1'

# Docker build cache cleanup - runs every Sunday at 00:00
0 0 * * 0 docker builder prune -a -f >> /var/log/docker-cleanup.log 2>&1
```

### Maintenance Notes
- **Docker Build Cache**: Can grow to 30-40 GB if not cleaned regularly. Auto-cleanup configured via cron.
- **PostgreSQL WAL**: Currently 353 MB (healthy). Monitor if grows beyond 1 GB.
- **Disk Alert Threshold**: Action required if usage exceeds 90% (currently 77%)

## Performance Metrics

- **Collection Speed**: 60,000-120,000 candles/minute
- **API Efficiency**: 1000 candles per request (maximum batch size)
- **Memory Usage**: <100MB for large collections (daily batching limits memory footprint)
- **Database Performance**: ~10,000 inserts/second with bulk operations
- **Time Coverage**: 1440 candles per day per symbol (1-minute intervals)
- **Daily Batching**: Process 1 day (1440 candles) at a time with immediate DB write
  - Maximum data loss on interruption: 1 day (1440 candles)
  - Previous risk: Entire collection period could be lost
  - Recovery: Automatically resumes from last completed day

## Supported Trading Pairs

**Futures Market (17 pairs):**
- BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, ADAUSDT
- BNBUSDT, LINKUSDT, XLMUSDT, LTCUSDT, DOTUSDT
- ARBUSDT, ATOMUSDT, ETCUSDT, NEARUSDT, POLUSDT
- VETUSDT, XMRUSDT

**Spot Market (16 pairs):**
- All futures pairs except XMRUSDT (not available on spot - API Error 10001)
- Most pairs available from: 2021-07-05 12:00:00 UTC (verified for BTCUSDT, ETHUSDT)
- BNBUSDT available from: 2022-03 (later launch date)

Additional symbols can be added in configuration files.

## External API Data Sources

### CoinMarketCap Global Metrics API

**Purpose:** Access to global cryptocurrency market capitalization and related metrics

**Available Endpoint (Free Plan):**
```
GET https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest
```

**API Key:** Configured in `indicators_config.yaml` (coinmarketcap_fear_and_greed.api_key)

**Available Data:**
- **Market Cap:** Total, Altcoins, DeFi, Stablecoins (e.g., $3.68T total)
- **24h Volumes:** Total, Altcoins, DeFi, Stablecoins, Derivatives (e.g., $181.85B total)
- **Dominance:** BTC (59.20%), ETH (12.63%) with 24h change tracking
- **Statistics:** Active cryptocurrencies, exchanges, trading pairs
- **Change Metrics:** 24h percentage changes for market cap and volume

**Update Frequency:**
- Official: Every 5 minutes
- Actual: Every 1 minute (validated via testing)
- Recommended query: Every 1 hour (optimal for API limits)

**API Limits (Free Plan):**
- 10,000 calls/month
- Hourly updates: 720 calls/month (7.2% of limit) ✅
- 15-min updates: 2,880 calls/month (28.8% of limit) ⚠️

**Status:** Researched and validated (see CHANGELOG.md) - ready for implementation

**Future Implementation:**
- Create `market_cap_global_loader.py`
- Store in `indicators_bybit_futures_1h` table
- Run via cron hourly for automatic historical data collection

## Development Notes

### Current Capabilities
- Complete historical data collection for any date range
- Smart chunking for periods larger than 1000 candles
- Automatic gap detection and filling
- Proper UTC timezone conversion for all timestamps
- Real-time progress tracking with tqdm
- PostgreSQL bulk insertion with conflict handling
- Checkpoint-based resume functionality
- Daemon mode for continuous monitoring

### Recent Improvements
- Renamed continuous_monitor.py to monitor.py for simplicity
- Added environment variable support via .env file
- Enhanced security by removing passwords from documentation
- Fixed gap detection logic for seamless data updates
- Improved progress tracking with real-time candle counts
- Added support for loading database credentials from environment
- API keys are now optional for historical data collection (public endpoints)
- Optimized for production use with stable checkpoint system
- **Multi-symbol support**: Both data_loader_futures.py and monitor.py now process multiple symbols
- **Enhanced progress bars**: Shows exact candle count instead of '?' and displays latest loaded timestamp
- **Symbol launch dates**: Automatic handling of different launch dates for each symbol
- **Database analysis tool**: Added check_db_symbols.py for comprehensive data completeness checks
- **EMA indicator support**: Added ema_loader.py with checkpoint system for resumable loading
- **RSI indicator support**: Added rsi_loader.py with single-pass calculation for 100% accuracy (MAJOR UPDATE 2025-11-11: Removed checkpoint system, fixed aggregation offset bug, 3.9x performance improvement - see detailed entry below)
- **VMA indicator support**: Added vma_loader.py for volume analysis with sequential period processing
- **ATR indicator support**: Added atr_loader.py with Wilder smoothing algorithm for volatility analysis (MAJOR REFACTORING 2025-11-12: Switched to OBV pattern for 99.99% accuracy - see detailed entry below)
- **ADX indicator support**: Added adx_loader.py with double Wilder smoothing for trend strength analysis (8 periods: 7, 10, 14, 20, 21, 25, 30, 50)
- **Batch processing for indicators**: EMA processes in 1-day batches, RSI and ATR use single-pass calculation (batches for DB writes only), VMA/ADX in 1-day batches
- **Multi-timeframe aggregation**: Automatic aggregation from 1m to 15m and 1h timeframes
- **Comprehensive indicators documentation**: INDICATORS_REFERENCE.md with formulas and trading strategies for all indicators
- **Enhanced console output**: Clean, optimized output for all indicator loaders with visual separators between trading pairs
- **Performance tracking**: All indicator loaders (RSI, SMA, EMA, VMA, ATR, OBV, MACD, Long/Short Ratio) now display total execution time upon completion
- **Long/Short Ratio bug fix**: Fixed critical bug where data wasn't saving to database (changed UPDATE to INSERT...ON CONFLICT pattern)
- **check_indicators_db.py fix**: Multi-timeframe detection for indicators - Long/Short Ratio now checked in 15m/1h tables instead of only 1m (where it's NULL by design)
- **Orchestrator automation**: Created start_all_loaders.py for automatic sequential loading of all indicators with comprehensive logging and statistics
- **CoinMarketCap Global Metrics research**: Validated API access to global cryptocurrency market capitalization data (total market cap, BTC/ETH dominance, volumes, etc.) - ready for future implementation
- **MACD indicator support**: Added macd_loader.py with 8 configurations (classic, crypto, aggressive, balanced, scalping, swing, longterm, ultralong) for different trading strategies
- **Bollinger Bands support**: Added bollinger_bands_loader.py with 13 configurations (11 SMA-based, 2 EMA-based) including squeeze detection
- **VWAP indicator support**: Added vwap_loader.py with 16 variants (1 daily reset + 15 rolling periods) for institutional-grade volume analysis
- **MFI indicator support**: Added mfi_loader.py with 5 periods (7, 10, 14, 20, 25) - volume-weighted alternative to RSI
- **Stochastic & Williams %R support**: Added stochastic_williams_loader.py combining Stochastic Oscillator (8 configurations) and Williams %R (5 periods)
- **Fear & Greed Index support**: Added fear_and_greed_loader.py for Alternative.me API and fear_and_greed_coinmarketcap_loader.py for CoinMarketCap global market metrics
- **CoinMarketCap Fear & Greed Loader optimization**: Two rounds of database optimization for dramatic performance improvement
  - First optimization: single connection + BETWEEN instead of DATE() + batch commits (65 sec → 5-10 sec for data processing)
  - Second optimization: checkpoint caching + DATE(MAX()) instead of MAX(DATE()) (60+ sec → 0.03 sec for checkpoint queries)
  - Overall speedup: ~2200x for checking new data (from 65-70 seconds to 0.03 seconds)
- **Comprehensive indicator suite**: Now supporting 14+ technical indicators across price action, momentum, volume, volatility, and sentiment analysis
- **Spot market support**: Created complete autonomous data collection system for Bybit spot market
  - Separate directory structure (data_collectors/bybit/spot/) with no symlink dependencies
  - Identical architecture to futures: data_loader_spot.py, monitor_spot.py, database.py, time_utils.py
  - Configuration file: data_collector_config_spot.yaml
  - Database table: candles_bybit_spot_1m
  - 16 trading pairs configured (all futures pairs except XMRUSDT which is not available on spot)
  - Data availability verified from 2021-07-05 12:00:00 UTC for most pairs
- **Daily batching implementation**: Both futures and spot data loaders now use daily batching
  - Process 1 day (1440 candles) at a time with immediate DB write after each day
  - Dramatically reduces risk: maximum 1 day loss on interruption (vs entire collection period)
  - Memory efficient: process large date ranges without memory overflow
  - Single clean progress bar showing daily progress with date, candle count, and status
  - Removed excessive logging: only essential INFO messages, internal operations moved to DEBUG
- **Future date limiting**: Automatic end_date capping to current UTC time
  - Prevents attempts to collect data from future dates
  - Added in TimeManager.get_collection_period() for both futures and spot
  - Logs warning when end_date is in the future and automatically adjusts to now_utc
- **Spot data availability research**: Verified earliest data dates via API testing
  - BTCUSDT: First candle at 2021-07-05 12:00:00 UTC
  - ETHUSDT: First candle at 2021-07-05 12:00:00 UTC (verified separately)
  - Most major pairs: Available from 2021-07-05 12:00:00 UTC
  - BNBUSDT: Available from 2022-03 (later launch)
  - XMRUSDT: Not available on spot (API Error 10001)
- **Production deployment optimization**: Spot directory is fully autonomous
  - All files are real copies (no symlinks to futures directory)
  - Can be deployed to VPS independently
  - Configuration file paths are relative to spot directory
  - Test scripts removed, only production files remain
- **Spot market monitoring deployed**: Real-time data monitoring system operational on VPS
  - monitor_spot.py running in daemon mode for continuous updates
  - All 16 trading pairs synchronized and updating in real-time
  - 1-2 minute latency (excellent for daemon monitoring)
  - monitor_config_spot.yaml configured with all available symbols
  - 991 candles added per symbol during initial sync (16+ hours of missed data)
  - Total: 30.1M+ candles across all spot pairs
  - Zero gaps verified across multiple symbols
- **EMA Loader Critical Bug Fix**: Fixed timestamp offset bug affecting all aggregated timeframes (15m, 1h) - 2025-11-10
  - **ROOT CAUSE #1 - Timestamp Offset**: SQL aggregation used period START instead of END for timestamps
    - BEFORE: timestamp `15:00` contained data from `15:00-15:59` (FUTURE data!)
    - AFTER: timestamp `15:00` contains data from `14:00-14:59` (CORRECT!)
    - Fixed SQL formula in `ema_loader.py` line 393 and `check_ema_data.py` lines 346-360
    - Error reduction: 100-400 points → <0.2 points (99.5% improvement!)
  - **ROOT CAUSE #2 - Insufficient Lookback**: Increased `lookback_multiplier` from 3x to 5x
    - 3x covers 95% of EMA weights, 5x covers 99% of EMA weights
    - Error reduction: 1.99 points → 0.01 points for optimal configuration
    - Applied in `ema_loader.py` line 555 and `check_ema_data.py` line 502
  - **Fix #3 - Aggregation Edge Case**: Added `adjusted_overlap_start` for proper 1m candle loading before aggregation
  - **Fix #4 - First Batch Cold Start**: Removed special case that skipped warm-up period for first batch
  - **Validation Results**: 99.99% accuracy for short periods (EMA 9-26), long periods within industry standards
  - **Impact**: Bug affected ALL indicators with aggregation (SMA, RSI, ATR, ADX, MACD, Bollinger, VWAP, MFI, Stochastic, Williams %R)
  - **Action Required**: All aggregated timeframes (15m, 1h) require full recalculation with `--force-reload`
  - **Documentation**: Full analysis in `indicators/tools/EMA_ROOT_CAUSE_REPORT.md`
- **RSI Loader Major Architecture Overhaul**: Complete rewrite to single-pass calculation (like validator) - 2025-11-11
  - **BUG FIX #1 - Aggregation Offset**: Fixed timestamp offset bug in candle aggregation (1m→15m, 1m→1h)
    - **BEFORE:** Timestamp was END of period (`+ INTERVAL '1 hour'`, `+ 1` for 15m) ❌
    - **AFTER:** Timestamp is START of period (Bybit standard) ✅
    - For 1h: `date_trunc('hour', timestamp)` (14:00 = 14:00:00-14:59:59)
    - For 15m: No `+ 1` in division (14:00 = 14:00:00-14:14:59)
    - Fixed in 4 locations: loader (2 queries) + validator (2 queries)
  - **ARCHITECTURE CHANGE #1 - Removed Checkpoint System**: Eliminated batch state accumulation
    - **Problem:** Checkpoint system accumulated rounding errors between batches
    - **Impact:** 19-42% accuracy loss (RSI-7: 81% → 100%, RSI-14: 58% → 100%)
    - **Deleted:** 138 lines of code (`load_checkpoint()`, `save_checkpoint()`, `get_checkpoint_state()`)
    - **Deleted:** `import json`, checkpoint directory, checkpoint JSON files
    - **Reason:** State persistence between batches caused mathematical drift
  - **ARCHITECTURE CHANGE #2 - Single-Pass Calculation**: Load all → calculate → write in batches
    - **BEFORE:** Batch processing with state between batches (checkpoint save/restore)
    - **AFTER:** Load ALL data (with lookback) → calculate RSI once → write results in batches
    - **New method:** `load_all_data()` - loads entire dataset with 10x lookback
    - **New method:** `calculate_rsi()` - single-pass Wilder smoothing (identical to validator)
    - **New method:** `write_results_in_batches()` - efficient batch DB writes
    - **Process:** 3 steps: (1) Load all data, (2) Calculate all periods, (3) Write in batches
  - **ARCHITECTURE CHANGE #3 - Unified with Validator**: Loader now identical to validator logic
    - Both use same `calculate_rsi()` method
    - Both use 10x lookback for Wilder convergence (99.996% accuracy)
    - No state management, no checkpoint files
    - Guarantees mathematical correctness
  - **PERFORMANCE IMPROVEMENT**: 3.9x faster than old checkpoint-based approach
    - Old: 31 seconds for 7 days (batch + checkpoint overhead)
    - New: 8 seconds for 7 days (single-pass calculation)
    - Speedup: Load once (0.6s) + Calculate (0.05s) + Write (4s) = 8s total
  - **ACCURACY IMPROVEMENT**: 100% mathematical correctness
    - Validation: 735 comparisons, 0 errors, 100% accuracy
    - RSI-7: 81.22% → 100% (+18.78%)
    - RSI-14: 57.84% → 100% (+42.16%)
    - RSI-25: 57.84% → 100% (+42.16%)
  - **CODE SIMPLIFICATION**: Removed 138 lines, added cleaner single-pass logic
    - Easier to maintain and understand
    - No checkpoint state to manage
    - Identical to proven validator approach
  - **Impact**: All RSI data requires recalculation with `--force-reload`
  - **Action Required**: Run full reload for all symbols: `python3 rsi_loader.py --symbol SYMBOL --force-reload`
  - **Validator**: `indicators/tests/check_full_data/check_rsi_data.py` - validates 100% accuracy
  - **BUG FIX #2 - Decimal Type Handling**: Added float64 conversion for PostgreSQL Decimal types
    - **Problem:** PostgreSQL returns `DECIMAL` type for numeric columns, incompatible with numpy float operations
    - **Error:** `TypeError: unsupported operand type(s) for +: 'float' and 'decimal.Decimal'`
    - **Impact:** Blocked production deployment on VPS (occurred with 2.9M candles for BTCUSDT 1m)
    - **Fix:** Convert closes array to float64 before calculations: `closes = np.asarray(closes, dtype=np.float64)`
    - **Location:** `rsi_loader.py` line 293, `check_rsi_data.py` line 397
    - **Applied:** Both loader and validator for consistency
    - **Tested:** ✅ Works with all data volumes (41 to 2.9M candles), maintains 100% accuracy
- **ATR Loader Critical Architecture Refactoring**: Switched from batch processing to OBV pattern - 2025-11-12
  - **ROOT CAUSE - Broken Wilder Smoothing Chain**: Batch processing reinitializes ATR with SMA in each batch
    - **BEFORE:** Each batch: `ATR[0] = SMA(TR[:period])` → breaks Wilder chain ❌
    - **AFTER:** Single pass: `ATR[i] = (ATR[i-1] * (period-1) + TR[i]) / period` → maintains full chain ✅
    - **Problem:** ATR uses pseudo-cumulative formula where each value depends on ALL previous values through chain
    - **Impact:** 66% accuracy with batch approach (83,280 errors out of 245,088 comparisons)
  - **BUG FIX #1 - ORDER BY Non-Deterministic Sort**: Fixed candle aggregation sorting
    - **BEFORE:** `ORDER BY period_start DESC` → non-deterministic (same value for all candles) ❌
    - **AFTER:** `ORDER BY timestamp DESC` → deterministic (gets last candle in period) ✅
    - **Location:** `atr_loader.py` line 339
    - **Impact:** Improved accuracy from 0.65% to 66%
  - **BUG FIX #2 - Force Reload Logic**: Fixed --force-reload flag to overwrite ALL data
    - **BEFORE:** Loaded from start, calculated full chain, but wrote only new data ❌
    - **AFTER:** With --force-reload, overwrites ALL data from beginning ✅
    - **Location:** `atr_loader.py` lines 700-712
  - **ARCHITECTURE CHANGE - OBV Pattern Implementation**:
    - **Method 1:** `load_all_candles()` - loads ALL candles from history start in single query
    - **Method 2:** `batch_update_atr()` - writes results in daily batches with progress bar
    - **Method 3:** Refactored `calculate_and_save_atr()` - calculate on full dataset, write in batches
    - **Key Insight:** Memory-efficient batches for WRITES, not CALCULATIONS
  - **ACCURACY IMPROVEMENT**: 66% → 99.99% (245,094 comparisons, 24 "errors")
    - All 24 "errors" are missing timestamps (Found None) from incomplete test run
    - Zero calculation errors - perfect Wilder smoothing chain maintained
    - Validation: ATR-7/14/21/30/50/100 all show 99.99% accuracy
  - **PERFORMANCE**: ~5-10 seconds calculation + ~30-35 minutes DB write per period
    - Calculation: 40,885 candles in 5.5 seconds (extremely fast)
    - DB Write: 1,704 days batched with progress tracking
    - Total: ~3-4 hours for all 6 periods × 3 timeframes × multiple symbols
  - **Impact**: All ATR data requires recalculation with `--force-reload`
  - **Action Required**: Run full reload: `python3 atr_loader.py --force-reload`
  - **Validator**: `indicators/tests/check_full_data/check_atr_data.py` - confirms 99.99% accuracy
- **ADX Orchestrator Integration**: Enabled ADX loader in start_all_loaders.py orchestrator (2025-11-17)
  - **Change**: Set `orchestrator.loaders.adx: true` in indicators_config.yaml (was previously disabled)
  - **Position**: ADX now runs automatically between bollinger_bands and stochastic loaders
  - **Impact**: ADX data (8 periods × 3 components = 24 columns) will be automatically updated during orchestrator runs
  - **Configuration**: No manual ADX loader runs needed - handled by `python3 start_all_loaders.py`
- **VPS Disk Space Investigation & Cleanup** (2025-12-17)
  - **Problem**: +40 GB unexpected disk usage on VPS, disk was 96% full (185 GB / 193 GB)
  - **Investigation**: Analyzed PostgreSQL databases, Docker, and filesystem
  - **Root Cause Found**: Docker Build Cache accumulated 32-38 GB from repeated image rebuilds
  - **Additional Finding**: `backtest_ml` table (2.9 GB) created 2025-12-08 to 2025-12-10 by former team member
    - 2,015,988 backtesting trades for `ML_Backtest_v1` strategy
    - 80 columns including trade data, indicators snapshot, and performance metrics
  - **Solution**: Cleaned Docker build cache with `docker builder prune -a -f`
  - **Result**: Freed 38 GB, disk now at 77% (147 GB / 193 GB), 47 GB free
  - **Prevention**: Added cron job for weekly auto-cleanup every Sunday at 00:00
  - **Database Sizes Documented**: Full breakdown of all tables added to documentation
- **Indicator Loaders Aggregation Bug Fix** (2025-12-17)
  - **Problem Overview**: Multiple indicator loaders had incorrect timestamp aggregation formulas
  - **Bug Type 1 - EMA/SMA (15m)**: Only ~25% data filled
    - **Symptom**: Values only existed for timestamps ending in `:15` (every hour)
    - **Root Cause**: Formula grouped ALL 1m candles within hour to same timestamp
  - **Bug Type 2 - EMA/SMA (1h)**: Data shifted by +1 hour forward
    - **Symptom**: Timestamp 14:00 contained data from candles 13:00-13:59
    - **Root Cause**: Formula `date_trunc('hour') + INTERVAL '60 minutes'` added extra hour
  - **Bug Type 3 - VMA/MACD/Stochastic/Williams**: Data shifted by +1 period
    - **Symptom**: All timestamps shifted forward by one period (15m or 1h)
    - **Root Cause**: Extra `+ INTERVAL` added at end of query
  - **Files Fixed (7 total)**:
    - **Loaders**: `ema_loader.py`, `sma_loader.py`, `vma_loader.py`, `macd_loader.py`, `stochastic_williams_loader.py`
    - **Validators**: `check_ema_data.py`, `check_sma_data.py`
  - **Old Formula (WRONG)**:
    ```sql
    date_trunc('hour', timestamp) + INTERVAL '{minutes} minutes'
    -- or with extra offset at end:
    period_start + INTERVAL '{minutes} minutes' as timestamp
    ```
  - **New Formula (CORRECT)**:
    - For 1h: `date_trunc('hour', timestamp)` (no offset)
    - For 15m: `date_trunc('hour') + INTERVAL '15m' * (EXTRACT(MINUTE) / 15)`
  - **Aggregation Standard (Bybit - timestamp = period START)**:
    | Timeframe | Timestamp | 1m candles included |
    |-----------|-----------|---------------------|
    | 1h | 14:00 | 14:00:00 - 14:59:59 |
    | 15m | 14:00 | 14:00:00 - 14:14:59 |
    | 15m | 14:15 | 14:15:00 - 14:29:59 |
    | 15m | 14:30 | 14:30:00 - 14:44:59 |
    | 15m | 14:45 | 14:45:00 - 14:59:59 |
  - **Indicators NOT Affected** (already correct): RSI, ATR, ADX, Bollinger Bands, VWAP, MFI, OBV, Long/Short Ratio, Fear & Greed
  - **Impact**: 15m AND 1h timeframes require full recalculation for: EMA, SMA, VMA, MACD, Stochastic, Williams %R
  - **Action Required**: Run on VPS with `--force-reload` for each affected loader:
    ```bash
    python3 ema_loader.py --force-reload
    python3 sma_loader.py --force-reload
    python3 vma_loader.py --force-reload
    python3 macd_loader.py --force-reload
    python3 stochastic_williams_loader.py --force-reload
    ```
  - **EMA Verification Completed** (2025-12-17):
    - EMA data recalculated with `--force-reload` on VPS
    - **Verification Method**: Manual aggregation check via SQL
    - **1h Verification**:
      - Timestamp 14:00 uses close from 14:59 (last 1m candle of period 14:00-14:59) ✅
      - EMA_9 calculated: 89242.75479850, actual: 89242.75479849, delta: 0.000000006
    - **15m Verification**:
      - Timestamp 14:15 uses close from 14:29 (last 1m candle of period 14:15-14:29) ✅
      - EMA_9 calculated: 89493.77781444, actual: 89493.77781444, delta: 0.0
    - **Coverage**: 100% for all timestamps (was 25% for 15m before fix)
    - **Status**: ✅ EMA fully verified and correct
  - **SMA Verification Completed** (2025-12-18):
    - SMA data recalculated with `--force-reload` on VPS
    - **Status**: ✅ SMA fully recalculated
  - **VMA Verification Completed** (2025-12-19):
    - VMA data recalculated with `--force-reload` on VPS
    - Added `--force-reload` flag to vma_loader.py
    - **Status**: ✅ VMA fully recalculated
  - **MACD Verification Completed** (2025-12-19):
    - MACD data recalculated with `--force-reload` on VPS
    - Added `--force-reload` flag to macd_loader.py
    - **Status**: ✅ MACD fully recalculated
  - **Stochastic + Williams %R Verification Completed** (2025-12-29):
    - Stochastic + Williams %R data recalculated with `--force-reload` on VPS
    - Added `--force-reload` flag to stochastic_williams_loader.py
    - **Status**: ✅ Stochastic + Williams %R fully recalculated
  - **All Aggregation Bug Fixes Complete** (2025-12-29):
    - All 5 affected indicators (EMA, SMA, VMA, MACD, Stochastic/Williams) recalculated
    - Timestamp offset bug fixed: timestamp = START of period (Bybit standard)
    - 15m/1h timeframes now correctly aggregated
- **check_data.py and check_data_spot.py Rewrite** (2026-01-20):
  - **Problem**: Scripts used old schema with `open_time` (bigint milliseconds), but tables use `timestamp` (TIMESTAMPTZ)
  - **Symptoms**:
    - check_data.py showed only 586K candles for BTCUSDT (actual: 3M+)
    - check_data_spot.py failed with "no password supplied" error
  - **Root Cause**: Old DatabaseManager architecture vs direct psycopg2 connection with dotenv
  - **Fix**: Complete rewrite of both scripts:
    - Replaced `open_time` with `timestamp` column
    - Replaced DatabaseManager with direct psycopg2 + dotenv connection
    - Added all symbols from DB automatically (not from config)
    - Added table size display
    - Improved delay formatting (мин/ч/дн)
    - Added status column (✅/⚠️/❌)
  - **Results**:
    - Futures: 17 symbols, 38.8M candles, 100% coverage
    - Spot: 16 symbols, 32M candles, 100% coverage
  - **Files Modified**:
    - `data_collectors/bybit/futures/check_data.py`
    - `data_collectors/bybit/spot/check_data_spot.py`
- **fear_and_greed_loader_alternative.py Import Fix** (2026-01-20):
  - **Problem**: `ModuleNotFoundError: No module named 'indicators.database'` when running from project root
  - **Root Cause**: sys.path.append used 3x dirname (went to PycharmProjects) instead of 2x (TradingChart root)
  - **Fix**: Changed `dirname(dirname(dirname(...)))` to `dirname(dirname(...))` and used `sys.path.insert(0, ...)` for priority
  - **Result**: Script now runs without PYTHONPATH: `python indicators/fear_and_greed_loader_alternative.py`
- **RSI Loader Incremental Update Optimization** (2026-01-20):
  - **Problem**: RSI loader rewrote ALL data on every run (~7 minutes for 51K records per timeframe)
  - **Root Cause**: No check for existing data - always calculated and wrote all records
  - **Solution**: Added incremental update - only write records where RSI IS NULL
  - **Implementation**:
    - New method `get_null_timestamps()` - finds timestamps where any RSI period IS NULL
    - Modified `process_timeframe()` - filters data before writing to DB
    - `--force-reload` flag bypasses optimization (full recalculation)
  - **Statistics Logging**:
    ```
    📊 Статистика оптимизации:
       • Всего рассчитано: 51,048 записей
       • Нужно записать: 1 записей (RSI IS NULL)
       • Пропущено: 51,047 записей (уже заполнены)
    ```
  - **Performance Improvement**:
    | Scenario | Before | After | Speedup |
    |----------|--------|-------|---------|
    | Data up to date | ~7 min | 10 sec | ~40x |
    | 1-10 new records | ~7 min | 15 sec | ~28x |
  - **File Modified**: `indicators/rsi_loader.py`
- **Fear & Greed Index Loaders Incremental Update Optimization** (2026-01-20):
  - **Problem**: Both loaders used checkpoint system that marked day as "processed" even with partial data
  - **Root Cause**: New records added by monitor after loader ran remained NULL forever
  - **Example**: Day 2025-10-14 had 00:00-11:00 filled, 15:00-23:00 remained NULL
  - **Solution**: Query for NULL dates directly instead of using checkpoint system
  - **Implementation**:
    - New method `get_null_dates()` - finds dates where Fear & Greed IS NULL
    - Modified `update_batch()` with `only_null` parameter - only updates NULL records
    - Rewrote `process_timeframe()` - iterates over NULL dates instead of checkpoint range
    - Added `--force-reload` flag - bypasses optimization (full update all records)
    - Added `--timeframe` flag - process specific timeframe only
  - **Performance Improvement**:
    | Scenario | Before | After | Notes |
    |----------|--------|-------|-------|
    | Data up to date | ~65 sec | <1 sec | Skips immediately |
    | 5 days with NULL | ~65 sec | ~2 sec | Only processes NULL dates |
    | Full reload | ~65 sec | ~65 sec | Same (--force-reload) |
  - **Verification Results** (after running loaders for all timeframes):
    | Source | 1m | 15m | 1h |
    |--------|-----|------|-----|
    | Alternative.me | 100.00% | 99.99% | 99.99% |
    | CoinMarketCap | 43.86% | 42.80% | 44.01% |
    - Alternative.me: 100% coverage within API range (data from 2018-02-01)
    - CoinMarketCap: ~44% expected - API only has data from 2023-06-29 (~2.7 years), our DB starts from 2020-03-25
    - Remaining gaps: Only newly added records (today's data) that will be filled on next run
  - **Files Modified**:
    - `indicators/fear_and_greed_loader_alternative.py`
    - `indicators/fear_and_greed_coinmarketcap_loader.py`
- **Long/Short Ratio Loader --force-reload Flag** (2026-01-23):
  - **Problem**: Historical gaps in L/S Ratio data (~47% coverage) because loader only processed new records
  - **Root Cause**: `get_date_range()` started from last filled date and moved forward only
  - **Solution**: Added `--force-reload` flag to start from API earliest date (2020-07-20)
  - **Implementation**:
    - Added `self.force_reload` attribute to `LongShortRatioLoader` class
    - Modified `get_date_range()` to start from `EARLIEST_DATA_DATE` when force_reload=True
    - Added `--force-reload` argument to CLI
  - **Usage**:
    ```bash
    python3 long_short_ratio_loader.py --force-reload                     # All symbols
    python3 long_short_ratio_loader.py --symbol BTCUSDT --force-reload    # Specific symbol
    ```
  - **Expected Result**: Coverage increase from ~47% to ~89% (API data available from 2020-07-20)
  - **File Modified**: `indicators/long_short_ratio_loader.py`
- **Funding Rate Loader Implementation** (2026-01-27):
  - **New Feature**: Added `funding_fee_loader.py` for loading Funding Rate data from Bybit API
  - **API Research**:
    - Endpoint: `/v5/market/funding/history`
    - All 17 trading pairs supported
    - Historical depth: BTCUSDT from 2020-03-25 (~5.8 years), ETHUSDT from 2020-10-21, others from 2021+
    - Funding updates every 8 hours (00:00, 08:00, 16:00 UTC)
    - API limit: 200 records per request
  - **Data Structure** (backward-fill logic):
    - `funding_rate_next` (DECIMAL 12,10): The funding rate that WILL be applied
    - `funding_time_next` (TIMESTAMPTZ): When the next funding will be calculated
    - Example: For timestamp 07:30, stores the rate that applies at 08:00
  - **Backward-Fill Logic**:
    - 00:00-07:59 → stores funding rate for 08:00
    - 08:00-15:59 → stores funding rate for 16:00
    - 16:00-23:59 → stores funding rate for 00:00 next day
  - **Two-Stage Loading with Automatic Gap Detection**:
    - **Stage 1**: Load new data (from last filled timestamp to now)
    - **Stage 2**: Check and fill gaps (find NULLs in API date range, fill automatically)
    - Gaps are detected using `earliest_api_dates` config (per-symbol API availability)
    - No need for `--force-reload` to fill gaps - automatic on every run
  - **earliest_api_dates** in config:
    ```yaml
    funding_rate:
      earliest_api_dates:
        BTCUSDT: "2020-03-25"
        ETHUSDT: "2020-10-21"
        BNBUSDT: "2021-06-29"
        # ... etc
    ```
  - **Usage**:
    ```bash
    python3 funding_fee_loader.py                                    # Normal run (new data + gap check)
    python3 funding_fee_loader.py --symbol BTCUSDT                   # Specific symbol
    python3 funding_fee_loader.py --timeframe 1h                     # Specific timeframe
    python3 funding_fee_loader.py --force-reload                     # Full reload from beginning
    ```
  - **Features**:
    - Graceful shutdown (Ctrl+C): first press finishes current operation, second force exits
    - tqdm progress bars for API loading and DB writing
    - Automatic gap detection and filling on every run
  - **Files Created**: `indicators/funding_fee_loader.py`
  - **Files Modified**: `indicators/indicators_config.yaml`, `indicators/start_all_loaders.py`
- **Premium Index Loader Architecture Rewrite** (2026-01-29):
  - **Problem 1 - Orphan Rows**: `INSERT...ON CONFLICT` created rows with only premium_index
  - **Problem 2 - max_pages Limit**: Loader stopped at 1M records (1000 pages × 1000 records)
  - **Problem 3 - Wrong Direction**: API pagination went newest→oldest, so interruption left old data missing
  - **Solution - Daily Batching** (like data_loader_futures):
    - Process one day at a time from **oldest to newest**
    - Commit after each day for reliable recovery
    - If interrupted, next run continues from last saved date
    - Uses UPDATE only (prevents orphan rows)
  - **Implementation**:
    - New `fetch_day_data()` - fetches one day of data
    - New `save_day_to_db()` - saves one day with UPDATE
    - Rewritten `load_premium_index_for_symbol()` - daily iteration with tqdm progress
    - Removed old `save_to_db()` (no longer needed)
  - **Benefits**:
    - Reliable recovery after interruption
    - Real-time progress (shows current date)
    - No record limit (was 1M, now unlimited)
    - Lower memory usage (1 day at a time)
  - **API Coverage**: 99.99% for BTCUSDT (212 NULL = real gaps in Bybit API from 2020)
  - **File Modified**: `indicators/premium_index_loader.py`
- **Ichimoku Cloud Indicator Implementation** (2026-01-29):
  - **New Feature**: Full Ichimoku Kinko Hyo indicator with 2 configurations
  - **Configurations**:
    - Crypto (9/26/52) - optimized for 24/7 crypto markets
    - Long (20/60/120) - for position trading
  - **Columns per config (8)**: tenkan, kijun, senkou_a, senkou_b, chikou, cloud_thick, price_cloud, tk_cross
  - **Total**: 16 columns per timeframe (2 configs × 8 columns)
  - **Implementation Details**:
    - Uses UPDATE (not INSERT) - consistent with other loaders
    - Chronological order (oldest to newest) - reliable recovery after interruption
    - `--force-reload` flag - rewrites all data from min_date
    - Senkou Span stored as "effective" values (cloud that applies to timestamp T)
    - Derived columns: cloud_thick (%), price_cloud (-1/0/1), tk_cross (-1/0/1)
  - **NULL Optimization** (fixed same day):
    - **Problem**: Loader re-processed entire range on every run (found 33 "natural" NULLs at data start)
    - **Root Cause**: First ~180 records can't have Ichimoku calculated (insufficient lookback history)
    - **Solution**: Added `get_raw_lookback_period()` method and `effective_min_date` calculation
    - **Logic**: Search for NULLs only from `effective_min_date` (min_date + raw_lookback), ignoring natural NULLs
    - **Result**: Repeated runs now show "✅ Все Ichimoku данные актуальны" and complete instantly
  - **Files Created/Modified**:
    - `indicators/ichimoku_loader.py` (NEW)
    - `indicators/indicators_config.yaml` (added ichimoku section)
    - `indicators/start_all_loaders.py` (added to LOADER_MAPPING)
    - `indicators/INDICATORS_REFERENCE.md` (full documentation)
- **SMA/EMA/RSI Loaders --check-nulls Flag** (2026-02-03):
  - **New Feature**: Added `--check-nulls` flag to detect and fill NULL values in the middle of data
  - **Problem Solved**: Monitor adds new records, but historical NULLs in the middle were never filled
  - **SMA Implementation** (non-cumulative indicator):
    - `get_null_timestamp_list()` - finds specific timestamps with NULL (excludes first 200 records)
    - `fill_null_values()` - loads data with lookback, calculates SMA, writes only NULL records
    - **Performance**: Fast - only loads data around NULL range with 200-candle lookback
    - **Boundary**: `unavoidable_null_boundary = min_data_date + (max_period * minutes)`
  - **EMA Implementation** (cumulative indicator):
    - `get_null_timestamp_list()` - same as SMA
    - `fill_null_values()` - **full recalculation from data start** for 100% accuracy
    - **Why full recalc**: EMA is cumulative - each value depends on ALL previous values
    - **Performance**: Slower but guarantees mathematical correctness
  - **RSI Implementation** (cumulative indicator - Wilder smoothing):
    - `get_null_timestamp_list()` - finds NULL timestamps (excludes first max_period+1 records)
    - `fill_null_values()` - **full recalculation from data start** for 100% accuracy
    - **Why full recalc**: RSI uses Wilder smoothing - cumulative formula where each value depends on previous
    - **Boundary**: `unavoidable_null_boundary = min_data_date + ((max_period + 1) * minutes)` (RSI needs period+1 values)
  - **Bug Fix**: Initial version filtered by date range (166K records), fixed to filter by actual NULL timestamps (5 records)
  - **Usage**:
    ```bash
    # SMA (fast - local recalculation)
    python3 sma_loader.py --check-nulls
    python3 sma_loader.py --check-nulls --symbol BTCUSDT
    python3 sma_loader.py --check-nulls --timeframe 1h

    # EMA (full recalculation from start)
    python3 ema_loader.py --check-nulls
    python3 ema_loader.py --check-nulls --symbol ETHUSDT
    python3 ema_loader.py --check-nulls --timeframe 15m

    # RSI (full recalculation from start)
    python3 rsi_loader.py --check-nulls
    python3 rsi_loader.py --check-nulls --symbol BTCUSDT
    python3 rsi_loader.py --check-nulls --timeframe 1h
    ```
  - **Files Modified**: `indicators/sma_loader.py`, `indicators/ema_loader.py`, `indicators/rsi_loader.py`
- **4h and 1d Timeframe Support** (2026-02-05):
  - **New Feature**: Added support for 4-hour and daily timeframes in indicator system
  - **Tables Created**:
    - `indicators_bybit_futures_4h` - 4-hour aggregated indicators (261 columns)
    - `indicators_bybit_futures_1d` - Daily aggregated indicators (261 columns)
  - **Aggregation Logic**:
    - 4h: Fixed intervals at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
    - 1d: Daily aggregation at 00:00 UTC (uses `date_trunc('day', timestamp)`)
  - **Implementation**:
    - Modified `sma_loader.py` `aggregate_candles()` method with new SQL queries
    - Updated `indicators_config.yaml` to include `4h` and `1d` in timeframes list
    - Tables created with `CREATE TABLE ... (LIKE indicators_bybit_futures_1h INCLUDING ALL)`
    - Indexes auto-created: `_pkey`, `_symbol_timestamp_idx`, `_timestamp_idx`
  - **Permissions**: Same as other indicator tables (trading_admin, trading_writer, trading_reader, trading_bot, yura_db_read)
  - **Status**: Tables created, pending initial data load via `sma_loader.py --timeframe 4h/1d`
  - **Files Modified**: `indicators/sma_loader.py`, `indicators/indicators_config.yaml`

### Security Notes
- Database passwords are stored in `.env` file (not in repository)
- Configuration files with passwords are gitignored
- Use environment variables for sensitive data
- **Three-tier user permission system**:
  - `trading_admin`: Full admin privileges (DDL, schema changes) - used by indicators loaders
  - `trading_writer`: Data collection (INSERT, UPDATE, SELECT, DELETE) - used by data_collectors
  - `trading_reader`: Read-only access (SELECT only) - used for analysis scripts
- **Password sources by component**:
  - `data_loader_futures.py` → `.env` (`DB_WRITER_PASSWORD`)
  - `data_loader_spot.py` → `.env` (`DB_WRITER_PASSWORD`)
  - `monitor.py` (futures) → `.env` (`DB_WRITER_PASSWORD`)
  - `monitor_spot.py` → `.env` (`DB_WRITER_PASSWORD`)
  - `indicators/*.py` → `indicators_config.yaml` or `indicators/database.py` (`trading_admin`)
- **Environment variables** (in `.env`):
  - `DB_ADMIN_PASSWORD` - for trading_admin user
  - `DB_WRITER_PASSWORD` - for trading_writer user
  - `DB_READER_PASSWORD` - for trading_reader user
- **Password change procedure**:
  1. Update passwords in PostgreSQL on VPS: `ALTER USER trading_admin PASSWORD 'new_password';`
  2. Update `.env` file in project root
  3. Update `indicators/database.py` (hardcoded default for admin)
  4. Update `indicators/indicators_config.yaml` (database.password field)
  5. **Sync files to VPS** (critical step!):
     ```bash
     # Option 1: Git pull (if passwords not in git)
     cd /root/TradingCharts && git pull origin main

     # Option 2: Manual copy
     scp indicators/database.py root@VPS_IP:/root/TradingCharts/indicators/
     scp indicators/indicators_config.yaml root@VPS_IP:/root/TradingCharts/indicators/
     ```
- **Special characters in passwords**: Passwords with `$`, `!`, `#` work correctly in Python/psycopg2. No escaping needed in code. Issues usually indicate file sync problems between local and VPS.

### Known Limitations
- No automated testing framework implemented yet
- No built-in alerting system for critical gaps
- Monitor processes one symbol at a time in sequence