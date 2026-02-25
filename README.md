# TradingChart - Cryptocurrency Trading Data Platform

A comprehensive system for collecting, storing, and analyzing cryptocurrency market data from multiple exchanges. Includes historical and real-time data collection, 26 technical indicator loaders, orderbook data from Bybit and Binance, and options/volatility data from Deribit.

## Features

- **Multi-Market Data Collection**: Futures (17 pairs) and Spot (16 pairs) from Bybit
- **26 Indicator Loaders**: Technical indicators, market data, orderbook, options (261 columns per timeframe)
- **5 Timeframes**: 1m, 15m, 1h, 4h, 1d with automatic aggregation from 1m base
- **Cross-Exchange Orderbook**: Bybit + Binance orderbook data in separate tables
- **Options & Volatility**: Deribit DVOL, DVOL indicators, aggregated options metrics
- **Real-Time Monitoring**: Daemon-mode monitors for continuous data updates
- **Orchestrator**: Automatic sequential loading of all indicators via `start_all_loaders.py`
- **PostgreSQL**: 137 GB database on VPS with optimized bulk operations

## Prerequisites

- Python 3.8+
- PostgreSQL database (hosted on VPS)
- Bybit API credentials (optional - NOT required for historical data)

## Installation

```bash
# Clone and install
git clone <repository-url>
cd TradingChart
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database passwords

# Copy config templates
cp data_collectors/data_collector_config.example.yaml data_collectors/data_collector_config.yaml
cp data_collectors/monitor_config.example.yaml data_collectors/monitor_config.yaml
```

## Quick Start

### 1. Collect Historical Data

```bash
# Futures market
cd data_collectors/bybit/futures
python3 data_loader_futures.py

# Spot market
cd data_collectors/bybit/spot
python3 data_loader_spot.py
```

### 2. Start Real-Time Monitoring

```bash
# Futures monitor (daemon mode)
cd data_collectors/bybit/futures
python3 monitor.py --daemon

# Spot monitor (daemon mode)
cd data_collectors/bybit/spot
python3 monitor_spot.py --daemon
```

### 3. Load Indicators

```bash
cd indicators

# Run ALL indicators sequentially (recommended)
python3 start_all_loaders.py

# Or run individual loaders
python3 sma_loader.py
python3 rsi_loader.py --symbol BTCUSDT --timeframe 1h
```

### 4. Check Data

```bash
# Futures data stats
cd data_collectors/bybit/futures && python3 check_data.py

# Spot data stats
cd data_collectors/bybit/spot && python3 check_data_spot.py

# Indicators status
cd indicators && python3 check_indicators_status.py
```

## Project Structure

```
TradingChart/
├── api/bybit/                          # Bybit API wrapper
├── data_collectors/bybit/
│   ├── futures/                        # Futures data collection (17 pairs)
│   │   ├── data_loader_futures.py      # Historical data collector
│   │   ├── monitor.py                  # Real-time monitor
│   │   ├── database.py                 # DB operations
│   │   └── check_data.py              # Data verification
│   └── spot/                           # Spot data collection (16 pairs)
│       ├── data_loader_spot.py         # Historical data collector
│       ├── monitor_spot.py             # Real-time monitor
│       └── check_data_spot.py          # Data verification
├── indicators/                         # 26 indicator loaders
│   ├── start_all_loaders.py            # Orchestrator (runs all loaders)
│   ├── sma_loader.py                   # SMA (5 periods)
│   ├── ema_loader.py                   # EMA (7 periods)
│   ├── rsi_loader.py                   # RSI (5 periods)
│   ├── macd_loader.py                  # MACD (8 configurations)
│   ├── bollinger_bands_loader.py       # Bollinger Bands (13 configs)
│   ├── atr_loader.py                   # ATR + NATR (6 periods)
│   ├── adx_loader.py                   # ADX (8 periods)
│   ├── vwap_loader.py                  # VWAP (16 variants)
│   ├── obv_loader.py                   # OBV
│   ├── vma_loader.py                   # VMA (5 periods)
│   ├── mfi_loader.py                   # MFI (5 periods)
│   ├── stochastic_williams_loader.py   # Stochastic + Williams %R
│   ├── ichimoku_loader.py              # Ichimoku Cloud (2 configs)
│   ├── hv_loader.py                    # Historical Volatility
│   ├── supertrend_loader.py            # SuperTrend (5 configs)
│   ├── long_short_ratio_loader.py      # Long/Short Ratio (Bybit API)
│   ├── open_interest_loader.py         # Open Interest (Bybit API)
│   ├── funding_fee_loader.py           # Funding Rate (Bybit API)
│   ├── premium_index_loader.py         # Premium Index (Bybit API)
│   ├── fear_and_greed_loader_alternative.py  # Fear & Greed (Alternative.me)
│   ├── fear_and_greed_coinmarketcap_loader.py # Market metrics (CMC)
│   ├── orderbook_bybit_loader.py       # Orderbook (Bybit archives)
│   ├── orderbook_binance_loader.py     # Orderbook (Binance archives)
│   ├── options_dvol_loader.py          # DVOL (Deribit API)
│   ├── options_dvol_indicators_loader.py    # DVOL indicators
│   ├── options_aggregated_loader.py    # Options aggregated metrics
│   ├── indicators_config.yaml          # Configuration
│   └── INDICATORS_REFERENCE.md         # Technical documentation
├── docs/                               # Reference documentation
│   ├── ORDERBOOK_REFERENCE.md          # Bybit orderbook columns
│   ├── ORDERBOOK_BINANCE_REFERENCE.md  # Binance orderbook columns
│   ├── DVOL_REFERENCE.md               # DVOL/options reference
│   └── OPTIONS_RESEARCH.md             # Options data research
└── requirements.txt
```

## Database Schema

### Tables Overview

| Table | Size | Rows | Columns | Description |
|-------|------|------|---------|-------------|
| indicators_bybit_futures_1m | 114 GB | 25.6M | 261 | 1-min indicators |
| indicators_bybit_futures_15m | 7.7 GB | 1.7M | 261 | 15-min indicators |
| indicators_bybit_futures_1h | 2 GB | 437K | 261 | 1-hour indicators |
| indicators_bybit_futures_4h | — | — | 261 | 4-hour indicators |
| indicators_bybit_futures_1d | — | — | 261 | Daily indicators |
| candles_bybit_futures_1m | 6.7 GB | 38M | 8 | Futures candles |
| candles_bybit_spot_1m | 5.5 GB | 31M | 8 | Spot candles |
| orderbook_bybit_futures_1m | 1.73 GB | 1.1M | 60 | Bybit orderbook |
| orderbook_binance_futures_1m | 550 MB | 1.6M | 46 | Binance orderbook |
| options_deribit_dvol_1h | ~2 MB | ~86K | 6 | DVOL hourly |
| options_deribit_dvol_1m | ~30 MB | ~536K | 6 | DVOL 1-minute |
| options_deribit_dvol_indicators_1h | — | — | 24 | DVOL indicators |
| options_deribit_aggregated_15m | — | — | 26 | Options metrics |

### Candle Columns

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMPTZ | Candle timestamp (UTC) |
| symbol | VARCHAR(20) | Trading pair |
| open, high, low, close | DECIMAL(20,8) | Price data |
| volume, turnover | DECIMAL(20,8) | Trading metrics |

## Supported Trading Pairs

**Futures (17 pairs):** BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, ADAUSDT, BNBUSDT, LINKUSDT, XLMUSDT, LTCUSDT, DOTUSDT, ARBUSDT, ATOMUSDT, ETCUSDT, NEARUSDT, POLUSDT, VETUSDT, XMRUSDT

**Spot (16 pairs):** All futures pairs except XMRUSDT (not available on spot)

## Performance

| Metric | Value |
|--------|-------|
| Collection speed | 60,000-120,000 candles/min |
| API batch size | 1,000 candles/request |
| Memory usage | <100MB (daily batching) |
| DB insert rate | ~10,000 inserts/sec |
| Database size | 137 GB |

## VPS Infrastructure

- **Host**: VPS with PostgreSQL, Docker containers
- **Monitors**: Futures + Spot monitors running 24/7 in daemon mode
- **Cron**: Indicator orchestrator runs daily at 01:00 UTC
- **Docker**: Backtester (Celery + Redis + Streamlit + Flower), Metabase BI

## Documentation

| Document | Description |
|----------|-------------|
| [CLAUDE.md](CLAUDE.md) | Full project reference (commands, architecture, schema) |
| [indicators/INDICATORS_REFERENCE.md](indicators/INDICATORS_REFERENCE.md) | Technical indicator formulas and strategies |
| [indicators/README.md](indicators/README.md) | Indicators navigation hub |
| [docs/ORDERBOOK_REFERENCE.md](docs/ORDERBOOK_REFERENCE.md) | Bybit orderbook column reference |
| [docs/ORDERBOOK_BINANCE_REFERENCE.md](docs/ORDERBOOK_BINANCE_REFERENCE.md) | Binance orderbook column reference |
| [docs/DVOL_REFERENCE.md](docs/DVOL_REFERENCE.md) | DVOL and options data reference |

## License

This project is licensed under the MIT License.
