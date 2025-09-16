# TradingChart - Cryptocurrency Data Collection System

A robust system for collecting and managing historical cryptocurrency candle data from Bybit exchange.

## 🚀 Features

- **Bulk Historical Data Collection**: Efficiently collect years of 1-minute candle data
- **Smart Chunking**: Automatically handles API limits (1000 candles per request)
- **UTC Timezone Support**: Accurate timestamp handling across all operations
- **PostgreSQL Integration**: Optimized bulk insertion with deduplication
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Error Recovery**: Comprehensive retry logic and error handling
- **Continuous Monitoring**: Automatic gap detection and real-time data updates

## 📋 Prerequisites

- Python 3.8+
- PostgreSQL database (hosted on VPS)
- Bybit API credentials (optional - NOT required for historical data)
- VPS with Ubuntu (for database hosting)

## 🛠 Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/TradingChart.git
cd TradingChart
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment and configuration:
```bash
# Create .env file from template
cp .env.example .env

# Edit .env with your database passwords
nano .env

# Copy example configs
cp data_collectors/data_collector_config.example.yaml data_collectors/data_collector_config.yaml
cp data_collectors/monitor_config.example.yaml data_collectors/monitor_config.yaml
```

4. PostgreSQL Database Setup:

The project uses a PostgreSQL database hosted on VPS with a three-tier user permission system:
- **trading_admin**: Full administrator privileges
- **trading_writer**: Data collection user (used by scripts)
- **trading_reader**: Read-only access for analysis

See [DATABASE_SETUP.md](DATABASE_SETUP.md) for detailed setup instructions.

## 🚀 Quick Start Guide

After installation, follow these steps to start collecting data:

1. **Test database connection**:
```bash
python3 test_monitor.py
```

2. **Collect historical data** (first-time setup):
```bash
cd data_collectors/bybit/futures
python3 data_loader_futures.py
```

3. **Keep data up-to-date** (run regularly):
```bash
cd data_collectors/bybit/futures
python3 monitor.py --check-once --symbol BTCUSDT
```

## 📊 Usage

### Historical Data Collection

Collect historical candle data for specified date ranges:

```bash
cd data_collectors/bybit/futures
python3 data_loader_futures.py
```

### Check Database

View collected data statistics:

```bash
cd data_collectors/bybit/futures
python3 check_data.py
```

Or use the test script from project root:
```bash
python3 test_monitor.py
```

### Continuous Monitoring

Automatically keep your data up-to-date with real-time monitoring:

```bash
cd data_collectors/bybit/futures

# One-time check for specific symbol
python3 monitor.py --check-once --symbol BTCUSDT

# Check all configured symbols once
python3 monitor.py --check-once

# Run in daemon mode (continuous monitoring)
python3 monitor.py --daemon

# With verbose output for debugging
python3 monitor.py --check-once --verbose

# Quiet mode (minimal output)
python3 monitor.py --daemon --quiet
```

The monitor will:
- Detect gaps in your data
- Fill missing candles automatically
- Keep data updated to the latest minute
- Handle multiple symbols from config

## 📁 Project Structure

```
TradingChart/
├── api/
│   └── bybit/
│       └── bybit_api_client.py    # Bybit API wrapper
├── data_collectors/
│   ├── bybit/
│   │   └── futures/
│   │       ├── data_loader_futures.py    # Main data collector
│   │       ├── monitor.py               # Real-time data monitor
│   │       ├── database.py               # Database operations
│   │       ├── time_utils.py            # Timezone utilities
│   │       ├── config_validator.py      # Configuration validation
│   │       ├── check_data.py           # Data verification
│   │       └── monitor_manager.sh      # Monitor control script
│   ├── data_collector_config.yaml      # Main configuration
│   └── monitor_config.yaml             # Monitor configuration
├── requirements.txt                    # Python dependencies
└── README.md                          # This file
```

## ⚙️ Configuration

### Database Configuration

The system connects to a PostgreSQL database on VPS (82.25.115.144) with:
- Database name: `trading_data`
- Table: `candles_bybit_futures_1m`
- User credentials managed through secure three-tier system

### Main Configuration (`data_collector_config.yaml`)

Key settings:
- `api`: Bybit API credentials
- `database`: PostgreSQL connection to VPS
  - Host: 82.25.115.144
  - Port: 5432
  - Database: trading_data
  - User: trading_writer (for data collection)
- `collection`: Time range and symbols to collect
- `exchange`: Rate limiting and retry settings

### Supported Trading Pairs

- BTCUSDT
- ETHUSDT
- SOLUSDT
- XRPUSDT
- ADAUSDT
- BNBUSDT
- LINKUSDT
- XLMUSDT
- LTCUSDT
- DOTUSDT

## 📈 Performance

- **Speed**: 60,000-120,000 candles/minute
- **API Rate**: 100 requests/minute (configurable)
- **Memory Usage**: <100MB for large collections
- **Storage**: ~150 bytes per candle in PostgreSQL

## 🔧 Database Schema

Table: `candles_bybit_futures_1m`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMPTZ | Candle timestamp (UTC) |
| symbol | VARCHAR(20) | Trading pair symbol |
| open | DECIMAL(20,8) | Opening price |
| high | DECIMAL(20,8) | Highest price |
| low | DECIMAL(20,8) | Lowest price |
| close | DECIMAL(20,8) | Closing price |
| volume | DECIMAL(20,8) | Trading volume |
| turnover | DECIMAL(20,8) | Trading turnover |

## 🖥️ Running as a Service

### Using tmux (Recommended for VPS)
```bash
# Create new tmux session
tmux new -s monitor

# Inside tmux, run monitor
cd data_collectors/bybit/futures
python3 monitor.py --daemon

# Detach from tmux: Press Ctrl+B, then D
# Reattach later: tmux attach -t monitor
```

### Using systemd (Linux)
Create `/etc/systemd/system/trading-monitor.service`:
```ini
[Unit]
Description=Trading Data Monitor
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/TradingChart/data_collectors/bybit/futures
ExecStart=/usr/bin/python3 monitor.py --daemon
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl enable trading-monitor
sudo systemctl start trading-monitor
```

## ✅ Current Status

- **Monitor.py**: Production-ready, actively maintains real-time data
- **Database**: Successfully collecting 2.8M+ BTCUSDT candles since 2020
- **Performance**: Updates within 1-2 minutes of real-time

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License.

## 🔗 Resources

- [Bybit API Documentation](https://bybit-exchange.github.io/docs/v5/intro)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📞 Support

For issues and questions, please open an issue on GitHub.