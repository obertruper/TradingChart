# TradingChart - Cryptocurrency Data Collection System

A robust system for collecting and managing historical cryptocurrency candle data from Bybit exchange.

## 🚀 Features

- **Bulk Historical Data Collection**: Efficiently collect years of 1-minute candle data
- **Smart Chunking**: Automatically handles API limits (1000 candles per request)
- **UTC Timezone Support**: Accurate timestamp handling across all operations
- **PostgreSQL Integration**: Optimized bulk insertion with deduplication
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Error Recovery**: Comprehensive retry logic and error handling
- **Continuous Monitoring**: Automatic gap detection and filling (experimental)

## 📋 Prerequisites

- Python 3.8+
- PostgreSQL database
- Bybit API credentials (optional for public data)

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

3. Set up configuration:
```bash
# Copy example configs
cp data_collectors/data_collector_config.example.yaml data_collectors/data_collector_config.yaml
cp data_collectors/continuous_monitor_config.example.yaml data_collectors/continuous_monitor_config.yaml

# Edit configs with your API keys and database credentials
nano data_collectors/data_collector_config.yaml
```

4. Create PostgreSQL database:
```sql
CREATE DATABASE trading_db;
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
python3 check_data.py
```

### Continuous Monitoring (Experimental)

Monitor and fill data gaps automatically:

```bash
# Single check
python3 continuous_monitor.py --check-once --symbol BTCUSDT

# Daemon mode (experimental)
./monitor_manager.sh start
```

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
│   │       ├── continuous_monitor.py     # Gap filling monitor
│   │       ├── database.py               # Database operations
│   │       ├── time_utils.py            # Timezone utilities
│   │       ├── config_validator.py      # Configuration validation
│   │       ├── check_data.py           # Data verification
│   │       └── monitor_manager.sh      # Monitor control script
│   ├── data_collector_config.yaml      # Main configuration
│   └── continuous_monitor_config.yaml  # Monitor configuration
├── requirements.txt                    # Python dependencies
└── README.md                          # This file
```

## ⚙️ Configuration

### Main Configuration (`data_collector_config.yaml`)

Key settings:
- `api`: Bybit API credentials
- `database`: PostgreSQL connection settings
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

## ⚠️ Known Issues

- **Continuous Monitor**: Gap detection logic needs improvements for periods >24 hours
- **State Management**: Resume functionality in continuous monitor is incomplete

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License.

## 🔗 Resources

- [Bybit API Documentation](https://bybit-exchange.github.io/docs/v5/intro)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📞 Support

For issues and questions, please open an issue on GitHub.