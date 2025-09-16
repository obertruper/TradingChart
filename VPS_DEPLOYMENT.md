# VPS Deployment Guide

## 📁 Required Files for VPS

### Core Python Scripts
```
data_collectors/bybit/futures/
├── monitor.py                 # Main monitoring script
├── data_loader_futures.py     # Historical data collector
├── database.py                # Database operations
├── time_utils.py             # Time utilities
├── config_validator.py       # Configuration validator
├── check_data.py            # Data verification tool
└── __init__.py             # Package init
```

### API Client
```
api/bybit/
├── bybit_api_client.py     # Bybit API wrapper
└── __init__.py
```

### Configuration Files
```
data_collectors/
├── monitor_config.yaml           # Monitor configuration
├── data_collector_config.yaml    # Data collector configuration
└── checkpoints/                  # Directory for state files (will be created)
```

### Environment & Dependencies
```
.env                        # Environment variables with passwords
requirements.txt            # Python dependencies
```

## 🚀 Deployment Steps

### 1. Prepare Files Locally

Create a deployment archive:
```bash
# Create deployment directory
mkdir vps_deployment
cd vps_deployment

# Copy required files
cp -r ../data_collectors ./
cp -r ../api ./
cp ../.env ./
cp ../requirements.txt ./

# Create archive
tar -czf trading_monitor_vps.tar.gz *
```

### 2. Upload to VPS

```bash
# Upload archive to VPS
scp trading_monitor_vps.tar.gz user@82.25.115.144:~/

# Connect to VPS
ssh user@82.25.115.144
```

### 3. Setup on VPS

```bash
# Extract files
cd ~
mkdir -p trading_monitor
cd trading_monitor
tar -xzf ../trading_monitor_vps.tar.gz

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
pip install python-dotenv

# Verify .env file has correct passwords
nano .env
```

### 4. Test Connection

```bash
# Test database connection
cd data_collectors/bybit/futures
python3 check_data.py

# Test monitor
python3 monitor.py --check-once --verbose
```

### 5. Run in tmux (Recommended)

```bash
# Create tmux session
tmux new -s trading_monitor

# Inside tmux
cd ~/trading_monitor/data_collectors/bybit/futures
source ~/trading_monitor/venv/bin/activate
python3 monitor.py --daemon

# Detach from tmux: Ctrl+B then D
```

### 6. Setup as systemd Service (Optional)

Create service file:
```bash
sudo nano /etc/systemd/system/trading-monitor.service
```

Content:
```ini
[Unit]
Description=Trading Data Monitor
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/home/your_username/trading_monitor/data_collectors/bybit/futures
Environment="PATH=/home/your_username/trading_monitor/venv/bin"
ExecStart=/home/your_username/trading_monitor/venv/bin/python monitor.py --daemon --quiet
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable trading-monitor
sudo systemctl start trading-monitor
sudo systemctl status trading-monitor
```

## 📋 File Checklist

### Essential Files (MUST HAVE):
- [ ] `monitor.py` - Real-time monitoring
- [ ] `database.py` - Database operations
- [ ] `time_utils.py` - Time handling
- [ ] `config_validator.py` - Config validation
- [ ] `bybit_api_client.py` - API client
- [ ] `monitor_config.yaml` - Monitor config
- [ ] `.env` - Database passwords
- [ ] `requirements.txt` - Dependencies

### Optional Files (Nice to have):
- [ ] `data_loader_futures.py` - For historical data
- [ ] `check_data.py` - For data verification
- [ ] `data_collector_config.yaml` - For historical collection

## 🔧 Configuration Files Content

### .env (REQUIRED - Add your passwords)
```env
# Database Configuration
DB_HOST=82.25.115.144
DB_PORT=5432
DB_NAME=trading_data

DB_WRITER_USER=trading_writer
DB_WRITER_PASSWORD=YOUR_ACTUAL_PASSWORD_HERE

# Bybit API (optional for public data)
BYBIT_API_KEY=
BYBIT_API_SECRET=
```

### monitor_config.yaml
Already configured with multiple symbols:
```yaml
monitoring:
  symbols:
    - "BTCUSDT"   # Since 2020-03-25
    - "ETHUSDT"   # Since 2021-03-15
    - "XRPUSDT"   # Since 2021-05-13
    - "SOLUSDT"   # Since 2021-10-15
    - "ADAUSDT"   # Since 2021-01-13
```

## 🔍 Monitoring & Logs

### Check logs:
```bash
# If using tmux
tmux attach -t trading_monitor

# If using systemd
sudo journalctl -u trading-monitor -f

# Check Python logs
tail -f ~/trading_monitor/data_collectors/bybit/futures/logs/monitor.log
```

### Monitor database status:
```bash
cd ~/trading_monitor/data_collectors/bybit/futures
python3 check_data.py
```

## 🚨 Troubleshooting

### Connection Issues
1. Check .env file has correct password
2. Verify VPS can reach database: `ping 82.25.115.144`
3. Check Python can import modules: `python3 -c "import psycopg2"`

### Monitor Not Updating
1. Run with verbose: `python3 monitor.py --check-once --verbose`
2. Check if data is already up-to-date (gap < 2 minutes)
3. Check Bybit API availability

### Memory Issues
- Monitor uses minimal memory (~50MB)
- If issues, check system: `free -h`
- Restart service: `sudo systemctl restart trading-monitor`

## 📝 Notes

- Monitor checks every minute in daemon mode
- Processes multiple symbols sequentially
- Shows exact candle count in progress bars (no more '?')
- Displays latest loaded timestamp during updates
- Skips updates if gap < 2 minutes (configurable)
- Automatically handles reconnection on errors
- Saves state in checkpoints directory
- **API Keys**: NOT required for historical data collection (OHLCV candles are public)
- **Data Collection**: Works with public Bybit endpoints without authentication
- **Multi-Symbol Support**: Both data_loader_futures.py and monitor.py handle multiple symbols efficiently