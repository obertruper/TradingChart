# Database Setup Guide - PostgreSQL on VPS

## Overview

This project uses PostgreSQL database hosted on a VPS with a three-tier user permission system for enhanced security and access control.

## VPS Database Configuration

### Connection Details
- **Host**: 82.25.115.144
- **Port**: 5432
- **Database**: trading_data
- **Table**: candles_bybit_futures_1m

### User Architecture

We use three separate PostgreSQL users with different permission levels:

#### 1. **trading_admin** (Full Administrator)
- **Purpose**: Database administration, table creation, schema changes
- **Permissions**: ALL PRIVILEGES on database
- **Password**: [STORED SECURELY - NOT IN REPO]
- **Usage**: Only for database maintenance and migrations

#### 2. **trading_writer** (Data Collector)
- **Purpose**: Used by data collection scripts to insert/update candle data
- **Permissions**: INSERT, UPDATE, SELECT, DELETE on tables
- **Password**: [STORED SECURELY - NOT IN REPO]
- **Connection Limit**: 5 concurrent connections
- **Usage**: Primary user for `data_loader_futures.py` and `monitor.py`

#### 3. **trading_reader** (Read-Only Access)
- **Purpose**: Used by analysis scripts and reporting tools
- **Permissions**: SELECT only on all tables
- **Password**: [STORED SECURELY - NOT IN REPO]
- **Connection Limit**: 10 concurrent connections
- **Usage**: For data analysis, reporting, and read-only operations

## Database Schema

### Table: candles_bybit_futures_1m

```sql
CREATE TABLE candles_bybit_futures_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DECIMAL(20,8) NOT NULL,
    high DECIMAL(20,8) NOT NULL,
    low DECIMAL(20,8) NOT NULL,
    close DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8),
    turnover DECIMAL(20,8),
    PRIMARY KEY (timestamp, symbol)
);
```

### Indexes
- `idx_symbol_timestamp` - Composite index on (symbol, timestamp)
- `idx_timestamp` - Index on timestamp for time-based queries

## Initial Setup Commands

### 1. Create Database and Users (Run on VPS)

```bash
sudo -u postgres psql

-- Create database
CREATE DATABASE trading_data;

-- Create users
CREATE USER trading_admin WITH ENCRYPTED PASSWORD 'YOUR_SECURE_ADMIN_PASSWORD';
CREATE USER trading_writer WITH ENCRYPTED PASSWORD 'YOUR_SECURE_WRITER_PASSWORD';
CREATE USER trading_reader WITH ENCRYPTED PASSWORD 'YOUR_SECURE_READER_PASSWORD';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE trading_data TO trading_admin;
ALTER DATABASE trading_data OWNER TO trading_admin;

\c trading_data

-- Grant schema permissions
GRANT USAGE ON SCHEMA public TO trading_writer, trading_reader;
GRANT CREATE ON SCHEMA public TO trading_writer;
GRANT INSERT, UPDATE, SELECT, DELETE ON ALL TABLES IN SCHEMA public TO trading_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO trading_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trading_writer;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO trading_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO trading_reader;

-- Set connection limits
ALTER USER trading_writer CONNECTION LIMIT 5;
ALTER USER trading_reader CONNECTION LIMIT 10;

\q
```

### 2. Create Table Structure

```bash
PGPASSWORD='YOUR_SECURE_ADMIN_PASSWORD' psql -h localhost -U trading_admin -d trading_data << EOF
CREATE TABLE IF NOT EXISTS candles_bybit_futures_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DECIMAL(20,8) NOT NULL,
    high DECIMAL(20,8) NOT NULL,
    low DECIMAL(20,8) NOT NULL,
    close DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8),
    turnover DECIMAL(20,8),
    PRIMARY KEY (timestamp, symbol)
);

CREATE INDEX idx_symbol_timestamp ON candles_bybit_futures_1m(symbol, timestamp);
CREATE INDEX idx_timestamp ON candles_bybit_futures_1m(timestamp);

-- Grant permissions on the new table
GRANT INSERT, UPDATE, SELECT, DELETE ON candles_bybit_futures_1m TO trading_writer;
GRANT SELECT ON candles_bybit_futures_1m TO trading_reader;
EOF
```

### 3. Configure PostgreSQL for Remote Access

Edit PostgreSQL configuration files on VPS:

```bash
# Edit postgresql.conf
sudo nano /etc/postgresql/16/main/postgresql.conf
# Change: listen_addresses = '*'

# Edit pg_hba.conf
sudo nano /etc/postgresql/16/main/pg_hba.conf
# Add: host    all    all    0.0.0.0/0    scram-sha-256

# Restart PostgreSQL
sudo systemctl restart postgresql

# Open firewall port
sudo ufw allow 5432/tcp
```

## Project Configuration

### data_collector_config.yaml

```yaml
database:
  type: "postgres"
  host: "82.25.115.144"
  port: 5432
  database: "trading_data"
  user: "trading_writer"  # Use writer for data collection
  password: "YOUR_SECURE_WRITER_PASSWORD"
  table_name: "candles_bybit_futures_1m"
```

### For Analysis Scripts (create separate config)

```yaml
database:
  type: "postgres"
  host: "82.25.115.144"
  port: 5432
  database: "trading_data"
  user: "trading_reader"  # Use reader for analysis
  password: "YOUR_SECURE_READER_PASSWORD"
  table_name: "candles_bybit_futures_1m"
```

## Testing Connections

### From Local Machine

```bash
# Test writer connection
PGPASSWORD='YOUR_SECURE_WRITER_PASSWORD' psql -h 82.25.115.144 -U trading_writer -d trading_data -c "SELECT current_user;"

# Test reader connection
PGPASSWORD='YOUR_SECURE_READER_PASSWORD' psql -h 82.25.115.144 -U trading_reader -d trading_data -c "SELECT current_user;"

# Test with Python
python3 -c "
import psycopg2
conn = psycopg2.connect(
    host='82.25.115.144',
    port=5432,
    database='trading_data',
    user='trading_writer',
    password='YOUR_SECURE_WRITER_PASSWORD'
)
print('Connected successfully!')
conn.close()
"
```

## Database Maintenance

### Backup Database (Run on VPS)

```bash
# Full backup as admin
PGPASSWORD='YOUR_SECURE_ADMIN_PASSWORD' pg_dump -h localhost -U trading_admin trading_data > backup_$(date +%Y%m%d_%H%M%S).sql

# Backup specific table
PGPASSWORD='YOUR_SECURE_ADMIN_PASSWORD' pg_dump -h localhost -U trading_admin -t candles_bybit_futures_1m trading_data > candles_backup_$(date +%Y%m%d).sql
```

### Monitor Database

```bash
# Check database size
sudo -u postgres psql -d trading_data -c "SELECT pg_size_pretty(pg_database_size('trading_data'));"

# Check active connections
sudo -u postgres psql -d trading_data -c "SELECT usename, count(*) FROM pg_stat_activity WHERE datname = 'trading_data' GROUP BY usename;"

# Check table statistics
PGPASSWORD='YOUR_SECURE_ADMIN_PASSWORD' psql -h localhost -U trading_admin -d trading_data -c "
SELECT
    symbol,
    COUNT(*) as candle_count,
    MIN(timestamp) as earliest,
    MAX(timestamp) as latest
FROM candles_bybit_futures_1m
GROUP BY symbol
ORDER BY symbol;
"
```

## Security Best Practices

1. **Never share admin credentials** - Use trading_admin only for maintenance
2. **Use appropriate user for each task**:
   - Data collection → trading_writer
   - Analysis/Reports → trading_reader
   - Maintenance → trading_admin
3. **Regular backups** - Schedule daily backups
4. **Monitor connections** - Check for unusual activity
5. **Update passwords periodically** - Every 3-6 months
6. **Restrict IP access** if possible (edit pg_hba.conf)

## Troubleshooting

### Connection Refused
- Check PostgreSQL is running: `sudo systemctl status postgresql`
- Verify firewall: `sudo ufw status`
- Check listen_addresses in postgresql.conf

### Permission Denied
- Verify user has correct permissions
- Check table ownership
- Re-grant permissions if needed

### Too Many Connections
- Check connection limits: `SELECT * FROM pg_user;`
- Close idle connections
- Increase limit if needed: `ALTER USER trading_writer CONNECTION LIMIT 10;`

## Performance Optimization

### Current Settings
- **Max connections**: trading_writer (5), trading_reader (10)
- **Indexes**: Optimized for symbol and timestamp queries
- **Bulk inserts**: Use COPY or batch INSERT for better performance

### Monitoring Performance

```bash
# Check slow queries
PGPASSWORD='YOUR_SECURE_ADMIN_PASSWORD' psql -h localhost -U trading_admin -d trading_data -c "
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
"
```

## Data Verification Script

Save as `check_vps_database.sh` on VPS:

```bash
#!/bin/bash
# Database verification script
# Checks all users, permissions, and table structure

# ... (full script content available in project)
```

Run with: `bash check_vps_database.sh`

---

Last Updated: 2025-09-15
VPS Provider: Your VPS Provider
Database Version: PostgreSQL 16.10