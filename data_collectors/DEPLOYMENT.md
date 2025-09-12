# Deployment Guide - Bybit Data Collectors

## 🚀 Варианты запуска

### 1. Простой запуск (разработка)

```bash
# Одноразовая проверка
python3 continuous_monitor.py --check-once --symbol BTCUSDT

# Daemon режим
python3 continuous_monitor.py --daemon
```

### 2. Использование Manager Script (рекомендуется)

```bash
# Запуск
./monitor_manager.sh start

# Остановка
./monitor_manager.sh stop

# Перезапуск
./monitor_manager.sh restart

# Статус
./monitor_manager.sh status

# Просмотр логов
./monitor_manager.sh logs

# Одноразовая проверка
./monitor_manager.sh check-once ETHUSDT
```

### 3. Systemd Service (production)

```bash
# Копирование service файла
sudo cp bybit-monitor.service /etc/systemd/system/

# Перезагрузка systemd
sudo systemctl daemon-reload

# Включение автозапуска
sudo systemctl enable bybit-monitor

# Запуск сервиса
sudo systemctl start bybit-monitor

# Проверка статуса
sudo systemctl status bybit-monitor

# Просмотр логов
sudo journalctl -u bybit-monitor -f
```

### 4. Docker Compose (изолированное окружение)

```bash
# Сборка и запуск
docker-compose up -d

# Просмотр логов
docker-compose logs -f data_monitor

# Остановка
docker-compose down

# Перезапуск
docker-compose restart data_monitor

# Проверка статуса
docker-compose ps
```

## 📊 Мониторинг

### Проверка работы скрипта

```bash
# Проверка процесса
ps aux | grep continuous_monitor

# Проверка последних записей в БД
psql -U macbook -d trading_db -c "
SELECT symbol, timestamp, open, high, low, close, volume 
FROM candles_bybit_futures_1m 
ORDER BY timestamp DESC 
LIMIT 10;"

# Проверка размера таблицы
psql -U macbook -d trading_db -c "
SELECT 
    symbol,
    COUNT(*) as candle_count,
    MIN(timestamp) as first_candle,
    MAX(timestamp) as last_candle
FROM candles_bybit_futures_1m 
GROUP BY symbol;"
```

### Просмотр логов

```bash
# Manager script logs
tail -f logs/monitor_daemon.log

# Application logs
tail -f logs/continuous_monitor.log

# Systemd logs
sudo journalctl -u bybit-monitor -f --since "1 hour ago"

# Docker logs
docker-compose logs -f --tail=100 data_monitor
```

## 🔧 Настройка

### Основные параметры в continuous_monitor_config.yaml

```yaml
monitoring:
  # Частота проверки
  check_interval_minutes: 1     # Для real-time мониторинга
  
  # Символы для мониторинга
  symbols:
    - "BTCUSDT"
    - "ETHUSDT"
    - "SOLUSDT"
  
  # Размер батча
  batch_size: 1000             # Максимум для Bybit API
  
  # Обработка ошибок
  max_consecutive_errors: 3    # Остановка после N ошибок подряд
  error_retry_delay: 300       # 5 минут перед повтором
```

### База данных

```yaml
database:
  host: "127.0.0.1"
  port: 5432
  database: "trading_db"
  user: "macbook"
  password: ""
  table_name: "candles_bybit_futures_1m"
```

## 🛡️ Безопасность

### Права доступа

```bash
# Ограничение прав на конфигурационные файлы
chmod 600 *_config.yaml

# Права на скрипты
chmod 755 monitor_manager.sh
chmod 755 continuous_monitor.py

# Права на директорию логов
chmod 755 logs/
```

### Переменные окружения (альтернатива конфигам)

```bash
export BYBIT_API_KEY="your_api_key"
export BYBIT_API_SECRET="your_api_secret"
export DB_PASSWORD="your_db_password"
```

## 📈 Производительность

### Оптимизация PostgreSQL

```sql
-- Создание индексов для быстрого поиска
CREATE INDEX idx_candles_symbol_timestamp 
ON candles_bybit_futures_1m(symbol, timestamp DESC);

-- Партиционирование таблицы по месяцам (опционально)
CREATE TABLE candles_bybit_futures_1m_2024_08 
PARTITION OF candles_bybit_futures_1m 
FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

-- Настройка autovacuum
ALTER TABLE candles_bybit_futures_1m 
SET (autovacuum_vacuum_scale_factor = 0.1);
```

### Мониторинг ресурсов

```bash
# CPU и память
htop -p $(pgrep -f continuous_monitor)

# Дисковое пространство
df -h /var/lib/postgresql

# Размер БД
psql -U macbook -d trading_db -c "
SELECT pg_size_pretty(pg_database_size('trading_db'));"
```

## 🔄 Backup и восстановление

### Backup базы данных

```bash
# Полный backup
pg_dump -U macbook -d trading_db > backup_$(date +%Y%m%d_%H%M%S).sql

# Только структура
pg_dump -U macbook -d trading_db --schema-only > schema.sql

# Только данные
pg_dump -U macbook -d trading_db --data-only > data.sql
```

### Восстановление

```bash
# Восстановление из backup
psql -U macbook -d trading_db < backup_20240101_120000.sql

# Восстановление с очисткой
dropdb trading_db
createdb trading_db
psql -U macbook -d trading_db < backup_20240101_120000.sql
```

## ⚠️ Troubleshooting

### Скрипт не запускается

```bash
# Проверка зависимостей
pip3 install -r requirements.txt

# Проверка подключения к БД
psql -U macbook -d trading_db -c "SELECT 1;"

# Проверка API ключей
python3 -c "from api.bybit.bybit_api_client import BybitClient; print('API OK')"
```

### Данные не загружаются

```bash
# Проверка последней ошибки
tail -n 50 logs/continuous_monitor.log | grep ERROR

# Проверка лимитов API
grep "rate limit" logs/continuous_monitor.log

# Ручная проверка API
curl "https://api.bybit.com/v5/market/kline?symbol=BTCUSDT&interval=1&limit=1"
```

### Высокое потребление памяти

```bash
# Ограничение размера батча в конфиге
batch_size: 500  # Уменьшить с 1000

# Перезапуск по расписанию
0 */6 * * * systemctl restart bybit-monitor
```

## 📊 Метрики успеха

- ✅ **Uptime**: >99% доступность сервиса
- ✅ **Latency**: <2 секунды от появления свечи до записи в БД
- ✅ **Coverage**: 100% покрытие всех минутных свечей
- ✅ **Error Rate**: <0.1% ошибок API запросов
- ✅ **Memory**: <200MB использования RAM
- ✅ **Storage**: ~2.1MB в день на символ