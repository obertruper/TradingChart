# Документация БД trading_operations

## Общая информация

- **База данных**: `trading_operations`
- **Сервер**: PostgreSQL 16.10
- **Хост**: 82.25.115.144
- **Порт**: 5432
- **Назначение**: Хранение истории торговых операций, сигналов, открытых позиций и портфельных метрик
- **Дата создания**: 2025-09-22

## Структура БД

```
PostgreSQL Server (порт 5432)
│
├── База данных: trading_data (137 GB — исторические данные)
│   └── Таблицы:
│       ├── candles_bybit_futures_1m (38M строк, 6.7 GB)
│       ├── candles_bybit_spot_1m (31M строк, 5.5 GB)
│       ├── indicators_bybit_futures_1m (25.6M строк, 114 GB)
│       ├── indicators_bybit_futures_15m (1.7M строк, 7.7 GB)
│       ├── indicators_bybit_futures_1h (437K строк, 2 GB)
│       ├── indicators_bybit_futures_4h
│       ├── indicators_bybit_futures_1d
│       ├── orderbook_bybit_futures_1m (~1.1M строк, 1.73 GB)
│       ├── orderbook_binance_futures_1m (1.6M строк, 550 MB)
│       ├── options_deribit_dvol_1h (~86K строк)
│       ├── options_deribit_dvol_1m (~536K строк)
│       ├── options_deribit_dvol_indicators_1h
│       ├── options_deribit_aggregated_15m
│       ├── backtest_ml (2M строк, 2.9 GB)
│       └── eda (пустая)
│
└── База данных: trading_operations (8.3 MB — торговые операции)
    └── Таблицы:
        ├── trade_history (история сделок) ✅
        ├── trading_signals (торговые сигналы) ✅
        ├── open_positions (открытые позиции) ✅
        └── portfolio_balance (планируется)
```

## Пользователи и права доступа

| Пользователь | Права | Назначение |
|--------------|-------|------------|
| postgres | Владелец БД | Администрирование |
| trading_admin | Все операции | Управление структурой |
| trading_writer | INSERT, UPDATE, DELETE, SELECT | Запись торговых данных |
| trading_reader | SELECT | Чтение для аналитики (Metabase) |

## Таблица: trade_history

**Назначение**: Хранение полной истории закрытых торговых сделок с детальной информацией о результатах

### Структура полей:

#### 🔑 Идентификаторы

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **id** | BIGSERIAL | PRIMARY KEY | Автоинкрементный уникальный ID записи |
| **custom_trade_id** | VARCHAR(100) | UNIQUE, NULL | Ваш внутренний ID сделки (например: "BTC_LONG_2025_001") |
| **exchange_trade_id** | VARCHAR(100) | NULL | ID сделки от биржи для сопоставления |

#### 📊 Базовая информация о сделке

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **symbol** | VARCHAR(20) | NOT NULL | Торговая пара (BTCUSDT, ETHUSDT и т.д.) |
| **exchange_name** | VARCHAR(20) | NOT NULL | Название биржи (BYBIT, BINANCE, OKX) |
| **market_type** | VARCHAR(10) | NOT NULL | Тип рынка: SPOT или FUTURES |
| **side** | VARCHAR(10) | NOT NULL | Направление сделки: LONG или SHORT |

#### ⏱️ Временные метки

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **open_time** | TIMESTAMPTZ | NOT NULL | Точное время открытия позиции |
| **close_time** | TIMESTAMPTZ | NOT NULL | Точное время закрытия позиции |
| **duration_minutes** | INTEGER | NULL | Длительность сделки в минутах (авто-расчет) |
| **break_even_time** | TIMESTAMPTZ | NULL | Время когда позиция вышла в безубыток |

#### 💰 Цены

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **open_price** | DECIMAL(20,8) | NOT NULL | Цена входа в позицию |
| **close_price** | DECIMAL(20,8) | NOT NULL | Цена выхода из позиции |
| **average_open_price** | DECIMAL(20,8) | NULL | Средняя цена входа (при нескольких ордерах/доливках) |
| **average_close_price** | DECIMAL(20,8) | NULL | Средняя цена выхода (при частичном закрытии) |

#### 📦 Объемы позиции

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **quantity** | DECIMAL(20,8) | NOT NULL | Объем позиции (в базовой валюте, например BTC) |
| **position_value** | DECIMAL(20,8) | NOT NULL | Размер позиции в USDT (quantity × open_price) |
| **partial_closes** | INTEGER | DEFAULT 0 | Количество частичных закрытий позиции |
| **close_type** | VARCHAR(20) | NOT NULL | Тип закрытия: FULL или PARTIAL |
| **remaining_quantity** | DECIMAL(20,8) | DEFAULT 0 | Остаток позиции после частичного закрытия |

#### 💼 Управление капиталом

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **account_balance_before** | DECIMAL(20,8) | NULL | Баланс счета ДО открытия сделки |
| **account_balance_after** | DECIMAL(20,8) | NULL | Баланс счета ПОСЛЕ закрытия сделки |
| **risk_percent_of_capital** | DECIMAL(5,2) | NULL | Риск в % от капитала (0-100%) |

#### 📈 Финансовый результат

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **pnl** | DECIMAL(20,8) | NOT NULL | Profit & Loss в USDT (без учета комиссий) |
| **pnl_percent** | DECIMAL(10,4) | NOT NULL | P&L в процентах от размера позиции |
| **gross_profit** | DECIMAL(20,8) | NULL | Валовая прибыль до вычета комиссий |
| **win_loss** | VARCHAR(10) | NOT NULL | Результат: WIN, LOSS или BREAKEVEN |
| **net_profit** | DECIMAL(20,8) | NULL | Чистая прибыль (pnl - все комиссии) |

#### 💸 Комиссии и сборы

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **open_fee** | DECIMAL(20,8) | DEFAULT 0 | Комиссия при открытии позиции |
| **close_fee** | DECIMAL(20,8) | DEFAULT 0 | Комиссия при закрытии позиции |
| **funding_fee** | DECIMAL(20,8) | DEFAULT 0 | Накопленный funding fee (для futures) |
| **total_fees** | DECIMAL(20,8) | DEFAULT 0 | Сумма всех комиссий |

#### 🎯 Причина и способ закрытия

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **close_reason** | VARCHAR(50) | NULL | Причина закрытия: TAKE_PROFIT, STOP_LOSS, MANUAL, TRAILING_STOP, LIQUIDATION, SIGNAL |

#### 📊 Экстремальные значения

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **max_profit** | DECIMAL(20,8) | NULL | Максимальная нереализованная прибыль во время сделки |
| **max_profit_percent** | DECIMAL(10,4) | NULL | Максимальная прибыль в процентах |
| **max_drawdown** | DECIMAL(20,8) | NULL | Максимальная просадка в USDT |
| **max_drawdown_percent** | DECIMAL(10,4) | NULL | Максимальная просадка в процентах |

#### 🛡️ Риск-менеджмент

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **initial_stop_loss** | DECIMAL(20,8) | NULL | Изначальный уровень стоп-лосса |
| **initial_take_profit** | DECIMAL(20,8) | NULL | Изначальный уровень тейк-профита |
| **risk_reward_planned** | DECIMAL(5,2) | NULL | Планируемое соотношение риск/прибыль |
| **risk_reward_actual** | DECIMAL(5,2) | NULL | Фактическое соотношение риск/прибыль |

#### 📉 Trailing Stop данные

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **trailing_stop_enabled** | BOOLEAN | DEFAULT FALSE | Был ли активирован trailing stop |
| **trailing_stop_type** | VARCHAR(20) | NULL | Тип: FIXED, PERCENT, ATR, BREAK_EVEN |
| **trailing_stop_distance** | DECIMAL(20,8) | NULL | Дистанция trailing stop (в пунктах или %) |
| **trailing_stop_activated_price** | DECIMAL(20,8) | NULL | Цена при которой активировался trailing |
| **trailing_stop_activated_time** | TIMESTAMPTZ | NULL | Время активации trailing stop |
| **trailing_stop_trigger_price** | DECIMAL(20,8) | NULL | Цена срабатывания trailing stop |
| **trailing_stop_max_price** | DECIMAL(20,8) | NULL | Макс. цена при trailing (для LONG) |
| **trailing_stop_min_price** | DECIMAL(20,8) | NULL | Мин. цена при trailing (для SHORT) |
| **trailing_stop_updates_count** | INTEGER | DEFAULT 0 | Сколько раз обновлялся уровень trailing |

#### 🔗 Связи и метаданные

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **account_id** | VARCHAR(50) | NULL | ID торгового аккаунта или бота (BOT_RSI_01, ACCOUNT_MAIN, USER_JOHN) |
| **strategy_name** | VARCHAR(50) | NULL | Название торговой стратегии |
| **signal_id** | BIGINT | NULL | ID сигнала из таблицы trading_signals |
| **leverage** | INTEGER | DEFAULT 1 | Кредитное плечо (1 для spot, 2-125 для futures) |
| **order_ids** | TEXT[] | NULL | Массив ID ордеров на бирже |
| **notes** | TEXT | NULL | Произвольные заметки о сделке |
| **created_at** | TIMESTAMPTZ | DEFAULT NOW() | Время добавления записи в БД |

### Индексы таблицы trade_history

| Индекс | Поля | Назначение |
|--------|------|------------|
| PRIMARY KEY | id | Уникальный идентификатор |
| UNIQUE | custom_trade_id | Быстрый поиск по внутреннему ID |
| idx_trade_history_custom_id | custom_trade_id | Поиск по пользовательскому ID |
| idx_trade_history_exchange_id | exchange_trade_id | Поиск по ID биржи |
| idx_trade_history_symbol_close | symbol, close_time DESC | Анализ по символам |
| idx_trade_history_exchange | exchange_name | Фильтрация по бирже |
| idx_trade_history_win_loss | win_loss | Быстрая фильтрация WIN/LOSS |
| idx_trade_history_pnl | pnl | Сортировка по прибыли |
| idx_trade_history_close_reason | close_reason | Анализ причин закрытия |
| idx_trade_history_strategy | strategy_name | Анализ по стратегиям |
| idx_trade_history_market_type | market_type | Разделение SPOT/FUTURES |
| idx_trade_history_duration | duration_minutes | Анализ по длительности |
| idx_trade_history_risk_percent | risk_percent_of_capital | Анализ риск-менеджмента |
| idx_trade_history_break_even | break_even_time | WHERE break_even_time IS NOT NULL |
| idx_trade_history_balance | account_balance_before, account_balance_after | Анализ изменения баланса |
| idx_trade_history_trailing | trailing_stop_enabled | WHERE trailing_stop_enabled = TRUE |
| idx_trade_history_account_id | account_id | Фильтрация по аккаунту/боту |
| idx_trade_history_account_close | account_id, close_time DESC | Анализ по аккаунту и времени |

## SQL команды

### Добавление колонки account_id (НОВОЕ)

```sql
-- Подключиться к БД
\c trading_operations

-- Добавить колонку для идентификации аккаунта/бота
ALTER TABLE trade_history
ADD COLUMN account_id VARCHAR(50);

-- Добавить комментарий к колонке
COMMENT ON COLUMN trade_history.account_id IS 'ID или логин торгового аккаунта/бота (например: BOT_RSI_01, ACCOUNT_MAIN, USER_JOHN)';

-- Создать индексы для account_id
CREATE INDEX idx_trade_history_account_id ON trade_history(account_id);
CREATE INDEX idx_trade_history_account_close ON trade_history(account_id, close_time DESC);
```

### Создание БД и настройка прав

```sql
-- Создание БД (выполнять от имени postgres)
CREATE DATABASE trading_operations OWNER postgres;

-- Настройка прав доступа
GRANT ALL ON DATABASE trading_operations TO trading_admin;
GRANT CONNECT ON DATABASE trading_operations TO trading_writer;
GRANT CONNECT ON DATABASE trading_operations TO trading_reader;

-- Переключение на новую БД
\c trading_operations

-- Настройка прав на схему
GRANT CREATE ON SCHEMA public TO trading_writer;
GRANT USAGE ON SCHEMA public TO trading_reader;

-- Права по умолчанию для будущих таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO trading_reader;
```

### Создание таблицы trade_history

```sql
CREATE TABLE trade_history (
    id BIGSERIAL PRIMARY KEY,

    -- Идентификаторы сделки
    custom_trade_id VARCHAR(100) UNIQUE,
    exchange_trade_id VARCHAR(100),

    -- Базовая информация
    symbol VARCHAR(20) NOT NULL,
    exchange_name VARCHAR(20) NOT NULL,
    market_type VARCHAR(10) NOT NULL,
    side VARCHAR(10) NOT NULL,

    -- Время сделки
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    duration_minutes INTEGER,
    break_even_time TIMESTAMPTZ,

    -- Цены
    open_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    average_open_price DECIMAL(20,8),
    average_close_price DECIMAL(20,8),

    -- Объемы и размер позиции
    quantity DECIMAL(20,8) NOT NULL,
    position_value DECIMAL(20,8) NOT NULL,
    partial_closes INTEGER DEFAULT 0,
    close_type VARCHAR(20) NOT NULL,
    remaining_quantity DECIMAL(20,8) DEFAULT 0,

    -- Управление капиталом
    account_balance_before DECIMAL(20,8),
    account_balance_after DECIMAL(20,8),
    risk_percent_of_capital DECIMAL(5,2),

    -- Финансовый результат
    pnl DECIMAL(20,8) NOT NULL,
    pnl_percent DECIMAL(10,4) NOT NULL,
    gross_profit DECIMAL(20,8),
    win_loss VARCHAR(10) NOT NULL,

    -- Комиссии
    open_fee DECIMAL(20,8) DEFAULT 0,
    close_fee DECIMAL(20,8) DEFAULT 0,
    funding_fee DECIMAL(20,8) DEFAULT 0,
    total_fees DECIMAL(20,8) DEFAULT 0,
    net_profit DECIMAL(20,8),

    -- Причина закрытия
    close_reason VARCHAR(50),

    -- Дополнительные метрики
    max_profit DECIMAL(20,8),
    max_profit_percent DECIMAL(10,4),
    max_drawdown DECIMAL(20,8),
    max_drawdown_percent DECIMAL(10,4),

    -- Риск-менеджмент
    initial_stop_loss DECIMAL(20,8),
    initial_take_profit DECIMAL(20,8),
    risk_reward_planned DECIMAL(5,2),
    risk_reward_actual DECIMAL(5,2),

    -- TRAILING STOP ДАННЫЕ
    trailing_stop_enabled BOOLEAN DEFAULT FALSE,
    trailing_stop_type VARCHAR(20),
    trailing_stop_distance DECIMAL(20,8),
    trailing_stop_activated_price DECIMAL(20,8),
    trailing_stop_activated_time TIMESTAMPTZ,
    trailing_stop_trigger_price DECIMAL(20,8),
    trailing_stop_max_price DECIMAL(20,8),
    trailing_stop_min_price DECIMAL(20,8),
    trailing_stop_updates_count INTEGER DEFAULT 0,

    -- Связи
    account_id VARCHAR(50),
    strategy_name VARCHAR(50),
    signal_id BIGINT,

    -- Метаданные
    leverage INTEGER DEFAULT 1,
    order_ids TEXT[],
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Создание индексов

```sql
-- Индексы для оптимизации поиска
CREATE INDEX idx_trade_history_custom_id ON trade_history(custom_trade_id);
CREATE INDEX idx_trade_history_exchange_id ON trade_history(exchange_trade_id);
CREATE INDEX idx_trade_history_symbol_close ON trade_history(symbol, close_time DESC);
CREATE INDEX idx_trade_history_exchange ON trade_history(exchange_name);
CREATE INDEX idx_trade_history_win_loss ON trade_history(win_loss);
CREATE INDEX idx_trade_history_pnl ON trade_history(pnl);
CREATE INDEX idx_trade_history_close_reason ON trade_history(close_reason);
CREATE INDEX idx_trade_history_strategy ON trade_history(strategy_name);
CREATE INDEX idx_trade_history_market_type ON trade_history(market_type);
CREATE INDEX idx_trade_history_duration ON trade_history(duration_minutes);
CREATE INDEX idx_trade_history_risk_percent ON trade_history(risk_percent_of_capital);
CREATE INDEX idx_trade_history_break_even ON trade_history(break_even_time)
    WHERE break_even_time IS NOT NULL;
CREATE INDEX idx_trade_history_balance ON trade_history(account_balance_before, account_balance_after);
CREATE INDEX idx_trade_history_trailing ON trade_history(trailing_stop_enabled)
    WHERE trailing_stop_enabled = TRUE;
CREATE INDEX idx_trade_history_account_id ON trade_history(account_id);
CREATE INDEX idx_trade_history_account_close ON trade_history(account_id, close_time DESC);
```

### Настройка прав на таблицу

```sql
-- Права для таблицы trade_history
GRANT ALL ON trade_history TO trading_admin;
GRANT ALL ON trade_history TO trading_writer;
GRANT SELECT ON trade_history TO trading_reader;

-- Права на sequence
GRANT ALL ON SEQUENCE trade_history_id_seq TO trading_admin;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE trade_history_id_seq TO trading_writer;
```

## Примеры запросов

### Общая статистика

```sql
-- Общая статистика по всем сделкам
SELECT
  COUNT(*) as total_trades,
  COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) as wins,
  COUNT(CASE WHEN win_loss = 'LOSS' THEN 1 END) as losses,
  ROUND(100.0 * COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as win_rate,
  SUM(net_profit) as total_pnl,
  AVG(pnl_percent) as avg_pnl_percent,
  AVG(duration_minutes) as avg_duration_minutes
FROM trade_history;
```

### Анализ по стратегиям

```sql
-- Производительность каждой стратегии
SELECT
  strategy_name,
  COUNT(*) as trades,
  COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) as wins,
  ROUND(100.0 * COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) / COUNT(*), 2) as win_rate,
  AVG(pnl_percent) as avg_pnl_percent,
  SUM(net_profit) as total_profit,
  MAX(net_profit) as best_trade,
  MIN(net_profit) as worst_trade
FROM trade_history
WHERE strategy_name IS NOT NULL
GROUP BY strategy_name
ORDER BY total_profit DESC;
```

### Анализ по символам

```sql
-- Статистика по торговым парам
SELECT
  symbol,
  COUNT(*) as trades,
  SUM(net_profit) as total_profit,
  AVG(pnl_percent) as avg_pnl_percent,
  COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) as wins,
  COUNT(CASE WHEN win_loss = 'LOSS' THEN 1 END) as losses
FROM trade_history
GROUP BY symbol
ORDER BY total_profit DESC;
```

### Сделки с trailing stop

```sql
-- Анализ эффективности trailing stop
SELECT
  symbol,
  close_time,
  trailing_stop_type,
  trailing_stop_distance,
  trailing_stop_trigger_price,
  pnl,
  pnl_percent,
  win_loss
FROM trade_history
WHERE trailing_stop_enabled = TRUE
ORDER BY close_time DESC;
```

### Кумулятивная прибыль

```sql
-- График накопленной прибыли по дням
SELECT
  DATE(close_time) as trading_date,
  COUNT(*) as daily_trades,
  SUM(net_profit) as daily_pnl,
  SUM(SUM(net_profit)) OVER (ORDER BY DATE(close_time)) as cumulative_pnl
FROM trade_history
GROUP BY DATE(close_time)
ORDER BY trading_date;
```

### Анализ риск-менеджмента

```sql
-- Проверка соблюдения правил риск-менеджмента
SELECT
  DATE(open_time) as date,
  AVG(risk_percent_of_capital) as avg_risk_percent,
  MAX(risk_percent_of_capital) as max_risk_percent,
  COUNT(CASE WHEN risk_percent_of_capital > 2 THEN 1 END) as high_risk_trades
FROM trade_history
WHERE risk_percent_of_capital IS NOT NULL
GROUP BY DATE(open_time)
ORDER BY date DESC;
```

### Топ прибыльных и убыточных сделок

```sql
-- Топ-10 лучших сделок
SELECT
  custom_trade_id,
  symbol,
  open_time,
  close_time,
  net_profit,
  pnl_percent,
  strategy_name
FROM trade_history
ORDER BY net_profit DESC
LIMIT 10;

-- Топ-10 худших сделок
SELECT
  custom_trade_id,
  symbol,
  open_time,
  close_time,
  net_profit,
  pnl_percent,
  close_reason
FROM trade_history
ORDER BY net_profit ASC
LIMIT 10;
```

### Анализ по аккаунтам/ботам

```sql
-- Статистика по каждому аккаунту/боту
SELECT
  account_id,
  COUNT(*) as total_trades,
  SUM(net_profit) as total_profit,
  AVG(pnl_percent) as avg_pnl_percent,
  COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) as wins,
  ROUND(100.0 * COUNT(CASE WHEN win_loss = 'WIN' THEN 1 END) / COUNT(*), 2) as win_rate
FROM trade_history
WHERE account_id IS NOT NULL
GROUP BY account_id
ORDER BY total_profit DESC;

-- Сравнение ботов по стратегиям
SELECT
  account_id,
  strategy_name,
  COUNT(*) as trades,
  SUM(net_profit) as profit,
  AVG(risk_percent_of_capital) as avg_risk
FROM trade_history
WHERE account_id LIKE 'BOT_%'
GROUP BY account_id, strategy_name
ORDER BY account_id, profit DESC;

-- Активность ботов по времени
SELECT
  account_id,
  DATE(close_time) as trading_date,
  COUNT(*) as daily_trades,
  SUM(net_profit) as daily_profit
FROM trade_history
WHERE account_id IS NOT NULL
GROUP BY account_id, DATE(close_time)
ORDER BY account_id, trading_date DESC;
```

## Подключение к Metabase

### Настройки подключения

```
Database type: PostgreSQL
Display name: Trading Operations
Host: localhost (или 172.17.0.1 для Docker)
Port: 5432
Database name: trading_operations
Username: trading_reader
Password: [см. конфигурационные файлы]
```

### Дашборды для Metabase

1. **Overview Dashboard** - общая статистика
2. **Strategy Performance** - анализ по стратегиям
3. **Risk Management** - контроль рисков
4. **P&L Timeline** - график прибыли во времени
5. **Symbol Analysis** - анализ по торговым парам
6. **Bot/Account Comparison** - сравнение производительности ботов и аккаунтов

## Связанные таблицы

### trading_signals ✅
- Хранение торговых сигналов для открытия/закрытия позиций
- Связь с trade_history через signal_id
- Документация: [TRADING_SIGNALS_DOCUMENTATION.md](TRADING_SIGNALS_DOCUMENTATION.md)

### open_positions ✅
- Текущие открытые торговые позиции
- Trailing stop, частичное закрытие, связь с сигналами
- Документация: [TRADING_OPEN_POSITIONS_DOCUMENTATION.md](TRADING_OPEN_POSITIONS_DOCUMENTATION.md)

### portfolio_balance (планируется)
- История баланса портфеля
- Метрики производительности

## Обслуживание БД

### Резервное копирование

```bash
# Бэкап БД
pg_dump -U trading_admin -h 82.25.115.144 -d trading_operations > trading_operations_backup.sql

# Восстановление
psql -U trading_admin -h 82.25.115.144 -d trading_operations < trading_operations_backup.sql
```

### Очистка и оптимизация

```sql
-- Анализ размера таблицы
SELECT
  pg_size_pretty(pg_total_relation_size('trade_history')) as total_size,
  pg_size_pretty(pg_relation_size('trade_history')) as table_size,
  pg_size_pretty(pg_indexes_size('trade_history')) as indexes_size;

-- Очистка и оптимизация
VACUUM ANALYZE trade_history;
```

## Контакты и поддержка

- **Проект**: TradingChart
- **Дата создания документации**: 2025-09-22
- **Последнее обновление**: 2026-02-25 (актуализация структуры БД)
- **Версия**: 1.2