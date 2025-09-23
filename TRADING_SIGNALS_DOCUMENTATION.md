# Документация таблицы trading_signals

## Общая информация

- **База данных**: `trading_operations`
- **Таблица**: `trading_signals`
- **Назначение**: Хранение торговых сигналов для открытия/закрытия позиций на бирже
- **Дата создания**: 2025-01-15
- **Версия**: 1.0

## Описание

Таблица `trading_signals` предназначена для хранения всех торговых сигналов, генерируемых различными ботами и стратегиями. Каждый сигнал содержит полную информацию, необходимую для открытия или закрытия позиции на бирже, включая:
- Параметры входа и выхода
- Оценку качества сигнала (score)
- Настройки риск-менеджмента
- Параметры trailing stop
- ID ордеров после исполнения

## Структура таблицы

### 🔑 Идентификация

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **id** | BIGSERIAL | PRIMARY KEY | Автоинкрементный уникальный идентификатор |
| **signal_uuid** | VARCHAR(100) | UNIQUE | Уникальный бизнес-идентификатор сигнала (например: SIG_BTC_2025_001) |
| **account_id** | VARCHAR(50) | NULL | ID бота или торгового аккаунта (BOT_RSI_01, ACCOUNT_MAIN) |

### ⏰ Временные метки (UTC)

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **created_at_utc** | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Время создания сигнала в UTC |
| **updated_at_utc** | TIMESTAMPTZ | NULL | Время последнего обновления (автообновление через триггер) |
| **valid_from_utc** | TIMESTAMPTZ | DEFAULT NOW() | Начало периода действия сигнала |
| **valid_until_utc** | TIMESTAMPTZ | NULL | Окончание периода действия сигнала |
| **ttl_seconds** | INTEGER | NULL | Time-to-live в секундах (альтернатива valid_until_utc) |
| **executed_at_utc** | TIMESTAMPTZ | NULL | Фактическое время исполнения сигнала |

### 📊 Основные параметры

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **status** | VARCHAR(20) | NOT NULL, DEFAULT 'pending' | Статус сигнала (см. ниже) |
| **symbol** | VARCHAR(20) | NOT NULL | Торговая пара (BTCUSDT, ETHUSDT) |
| **exchange_name** | VARCHAR(20) | NULL | Название биржи (BYBIT, BINANCE, OKX) |
| **market_type** | VARCHAR(10) | NULL | Тип рынка: SPOT или FUTURES |
| **signal_type** | VARCHAR(20) | NOT NULL | Тип сигнала (см. ниже) |
| **timeframe** | VARCHAR(10) | NULL | Таймфрейм анализа (1m, 5m, 15m, 1h, 4h, 1d) |

#### Возможные значения status:
- `pending` - Ожидает исполнения
- `active` - Активен и готов к исполнению
- `executed` - Успешно исполнен
- `cancelled` - Отменен вручную
- `expired` - Истек срок действия
- `failed` - Ошибка при исполнении

#### Возможные значения signal_type:
- `OPEN_LONG` - Открыть длинную позицию
- `OPEN_SHORT` - Открыть короткую позицию
- `CLOSE_LONG` - Закрыть длинную позицию
- `CLOSE_SHORT` - Закрыть короткую позицию
- `CLOSE_ALL` - Закрыть все позиции

### 📈 Оценка качества сигнала

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **score** | DECIMAL(5,2) | NULL | Оценка силы сигнала от 0 до 100 |
| **confidence_level** | VARCHAR(10) | NULL | Уровень уверенности: LOW, MEDIUM, HIGH, VERY_HIGH |
| **priority** | INTEGER | DEFAULT 5 | Приоритет исполнения от 1 до 10 (10 - максимальный) |

### 💰 Ценовые уровни

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **current_price** | DECIMAL(20,8) | NOT NULL | Текущая цена актива при генерации сигнала |
| **entry_price_min** | DECIMAL(20,8) | NULL | Минимальная приемлемая цена входа |
| **entry_price_max** | DECIMAL(20,8) | NULL | Максимальная приемлемая цена входа |
| **entry_price_optimal** | DECIMAL(20,8) | NULL | Оптимальная цена входа |
| **stop_loss** | DECIMAL(20,8) | NULL | Уровень стоп-лосса |
| **take_profit_1** | DECIMAL(20,8) | NULL | Первый уровень тейк-профита |
| **take_profit_2** | DECIMAL(20,8) | NULL | Второй уровень тейк-профита |
| **take_profit_3** | DECIMAL(20,8) | NULL | Третий уровень тейк-профита |

### 📉 Trailing Stop

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **trailing_stop_enabled** | BOOLEAN | DEFAULT FALSE | Включен ли trailing stop |
| **trailing_stop_type** | VARCHAR(20) | NULL | Тип: FIXED, PERCENT, ATR, BREAK_EVEN |
| **trailing_stop_activation_price** | DECIMAL(20,8) | NULL | Цена активации trailing stop |
| **trailing_stop_distance** | DECIMAL(20,8) | NULL | Дистанция в пунктах или процентах |
| **trailing_stop_step** | DECIMAL(20,8) | NULL | Минимальный шаг изменения trailing stop |

### 📦 Параметры позиции

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **suggested_quantity** | DECIMAL(20,8) | NULL | Рекомендуемый объем позиции |
| **suggested_leverage** | INTEGER | DEFAULT 1 | Рекомендуемое кредитное плечо |
| **position_size_usdt** | DECIMAL(20,8) | NULL | Размер позиции в USDT |
| **risk_amount** | DECIMAL(20,8) | NULL | Абсолютная сумма риска в USDT |
| **risk_percent** | DECIMAL(5,2) | NULL | Процент риска от капитала |
| **risk_reward_ratio** | DECIMAL(5,2) | NULL | Соотношение риск/прибыль |

### 🔗 Источники и стратегия

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **strategy_name** | VARCHAR(50) | NULL | Название стратегии, создавшей сигнал |
| **signal_source** | VARCHAR(100) | NULL | Источник сигнала (индикатор, паттерн и т.д.) |
| **indicators_data** | JSONB | NULL | JSON с данными индикаторов при генерации |
| **market_conditions** | JSONB | NULL | JSON с рыночными условиями |

#### Пример indicators_data:
```json
{
  "rsi": 32.5,
  "macd": {
    "value": 0.5,
    "signal": 0.3,
    "histogram": 0.2
  },
  "ema50": 49500,
  "ema200": 48000,
  "volume_spike": true
}
```

#### Пример market_conditions:
```json
{
  "trend": "uptrend",
  "volatility": "high",
  "volume": "increasing",
  "market_phase": "accumulation"
}
```

### 🏷️ ID ордеров на бирже

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **order_id** | VARCHAR(100) | NULL | ID основного ордера на бирже |
| **position_id** | VARCHAR(100) | NULL | ID открытой позиции на бирже |
| **parent_order_id** | VARCHAR(100) | NULL | ID родительского ордера (для связанных) |
| **sl_order_id** | VARCHAR(100) | NULL | ID ордера стоп-лосса |
| **tp_order_ids** | TEXT[] | NULL | Массив ID ордеров тейк-профита |

### ✅ Результаты исполнения

| Поле | Тип | Обязательное | Описание |
|------|-----|--------------|----------|
| **execution_price** | DECIMAL(20,8) | NULL | Фактическая цена исполнения |
| **execution_quantity** | DECIMAL(20,8) | NULL | Фактический исполненный объем |
| **execution_result** | VARCHAR(20) | NULL | Результат: full, partial, failed |
| **signal_reason** | TEXT | NULL | Подробное описание причины сигнала |
| **cancel_reason** | VARCHAR(100) | NULL | Причина отмены сигнала |
| **error_message** | TEXT | NULL | Сообщение об ошибке при исполнении |
| **notes** | TEXT | NULL | Дополнительные заметки |

## Индексы

| Название индекса | Поля | Условие | Назначение |
|-----------------|------|---------|------------|
| **PRIMARY KEY** | id | - | Первичный ключ |
| **UNIQUE** | signal_uuid | - | Уникальность бизнес-идентификатора |
| **idx_signals_status** | status | WHERE status IN ('pending', 'active') | Быстрый поиск активных сигналов |
| **idx_signals_symbol_created** | symbol, created_at_utc DESC | - | Поиск по символу и времени |
| **idx_signals_account** | account_id | - | Фильтрация по боту/аккаунту |
| **idx_signals_strategy** | strategy_name | - | Анализ по стратегиям |
| **idx_signals_score** | score DESC | - | Поиск сигналов с высоким score |
| **idx_signals_valid_until** | valid_until_utc | WHERE status = 'active' | Поиск активных неистекших сигналов |
| **idx_signals_executed** | executed_at_utc DESC | WHERE executed_at_utc IS NOT NULL | История исполненных сигналов |
| **idx_signals_order_id** | order_id | - | Поиск по ID ордера |
| **idx_signals_position_id** | position_id | - | Поиск по ID позиции |
| **idx_signals_trailing** | trailing_stop_enabled | WHERE trailing_stop_enabled = TRUE | Сигналы с trailing stop |
| **idx_signals_active_symbol** | symbol, status, created_at_utc DESC | WHERE status IN ('pending', 'active') | Комплексный поиск активных |

## Триггеры

### update_trading_signals_updated_at

**Назначение**: Автоматическое обновление поля `updated_at_utc` при любом изменении записи.

**Функция**:
```sql
CREATE OR REPLACE FUNCTION update_updated_at_utc()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at_utc = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Триггер**:
```sql
CREATE TRIGGER update_trading_signals_updated_at
BEFORE UPDATE ON trading_signals
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_utc();
```

## Проверки (CHECK constraints)

1. **score**: Значение от 0 до 100
2. **status**: Только разрешенные значения (pending, active, executed, cancelled, expired, failed)
3. **signal_type**: Только разрешенные типы сигналов
4. **confidence_level**: LOW, MEDIUM, HIGH, VERY_HIGH или NULL
5. **trailing_stop_type**: FIXED, PERCENT, ATR, BREAK_EVEN или NULL
6. **market_type**: SPOT, FUTURES или NULL
7. **execution_result**: full, partial, failed или NULL

## Примеры использования

### 1. Создание нового сигнала

```sql
INSERT INTO trading_signals (
    signal_uuid,
    account_id,
    symbol,
    exchange_name,
    market_type,
    signal_type,
    score,
    confidence_level,
    current_price,
    entry_price_optimal,
    stop_loss,
    take_profit_1,
    take_profit_2,
    suggested_quantity,
    risk_percent,
    risk_reward_ratio,
    strategy_name,
    indicators_data,
    valid_until_utc
) VALUES (
    'SIG_BTC_2025_001',
    'BOT_RSI_STRATEGY',
    'BTCUSDT',
    'BYBIT',
    'FUTURES',
    'OPEN_LONG',
    85.5,
    'HIGH',
    50000.00,
    49950.00,
    49000.00,
    51000.00,
    52000.00,
    0.1,
    2.0,
    3.5,
    'RSI_OVERSOLD_STRATEGY',
    '{"rsi": 28.5, "rsi_prev": 32.1, "volume_ratio": 1.8}'::jsonb,
    NOW() + INTERVAL '2 hours'
);
```

### 2. Поиск активных сигналов с высоким score

```sql
SELECT
    signal_uuid,
    symbol,
    signal_type,
    score,
    entry_price_optimal,
    stop_loss,
    take_profit_1,
    EXTRACT(EPOCH FROM (valid_until_utc - NOW()))/60 as minutes_remaining
FROM trading_signals
WHERE status = 'active'
    AND score > 70
    AND valid_until_utc > NOW()
ORDER BY score DESC, priority DESC;
```

### 3. Обновление статуса после исполнения

```sql
UPDATE trading_signals
SET
    status = 'executed',
    executed_at_utc = NOW(),
    execution_price = 49945.50,
    execution_quantity = 0.098,
    execution_result = 'partial',
    order_id = 'BYBIT_ORDER_123456789',
    position_id = 'BYBIT_POS_987654321',
    sl_order_id = 'BYBIT_SL_111111',
    tp_order_ids = ARRAY['BYBIT_TP_222222', 'BYBIT_TP_333333']
WHERE signal_uuid = 'SIG_BTC_2025_001'
    AND status = 'active';
```

### 4. Отмена устаревших сигналов

```sql
UPDATE trading_signals
SET
    status = 'expired',
    cancel_reason = 'TTL expired'
WHERE status IN ('pending', 'active')
    AND valid_until_utc < NOW();
```

### 5. Статистика по стратегиям

```sql
SELECT
    strategy_name,
    COUNT(*) as total_signals,
    COUNT(CASE WHEN status = 'executed' THEN 1 END) as executed_signals,
    AVG(score) as avg_score,
    COUNT(CASE WHEN score > 80 THEN 1 END) as high_score_signals,
    ROUND(100.0 * COUNT(CASE WHEN status = 'executed' THEN 1 END) /
          NULLIF(COUNT(*), 0), 2) as execution_rate
FROM trading_signals
WHERE created_at_utc > NOW() - INTERVAL '7 days'
GROUP BY strategy_name
ORDER BY total_signals DESC;
```

### 6. Поиск сигналов с trailing stop

```sql
SELECT
    signal_uuid,
    symbol,
    trailing_stop_type,
    trailing_stop_activation_price,
    trailing_stop_distance,
    current_price,
    (trailing_stop_activation_price - current_price) as distance_to_activation
FROM trading_signals
WHERE trailing_stop_enabled = TRUE
    AND status = 'active'
ORDER BY created_at_utc DESC;
```

### 7. Анализ времени жизни сигналов

```sql
SELECT
    DATE(created_at_utc) as date,
    AVG(EXTRACT(EPOCH FROM (executed_at_utc - created_at_utc))/60) as avg_minutes_to_execution,
    MIN(EXTRACT(EPOCH FROM (executed_at_utc - created_at_utc))/60) as min_minutes,
    MAX(EXTRACT(EPOCH FROM (executed_at_utc - created_at_utc))/60) as max_minutes,
    COUNT(*) as signals_count
FROM trading_signals
WHERE status = 'executed'
    AND executed_at_utc IS NOT NULL
GROUP BY DATE(created_at_utc)
ORDER BY date DESC
LIMIT 30;
```

## Технические рекомендации

### 1. Управление временем жизни сигналов

Используйте один из двух подходов:
- **valid_until_utc** - для абсолютного времени окончания
- **ttl_seconds** - для относительного времени от создания

```sql
-- Вариант 1: Абсолютное время
INSERT INTO trading_signals (valid_until_utc, ...)
VALUES (NOW() + INTERVAL '2 hours', ...);

-- Вариант 2: TTL
INSERT INTO trading_signals (ttl_seconds, ...)
VALUES (7200, ...);

-- Проверка истекших по TTL
SELECT * FROM trading_signals
WHERE status = 'active'
    AND created_at_utc + (ttl_seconds * INTERVAL '1 second') < NOW();
```

### 2. Работа с JSONB полями

```sql
-- Поиск по значению в JSON
SELECT * FROM trading_signals
WHERE (indicators_data->>'rsi')::numeric < 30;

-- Обновление JSON поля
UPDATE trading_signals
SET indicators_data = indicators_data || '{"macd_cross": true}'::jsonb
WHERE signal_uuid = 'SIG_001';

-- Извлечение вложенных данных
SELECT
    signal_uuid,
    indicators_data->'macd'->>'value' as macd_value,
    indicators_data->'macd'->>'signal' as macd_signal
FROM trading_signals;
```

### 3. Использование score для приоритизации

```sql
-- Формула комплексной оценки
SELECT
    signal_uuid,
    symbol,
    score,
    priority,
    (score * 0.7 + priority * 3) as weighted_score
FROM trading_signals
WHERE status = 'active'
ORDER BY weighted_score DESC;
```

### 4. Очистка старых данных

```sql
-- Архивация старых сигналов
INSERT INTO trading_signals_archive
SELECT * FROM trading_signals
WHERE created_at_utc < NOW() - INTERVAL '30 days';

-- Удаление после архивации
DELETE FROM trading_signals
WHERE created_at_utc < NOW() - INTERVAL '30 days'
    AND status IN ('executed', 'cancelled', 'expired');
```

### 5. Предотвращение дублей

```sql
-- Проверка перед вставкой
INSERT INTO trading_signals (signal_uuid, symbol, signal_type, ...)
SELECT 'NEW_SIGNAL_001', 'BTCUSDT', 'OPEN_LONG', ...
WHERE NOT EXISTS (
    SELECT 1 FROM trading_signals
    WHERE symbol = 'BTCUSDT'
        AND signal_type = 'OPEN_LONG'
        AND status IN ('pending', 'active')
        AND created_at_utc > NOW() - INTERVAL '5 minutes'
);
```

### 6. Мониторинг производительности

```sql
-- Размер таблицы
SELECT
    pg_size_pretty(pg_total_relation_size('trading_signals')) as total_size,
    pg_size_pretty(pg_relation_size('trading_signals')) as table_size,
    pg_size_pretty(pg_indexes_size('trading_signals')) as indexes_size,
    (SELECT COUNT(*) FROM trading_signals) as row_count;

-- Анализ использования индексов
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = 'trading_signals'
ORDER BY idx_scan DESC;
```

## Связи с другими таблицами

### trade_history
После исполнения сигнала и закрытия позиции, в таблице `trade_history` создается запись с:
- `signal_id` - ссылка на `trading_signals.id`
- Те же `order_id` и `position_id` для сопоставления

### Схема связей:
```
trading_signals (генерация)
    ↓
    → Исполнение на бирже
    → Получение order_id, position_id
    → UPDATE trading_signals SET order_id = ...
    ↓
trade_history (после закрытия)
    → signal_id = trading_signals.id
    → Полная история сделки
```

## Права доступа

| Пользователь | Права | Использование |
|--------------|-------|--------------|
| trading_admin | ALL | Полное управление |
| trading_writer | ALL | Боты записывают сигналы |
| trading_reader | SELECT | Аналитика, Metabase |

## Обслуживание

### Регулярные задачи:

1. **Каждые 5 минут**: Отметить expired сигналы
```sql
UPDATE trading_signals
SET status = 'expired'
WHERE status IN ('pending', 'active')
    AND valid_until_utc < NOW();
```

2. **Каждый час**: Проверка зависших сигналов
```sql
SELECT * FROM trading_signals
WHERE status = 'active'
    AND updated_at_utc < NOW() - INTERVAL '1 hour';
```

3. **Ежедневно**: Очистка старых canceled/expired
```sql
DELETE FROM trading_signals
WHERE status IN ('cancelled', 'expired')
    AND created_at_utc < NOW() - INTERVAL '7 days';
```

4. **Еженедельно**: VACUUM и анализ
```sql
VACUUM ANALYZE trading_signals;
```

## Версионность

- **v1.0** (2025-01-15): Первоначальная версия с полями для score, trailing stop, UTC timestamps

## Контакты

- **Проект**: TradingChart
- **База данных**: trading_operations
- **Последнее обновление документации**: 2025-01-15