# Документация таблицы `open_positions`

## Обзор

Таблица `open_positions` хранит информацию о текущих открытых торговых позициях. Она содержит статические данные о позиции (цены входа, объемы, уровни риск-менеджмента), в то время как real-time данные (текущая цена, P&L) отслеживаются самим торговым ботом.

## Структура базы данных

- **Сервер**: 82.25.115.144 (VPS)
- **Порт**: 5432
- **База данных**: trading_operations
- **Схема**: public
- **Таблица**: open_positions

## Структура таблицы

### Идентификаторы и метаданные

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `id` | BIGSERIAL | PRIMARY KEY | Уникальный автоинкрементный идентификатор |
| `position_uuid` | VARCHAR(100) | UNIQUE NOT NULL | Уникальный UUID позиции для внешней идентификации |
| `account_id` | VARCHAR(50) | NOT NULL | ID торгового аккаунта/бота |
| `signal_id` | BIGINT | FOREIGN KEY → trading_signals(id) | Связь с исходным торговым сигналом |

### Временные метки

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `created_at_utc` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Время создания записи в БД (UTC) |
| `updated_at_utc` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Время последнего обновления записи (UTC) |
| `opened_at_utc` | TIMESTAMPTZ | NOT NULL | Фактическое время открытия позиции на бирже (UTC) |

### Информация о позиции

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `symbol` | VARCHAR(20) | NOT NULL | Торговая пара (например, BTCUSDT) |
| `side` | VARCHAR(10) | NOT NULL, CHECK IN ('buy', 'sell') | Направление позиции |
| `status` | VARCHAR(20) | NOT NULL DEFAULT 'open' | Статус: open, partially_closed, closing |

### Параметры входа

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `entry_price` | DECIMAL(20,8) | NOT NULL | Средняя цена входа в позицию |
| `quantity` | DECIMAL(20,8) | NOT NULL | Начальный объем позиции |
| `leverage` | INTEGER | DEFAULT 1 | Кредитное плечо |
| `margin_mode` | VARCHAR(20) | DEFAULT 'isolated' | Режим маржи: isolated или cross |

### Управление рисками

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `stop_loss_price` | DECIMAL(20,8) | NULL | Цена стоп-лосса |
| `take_profit_price` | DECIMAL(20,8) | NULL | Цена тейк-профита |

### Trailing Stop параметры

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `trailing_stop_enabled` | BOOLEAN | DEFAULT FALSE | Включен ли trailing stop |
| `trailing_stop_percent` | DECIMAL(5,2) | NULL | Процент для trailing stop (например, 2.5 для 2.5%) |
| `trailing_stop_activation_price` | DECIMAL(20,8) | NULL | Цена активации trailing stop |
| `trailing_stop_current_price` | DECIMAL(20,8) | NULL | Текущий уровень trailing stop |
| `trailing_stop_triggered_at_utc` | TIMESTAMPTZ | NULL | Время срабатывания trailing stop (UTC) |

### Идентификаторы ордеров

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `entry_order_id` | VARCHAR(100) | NULL | ID ордера входа на бирже |
| `stop_loss_order_id` | VARCHAR(100) | NULL | ID стоп-лосс ордера на бирже |
| `take_profit_order_id` | VARCHAR(100) | NULL | ID тейк-профит ордера на бирже |

### Частичное закрытие

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `partial_closes` | INTEGER | DEFAULT 0 | Количество частичных закрытий |
| `remaining_quantity` | DECIMAL(20,8) | NULL | Оставшийся объем позиции |
| `total_closed_quantity` | DECIMAL(20,8) | DEFAULT 0 | Суммарный закрытый объем |

### Финансовые показатели

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `initial_margin` | DECIMAL(20,8) | NULL | Начальная маржа |
| `commission_paid` | DECIMAL(20,8) | DEFAULT 0 | Уплаченная комиссия |
| `funding_paid` | DECIMAL(20,8) | DEFAULT 0 | Уплаченный funding |

### Закрытие позиции

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `close_reason` | VARCHAR(50) | NULL | Причина закрытия (stop_loss, take_profit, manual, liquidation) |

### Дополнительные данные

| Поле | Тип | Ограничения | Описание |
|------|-----|-------------|----------|
| `tags` | TEXT[] | NULL | Массив тегов для категоризации |
| `metadata` | JSONB | NULL | Дополнительные данные в JSON формате |

## Индексы

| Индекс | Поля | Назначение |
|--------|------|------------|
| `PRIMARY KEY` | id | Первичный ключ |
| `UNIQUE` | position_uuid | Уникальность UUID |
| `idx_open_positions_account_id` | account_id | Поиск по аккаунту |
| `idx_open_positions_symbol` | symbol | Поиск по символу |
| `idx_open_positions_status` | status | Фильтрация по статусу |
| `idx_open_positions_opened_at` | opened_at_utc | Сортировка по времени |
| `idx_open_positions_signal_id` | signal_id | Связь с сигналами |
| `idx_open_positions_account_symbol` | account_id, symbol | Составной индекс |
| `idx_open_positions_account_status` | account_id, status | Составной индекс |

## Триггеры

### update_open_positions_updated_at
- **Тип**: BEFORE UPDATE
- **Функция**: update_updated_at_column()
- **Назначение**: Автоматическое обновление поля updated_at_utc при любом изменении записи

## Права доступа

| Пользователь | Права | Описание |
|--------------|-------|----------|
| `postgres` | OWNER | Полный доступ (суперпользователь) |
| `trading_admin` | ALL PRIVILEGES | Полный доступ для администрирования |
| `trading_writer` | SELECT, INSERT, UPDATE, DELETE | Доступ для записи данных |
| `trading_reader` | SELECT | Только чтение для аналитики |

## Примеры SQL запросов

### Открытие новой позиции

```sql
INSERT INTO open_positions (
    position_uuid,
    account_id,
    signal_id,
    opened_at_utc,
    symbol,
    side,
    entry_price,
    quantity,
    leverage,
    margin_mode,
    stop_loss_price,
    take_profit_price,
    entry_order_id,
    initial_margin,
    remaining_quantity,
    metadata
) VALUES (
    'pos-2024-01-15-001',
    'bot-001',
    123,
    '2024-01-15 10:30:00+00',
    'BTCUSDT',
    'buy',
    95000.50,
    0.01,
    10,
    'isolated',
    94000.00,
    97000.00,
    'bybit-order-12345',
    95.00,
    0.01,
    '{"strategy": "MA_crossover", "timeframe": "1h"}'::jsonb
) RETURNING id, position_uuid;
```

### Получение всех открытых позиций для аккаунта

```sql
SELECT
    position_uuid,
    symbol,
    side,
    entry_price,
    remaining_quantity,
    stop_loss_price,
    take_profit_price,
    opened_at_utc
FROM open_positions
WHERE account_id = 'bot-001'
    AND status = 'open'
ORDER BY opened_at_utc DESC;
```

### Обновление trailing stop

```sql
UPDATE open_positions
SET
    trailing_stop_enabled = true,
    trailing_stop_percent = 2.5,
    trailing_stop_activation_price = 96000.00,
    trailing_stop_current_price = 95500.00
WHERE position_uuid = 'pos-2024-01-15-001';
```

### Частичное закрытие позиции

```sql
UPDATE open_positions
SET
    status = 'partially_closed',
    partial_closes = partial_closes + 1,
    remaining_quantity = 0.005,
    total_closed_quantity = total_closed_quantity + 0.005
WHERE position_uuid = 'pos-2024-01-15-001';
```

### Полное закрытие позиции

```sql
-- Обновляем статус в open_positions
UPDATE open_positions
SET
    status = 'closing',
    close_reason = 'take_profit',
    remaining_quantity = 0,
    total_closed_quantity = quantity
WHERE position_uuid = 'pos-2024-01-15-001';

-- После закрытия можно удалить запись или оставить для истории
-- Рекомендуется переносить данные в trade_history
```

### Анализ открытых позиций

```sql
-- Сводка по открытым позициям
SELECT
    account_id,
    COUNT(*) as open_positions,
    COUNT(DISTINCT symbol) as unique_symbols,
    SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) as long_positions,
    SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) as short_positions,
    SUM(initial_margin) as total_margin
FROM open_positions
WHERE status = 'open'
GROUP BY account_id;

-- Позиции с активным trailing stop
SELECT
    position_uuid,
    symbol,
    side,
    entry_price,
    trailing_stop_current_price,
    trailing_stop_percent,
    (trailing_stop_current_price - entry_price) / entry_price * 100 as distance_percent
FROM open_positions
WHERE trailing_stop_enabled = true
    AND status = 'open'
ORDER BY opened_at_utc DESC;
```

### Поиск позиций требующих внимания

```sql
-- Позиции без стоп-лосса
SELECT position_uuid, symbol, side, entry_price, quantity
FROM open_positions
WHERE status = 'open'
    AND stop_loss_price IS NULL;

-- Долго открытые позиции (более 7 дней)
SELECT
    position_uuid,
    symbol,
    side,
    entry_price,
    opened_at_utc,
    EXTRACT(DAY FROM NOW() - opened_at_utc) as days_open
FROM open_positions
WHERE status = 'open'
    AND opened_at_utc < NOW() - INTERVAL '7 days'
ORDER BY opened_at_utc;
```

## Обслуживание таблицы

### Регулярная очистка

```sql
-- Удаление закрытых позиций старше 30 дней
-- (предполагается, что данные перенесены в trade_history)
DELETE FROM open_positions
WHERE status = 'closing'
    AND updated_at_utc < NOW() - INTERVAL '30 days';

-- Проверка размера таблицы
SELECT
    pg_size_pretty(pg_total_relation_size('open_positions')) as total_size,
    pg_size_pretty(pg_relation_size('open_positions')) as table_size,
    pg_size_pretty(pg_indexes_size('open_positions')) as indexes_size;
```

### Резервное копирование

```bash
# Экспорт таблицы
pg_dump -h 82.25.115.144 -U trading_admin -d trading_operations \
    -t open_positions --data-only \
    -f open_positions_backup_$(date +%Y%m%d).sql

# Импорт данных
psql -h 82.25.115.144 -U trading_admin -d trading_operations \
    -f open_positions_backup_20240115.sql
```

## Интеграция с другими таблицами

### Связь с trading_signals

```sql
-- Получить позиции с информацией о сигналах
SELECT
    op.position_uuid,
    op.symbol,
    op.side,
    op.entry_price,
    ts.indicator_type,
    ts.score,
    ts.timeframe
FROM open_positions op
LEFT JOIN trading_signals ts ON op.signal_id = ts.id
WHERE op.status = 'open'
ORDER BY op.opened_at_utc DESC;
```

### Перенос в trade_history при закрытии

```sql
-- Пример переноса закрытой позиции в trade_history
WITH closed_position AS (
    SELECT * FROM open_positions
    WHERE position_uuid = 'pos-2024-01-15-001'
)
INSERT INTO trade_history (
    account_id,
    symbol,
    side,
    entry_price,
    exit_price,
    quantity,
    profit_loss,
    profit_loss_percentage,
    commission,
    entry_time_utc,
    exit_time_utc,
    trade_duration_minutes,
    exit_reason,
    metadata
)
SELECT
    account_id,
    symbol,
    side,
    entry_price,
    96500.00, -- exit price from closing transaction
    quantity,
    (96500.00 - entry_price) * quantity, -- profit calculation
    ((96500.00 - entry_price) / entry_price) * 100, -- percentage
    commission_paid,
    opened_at_utc,
    NOW(),
    EXTRACT(EPOCH FROM (NOW() - opened_at_utc)) / 60,
    close_reason,
    metadata
FROM closed_position;
```

## Мониторинг и алерты

### Проверка критических условий

```sql
-- Позиции близкие к стоп-лоссу (для мониторинга)
-- Требует знание текущей цены, которая хранится в боте
SELECT
    position_uuid,
    symbol,
    side,
    entry_price,
    stop_loss_price,
    ABS(stop_loss_price - entry_price) / entry_price * 100 as stop_distance_percent
FROM open_positions
WHERE status = 'open'
    AND stop_loss_price IS NOT NULL
ORDER BY stop_distance_percent;

-- Проверка дубликатов
SELECT account_id, symbol, side, COUNT(*) as duplicate_count
FROM open_positions
WHERE status = 'open'
GROUP BY account_id, symbol, side
HAVING COUNT(*) > 1;
```

## Важные замечания

1. **Real-time данные**: Таблица не хранит текущую цену и P&L - эти данные отслеживает бот
2. **UUID формат**: Используйте консистентный формат UUID для всех позиций
3. **Временные зоны**: Все временные метки хранятся в UTC
4. **Частичное закрытие**: При частичном закрытии обновляйте remaining_quantity и total_closed_quantity
5. **Trailing Stop**: Бот должен обновлять trailing_stop_current_price при движении цены
6. **Очистка данных**: Регулярно переносите закрытые позиции в trade_history
7. **Индексы**: Используйте составные индексы для часто используемых комбинаций полей

## Контакты и поддержка

- **База данных**: trading_operations на VPS 82.25.115.144
- **Документация БД**: `/TRADING_OPERATIONS_DB_DOCUMENTATION.md`
- **Связанные таблицы**: trading_signals, trade_history