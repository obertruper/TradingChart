# Indicators Module

Модуль для расчета и хранения технических индикаторов в PostgreSQL.

## Структура

```
indicators/
├── sma_loader.py           # Универсальный загрузчик SMA индикаторов
├── database.py             # Модуль работы с БД
├── config.yaml            # Конфигурация
└── test_indicators/       # Тестовые скрипты
    ├── check_progress.py      # Проверка прогресса загрузки
    ├── test_connection.py     # Тест подключения к БД
    └── test_dynamic_columns.py # Проверка структуры таблицы
```

## SMA Loader

Универсальный загрузчик SMA (Simple Moving Average) индикаторов с поддержкой:
- ✅ Динамического создания колонок
- ✅ Пакетной обработки больших объемов
- ✅ Множественных символов
- ✅ Инкрементальных обновлений

### Использование

#### Базовая загрузка (периоды по умолчанию: 10, 20, 50, 100, 200):
```bash
python indicators/sma_loader.py
```

#### Загрузка с кастомными периодами:
```bash
python indicators/sma_loader.py --periods 5,15,30,75,150
```

#### Добавление нового периода к существующим:
```bash
python indicators/sma_loader.py --periods 30
```

#### Загрузка для другой монеты:
```bash
python indicators/sma_loader.py --symbol ETHUSDT
```

#### Изменение размера батча (для оптимизации):
```bash
python indicators/sma_loader.py --batch-days 7
```

### Функционал

1. **Автоматическое создание колонок**
   - Скрипт проверяет существование колонок для указанных периодов
   - Автоматически создает отсутствующие колонки

2. **Инкрементальные обновления**
   - Определяет последние загруженные данные
   - Загружает только новые данные

3. **Обработка больших объемов**
   - Батчевая обработка по дням
   - Прогресс-бар для отслеживания

## Структура БД

### Таблица: `indicators_bybit_futures_1m`

```sql
CREATE TABLE indicators_bybit_futures_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sma_10 DECIMAL(20,8),
    sma_20 DECIMAL(20,8),
    sma_50 DECIMAL(20,8),
    sma_100 DECIMAL(20,8),
    sma_200 DECIMAL(20,8),
    -- Колонки добавляются динамически при необходимости
    PRIMARY KEY (timestamp, symbol)
);
```

## Тестовые скрипты

### Проверка подключения:
```bash
python indicators/test_indicators/test_connection.py
```

### Проверка структуры таблицы и SMA периодов:
```bash
python indicators/test_indicators/test_dynamic_columns.py
```

### Мониторинг прогресса загрузки:
```bash
python indicators/test_indicators/check_progress.py
```

## Примеры запросов к БД

### Получить последние SMA для BTCUSDT:
```sql
SELECT timestamp, sma_20, sma_50, sma_200
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
ORDER BY timestamp DESC
LIMIT 100;
```

### Найти золотые кресты (SMA50 > SMA200):
```sql
SELECT timestamp
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND sma_50 > sma_200
  AND LAG(sma_50) OVER (ORDER BY timestamp) <= LAG(sma_200) OVER (ORDER BY timestamp);
```

### Статистика по символам:
```sql
SELECT
    symbol,
    COUNT(*) as records,
    MIN(timestamp) as first_date,
    MAX(timestamp) as last_date
FROM indicators_bybit_futures_1m
GROUP BY symbol;
```

## Следующие шаги

По аналогии с `sma_loader.py` можно создать загрузчики для других индикаторов:
- `ema_loader.py` - Exponential Moving Average
- `rsi_loader.py` - Relative Strength Index
- `macd_loader.py` - MACD
- И другие...

Каждый загрузчик будет использовать тот же принцип:
1. Проверка/создание колонок
2. Инкрементальная загрузка
3. Батчевая обработка