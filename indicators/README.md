# Indicators Module

Модуль для расчета и хранения технических индикаторов в PostgreSQL.

## 🚀 Основные возможности

- ✅ **Мульти-таймфрейм поддержка** - любые таймфреймы (1m, 5m, 15m, 30m, 1h, 4h, 1d и т.д.)
- ✅ **Автоматическая агрегация** - из 1m данных в старшие таймфреймы
- ✅ **Динамическое создание таблиц и колонок** - автоматически для новых таймфреймов и периодов
- ✅ **Умный анализ каждого SMA периода** - определяет самый отстающий и начинает с него
- ✅ **Детальный прогресс-бар** - показывает процент, время, количество записей
- ✅ **Инкрементальные обновления** - продолжает с последней загруженной записи
- ✅ **Пакетная обработка** - эффективная загрузка больших объемов данных
- ✅ **Логирование в файлы** - с таймстампами для каждого запуска
- ✅ **Конфигурация через YAML** - простое управление таймфреймами и периодами

## Структура

```
indicators/
├── sma_loader.py          # Универсальный загрузчик SMA с мульти-таймфрейм поддержкой
├── ema_loader.py          # Загрузчик EMA с батчевой обработкой и checkpoint
├── rsi_loader.py          # Загрузчик RSI с батчевой обработкой
├── database.py            # Модуль работы с БД
├── indicators_config.yaml # Конфигурация (таймфреймы, периоды SMA/EMA/RSI, символы)
├── logs/                  # Папка с лог-файлами
│   ├── sma_*.log         # Логи SMA загрузчика
│   ├── ema_*.log         # Логи EMA загрузчика
│   └── rsi_*.log         # Логи RSI загрузчика
├── tools/                 # Утилиты
│   ├── manage_columns.py # Управление колонками БД
│   ├── view_logs.py      # Просмотр и анализ логов
│   └── fix_nulls.py      # Исправление NULL/NaN
└── test_indicators/       # Тестовые скрипты
    └── check_progress.py # Проверка прогресса загрузки
```

## SMA Loader - Как это работает

### 🎯 Главные особенности:

#### 1. Мульти-таймфрейм поддержка
Автоматически создает и управляет таблицами для разных таймфреймов:
- `indicators_bybit_futures_1m` - минутные данные (SMA рассчитывается по **CLOSE**)
- `indicators_bybit_futures_15m` - 15-минутные данные (SMA рассчитывается по **OPEN**)
- `indicators_bybit_futures_1h` - часовые данные (SMA рассчитывается по **OPEN**)
- И любые другие из config.yaml

**⚠️ Важно:** Для агрегированных таймфреймов (15m, 1h и т.д.) SMA рассчитывается по цене **OPEN** (открытия) каждого периода. Это стандартный подход в техническом анализе.

#### 2. Умное управление SMA периодами
При запуске скрипт:
1. **Анализирует каждый SMA период** отдельно (SMA_10, SMA_30 и т.д.)
2. **Находит самый отстающий** период
3. **Начинает загрузку** с самой ранней даты
4. **Показывает детальный прогресс** с процентами и временем

#### 3. Автоматическое управление
При добавлении нового периода или таймфрейма в `config.yaml`, скрипт автоматически:
1. **Создает новые таблицы/колонки** в БД
2. **Обнаруживает пробелы** в данных
3. **Заполняет исторические данные**
4. **Продолжает обновление** всех периодов

### 📋 Конфигурация (indicators_config.yaml)

```yaml
indicators:
  sma:
    enabled: true
    periods: [10, 20, 30, 40, 50, 100, 200]  # Просто добавьте новый период сюда!
  ema:
    enabled: true
    periods: [9, 12, 21, 26, 50, 100, 200]   # Периоды EMA
    batch_days: 1  # Размер батча в днях (1-7 рекомендуется)
  rsi:
    enabled: true
    periods: [7, 9, 14, 21, 25]  # Периоды RSI для разных стратегий
    batch_days: 7  # Размер батча (RSI быстрее, можно больше)
```

### Использование

#### Загрузка SMA:
```bash
# Загрузка всех таймфреймов из indicators_config.yaml
python indicators/sma_loader.py

# Загрузка конкретных таймфреймов
python indicators/sma_loader.py --timeframe 15m
python indicators/sma_loader.py --timeframes 1m,15m,1h

# Для другой монеты
python indicators/sma_loader.py --symbol ETHUSDT --timeframes 5m,30m

# Изменение размера батча (для оптимизации)
python indicators/sma_loader.py --batch-days 7  # По умолчанию 30
```

#### Загрузка EMA:
```bash
# Загрузка EMA для всех таймфреймов из indicators_config.yaml
python indicators/ema_loader.py

# EMA автоматически:
# - Продолжает с последнего checkpoint
# - Обрабатывает данные батчами по 1-3 дня
# - Сохраняет прогресс после каждого батча
# - Поддерживает прерывание и возобновление (Ctrl+C безопасно)
```

#### Загрузка RSI:
```bash
# Загрузка RSI для всех таймфреймов из indicators_config.yaml
python indicators/rsi_loader.py

# Загрузка конкретного таймфрейма
python indicators/rsi_loader.py --timeframe 1m
python indicators/rsi_loader.py --timeframe 15m
python indicators/rsi_loader.py --timeframe 1h

# С увеличенным размером батча для скорости
python indicators/rsi_loader.py --batch-days 14

# RSI особенности:
# - Быстрее чем EMA (можно использовать батчи 7-14 дней)
# - Все периоды считаются параллельно
# - Checkpoint система для возобновления
# - Для таймфреймов > 1m автоматически агрегирует данные из минутных свечей
```

### 🔄 Пример работы при добавлении нового периода

```bash
# 1. Добавили в indicators_config.yaml новый период 40:
#    periods: [10, 20, 30, 40, 50, 100, 200]

# 2. Запускаем скрипт:
python indicators/sma_loader.py

# 3. Скрипт автоматически:
#    ✅ Создает колонку sma_40
#    ✅ Обнаруживает пробел (362 дня)
#    ✅ Заполняет исторические данные
#    ✅ Продолжает обновление всех периодов
```

### 📊 Мониторинг и логирование

#### Проверка прогресса:
```bash
python indicators/test_indicators/check_progress.py
```

#### Просмотр логов:
```bash
# Список всех логов
python indicators/tools/view_logs.py --list

# Последний лог
python indicators/tools/view_logs.py --view

# Следить в реальном времени
python indicators/tools/view_logs.py --follow

# Анализ лога
python indicators/tools/view_logs.py --analyze
```

## Структура БД

### Таблица: `indicators_bybit_futures_1m`

```sql
CREATE TABLE indicators_bybit_futures_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    -- SMA колонки
    sma_10 DECIMAL(20,8),
    sma_20 DECIMAL(20,8),
    sma_50 DECIMAL(20,8),
    sma_100 DECIMAL(20,8),
    sma_200 DECIMAL(20,8),
    -- EMA колонки
    ema_9 DECIMAL(20,8),
    ema_12 DECIMAL(20,8),
    ema_21 DECIMAL(20,8),
    ema_26 DECIMAL(20,8),
    ema_50 DECIMAL(20,8),
    ema_100 DECIMAL(20,8),
    ema_200 DECIMAL(20,8),
    -- RSI колонки
    rsi_7 DECIMAL(10,4),
    rsi_9 DECIMAL(10,4),
    rsi_14 DECIMAL(10,4),
    rsi_21 DECIMAL(10,4),
    rsi_25 DECIMAL(10,4),
    -- Колонки добавляются динамически при необходимости
    PRIMARY KEY (timestamp, symbol)
);
```

### 💡 Особенности расчета индикаторов

#### SMA (Simple Moving Average):
**Для разных таймфреймов используются разные цены:**
- **1m**: SMA рассчитывается по цене **CLOSE** (закрытия) каждой минутной свечи
- **15m, 1h, 4h и другие**: SMA рассчитывается по цене **OPEN** (открытия) каждого периода

#### EMA (Exponential Moving Average):
- **Формула**: EMA = Price × α + EMA_prev × (1 - α), где α = 2 / (period + 1)
- **Особенность**: Требует последовательного расчета (каждое значение зависит от предыдущего)
- **Checkpoint система**: Автоматически сохраняет и восстанавливает прогресс
- **Батчевая обработка**: Обрабатывает данные порциями по 1-3 дня для оптимальной производительности

#### RSI (Relative Strength Index):
- **Формула**: RSI = 100 - (100 / (1 + RS)), где RS = средний прирост / средние потери
- **Диапазон**: 0-100 (>70 перекупленность, <30 перепроданность)
- **Батчевая обработка**: Рекомендуется 7-14 дней для оптимальной скорости
- **Параллельный расчет**: Все периоды считаются одновременно

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

## Текущие индикаторы

### ✅ Реализованные индикаторы:
- **SMA** (Simple Moving Average) - `sma_loader.py`
  - Периоды: 10, 20, 30, 40, 50, 100, 200
- **EMA** (Exponential Moving Average) - `ema_loader.py`
  - Периоды: 9, 12, 21, 26, 50, 100, 200
- **RSI** (Relative Strength Index) - `rsi_loader.py`
  - Периоды: 7, 9, 14, 21, 25

### 📋 Планируемые индикаторы:
- `macd_loader.py` - MACD (будет использовать существующие EMA_12 и EMA_26)
- `stochastic_loader.py` - Stochastic Oscillator
- `atr_loader.py` - Average True Range
- `bollinger_loader.py` - Bollinger Bands
- И другие...

Каждый загрузчик будет использовать тот же принцип:
1. Проверка/создание колонок
2. Инкрементальная загрузка
3. Батчевая обработка