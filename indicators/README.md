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
├── database.py            # Модуль работы с БД
├── config.yaml            # Конфигурация (таймфреймы, периоды SMA, символы)
├── logs/                  # Папка с лог-файлами
│   └── sma_*.log         # Логи каждого запуска
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

### 📋 Конфигурация (config.yaml)

```yaml
indicators:
  sma:
    enabled: true
    periods: [10, 20, 30, 40, 50, 100, 200]  # Просто добавьте новый период сюда!
```

### Использование

#### Загрузка всех таймфреймов из config.yaml:
```bash
python indicators/sma_loader.py
```

#### Загрузка конкретных таймфреймов:
```bash
# Один таймфрейм
python indicators/sma_loader.py --timeframe 15m

# Несколько таймфреймов
python indicators/sma_loader.py --timeframes 1m,15m,1h

# Для другой монеты
python indicators/sma_loader.py --symbol ETHUSDT --timeframes 5m,30m
```

#### Изменение размера батча (для оптимизации):
```bash
python indicators/sma_loader.py --batch-days 7  # По умолчанию 30
```

### 🔄 Пример работы при добавлении нового периода

```bash
# 1. Добавили в config.yaml новый период 40:
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
    sma_10 DECIMAL(20,8),
    sma_20 DECIMAL(20,8),
    sma_50 DECIMAL(20,8),
    sma_100 DECIMAL(20,8),
    sma_200 DECIMAL(20,8),
    -- Колонки добавляются динамически при необходимости
    PRIMARY KEY (timestamp, symbol)
);
```

### 💡 Особенность расчета SMA

**Для разных таймфреймов используются разные цены:**
- **1m**: SMA рассчитывается по цене **CLOSE** (закрытия) каждой минутной свечи
- **15m, 1h, 4h и другие**: SMA рассчитывается по цене **OPEN** (открытия) каждого периода

Это стандартный подход в техническом анализе, обеспечивающий стабильность и предсказуемость индикатора.

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