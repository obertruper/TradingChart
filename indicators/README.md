# Indicators Module

Модуль для расчета и хранения технических индикаторов в PostgreSQL.

## 🚀 Основные возможности

- ✅ **Автоматическое обнаружение и заполнение пробелов** - при добавлении новых периодов
- ✅ **Динамическое создание колонок** - автоматически создает колонки для новых периодов
- ✅ **Инкрементальные обновления** - продолжает с последней загруженной записи
- ✅ **Пакетная обработка** - эффективная загрузка больших объемов данных
- ✅ **Логирование в файлы** - все операции логируются для анализа
- ✅ **Конфигурация через YAML** - простое управление периодами и настройками

## Структура

```
indicators/
├── sma_loader.py           # Универсальный загрузчик SMA индикаторов
├── database.py             # Модуль работы с БД
├── config.yaml            # Конфигурация (периоды SMA и другие настройки)
├── logs/                  # Папка с лог-файлами
│   └── sma_loader_*.log  # Логи каждого запуска
├── tools/                 # Утилиты
│   ├── manage_columns.py # Управление колонками БД
│   ├── view_logs.py      # Просмотр и анализ логов
│   └── fix_nulls.py      # Исправление NULL/NaN
└── test_indicators/       # Тестовые скрипты
    ├── check_progress.py      # Проверка прогресса загрузки
    ├── test_connection.py     # Тест подключения к БД
    └── test_dynamic_columns.py # Проверка структуры таблицы
```

## SMA Loader - Как это работает

### 🎯 Главная особенность: Автоматическое управление периодами

Когда вы добавляете новый период в `config.yaml`, скрипт автоматически:
1. **Создает новую колонку** в БД
2. **Обнаруживает пробел** (что новая колонка пустая)
3. **Заполняет исторические данные** для нового периода
4. **Продолжает обновление** всех периодов вместе

### 📋 Конфигурация (config.yaml)

```yaml
indicators:
  sma:
    enabled: true
    periods: [10, 20, 30, 40, 50, 100, 200]  # Просто добавьте новый период сюда!
```

### Использование

#### Основной способ - через config.yaml:
```bash
# Периоды читаются из config.yaml
python indicators/sma_loader.py
```

#### Ручное указание периодов (опционально):
```bash
# Переопределить периоды из командной строки
python indicators/sma_loader.py --periods 5,15,30,75,150
```

#### Загрузка для другой монеты:
```bash
python indicators/sma_loader.py --symbol ETHUSDT
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