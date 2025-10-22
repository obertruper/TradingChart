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
├── sma_loader.py             # Универсальный загрузчик SMA с мульти-таймфрейм поддержкой
├── ema_loader.py             # Загрузчик EMA с батчевой обработкой и checkpoint
├── rsi_loader.py             # Загрузчик RSI с автоопределением пустых столбцов
├── vma_loader.py             # Загрузчик VMA с последовательной обработкой периодов
├── atr_loader.py             # Загрузчик ATR с сглаживанием Уайлдера
├── macd_loader.py            # Загрузчик MACD с независимым расчётом EMA
├── bollinger_bands_loader.py # Загрузчик Bollinger Bands с SMA/EMA базой
├── adx_loader.py             # Загрузчик ADX с двойным сглаживанием Уайлдера
├── long_short_ratio_loader.py # Загрузчик Long/Short Ratio от Bybit API
├── check_vma_status.py       # Проверка статуса VMA в базе данных
├── check_atr_status.py       # Проверка статуса ATR в базе данных
├── check_macd_status.py      # Проверка статуса MACD в базе данных
├── check_bollinger_status.py # Проверка статуса Bollinger Bands в базе данных
├── check_adx_status.py       # Проверка статуса ADX в базе данных
├── database.py               # Модуль работы с БД
├── indicators_config.yaml    # Конфигурация (таймфреймы, периоды SMA/EMA/RSI/VMA/ATR/MACD/BB/ADX/L/S, символы)
├── logs/                     # Папка с лог-файлами
│   ├── sma_*.log            # Логи SMA загрузчика
│   ├── ema_*.log            # Логи EMA загрузчика
│   ├── rsi_*.log            # Логи RSI загрузчика
│   ├── vma_*.log            # Логи VMA загрузчика
│   ├── atr_*.log            # Логи ATR загрузчика
│   ├── macd_*.log           # Логи MACD загрузчика
│   ├── bollinger_bands_*.log # Логи Bollinger Bands загрузчика
│   ├── adx_*.log            # Логи ADX загрузчика
│   └── long_short_ratio_*.log # Логи Long/Short Ratio загрузчика
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
  vma:
    enabled: true
    periods: [10, 20, 50, 100, 200]  # Периоды VMA для анализа объемов
    batch_days: 1  # Размер батча (1 день для точного контроля)
  atr:
    enabled: true
    periods: [7, 14, 21, 30, 50, 100]  # Периоды ATR для анализа волатильности
    batch_days: 1  # Размер батча (1 день, последовательный расчет)
  macd:
    enabled: true
    configurations:  # 8 конфигураций MACD (classic, crypto, aggressive, balanced, scalping, swing, longterm, ultralong)
      - {name: "classic", fast: 12, slow: 26, signal: 9}  # Стандарт индустрии
      - {name: "crypto", fast: 6, slow: 13, signal: 5}    # Для криптовалют
    batch_days: 1  # Размер батча (1 день для точного контроля)
    lookback_multiplier: 3  # Множитель lookback для точности EMA
  bollinger_bands:
    enabled: true
    configurations:  # 13 конфигураций BB (11 SMA + 2 EMA)
      - {name: "classic", period: 20, std_dev: 2.0, base: "sma"}  # Стандарт индустрии (Джон Боллинджер)
      - {name: "golden", period: 20, std_dev: 1.618, base: "sma"}  # Золотое сечение Фибоначчи
      - {name: "classic_ema", period: 20, std_dev: 2.0, base: "ema"}  # Быстрая реакция (EMA база)
    batch_days: 1  # Размер батча (1 день для точного контроля)
    lookback_multiplier: 3  # Множитель lookback для точности
    squeeze_threshold: 5.0  # Порог для определения squeeze (bandwidth < 5%)
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

# Загрузка конкретного таймфрейма с указанием размера батча
python indicators/ema_loader.py --timeframe 1m --batch-days 1
python indicators/ema_loader.py --timeframe 15m --batch-days 3
python indicators/ema_loader.py --timeframe 1h --batch-days 7

# EMA автоматически:
# - Продолжает с последнего checkpoint
# - Обрабатывает данные батчами (рекомендуется 1-7 дней)
# - Сохраняет прогресс после каждого батча
# - Поддерживает прерывание и возобновление (Ctrl+C безопасно)
```

#### Загрузка RSI:
```bash
# Загрузка RSI с автоматическим определением пустых столбцов
python indicators/rsi_loader.py

# Загрузка конкретного таймфрейма
python indicators/rsi_loader.py --timeframe 1m --batch-days 7
python indicators/rsi_loader.py --timeframe 15m --batch-days 30
python indicators/rsi_loader.py --timeframe 1h --batch-days 60

# Принудительная загрузка с конкретной даты
python indicators/rsi_loader.py --timeframe 1m --batch-days 7 --start-date "2020-03-26"
python indicators/rsi_loader.py --timeframe 15m --batch-days 30 --start-date "2020-03-27"
python indicators/rsi_loader.py --timeframe 1h --batch-days 60 --start-date "2020-04-02"

# RSI особенности:
# - Автоматическое определение и группировка пустых столбцов:
#   • Пустые периоды (<50% заполнения) - загружаются с начала
#   • Частично заполненные (50-95%) - продолжают с последнего checkpoint
#   • Полные (>95%) - только обновление последних данных
# - Раздельная обработка каждой группы периодов
# - Поддерживает параметр --start-date для принудительной загрузки
# - Все периоды в группе считаются параллельно
# - Checkpoint система для возобновления после прерывания
# - Для таймфреймов > 1m автоматически агрегирует данные из минутных свечей
```

#### Загрузка VMA:
```bash
# Загрузка VMA для всех таймфреймов из indicators_config.yaml
python indicators/vma_loader.py

# Загрузка конкретного таймфрейма
python indicators/vma_loader.py --timeframe 1m --batch-days 1
python indicators/vma_loader.py --timeframe 15m --batch-days 1
python indicators/vma_loader.py --timeframe 1h --batch-days 1

# VMA особенности:
# - Последовательная обработка периодов (10, 20, 50, 100, 200)
# - Каждый период независимо отслеживает свою последнюю дату
# - Lookback период для корректного расчета на границах батчей
# - Для таймфреймов > 1m агрегирует объемы через SUM
# - Checkpoint система для возобновления после прерывания
# - Не обрабатывает текущий незавершенный период (ждет полного завершения)
```

#### Загрузка ATR:
```bash
# Загрузка ATR для всех таймфреймов из indicators_config.yaml
python indicators/atr_loader.py

# Загрузка конкретного таймфрейма
python indicators/atr_loader.py --timeframe 1m --batch-days 1
python indicators/atr_loader.py --timeframe 15m --batch-days 1
python indicators/atr_loader.py --timeframe 1h --batch-days 1

# ATR особенности:
# - Последовательная обработка периодов (7, 14, 21, 30, 50, 100)
# - Каждый период независимо отслеживает свою последнюю дату
# - Lookback период = period × 2 для стабильности сглаживания Уайлдера
# - Использует сглаживание Уайлдера (не простое среднее)
# - Для таймфреймов > 1m агрегирует High/Low/Close из минутных свечей
# - Checkpoint система для возобновления после прерывания
# - Последовательный расчет (каждое значение зависит от предыдущего)
```

#### Загрузка MACD:
```bash
# Загрузка MACD для всех таймфреймов и конфигураций из indicators_config.yaml
python indicators/macd_loader.py

# Загрузка конкретного таймфрейма
python indicators/macd_loader.py --timeframe 1m --batch-days 1
python indicators/macd_loader.py --timeframe 15m --batch-days 1
python indicators/macd_loader.py --timeframe 1h --batch-days 1

# Использование больших батчей для ускорения
python indicators/macd_loader.py --batch-days 7

# Проверка статуса MACD
python indicators/check_macd_status.py
python indicators/check_macd_status.py --examples  # Показать примеры значений
python indicators/check_macd_status.py --gaps      # Проверить пропуски

# MACD особенности:
# - 8 конфигураций (classic, crypto, aggressive, balanced, scalping, swing, longterm, ultralong)
# - Каждая конфигурация = 3 колонки (line, signal, histogram)
# - Последовательная обработка конфигураций (можно прервать)
# - Независимый расчёт EMA (не зависит от ema_loader.py)
# - Lookback = max(slow, signal) × 3 для точности EMA
# - Для таймфреймов > 1m использует LAST(close) для агрегации
# - Checkpoint система для возобновления
```

#### Загрузка Bollinger Bands:
```bash
# Базовый запуск (все 13 конфигураций BB)
python indicators/bollinger_bands_loader.py

# Указать символ
python indicators/bollinger_bands_loader.py --symbol BTCUSDT

# Указать таймфрейм
python indicators/bollinger_bands_loader.py --timeframe 1h

# Изменить размер батча (по умолчанию 1 день)
python indicators/bollinger_bands_loader.py --batch-days 3

# Проверка статуса Bollinger Bands
python indicators/check_bollinger_status.py
python indicators/check_bollinger_status.py --examples    # Показать примеры значений
python indicators/check_bollinger_status.py --gaps        # Проверить пропуски
python indicators/check_bollinger_status.py --squeeze     # Показать события сжатия

# Bollinger Bands особенности:
# - 13 конфигураций (11 SMA + 2 EMA базированных)
# - Каждая конфигурация = 6 колонок (upper, middle, lower, %B, bandwidth, squeeze)
# - Всего 78 колонок в БД (13 × 6)
# - Последовательная обработка конфигураций (можно прервать)
# - Независимый расчёт SMA/EMA (не зависит от sma_loader.py/ema_loader.py)
# - Lookback = period × 3 для точности на границах батчей
# - Для таймфреймов > 1m использует LAST(close) для агрегации
# - Squeeze threshold = 5% (bandwidth < 5% → squeeze = true)
# - Checkpoint система для возобновления
# - Порядок обработки: от коротких к длинным (3 → 89)
# - Автоматическая конвертация типов (Decimal → float, numpy → Python)
```

#### Загрузка ADX (Average Directional Index):
```bash
# Базовый запуск (все 8 периодов ADX)
python indicators/adx_loader.py

# Указать символ
python indicators/adx_loader.py --symbol BTCUSDT

# Указать таймфрейм
python indicators/adx_loader.py --timeframe 1h

# Указать конкретный период
python indicators/adx_loader.py --period 14

# Изменить размер батча (по умолчанию 1 день)
python indicators/adx_loader.py --batch-days 3

# Проверка статуса ADX
python indicators/check_adx_status.py
python indicators/check_adx_status.py --comparison    # Показать значения для сравнения с TradingView

# ADX особенности:
# - 8 периодов (7, 10, 14, 20, 21, 25, 30, 50)
# - Каждый период = 3 компонента (adx, +DI, -DI)
# - Всего 24 колонки в БД (8 × 3)
# - ADX показывает силу тренда (0-100), +DI/-DI показывают направление
# - Последовательная обработка периодов (можно прервать)
# - Двойное сглаживание Уайлдера (TR/+DM/-DM → сглаживание → DI → DX → сглаживание → ADX)
# - Lookback = period × 4 для точности двойного сглаживания
# - Для таймфреймов > 1m агрегация: MAX(high), MIN(low), LAST(close)
# - Checkpoint система для возобновления
# - Порядок обработки: от коротких к длинным (7 → 50)
# - Автоматическая конвертация типов (Decimal → float, numpy → Python)
# - Интерпретация: ADX < 25 (слабый тренд), 25-50 (сильный), 50-75 (очень сильный), 75-100 (экстремальный)
```

### 🐛 Известные проблемы и решения

#### Bollinger Bands: Исправленные ошибки при первом запуске

**Проблема 1**: `'_GeneratorContextManager' object has no attribute 'close'`
- **Причина**: Двойная обертка context manager
- **Решение**: Использование `self.db.get_connection()` напрямую (2025-10-16)

**Проблема 2**: `column "bollinger_bands_*" does not exist`
- **Причина**: Проверка данных до создания колонок
- **Решение**: Вызов `ensure_columns_exist()` перед `get_last_processed_date()` (2025-10-16)

**Проблема 3**: `unsupported operand type(s) for -: 'decimal.Decimal' and 'float'`
- **Причина**: PostgreSQL возвращает Decimal, pandas работает с float
- **Решение**: `close_prices.astype(float)` в начале расчетов (2025-10-16)

**Проблема 4**: `schema "np" does not exist`
- **Причина**: numpy типы (np.float64) передаются в SQL без конвертации
- **Решение**: Конвертация в Python native типы через `float()` перед записью в БД (2025-10-16)

Все проблемы исправлены, скрипт полностью рабочий! ✅

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
    -- VMA колонки
    vma_10 DECIMAL(20,8),
    vma_20 DECIMAL(20,8),
    vma_50 DECIMAL(20,8),
    vma_100 DECIMAL(20,8),
    vma_200 DECIMAL(20,8),
    -- ATR колонки
    atr_7 DECIMAL(20,8),
    atr_14 DECIMAL(20,8),
    atr_21 DECIMAL(20,8),
    atr_30 DECIMAL(20,8),
    atr_50 DECIMAL(20,8),
    atr_100 DECIMAL(20,8),
    -- MACD колонки (8 конфигураций × 3 компонента = 24 колонки)
    macd_12_26_9_line DECIMAL(20,8), macd_12_26_9_signal DECIMAL(20,8), macd_12_26_9_histogram DECIMAL(20,8),  -- Classic
    macd_6_13_5_line DECIMAL(20,8), macd_6_13_5_signal DECIMAL(20,8), macd_6_13_5_histogram DECIMAL(20,8),  -- Crypto
    macd_5_35_5_line DECIMAL(20,8), macd_5_35_5_signal DECIMAL(20,8), macd_5_35_5_histogram DECIMAL(20,8),  -- Aggressive
    macd_8_17_9_line DECIMAL(20,8), macd_8_17_9_signal DECIMAL(20,8), macd_8_17_9_histogram DECIMAL(20,8),  -- Balanced
    macd_5_13_3_line DECIMAL(20,8), macd_5_13_3_signal DECIMAL(20,8), macd_5_13_3_histogram DECIMAL(20,8),  -- Scalping
    macd_10_21_9_line DECIMAL(20,8), macd_10_21_9_signal DECIMAL(20,8), macd_10_21_9_histogram DECIMAL(20,8),  -- Swing
    macd_21_55_13_line DECIMAL(20,8), macd_21_55_13_signal DECIMAL(20,8), macd_21_55_13_histogram DECIMAL(20,8),  -- Longterm
    macd_50_200_9_line DECIMAL(20,8), macd_50_200_9_signal DECIMAL(20,8), macd_50_200_9_histogram DECIMAL(20,8),  -- Ultralong
    -- Bollinger Bands колонки (13 конфигураций × 6 компонентов = 78 колонок)
    -- SMA-based (11 конфигураций)
    bollinger_bands_sma_3_2_0_upper DECIMAL(20,8), bollinger_bands_sma_3_2_0_middle DECIMAL(20,8), bollinger_bands_sma_3_2_0_lower DECIMAL(20,8), bollinger_bands_sma_3_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_3_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_3_2_0_squeeze BOOLEAN,  -- Ultrafast
    bollinger_bands_sma_5_2_0_upper DECIMAL(20,8), bollinger_bands_sma_5_2_0_middle DECIMAL(20,8), bollinger_bands_sma_5_2_0_lower DECIMAL(20,8), bollinger_bands_sma_5_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_5_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_5_2_0_squeeze BOOLEAN,  -- Scalping
    bollinger_bands_sma_10_1_5_upper DECIMAL(20,8), bollinger_bands_sma_10_1_5_middle DECIMAL(20,8), bollinger_bands_sma_10_1_5_lower DECIMAL(20,8), bollinger_bands_sma_10_1_5_percent_b DECIMAL(10,4), bollinger_bands_sma_10_1_5_bandwidth DECIMAL(10,4), bollinger_bands_sma_10_1_5_squeeze BOOLEAN,  -- Short
    bollinger_bands_sma_14_2_0_upper DECIMAL(20,8), bollinger_bands_sma_14_2_0_middle DECIMAL(20,8), bollinger_bands_sma_14_2_0_lower DECIMAL(20,8), bollinger_bands_sma_14_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_14_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_14_2_0_squeeze BOOLEAN,  -- Intraday
    bollinger_bands_sma_20_1_0_upper DECIMAL(20,8), bollinger_bands_sma_20_1_0_middle DECIMAL(20,8), bollinger_bands_sma_20_1_0_lower DECIMAL(20,8), bollinger_bands_sma_20_1_0_percent_b DECIMAL(10,4), bollinger_bands_sma_20_1_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_20_1_0_squeeze BOOLEAN,  -- Tight
    bollinger_bands_sma_20_1_618_upper DECIMAL(20,8), bollinger_bands_sma_20_1_618_middle DECIMAL(20,8), bollinger_bands_sma_20_1_618_lower DECIMAL(20,8), bollinger_bands_sma_20_1_618_percent_b DECIMAL(10,4), bollinger_bands_sma_20_1_618_bandwidth DECIMAL(10,4), bollinger_bands_sma_20_1_618_squeeze BOOLEAN,  -- Golden
    bollinger_bands_sma_20_2_0_upper DECIMAL(20,8), bollinger_bands_sma_20_2_0_middle DECIMAL(20,8), bollinger_bands_sma_20_2_0_lower DECIMAL(20,8), bollinger_bands_sma_20_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_20_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_20_2_0_squeeze BOOLEAN,  -- Classic
    bollinger_bands_sma_20_3_0_upper DECIMAL(20,8), bollinger_bands_sma_20_3_0_middle DECIMAL(20,8), bollinger_bands_sma_20_3_0_lower DECIMAL(20,8), bollinger_bands_sma_20_3_0_percent_b DECIMAL(10,4), bollinger_bands_sma_20_3_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_20_3_0_squeeze BOOLEAN,  -- Wide
    bollinger_bands_sma_21_2_0_upper DECIMAL(20,8), bollinger_bands_sma_21_2_0_middle DECIMAL(20,8), bollinger_bands_sma_21_2_0_lower DECIMAL(20,8), bollinger_bands_sma_21_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_21_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_21_2_0_squeeze BOOLEAN,  -- Fibonacci
    bollinger_bands_sma_34_2_0_upper DECIMAL(20,8), bollinger_bands_sma_34_2_0_middle DECIMAL(20,8), bollinger_bands_sma_34_2_0_lower DECIMAL(20,8), bollinger_bands_sma_34_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_34_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_34_2_0_squeeze BOOLEAN,  -- Fibonacci Medium
    bollinger_bands_sma_89_2_0_upper DECIMAL(20,8), bollinger_bands_sma_89_2_0_middle DECIMAL(20,8), bollinger_bands_sma_89_2_0_lower DECIMAL(20,8), bollinger_bands_sma_89_2_0_percent_b DECIMAL(10,4), bollinger_bands_sma_89_2_0_bandwidth DECIMAL(10,4), bollinger_bands_sma_89_2_0_squeeze BOOLEAN,  -- Fibonacci Long
    -- EMA-based (2 конфигурации)
    bollinger_bands_ema_20_2_0_upper DECIMAL(20,8), bollinger_bands_ema_20_2_0_middle DECIMAL(20,8), bollinger_bands_ema_20_2_0_lower DECIMAL(20,8), bollinger_bands_ema_20_2_0_percent_b DECIMAL(10,4), bollinger_bands_ema_20_2_0_bandwidth DECIMAL(10,4), bollinger_bands_ema_20_2_0_squeeze BOOLEAN,  -- Classic EMA
    bollinger_bands_ema_20_1_618_upper DECIMAL(20,8), bollinger_bands_ema_20_1_618_middle DECIMAL(20,8), bollinger_bands_ema_20_1_618_lower DECIMAL(20,8), bollinger_bands_ema_20_1_618_percent_b DECIMAL(10,4), bollinger_bands_ema_20_1_618_bandwidth DECIMAL(10,4), bollinger_bands_ema_20_1_618_squeeze BOOLEAN,  -- Golden EMA
    -- ADX колонки (8 периодов × 3 компонента = 24 колонки)
    adx_7 DECIMAL(10,4), adx_7_plus_di DECIMAL(10,4), adx_7_minus_di DECIMAL(10,4),  -- Period 7 (scalping)
    adx_10 DECIMAL(10,4), adx_10_plus_di DECIMAL(10,4), adx_10_minus_di DECIMAL(10,4),  -- Period 10 (short-term swing)
    adx_14 DECIMAL(10,4), adx_14_plus_di DECIMAL(10,4), adx_14_minus_di DECIMAL(10,4),  -- Period 14 (classic Wilder's original)
    adx_20 DECIMAL(10,4), adx_20_plus_di DECIMAL(10,4), adx_20_minus_di DECIMAL(10,4),  -- Period 20 (medium-term)
    adx_21 DECIMAL(10,4), adx_21_plus_di DECIMAL(10,4), adx_21_minus_di DECIMAL(10,4),  -- Period 21 (Fibonacci)
    adx_25 DECIMAL(10,4), adx_25_plus_di DECIMAL(10,4), adx_25_minus_di DECIMAL(10,4),  -- Period 25 (balanced)
    adx_30 DECIMAL(10,4), adx_30_plus_di DECIMAL(10,4), adx_30_minus_di DECIMAL(10,4),  -- Period 30 (monthly)
    adx_50 DECIMAL(10,4), adx_50_plus_di DECIMAL(10,4), adx_50_minus_di DECIMAL(10,4),  -- Period 50 (long-term)
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

#### VMA (Volume Moving Average):
- **Формула**: VMA = (V₁ + V₂ + ... + Vₙ) / n, где V = объем торгов
- **Агрегация объемов**: Для таймфреймов > 1m используется **SUM(volume)**, не LAST или AVG
- **Батчевая обработка**: 1 день для точного контроля и мониторинга прогресса
- **Последовательный расчет**: Периоды обрабатываются по очереди (10, 20, 50, 100, 200)
- **Применение**: Подтверждение силы ценовых движений, фильтрация слабых сигналов

#### ATR (Average True Range):
- **Формула**: TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|), ATR = сглаживание Уайлдера от TR
- **Сглаживание Уайлдера**: ATR = (ATR_prev × (period-1) + TR) / period
- **Агрегация**: Для таймфреймов > 1m: High=MAX(high), Low=MIN(low), Close=LAST(close)
- **Батчевая обработка**: 1 день для точного контроля (последовательный расчет)
- **Lookback**: period × 2 для стабильности сглаживания
- **Применение**: Измерение волатильности, динамические стоп-лоссы, размер позиции

#### MACD (Moving Average Convergence Divergence):
- **Компоненты**: MACD Line (Fast EMA - Slow EMA), Signal Line (EMA от MACD), Histogram (MACD - Signal)
- **Формула**: MACD = EMA(fast) - EMA(slow), Signal = EMA(MACD, signal), Histogram = MACD - Signal
- **8 конфигураций**: classic (12,26,9), crypto (6,13,5), aggressive (5,35,5), balanced (8,17,9), scalping (5,13,3), swing (10,21,9), longterm (21,55,13), ultralong (50,200,9)
- **Независимый расчёт**: EMA рассчитывается на лету из close цен (не зависит от ema_loader)
- **Агрегация**: Для таймфреймов > 1m: Close=LAST(close) из минутных свечей
- **Lookback**: max(slow, signal) × 3 для точности EMA
- **Применение**: Определение тренда, точки входа/выхода, дивергенции, импульс движения

#### Bollinger Bands (BB):
- **Компоненты**: Upper Band (Middle + k×σ), Middle Band (SMA/EMA), Lower Band (Middle - k×σ), %B, Bandwidth, Squeeze
- **Формулы**:
  - Middle Band = SMA(period) или EMA(period)
  - Upper Band = Middle + (std_dev × σ)
  - Lower Band = Middle - (std_dev × σ)
  - %B (Percent B) = (Close - Lower) / (Upper - Lower) — позиция цены внутри полос (0.0-1.0)
  - Bandwidth = (Upper - Lower) / Middle × 100 — ширина полос в процентах
  - Squeeze = Bandwidth < 5% — флаг сжатия полос (низкая волатильность)
- **13 конфигураций**:
  - **SMA-based (11)**: ultrafast (3,2.0), scalping (5,2.0), short (10,1.5), intraday (14,2.0), tight (20,1.0), golden (20,1.618), classic (20,2.0), wide (20,3.0), fibonacci (21,2.0), fibonacci_medium (34,2.0), fibonacci_long (89,2.0)
  - **EMA-based (2)**: classic_ema (20,2.0), golden_ema (20,1.618)
- **Независимый расчёт**: SMA/EMA рассчитывается на лету (не зависит от sma_loader/ema_loader)
- **Агрегация**: Для таймфреймов > 1m: Close=LAST(close), затем расчёт σ от агрегированных цен
- **Lookback**: period × 3 для точности на границах батчей
- **Squeeze threshold**: Фиксированный порог 5% для определения сжатия
- **Порядок обработки**: От коротких к длинным (3 → 89)
- **Применение**: Определение волатильности, перекупленность/перепроданность (%B), breakthrough/breakout (squeeze), поддержка/сопротивление

#### ADX (Average Directional Index):
- **Компоненты**: ADX (сила тренда 0-100), +DI (бычье направление), -DI (медвежье направление)
- **Формулы**:
  - TR (True Range) = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
  - +DM = High - PrevHigh (если > 0 и > down_move), иначе 0
  - -DM = PrevLow - Low (если > 0 и > up_move), иначе 0
  - Сглаживание Уайлдера: smoothed = (prev × (period-1) + current) / period
  - +DI = 100 × Wilder(+DM) / Wilder(TR)
  - -DI = 100 × Wilder(-DM) / Wilder(TR)
  - DX = 100 × |+DI - -DI| / (+DI + -DI)
  - ADX = Wilder(DX) — двойное сглаживание
- **8 периодов**: 7 (scalping), 10 (short-term swing), 14 (classic Wilder's original), 20 (medium-term), 21 (Fibonacci), 25 (balanced), 30 (monthly), 50 (long-term)
- **Интерпретация ADX**:
  - 0-25: Слабый/отсутствующий тренд (боковое движение)
  - 25-50: Сильный тренд (хорошо для трендовых стратегий)
  - 50-75: Очень сильный тренд (отличные условия для торговли)
  - 75-100: Экстремально сильный тренд (возможно истощение)
- **Интерпретация +DI/-DI**:
  - +DI > -DI: Бычий тренд (восходящий)
  - -DI > +DI: Медвежий тренд (нисходящий)
  - Разница показывает силу направления
- **Двойное сглаживание**: TR/+DM/-DM → сглаживание → +DI/-DI → DX → сглаживание → ADX
- **Агрегация**: Для таймфреймов > 1m: High=MAX(high), Low=MIN(low), Close=LAST(close)
- **Lookback**: period × 4 для точности двойного сглаживания
- **Порядок обработки**: От коротких к длинным (7 → 50)
- **Применение**: Определение силы тренда, фильтр для входа в позицию (ADX > 25), определение направления тренда (+DI vs -DI)

## Тестовые скрипты и утилиты

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

### Проверка статуса индикаторов:
```bash
# Общая проверка всех индикаторов
python indicators/check_indicators_status.py

# Проверка только RSI индикаторов
python indicators/check_rsi_status.py

# Проверка только VMA индикаторов
python indicators/check_vma_status.py

# Проверка только ATR индикаторов
python indicators/check_atr_status.py

# Проверка только MACD индикаторов
python indicators/check_macd_status.py
python indicators/check_macd_status.py --examples  # С примерами значений
python indicators/check_macd_status.py --gaps      # С проверкой пропусков
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
  - Время загрузки: ~5-10 минут для полной истории (все периоды)
  - Использует batch UPDATE для высокой производительности

- **EMA** (Exponential Moving Average) - `ema_loader.py`
  - Периоды: 9, 12, 21, 26, 50, 100, 200
  - Время загрузки: ~30-40 часов для полной истории
  - Использует построчные UPDATE из-за последовательной природы расчета

- **RSI** (Relative Strength Index) - `rsi_loader.py`
  - Периоды: 7, 9, 14, 21, 25
  - Время загрузки:
    - 1h: ~1 час (с batch-days=60)
    - 15m: ~3 часа (с batch-days=30)
    - 1m: ~30+ часов (с batch-days=7)
  - Все периоды рассчитываются параллельно в одном проходе

- **VMA** (Volume Moving Average) - `vma_loader.py`
  - Периоды: 10, 20, 50, 100, 200
  - Время загрузки: зависит от таймфрейма и объема данных
  - Периоды обрабатываются последовательно
  - Использует SUM(volume) для агрегации старших таймфреймов

- **ATR** (Average True Range) - `atr_loader.py`
  - Периоды: 7, 14, 21, 30, 50, 100
  - Время загрузки: зависит от таймфрейма (последовательный расчет как EMA)
  - Периоды обрабатываются последовательно
  - Использует сглаживание Уайлдера для плавности
  - Lookback период = period × 2 для стабильности

- **MACD** (Moving Average Convergence Divergence) - `macd_loader.py`
  - 8 конфигураций: classic (12,26,9), crypto (6,13,5), aggressive (5,35,5), balanced (8,17,9), scalping (5,13,3), swing (10,21,9), longterm (21,55,13), ultralong (50,200,9)
  - Каждая конфигурация = 3 колонки (line, signal, histogram) = 24 колонки всего
  - Время загрузки: ~30-40 часов для полной истории 1m (зависит от таймфрейма)
  - Конфигурации обрабатываются последовательно
  - Независимый расчёт EMA (не требует ema_loader.py)
  - Использует построчные UPDATE из-за последовательной природы EMA
  - Lookback период = max(slow, signal) × 3 для точности

- **Bollinger Bands** (BB) - `bollinger_bands_loader.py`
  - 13 конфигураций (11 SMA-based + 2 EMA-based): ultrafast (3,2.0), scalping (5,2.0), short (10,1.5), intraday (14,2.0), tight (20,1.0), golden (20,1.618), classic (20,2.0), wide (20,3.0), fibonacci (21,2.0), fibonacci_medium (34,2.0), fibonacci_long (89,2.0), classic_ema (20,2.0), golden_ema (20,1.618)
  - Каждая конфигурация = 6 колонок (upper, middle, lower, %B, bandwidth, squeeze) = 78 колонок всего
  - Время загрузки: ~30-40 часов для полной истории 1m (зависит от таймфрейма)
  - Конфигурации обрабатываются последовательно (от коротких к длинным)
  - Независимый расчёт SMA/EMA (не требует sma_loader.py/ema_loader.py)
  - Автоматическая конвертация типов (Decimal → float, numpy → Python native)
  - Использует batch UPDATE для производительности
  - Lookback период = period × 3 для точности на границах батчей
  - Squeeze threshold = 5% (фиксированный)
  - Checkpoint система для возобновления

- **ADX** (Average Directional Index) - `adx_loader.py`
  - 8 периодов: 7 (scalping), 10 (short-term), 14 (classic Wilder's), 20 (medium-term), 21 (Fibonacci), 25 (balanced), 30 (monthly), 50 (long-term)
  - Каждый период = 3 компонента (adx, +DI, -DI) = 24 колонки всего
  - Время загрузки: ~30-40 часов для полной истории 1m (зависит от таймфрейма)
  - Периоды обрабатываются последовательно (от коротких к длинным: 7 → 50)
  - Двойное сглаживание Уайлдера (TR/+DM/-DM → DI → DX → ADX)
  - Автоматическая конвертация типов (Decimal → float, numpy → Python native)
  - Использует batch UPDATE для производительности
  - Lookback период = period × 4 для точности двойного сглаживания
  - Checkpoint система для возобновления
  - Интерпретация: ADX < 25 (слабый тренд), 25-50 (сильный), 50-75 (очень сильный), 75-100 (экстремальный)
  - +DI vs -DI показывает направление тренда

- **Long/Short Ratio** (Соотношение длинных и коротких позиций) - `long_short_ratio_loader.py`
  - Источник: Bybit API `/v5/market/account-ratio` (реальные данные биржи)
  - 3 колонки: buy_ratio, sell_ratio, long_short_ratio (buy/sell)
  - Исторические данные с 20 июля 2020 года
  - Поддерживаемые таймфреймы: 15m, 1h (1m = NULL, API не поддерживает)
  - Время загрузки: зависит от таймфрейма (~185 батчей для 15m, ~47 для 1h на символ)
  - Batch размер: 1000 записей (максимум API)
  - Инкрементальная обработка 1m таймфрейма:
    - Первый запуск: устанавливает NULL для всех записей
    - Повторные запуски: обрабатывает только новые записи (не перезаписывает)
    - Безопасно для многократного запуска
  - Контр-трендовый индикатор: высокий ratio → риск падения, низкий → риск роста
  - Применение: market sentiment analysis, liquidation hunting, дивергенции с ценой

### 📋 Планируемые индикаторы:
- `stochastic_loader.py` - Stochastic Oscillator
- И другие...

Каждый загрузчик будет использовать тот же принцип:
1. Проверка/создание колонок
2. Инкрементальная загрузка
3. Батчевая обработка