# Data Pipeline & Gap Filling Guide

Документ описывает как устроен пайплайн расчёта данных и что делать при обнаружении gap (пропусков/NULL) для каждого индикатора.

**Последнее обновление:** 2026-02-26

---

## 1. Общий пайплайн данных

```
Bybit API (1m свечи)
       │
       ▼
┌─────────────────────┐
│  candles_bybit_*_1m │  ← data_loader_futures.py / data_loader_spot.py (историч.)
│  (OHLCV, 8 колонок) │  ← monitor.py / monitor_spot.py (real-time, daemon)
└─────────┬───────────┘
          │
          ▼
┌──────────────────────────────┐
│  indicators_bybit_futures_*  │  ← 23 indicator loaders
│  (261 колонка, 5 таблиц)    │     start_all_loaders.py (оркестратор, cron 01:00 UTC)
│  1m → 15m → 1h → 4h → 1d   │
└──────────────────────────────┘

Отдельные таблицы (не в indicators):
  ├── orderbook_bybit_futures_1m     ← orderbook_bybit_loader.py
  ├── orderbook_binance_futures_1m   ← orderbook_binance_loader.py
  ├── options_deribit_dvol_1h/1m     ← options_dvol_loader.py
  ├── options_deribit_dvol_indicators_1h  ← options_dvol_indicators_loader.py
  └── options_deribit_aggregated_15m ← options_aggregated_loader.py
```

### Как загрузчик индикатора работает (стандартный паттерн)

```
1. Загрузка конфига     → indicators_config.yaml (периоды, символы, таймфреймы)
2. Создание колонок     → ensure_columns() — ALTER TABLE ADD COLUMN IF NOT EXISTS
3. Определение диапазона → get_date_range() — от последней заполненной даты до now
4. Загрузка свечей      → SELECT из candles_bybit_futures_1m
5. Агрегация            → 1m → 15m/1h/4h/1d (если нужно)
6. Расчёт индикатора    → pandas/numpy на DataFrame
7. Запись в БД          → UPDATE ... SET col = val WHERE timestamp = %s AND symbol = %s
8. Прогресс             → tqdm progress bar
```

**Важно:** Все загрузчики используют **UPDATE** (не INSERT). Строки должны уже существовать в таблице indicators.

---

## 2. Типы gap и их причины

| Тип gap | Причина | Где возникает |
|---------|---------|---------------|
| **NULL в свечах** | API недоступен, биржа не торгует | candles_bybit_*_1m |
| **NULL в индикаторах** | Загрузчик не дошёл, монитор добавил новые строки | indicators_bybit_futures_* |
| **Естественный NULL** | Первые N записей (warm-up период индикатора) | indicators — нормально |
| **NULL в API-данных** | API не поддерживает таймфрейм (L/S Ratio на 1m = NULL) | По дизайну |

### Как обнаружить gap

```bash
# Проверка свечей
cd data_collectors/bybit/futures && python3 check_data.py

# Проверка индикаторов
cd indicators && python3 check_indicators_status.py

# Проверка конкретного индикатора
python3 check_atr_status.py
python3 check_adx_status.py
```

---

## 3. Классификация индикаторов по сложности gap filling

### Ключевая концепция: cumulative vs non-cumulative

```
NON-CUMULATIVE (независимые значения):
  Каждое значение зависит ТОЛЬКО от фиксированного окна данных.
  Gap filling: загрузить данные вокруг gap + lookback → пересчитать → быстро.
  Пример: SMA(50) в точке T = среднее close[T-49 : T]

CUMULATIVE (цепочка зависимостей):
  Каждое значение зависит от ВСЕХ предыдущих значений через цепочку.
  Gap filling: ПОЛНЫЙ пересчёт с начала истории → медленно, но единственный способ.
  Пример: EMA(50) в точке T = α × close[T] + (1-α) × EMA(50)[T-1]
           RSI: Wilder smoothing — каждое avg_gain зависит от предыдущего
           ATR: ATR[i] = (ATR[i-1] × (period-1) + TR[i]) / period
```

---

## 4. Сводная таблица: все 26 загрузчиков

### Технические индикаторы (15 загрузчиков)

| # | Индикатор | Тип | Колонок | Gap filling | Команда |
|---|-----------|-----|---------|-------------|---------|
| 1 | **SMA** | Non-cumulative | 5 | Локальный lookback (быстро) | `--check-nulls` |
| 2 | **EMA** | **Cumulative** | 7 | **Полный пересчёт с начала** | `--check-nulls` (медленно!) |
| 3 | **RSI** | **Cumulative** (Wilder) | 5 | **Полный пересчёт с начала** | `--check-nulls` (медленно!) |
| 4 | **VMA** | Non-cumulative | 5 | Локальный lookback (быстро) | `--check-nulls` |
| 5 | **ATR + NATR** | **Cumulative** (Wilder) | 12 | **Полный пересчёт с начала** | `--force-reload` |
| 6 | **ADX** | **Cumulative** (2× Wilder) | 24 | Пересчёт с учётом boundary | `--check-nulls` |
| 7 | **MACD** | **Cumulative** (EMA-based) | 24 | Пересчёт от boundary | `--check-nulls` |
| 8 | **Bollinger Bands** | Non-cumulative | 78 | Локальный lookback (быстро) | `--check-nulls` |
| 9 | **VWAP** | Non-cumulative | 16 | Локальный lookback (быстро) | `--check-nulls` |
| 10 | **MFI** | Non-cumulative | 5 | Локальный lookback (быстро) | `--check-nulls` |
| 11 | **Stochastic + Williams %R** | Non-cumulative | 21 | Локальный lookback (быстро) | `--check-nulls` |
| 12 | **OBV** | **Cumulative** | 1 | **Всегда полный пересчёт** | Автоматически |
| 13 | **Ichimoku** | Non-cumulative* | 16 | Локальный lookback | `--check-nulls` |
| 14 | **HV** | Non-cumulative | 8 | Локальный lookback | `--check-nulls` |
| 15 | **SuperTrend** | **Cumulative** (ATR-based) | 21 | Полный пересчёт | `--force-reload` |

### Рыночные данные из API (6 загрузчиков)

| # | Данные | Источник | Колонок | Gap filling | Команда |
|---|--------|----------|---------|-------------|---------|
| 16 | **Open Interest** | Bybit API | 1 | Перезапросить API | `--force-reload` |
| 17 | **Funding Rate** | Bybit API | 2 | Авто-gap detection на каждом запуске | Автоматически |
| 18 | **Long/Short Ratio** | Bybit API | 3 | Перезапросить API с 2020-07-20 | `--force-reload` |
| 19 | **Premium Index** | Bybit API | 1 | Авто-gap detection после основной загрузки | Автоматически |
| 20 | **Fear & Greed (Alt.)** | Alternative.me | 2 | Запрос NULL дат из БД | `--check-nulls` |
| 21 | **Fear & Greed (CMC)** | CoinMarketCap | 2+ | Запрос NULL дат из БД | `--check-nulls` |

### Orderbook и Options (5 загрузчиков, отдельные таблицы)

| # | Данные | Источник | Колонок | Gap filling | Команда |
|---|--------|----------|---------|-------------|---------|
| 22 | **Orderbook Bybit** | Bybit archives | 60 | Перезагрузить дни | `--force-reload` |
| 23 | **Orderbook Binance** | Binance archives | 46 | Найти NULL дни, перезагрузить | `--check-nulls` |
| 24 | **DVOL** | Deribit API | 6 | Перезапросить API | `--force-reload` |
| 25 | **DVOL Indicators** | Расчёт из DVOL | 22 | Пересчитать группу | `--force-reload` / `--group` |
| 26 | **Options Aggregated** | Расчёт из raw | 24 | Пересчитать группу | `--force-reload` / `--group` |

---

## 5. Детали gap filling по категориям

### 5.1 Non-cumulative индикаторы (быстрое заполнение)

**SMA, VMA, Bollinger Bands, VWAP, MFI, Stochastic, Williams %R, HV, Ichimoku**

Каждое значение зависит только от фиксированного окна (например, SMA(200) = среднее последних 200 close). Поэтому для заполнения NULL достаточно загрузить данные вокруг gap с небольшим lookback.

**Как работает `--check-nulls`:**
1. Находит timestamps с NULL (исключая естественный warm-up в начале данных)
2. Загружает свечи вокруг NULL с lookback = max_period (напр. 200 для SMA)
3. Рассчитывает индикатор только для NULL точек
4. Записывает результат в БД

**Скорость:** секунды — обрабатывает только проблемные точки.

**Естественный NULL boundary (первые записи, где NULL нормален):**
- SMA: первые `max_period` записей (200 записей)
- Bollinger: первые `max_period` записей
- Stochastic: первые `K_period + D_period` записей
- Ichimoku: первые `max(senkou_b_period, chikou_period)` записей
- HV: первые `max_period` записей

```bash
# Заполнить gap для SMA (быстро)
python3 sma_loader.py --check-nulls
python3 sma_loader.py --check-nulls --symbol BTCUSDT

# Заполнить gap для Bollinger (быстро)
python3 bollinger_bands_loader.py --check-nulls

# Заполнить gap для VWAP (быстро)
python3 vwap_loader.py --check-nulls
```

---

### 5.2 Cumulative индикаторы (полный пересчёт)

**EMA, RSI, ATR, ADX, MACD, OBV, SuperTrend**

Каждое значение зависит от всех предыдущих через цепочку формул. Один NULL в середине ломает всю цепочку после него.

**Почему нельзя заполнить локально:**
```
EMA[100] = α × close[100] + (1-α) × EMA[99]
EMA[99]  = α × close[99]  + (1-α) × EMA[98]
...
EMA[1]   = close[1]  ← начало цепочки

Если EMA[50] = NULL → все EMA[51..100] будут неточными
```

**Аналогично для RSI (Wilder smoothing):**
```
avg_gain[i] = (avg_gain[i-1] × (period-1) + gain[i]) / period
```

**Аналогично для ATR (Wilder smoothing):**
```
ATR[i] = (ATR[i-1] × (period-1) + TR[i]) / period
```

**ADX ещё сложнее — двойное сглаживание Wilder:**
```
DI smoothing (первый Wilder) → DX calculation → ADX smoothing (второй Wilder)
```

**Как работает `--check-nulls` для cumulative индикаторов:**
1. Находит timestamps с NULL (после естественного boundary)
2. Загружает ВСЕ данные **с самого начала истории**
3. Рассчитывает индикатор полностью (полная цепочка)
4. Записывает только NULL точки (остальные не трогает)

**Скорость:** минуты-часы (загрузка всей истории + полный расчёт).

```bash
# EMA — полный пересчёт при --check-nulls (медленно, но точно)
python3 ema_loader.py --check-nulls
python3 ema_loader.py --check-nulls --symbol ETHUSDT

# RSI — полный пересчёт (Wilder smoothing)
python3 rsi_loader.py --check-nulls

# ATR — только --force-reload (нет --check-nulls, всегда полный пересчёт)
python3 atr_loader.py --force-reload

# OBV — всегда полный пересчёт (по дизайну)
python3 obv_loader.py

# MACD — пересчёт от boundary (EMA-based)
python3 macd_loader.py --check-nulls

# ADX — пересчёт (двойной Wilder)
python3 adx_loader.py --check-nulls

# SuperTrend — полный пересчёт
python3 supertrend_loader.py --force-reload
```

---

### 5.3 API-sourced данные (перезапрос с API)

**Open Interest, Funding Rate, Long/Short Ratio, Premium Index, Fear & Greed**

Gap в этих данных означает, что данные не были загружены с API. Заполнение — повторный запрос к API.

**Особенности каждого загрузчика:**

| Загрузчик | Gap detection | Заполнение | Примечание |
|-----------|---------------|------------|------------|
| **Funding Rate** | Автоматически на каждом запуске (stage 2) | Находит NULL в диапазоне API → запрашивает | Самый умный — не нужен --force-reload для gap |
| **Premium Index** | Автоматически после основной загрузки | Находит NULL → запрашивает дни у API | Daily batching oldest→newest |
| **Fear & Greed (оба)** | `--check-nulls` ищет даты с NULL | Запрашивает только NULL даты у API | Быстро — обрабатывает только пропуски |
| **Open Interest** | Нет авто-detection | `--force-reload` с Oct 2023 | Перезагрузит всё |
| **Long/Short Ratio** | Нет авто-detection | `--force-reload` с 2020-07-20 | Перезагрузит всё |

**NULL по дизайну (это НЕ gap):**
- Long/Short Ratio на 1m = NULL (API не поддерживает, данные только 15m и 1h)
- Open Interest на 1m = NULL (минимальный API интервал 5min)

```bash
# Funding Rate — gap заполнится автоматически при обычном запуске
python3 funding_fee_loader.py

# Fear & Greed — найти и заполнить NULL даты
python3 fear_and_greed_loader_alternative.py --check-nulls
python3 fear_and_greed_coinmarketcap_loader.py --check-nulls

# Open Interest — полная перезагрузка
python3 open_interest_loader.py --force-reload

# Long/Short Ratio — полная перезагрузка
python3 long_short_ratio_loader.py --force-reload
```

---

### 5.4 Orderbook данные (перезагрузка дней)

**Orderbook Bybit, Orderbook Binance**

Gap = дни, в которые данные не были загружены или загрузились с ошибкой.

| Загрузчик | Gap detection | Заполнение |
|-----------|---------------|------------|
| **Orderbook Bybit** | Нет авто-detection | `--force-reload` с 2023-01-18 |
| **Orderbook Binance** | `--check-nulls` ищет дни с NULL bookDepth | Перезагружает только NULL дни |

**Binance особенности:**
- bookTicker: NULL до 2023-05-16 и после 2024-04-01 — **это нормально** (Binance прекратил публикацию)
- bookDepth ±0.2% levels: NULL до 2026-01-15 — **нормально** (новый формат)
- `--check-nulls` также запускает monthly bookTicker fallback (ищет месячные архивы)

```bash
# Binance — найти и перезагрузить NULL дни
python3 orderbook_binance_loader.py --check-nulls

# Bybit — полная перезагрузка (долго)
python3 orderbook_bybit_loader.py --force-reload
```

---

### 5.5 Options/DVOL данные (перезапрос или пересчёт)

| Загрузчик | Тип данных | Gap filling |
|-----------|-----------|-------------|
| **DVOL** | Raw OHLC с Deribit API | `--force-reload` (перезапрос) |
| **DVOL Indicators** | Расчёт из DVOL close + HV_30 | `--force-reload` или `--group <name>` |
| **Options Aggregated** | Расчёт из options_deribit_raw | `--force-reload` или `--group <name>` |

**Per-group архитектура:** DVOL Indicators и Options Aggregated отслеживают каждую группу независимо. Можно пересчитать одну группу, не трогая остальные.

```bash
# DVOL — перезагрузить всё
python3 options_dvol_loader.py --force-reload

# DVOL Indicators — пересчитать одну группу
python3 options_dvol_indicators_loader.py --group rsi --currency BTC
python3 options_dvol_indicators_loader.py --force-reload  # все группы

# Options Aggregated — пересчитать одну группу
python3 options_aggregated_loader.py --group iv --currency ETH
python3 options_aggregated_loader.py --force-reload  # все группы
```

---

## 6. Шпаргалка: что делать при gap

### Сценарий 1: Монитор добавил новые записи, индикаторы NULL

Самый частый случай. Монитор (monitor.py) добавляет свечи в candles, но индикаторы ещё не рассчитаны для новых строк.

```bash
# Вариант A: Запустить оркестратор (считает ВСЕ индикаторы последовательно)
cd indicators
python3 start_all_loaders.py

# Вариант B: Запустить конкретный загрузчик (инкрементально)
python3 sma_loader.py          # досчитает только новые записи
python3 ema_loader.py          # досчитает только новые записи
```

### Сценарий 2: NULL в середине данных

Обнаружены NULL значения не в начале (warm-up), а в середине исторических данных.

```bash
# Шаг 1: Проверить масштаб проблемы
python3 check_indicators_status.py

# Шаг 2: Для non-cumulative — быстрое заполнение
python3 sma_loader.py --check-nulls
python3 bollinger_bands_loader.py --check-nulls
python3 vwap_loader.py --check-nulls

# Шаг 3: Для cumulative — полный пересчёт (медленнее)
python3 ema_loader.py --check-nulls          # полный пересчёт
python3 rsi_loader.py --check-nulls          # полный пересчёт
python3 atr_loader.py --force-reload         # полный пересчёт
```

### Сценарий 3: Данные повреждены или неточны

После исправления бага в расчёте, нужен полный пересчёт.

```bash
# Полный пересчёт конкретного индикатора
python3 sma_loader.py --force-reload
python3 ema_loader.py --force-reload
python3 rsi_loader.py --force-reload
python3 atr_loader.py --force-reload

# Полный пересчёт для конкретного символа и таймфрейма
python3 rsi_loader.py --force-reload --symbol BTCUSDT --timeframe 1h
```

### Сценарий 4: Gap в свечах (candles)

Пропуски в базовых данных OHLCV. Нужно заполнить свечи, потом индикаторы.

```bash
# Шаг 1: Заполнить gap в свечах
cd data_collectors/bybit/futures
python3 monitor.py --check-once --symbol BTCUSDT  # проверит и заполнит

# Шаг 2: Пересчитать индикаторы для заполненного периода
cd indicators
python3 start_all_loaders.py
```

---

## 7. Время выполнения gap filling

| Операция | Время | Пример |
|----------|-------|--------|
| SMA `--check-nulls` (5 NULL) | ~10 сек | Локальный lookback |
| EMA `--check-nulls` (5 NULL) | ~2-5 мин | Полный пересчёт 1 символа |
| RSI `--check-nulls` (5 NULL) | ~2-5 мин | Полный пересчёт 1 символа |
| ATR `--force-reload` (1 период) | ~35 мин | DB write — узкое место |
| ADX `--check-nulls` | ~5-10 мин | Пересчёт от boundary |
| `start_all_loaders.py` (инкрементально) | ~30-60 мин | Все 26 загрузчиков |
| `start_all_loaders.py --check-nulls` | ~2-4 часа | Все загрузчики с проверкой NULL |
| Полный `--force-reload` всех индикаторов | ~12-24 часа | Перезапись всей БД |

---

## 8. Таймфреймы и агрегация

Все индикаторы рассчитываются на 5 таймфреймах:

| Таймфрейм | Таблица | Агрегация | Записей |
|-----------|---------|-----------|---------|
| 1m | indicators_bybit_futures_1m | Прямое чтение | 25.6M |
| 15m | indicators_bybit_futures_15m | 15 × 1m свечей | 1.7M |
| 1h | indicators_bybit_futures_1h | 60 × 1m свечей | 437K |
| 4h | indicators_bybit_futures_4h | 240 × 1m свечей | — |
| 1d | indicators_bybit_futures_1d | 1440 × 1m свечей | — |

**Стандарт Bybit:** timestamp = START периода.
- Timestamp `14:00` (1h) содержит данные свечей `14:00:00` - `14:59:59`
- Timestamp `14:15` (15m) содержит данные свечей `14:15:00` - `14:29:59`

**Формулы агрегации в SQL:**
```sql
-- 1h
date_trunc('hour', timestamp)

-- 15m
date_trunc('hour', timestamp) + INTERVAL '15 minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / 15)

-- 4h
date_trunc('day', timestamp) + INTERVAL '4 hours' * (EXTRACT(HOUR FROM timestamp)::integer / 4)

-- 1d
date_trunc('day', timestamp)
```
