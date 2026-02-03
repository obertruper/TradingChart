# Orderbook Data Collection Planning

## Overview

Документ для планирования сбора и хранения данных orderbook (стакана) с Bybit.

## Data Source

### URL Pattern
```
https://quote-saver.bycsi.com/orderbook/linear/{SYMBOL}/{DATE}_{SYMBOL}_ob{DEPTH}.data.zip
```

Пример:
```
https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2026-02-01_BTCUSDT_ob200.data.zip
```

### Available Depth Levels
- `ob1` - Top of book (1 level)
- `ob50` - 50 levels
- `ob200` - 200 levels
- `ob500` - 500 levels (not verified)
- `ob1000` - 1000 levels (not verified)

### File Size Estimates (per day, per symbol)

| Depth | Compressed | Uncompressed |
|-------|------------|--------------|
| ob200 | ~290 MB    | ~1.6 GB      |
| ob50  | ~TBD       | ~TBD         |
| ob1   | ~TBD       | ~TBD         |

## Data Format

### File Structure
- ZIP архив содержит один файл: `{DATE}_{SYMBOL}_ob{DEPTH}.data`
- Формат: JSON Lines (один JSON объект на строку)
- Кодировка: UTF-8

### Message Types

#### 1. Snapshot (полный снимок стакана)
```json
{
  "topic": "orderbook.200.BTCUSDT",
  "type": "snapshot",
  "ts": 1769904001070,
  "data": {
    "s": "BTCUSDT",
    "b": [["78704.30", "2.038"], ["78704.20", "0.002"], ...],
    "a": [["78704.40", "0.176"], ["78704.60", "0.001"], ...],
    "u": 16165145,
    "seq": 519462036654
  },
  "cts": 1769904001066
}
```

#### 2. Delta (инкрементальное обновление)
```json
{
  "topic": "orderbook.200.BTCUSDT",
  "type": "delta",
  "ts": 1769904001168,
  "data": {
    "s": "BTCUSDT",
    "b": [["78704.30", "1.036"], ...],
    "a": [["78704.40", "0.322"], ...],
    "u": 16165146,
    "seq": 519462037113
  },
  "cts": 1769904001166
}
```

### Field Description

| Field | Description |
|-------|-------------|
| `topic` | Канал подписки (orderbook.{depth}.{symbol}) |
| `type` | Тип сообщения: `snapshot` или `delta` |
| `ts` | Timestamp сервера Bybit (milliseconds) |
| `cts` | Cross timestamp - время создания сообщения (milliseconds) |
| `data.s` | Symbol (торговая пара) |
| `data.b` | Bids (заявки на покупку): [[price, size], ...] |
| `data.a` | Asks (заявки на продажу): [[price, size], ...] |
| `data.u` | Update ID |
| `data.seq` | Sequence number |

### Update Frequency
- Интервал между сообщениями: ~100ms (10 updates/sec)
- Примерно 864,000 сообщений в день на символ

---

## Implementation Options

### Option 1: Raw Data Storage (PostgreSQL)

**Описание:** Хранить все сообщения в PostgreSQL как есть.

**Таблица:**
```sql
CREATE TABLE orderbook_raw (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    type VARCHAR(10) NOT NULL,  -- snapshot/delta
    update_id BIGINT,
    seq BIGINT,
    bids JSONB,
    asks JSONB
);
```

**Pros:**
- Полное сохранение всех данных
- Возможность воспроизвести любой момент времени
- Гибкость в последующем анализе

**Cons:**
- Огромный объём данных (~1.6 GB/день/символ)
- 17 символов × 365 дней = ~10 TB/год
- Высокая нагрузка на БД при записи

---

### Option 2: Aggregated Metrics (PostgreSQL)

**Описание:** Агрегировать данные в метрики с заданным интервалом (1s, 5s, 1m).

**Метрики для хранения:**
```sql
CREATE TABLE orderbook_metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    -- Best bid/ask
    best_bid DECIMAL(20,8),
    best_ask DECIMAL(20,8),
    spread DECIMAL(20,8),
    spread_percent DECIMAL(10,6),

    -- Depth at levels (cumulative volume)
    bid_depth_5 DECIMAL(20,8),   -- Volume within 0.5% of best bid
    bid_depth_10 DECIMAL(20,8),  -- Volume within 1.0% of best bid
    bid_depth_50 DECIMAL(20,8),  -- Volume within 5.0% of best bid
    ask_depth_5 DECIMAL(20,8),
    ask_depth_10 DECIMAL(20,8),
    ask_depth_50 DECIMAL(20,8),

    -- Imbalance
    imbalance_ratio DECIMAL(10,6),  -- bid_volume / ask_volume

    -- Order count
    bid_levels INT,
    ask_levels INT,

    PRIMARY KEY (timestamp, symbol)
);
```

**Интервалы агрегации:**
- 1 second: 86,400 записей/день
- 5 seconds: 17,280 записей/день
- 1 minute: 1,440 записей/день

**Pros:**
- Значительно меньший объём данных
- Быстрые запросы для анализа
- Готовые метрики для индикаторов

**Cons:**
- Потеря детализации
- Невозможно восстановить исходный стакан

---

### Option 3: Hybrid Approach

**Описание:** Комбинация raw storage + aggregated metrics.

1. **Raw data** → хранить в файлах (Parquet/compressed JSON)
2. **Metrics** → хранить в PostgreSQL для быстрого доступа

**Структура:**
```
/data/orderbook/
├── raw/
│   └── BTCUSDT/
│       └── 2026-02-01_ob200.parquet.gz
└── db: orderbook_metrics (PostgreSQL)
```

**Pros:**
- Полные данные доступны для исследований
- Быстрый доступ к метрикам через БД
- Оптимальный баланс объёма и функциональности

**Cons:**
- Сложнее в реализации
- Два источника данных

---

### Option 4: Time-Series Database (TimescaleDB/ClickHouse)

**Описание:** Использовать специализированную БД для временных рядов.

**TimescaleDB (расширение PostgreSQL):**
```sql
CREATE TABLE orderbook_ticks (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    best_bid DECIMAL(20,8),
    best_ask DECIMAL(20,8),
    bid_volume DECIMAL(20,8),
    ask_volume DECIMAL(20,8)
);

SELECT create_hypertable('orderbook_ticks', 'timestamp');
```

**Pros:**
- Оптимизировано для временных рядов
- Автоматическое партиционирование
- Сжатие данных

**Cons:**
- Требует установки расширения
- Новая технология в стеке

---

## Derived Indicators

Список индикаторов, которые можно рассчитать из orderbook данных:

### 1. Spread Indicators
- **Spread** = best_ask - best_bid
- **Spread %** = spread / mid_price * 100
- **Average Spread (SMA)** - среднее значение спреда за период

### 2. Depth Indicators
- **Bid/Ask Depth** - суммарный объём на N уровнях
- **Depth Ratio** = bid_depth / ask_depth
- **Cumulative Depth at % levels** - объём в пределах X% от mid price

### 3. Imbalance Indicators
- **Order Book Imbalance (OBI)** = (bid_volume - ask_volume) / (bid_volume + ask_volume)
- **Weighted Imbalance** - с учётом расстояния от mid price

### 4. Liquidity Indicators
- **Slippage Estimate** - ожидаемое проскальзывание для объёма X
- **Market Impact** - изменение цены при исполнении ордера

### 5. Flow Indicators
- **Delta** = aggressive_buy_volume - aggressive_sell_volume
- **Cumulative Delta**

---

## Questions to Decide

1. **Какой depth level использовать?**
   - ob200 достаточно для большинства анализов
   - ob50 может быть достаточно для spread/imbalance
   - ob1 только для best bid/ask

2. **Какой интервал агрегации?**
   - 1s - высокая детализация, много данных
   - 5s - хороший баланс
   - 1m - минимум данных, подходит для sync с candles

3. **Какие символы собирать?**
   - Все 17 futures символов?
   - Только топ-5 (BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, BNBUSDT)?

4. **Период хранения raw data?**
   - 7 дней
   - 30 дней
   - Без лимита (архивное хранение)

5. **Storage backend для raw data?**
   - Local files (Parquet)
   - S3/Object Storage
   - PostgreSQL JSONB

---

## Sample Data Analysis

Анализ файла: `2026-02-01_BTCUSDT_ob200.data`

### Общая статистика
```
Размер: 1.6 GB (uncompressed), 277 MB (compressed)
Формат: JSON Lines
Сообщений за день: ~864,000
Сообщений за минуту: ~600
Интервал: ~100ms между сообщениями (10 updates/sec)
```

### Важно: Snapshot vs Delta

**Snapshot** - полный снимок стакана (200 уровней bids + 200 уровней asks)
**Delta** - только изменения (обновлённые уровни)

Для восстановления стакана в любой момент:
1. Взять последний snapshot
2. Последовательно применить все delta

**При применении delta:**
- Если `size > 0` → обновить уровень
- Если `size = 0` → удалить уровень

---

## Выбранная реализация: Полная таблица (56 колонок)

Максимальный уровень детализации для ML обучения. Все данные извлекаются ТОЛЬКО из orderbook файлов.

### Полный список колонок

| # | Группа | Колонка | Описание |
|---|--------|---------|----------|
| | **PRICE** | | Ценовые данные на конец минуты |
| 1 | | best_bid | Лучшая цена покупки |
| 2 | | best_ask | Лучшая цена продажи |
| 3 | | mid_price | (best_bid + best_ask) / 2 |
| 4 | | microprice | Взвешенная по объёму на лучших уровнях |
| 5 | | vwap_bid | VWAP для bid стороны (все 200 уровней) |
| 6 | | vwap_ask | VWAP для ask стороны |
| | **SPREAD** | | Спред и его динамика |
| 7 | | spread | Спред на конец минуты |
| 8 | | spread_pct | Спред в % от mid |
| 9 | | spread_min | Минимальный за минуту |
| 10 | | spread_max | Максимальный за минуту |
| 11 | | spread_avg | Средний за минуту |
| 12 | | spread_std | Стд. отклонение за минуту |
| | **BID VOLUME** | | Объёмы на стороне покупки |
| 13 | | bid_vol_01pct | Объём в пределах 0.1% от mid |
| 14 | | bid_vol_05pct | Объём в пределах 0.5% от mid |
| 15 | | bid_vol_10pct | Объём в пределах 1.0% от mid |
| 16 | | bid_volume | Полный объём (200 уровней) |
| 17 | | bid_vol_avg | Средний за минуту |
| 18 | | bid_vol_std | Стд. отклонение за минуту |
| | **ASK VOLUME** | | Объёмы на стороне продажи |
| 19 | | ask_vol_01pct | Объём в пределах 0.1% от mid |
| 20 | | ask_vol_05pct | Объём в пределах 0.5% от mid |
| 21 | | ask_vol_10pct | Объём в пределах 1.0% от mid |
| 22 | | ask_volume | Полный объём (200 уровней) |
| 23 | | ask_vol_avg | Средний за минуту |
| 24 | | ask_vol_std | Стд. отклонение за минуту |
| | **IMBALANCE** | | Дисбаланс стакана |
| 25 | | imbalance | (bid - ask) / (bid + ask), на конец минуты |
| 26 | | imbalance_01pct | Imbalance в пределах 0.1% от mid |
| 27 | | imbalance_min | Минимум за минуту |
| 28 | | imbalance_max | Максимум за минуту |
| 29 | | imbalance_avg | Среднее за минуту |
| 30 | | imbalance_std | Стд. отклонение |
| 31 | | imbalance_range | max - min |
| | **PRESSURE** | | Давление покупателей/продавцов |
| 32 | | buy_pressure | bid_vol_01pct / ask_vol_01pct |
| 33 | | depth_ratio | bid_vol_10pct / ask_vol_10pct |
| | **WALLS** | | "Стены" в стакане (крупные ордера) |
| 34 | | bid_wall_price | Цена с максимальным bid объёмом |
| 35 | | bid_wall_volume | Объём на этом уровне |
| 36 | | bid_wall_distance_pct | Расстояние от mid в % |
| 37 | | ask_wall_price | Цена с максимальным ask объёмом |
| 38 | | ask_wall_volume | Объём на этом уровне |
| 39 | | ask_wall_distance_pct | Расстояние от mid в % |
| | **SLIPPAGE** | | Проскальзывание (в долларовом эквиваленте) |
| 40 | | slippage_buy_10k | Проскальзывание при покупке на $10,000 |
| 41 | | slippage_buy_50k | Проскальзывание при покупке на $50,000 |
| 42 | | slippage_buy_100k | Проскальзывание при покупке на $100,000 |
| 43 | | slippage_sell_10k | Проскальзывание при продаже на $10,000 |
| 44 | | slippage_sell_50k | Проскальзывание при продаже на $50,000 |
| 45 | | slippage_sell_100k | Проскальзывание при продаже на $100,000 |
| | **LIQUIDITY** | | Метрики ликвидности |
| 46 | | liquidity_score | (bid_volume + ask_volume) / 2 |
| 47 | | bid_concentration | Топ-3 уровня / total |
| 48 | | ask_concentration | Топ-3 уровня / total |
| | **VOLATILITY** | | Волатильность внутри минуты |
| 49 | | mid_price_range | max(mid) - min(mid) за минуту |
| 50 | | mid_price_std | Стд. отклонение mid за минуту |
| 51 | | price_momentum | (close_mid - open_mid) / open_mid |
| | **ACTIVITY** | | Активность обновлений |
| 52 | | bid_levels | Количество ненулевых уровней bid |
| 53 | | ask_levels | Количество ненулевых уровней ask |
| 54 | | message_count | Количество обновлений за минуту |
| 55 | | best_bid_changes | Сколько раз менялся best_bid |
| 56 | | best_ask_changes | Сколько раз менялся best_ask |

---

### Описание групп колонок

#### PRICE (цены)
- **best_bid/best_ask** — базовые цены стакана
- **mid_price** — "справедливая" цена для анализа
- **microprice** — более точная оценка цены: `(best_bid × ask_vol + best_ask × bid_vol) / (bid_vol + ask_vol)`
- **vwap_bid/vwap_ask** — средневзвешенные цены по всем 200 уровням

#### SPREAD (спред)
- Низкий спред = высокая ликвидность
- **spread_max** — детектор моментов низкой ликвидности (если `spread_max >> spread`, был "пустой" стакан)
- **spread_std** — стабильность ликвидности

#### VOLUME (объёмы)
- **vol_01pct** — ликвидность "у цены" (самая важная для исполнения)
- **vol_05pct/vol_10pct** — глубина стакана
- **vol_avg/vol_std** — стабильность объёмов за минуту

#### IMBALANCE (дисбаланс)
- Формула: `(bid_volume - ask_volume) / (bid_volume + ask_volume)`
- Диапазон: от -1 (только продавцы) до +1 (только покупатели)
- **imbalance_range** = `max - min` — волатильность давления за минуту

#### WALLS (стены)
- Крупные ордера, которые могут служить поддержкой/сопротивлением
- **wall_distance_pct** — насколько далеко стена от текущей цены

#### SLIPPAGE (проскальзывание)
- Используем **долларовые значения** (10k, 50k, 100k) вместо BTC для универсальности
- Позволяет сравнивать ликвидность между разными парами (BTCUSDT, ETHUSDT, SOLUSDT)

#### LIQUIDITY (ликвидность)
- **liquidity_score** — общий показатель ликвидности
- **concentration** — насколько объём сконцентрирован на топ-уровнях (высокая = "стена")

#### VOLATILITY (волатильность)
- **mid_price_range** — диапазон цены внутри минуты
- **price_momentum** — направление движения за минуту

#### ACTIVITY (активность)
- **message_count** — ~600 в норме, меньше = низкая активность
- **best_bid/ask_changes** — частота изменения лучших цен

---

### Пример данных

| Группа | Колонка | 02:00:00 | 02:01:00 | 02:02:00 |
|--------|---------|----------|----------|----------|
| **PRICE** | best_bid | 78645.50 | 78640.70 | 78786.70 |
| | best_ask | 78645.60 | 78640.80 | 78786.80 |
| | mid_price | 78645.55 | 78640.75 | 78786.75 |
| | microprice | 78645.54 | 78640.76 | 78786.74 |
| **SPREAD** | spread | 0.10 | 0.10 | 0.10 |
| | spread_pct | 0.000127 | 0.000127 | 0.000127 |
| | spread_min | 0.10 | 0.10 | 0.10 |
| | spread_max | 4.90 | 3.00 | 0.10 |
| | spread_avg | 0.11 | 0.11 | 0.10 |
| **BID** | bid_vol_01pct | 58.713 | 104.380 | 93.520 |
| | bid_volume | 58.713 | 104.380 | 93.520 |
| | bid_vol_avg | 73.101 | 87.538 | 88.970 |
| **ASK** | ask_vol_01pct | 84.503 | 79.105 | 78.204 |
| | ask_volume | 84.503 | 79.105 | 78.204 |
| | ask_vol_avg | 82.125 | 72.545 | 73.152 |
| **IMBALANCE** | imbalance | -0.1801 | 0.1377 | 0.0892 |
| | imbalance_min | -0.4359 | -0.4725 | -0.3390 |
| | imbalance_max | 0.5533 | 0.6440 | 0.5648 |
| | imbalance_range | 0.9892 | 1.1165 | 0.9038 |
| **PRESSURE** | buy_pressure | 0.69 | 1.32 | 1.20 |
| | depth_ratio | 0.69 | 1.32 | 1.20 |
| **WALLS** | bid_wall_price | 78600.00 | 78590.00 | 78750.00 |
| | bid_wall_volume | 15.5 | 22.3 | 18.7 |
| | bid_wall_distance_pct | 0.058 | 0.065 | 0.047 |
| **SLIPPAGE** | slippage_buy_10k | 0.15 | 0.12 | 0.13 |
| | slippage_buy_100k | 1.85 | 1.42 | 1.55 |
| **VOLATILITY** | mid_price_range | 94.20 | 34.50 | 155.80 |
| | price_momentum | -0.0007 | -0.0001 | 0.0019 |
| **ACTIVITY** | message_count | 590 | 600 | 600 |
| | best_bid_changes | 145 | 132 | 168 |

**Интерпретация:**
- `spread_max = 4.90` в 02:00 → был момент низкой ликвидности
- `imbalance_range = 1.1165` в 02:01 → сильная борьба покупателей/продавцов
- `buy_pressure = 1.32` в 02:01 → покупатели доминируют
- `price_momentum = 0.0019` в 02:02 → цена растёт

---

### SQL схема таблицы

```sql
CREATE TABLE orderbook_bybit_futures_1m (
    -- Первичный ключ
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    -- PRICE (6 колонок)
    best_bid DECIMAL(20,8),
    best_ask DECIMAL(20,8),
    mid_price DECIMAL(20,8),
    microprice DECIMAL(20,8),
    vwap_bid DECIMAL(20,8),
    vwap_ask DECIMAL(20,8),

    -- SPREAD (6 колонок)
    spread DECIMAL(20,8),
    spread_pct DECIMAL(10,6),
    spread_min DECIMAL(20,8),
    spread_max DECIMAL(20,8),
    spread_avg DECIMAL(20,8),
    spread_std DECIMAL(20,8),

    -- BID VOLUME (6 колонок)
    bid_vol_01pct DECIMAL(20,8),
    bid_vol_05pct DECIMAL(20,8),
    bid_vol_10pct DECIMAL(20,8),
    bid_volume DECIMAL(20,8),
    bid_vol_avg DECIMAL(20,8),
    bid_vol_std DECIMAL(20,8),

    -- ASK VOLUME (6 колонок)
    ask_vol_01pct DECIMAL(20,8),
    ask_vol_05pct DECIMAL(20,8),
    ask_vol_10pct DECIMAL(20,8),
    ask_volume DECIMAL(20,8),
    ask_vol_avg DECIMAL(20,8),
    ask_vol_std DECIMAL(20,8),

    -- IMBALANCE (7 колонок)
    imbalance DECIMAL(10,6),
    imbalance_01pct DECIMAL(10,6),
    imbalance_min DECIMAL(10,6),
    imbalance_max DECIMAL(10,6),
    imbalance_avg DECIMAL(10,6),
    imbalance_std DECIMAL(10,6),
    imbalance_range DECIMAL(10,6),

    -- PRESSURE (2 колонки)
    buy_pressure DECIMAL(10,6),
    depth_ratio DECIMAL(10,6),

    -- WALLS (6 колонок)
    bid_wall_price DECIMAL(20,8),
    bid_wall_volume DECIMAL(20,8),
    bid_wall_distance_pct DECIMAL(10,6),
    ask_wall_price DECIMAL(20,8),
    ask_wall_volume DECIMAL(20,8),
    ask_wall_distance_pct DECIMAL(10,6),

    -- SLIPPAGE (6 колонок) - долларовый эквивалент для универсальности
    slippage_buy_10k DECIMAL(20,8),
    slippage_buy_50k DECIMAL(20,8),
    slippage_buy_100k DECIMAL(20,8),
    slippage_sell_10k DECIMAL(20,8),
    slippage_sell_50k DECIMAL(20,8),
    slippage_sell_100k DECIMAL(20,8),

    -- LIQUIDITY (3 колонки)
    liquidity_score DECIMAL(20,8),
    bid_concentration DECIMAL(10,6),
    ask_concentration DECIMAL(10,6),

    -- VOLATILITY (3 колонки)
    mid_price_range DECIMAL(20,8),
    mid_price_std DECIMAL(20,8),
    price_momentum DECIMAL(10,6),

    -- ACTIVITY (5 колонок)
    bid_levels INT,
    ask_levels INT,
    message_count INT,
    best_bid_changes INT,
    best_ask_changes INT,

    PRIMARY KEY (timestamp, symbol)
);

-- Индексы
CREATE INDEX idx_ob_symbol_ts ON orderbook_bybit_futures_1m (symbol, timestamp);
CREATE INDEX idx_ob_timestamp ON orderbook_bybit_futures_1m (timestamp);
```

### Оценка размера данных

| Метрика | Значение |
|---------|----------|
| Колонок | 56 (+ timestamp, symbol = 58) |
| Байт на запись | ~500 bytes |
| Записей в день (17 символов) | 24,480 |
| Записей в год | ~9,000,000 |
| **Размер в год (без индексов)** | **~4.5 GB** |

**Сравнение с другими таблицами:**
- candles_bybit_futures_1m: ~90 bytes/row, 8 колонок
- indicators_bybit_futures_1m: ~4.7 KB/row, 206 колонок
- orderbook_bybit_futures_1m: ~500 bytes/row, 58 колонок

### Почему отдельная таблица?

Можно было бы добавить колонки в `indicators_bybit_futures_1m`, но:
- Таблица уже 113 GB и 206 колонок
- Независимая загрузка данных
- Можно удалить/пересоздать без влияния на indicators
- Проще управлять и отлаживать

### Примечание о Slippage колонках

Используем **долларовые значения** ($10k, $50k, $100k) вместо количества монет (1 BTC, 5 BTC):
- Универсально для всех торговых пар
- $10k BTCUSDT ≈ $10k ETHUSDT ≈ $10k SOLUSDT
- Легко сравнивать ликвидность между парами

---

## Алгоритм обработки raw data

```python
def process_orderbook_file(filepath: str) -> List[Dict]:
    """
    Обрабатывает raw orderbook file и возвращает 1-минутные метрики
    """
    orderbook = {'bids': {}, 'asks': {}}
    minutes_data = defaultdict(list)

    with open(filepath, 'r') as f:
        for line in f:
            msg = json.loads(line)

            # Применяем snapshot или delta
            if msg['type'] == 'snapshot':
                orderbook['bids'] = {float(b[0]): float(b[1]) for b in msg['data']['b']}
                orderbook['asks'] = {float(a[0]): float(a[1]) for a in msg['data']['a']}
            else:
                for price, size in msg['data']['b']:
                    p, s = float(price), float(size)
                    if s == 0:
                        orderbook['bids'].pop(p, None)
                    else:
                        orderbook['bids'][p] = s
                for price, size in msg['data']['a']:
                    p, s = float(price), float(size)
                    if s == 0:
                        orderbook['asks'].pop(p, None)
                    else:
                        orderbook['asks'][p] = s

            # Группируем по минутам
            minute = datetime.fromtimestamp(msg['ts']/1000).replace(second=0, microsecond=0)
            minutes_data[minute].append(copy.deepcopy(orderbook))

    # Для каждой минуты берём ПОСЛЕДНЕЕ состояние (конец минуты)
    results = []
    for minute, states in minutes_data.items():
        final_state = states[-1]
        metrics = calculate_metrics(final_state, len(states))
        metrics['timestamp'] = minute
        results.append(metrics)

    return results
```

---

## Next Steps

1. [x] Определить приоритетный вариант реализации → **1-минутная агрегация**
2. [x] Создать схему БД для выбранного варианта → **orderbook_bybit_futures_1m**
3. [ ] Создать таблицу на VPS
4. [ ] Написать скрипт загрузки и обработки raw data
5. [ ] Написать loader для загрузки в PostgreSQL
6. [ ] Протестировать на данных за 1 день
7. [ ] Запустить массовую загрузку исторических данных

---

## References

- [Bybit WebSocket API - Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
- [Bybit Historical Data](https://public.bybit.com/)
- Sample data: `data_samples/orderbook/2026-02-01_BTCUSDT_ob200.data.zip`
