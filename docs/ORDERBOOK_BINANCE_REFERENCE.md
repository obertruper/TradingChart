# Справочник: Binance Orderbook Data (Данные стакана Binance)

Документ описывает таблицу `orderbook_binance_futures_1m` — агрегированные данные стакана с биржи Binance Futures.

**Скрипт:** `indicators/orderbook_binance_loader.py`
**Таблица:** `orderbook_binance_futures_1m`
**Дата создания:** 2026-02-10

---

## Оглавление

1. [Зачем нужны данные Binance](#зачем-нужны-данные-binance)
2. [Источники данных](#источники-данных)
3. [Структура таблицы](#структура-таблицы)
4. [bookTicker колонки — Price](#bookticker--price-6-колонок)
5. [bookTicker колонки — Spread](#bookticker--spread-6-колонок)
6. [bookTicker колонки — Volatility](#bookticker--volatility-3-колонки)
7. [bookTicker колонки — Activity](#bookticker--activity-3-колонки)
8. [bookTicker колонки — Quantity Stats](#bookticker--quantity-stats-4-колонки)
9. [bookDepth колонки — Bid/Ask Depth](#bookdepth--bidask-depth-10-колонок)
10. [bookDepth колонки — Notional](#bookdepth--notional-2-колонки)
11. [bookDepth колонки — ±0.2% Levels](#bookdepth--02-levels-2-колонки)
12. [bookDepth колонки — Imbalance](#bookdepth--imbalance-4-колонки)
13. [bookDepth колонки — Pressure & Liquidity](#bookdepth--pressure--liquidity-3-колонки)
14. [bookDepth колонки — Activity](#bookdepth--activity-1-колонка)
15. [Сравнение с Bybit Orderbook](#сравнение-с-bybit-orderbook)
16. [Применение для ML](#применение-для-ml)
17. [SQL примеры](#sql-примеры)
18. [Технические детали](#технические-детали)

---

## Зачем нужны данные Binance

**Binance** — крупнейшая криптобиржа, занимающая **55-65% мирового объёма фьючерсных торгов**. Данные Binance дополняют существующие данные Bybit:

| Аспект | Bybit (`orderbook_bybit_futures_1m`) | Binance (`orderbook_binance_futures_1m`) |
|--------|--------------------------------------|------------------------------------------|
| Доля рынка | ~10-15% | ~55-65% |
| Источник | Архивы Bybit (JSON, snapshots+deltas) | Публичные архивы `data.binance.vision` (CSV) |
| Глубина | Полный стакан (200-500 уровней) | Агрегированная (% уровни: ±1-5%) |
| Колонок | 60 (вкл. 2 JSONB top-50 levels) | 46 (только числовые) |
| Детализация | Walls, Slippage, Concentration, JSONB levels | Depth по %, Imbalance, Bid/Ask tickers |

### Преимущества кросс-биржевого анализа

1. **Арбитражные сигналы**: расхождение imbalance между Binance и Bybit
2. **Подтверждение ликвидности**: если оба стакана показывают одинаковый дисбаланс — сигнал сильнее
3. **Макро-ликвидность**: Binance показывает «глобальную» картину рынка
4. **Предсказание потоков**: крупные ордера на Binance часто предшествуют движению на Bybit

---

## Источники данных

### bookDepth (с 2023-01-01)

**URL:** `https://data.binance.vision/data/futures/um/daily/bookDepth/{SYMBOL}/{SYMBOL}-bookDepth-{YYYY-MM-DD}.zip`

**Формат CSV:** `timestamp, percentage, depth, notional`
- `timestamp` — дата и время снимка (строка, напр. `2023-01-01 00:06:05`)
- `percentage` — процентный уровень от цены (напр. `-1`, `-2`, `+3`, `0.2`)
- `depth` — объём в базовой валюте (BTC)
- `notional` — объём в USD

**Характеристики:**
- ~2800 снимков/день (~30 секундный интервал)
- 10-12 строк на снимок (по одной на каждый % уровень)
- ~500 KB/день ZIP
- Агрегация: берём **LAST снимок** за каждую минуту

### bookTicker (с 2023-05-16)

**URL:** `https://data.binance.vision/data/futures/um/daily/bookTicker/{SYMBOL}/{SYMBOL}-bookTicker-{YYYY-MM-DD}.zip`

**Формат CSV:** `update_id, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, transaction_time, event_time`
- `transaction_time` — время транзакции в миллисекундах
- `event_time` — время события в миллисекундах

**Характеристики:**
- ~4.5 миллионов тиков/день
- 50-130 MB/день ZIP
- Агрегация: pandas `groupby('minute').agg()` — LAST, MIN, MAX, AVG, STD, COUNT

### Процесс обработки

```
Для каждого дня:
  1. Скачать bookDepth ZIP (~500 KB) → распаковать → агрегировать
     └→ LAST snapshot за минуту → depth/notional по % уровням → imbalance/pressure
  2. Если дата >= 2023-05-16:
     Скачать bookTicker ZIP (50-130 MB) → распаковать → агрегировать
     └→ pandas groupby().agg() → best bid/ask, spread stats, activity
  3. Merge depth + ticker → 1440 строк (1 на минуту)
  4. INSERT...ON CONFLICT DO UPDATE → COMMIT
```

---

## Структура таблицы

**Таблица:** `orderbook_binance_futures_1m`
**Колонок:** 46 (2 ключевых + 22 bookTicker + 22 bookDepth)
**Primary Key:** `(timestamp, symbol)`

| # | Источник | Группа | Кол-во | Доступно с |
|---|----------|--------|--------|------------|
| 1 | bookTicker | Price | 6 | 2023-05-16 |
| 2 | bookTicker | Spread | 6 | 2023-05-16 |
| 3 | bookTicker | Volatility | 3 | 2023-05-16 |
| 4 | bookTicker | Activity | 3 | 2023-05-16 |
| 5 | bookTicker | Quantity Stats | 4 | 2023-05-16 |
| 6 | bookDepth | Bid/Ask Depth | 10 | 2023-01-01 |
| 7 | bookDepth | Notional | 2 | 2023-01-01 |
| 8 | bookDepth | ±0.2% Levels | 2 | 2026-01-15 |
| 9 | bookDepth | Imbalance | 4 | 2023-01-01 |
| 10 | bookDepth | Pressure & Liquidity | 3 | 2023-01-01 |
| 11 | bookDepth | Activity | 1 | 2023-01-01 |
| | | **Итого** | **44 данных** | |

---

## bookTicker — Price (6 колонок)

**Колонки:** `best_bid`, `best_ask`, `best_bid_qty`, `best_ask_qty`, `mid_price`, `microprice`

NULL до 2023-05-16 (bookTicker недоступен).

### best_bid, best_ask `DECIMAL(20,8)`

**Лучшая цена покупки/продажи** на Binance на конец минуты (LAST тик).

### best_bid_qty, best_ask_qty `DECIMAL(20,8)`

**Объём на лучшей цене** — количество актива на лучшем bid/ask уровне.

### mid_price `DECIMAL(20,8)`

**Средняя цена** = `(best_bid + best_ask) / 2`.

### microprice `DECIMAL(20,8)`

**Взвешенная цена** — более точная оценка «справедливой» цены:

```
microprice = (best_bid × best_ask_qty + best_ask × best_bid_qty) / (best_bid_qty + best_ask_qty)
```

**Для ML:** `microprice - mid_price` показывает краткосрочное давление.

---

## bookTicker — Spread (6 колонок)

**Колонки:** `spread`, `spread_pct`, `spread_min`, `spread_max`, `spread_avg`, `spread_std`

### spread, spread_pct `DECIMAL(20,8)` / `DECIMAL(10,6)`

**Спред** = `best_ask - best_bid`. `spread_pct` = `spread / mid_price × 100`.

### spread_min, spread_max, spread_avg, spread_std

Статистика спреда за минуту из ~4500 тиков. Показывает стабильность ликвидности.

**Для анализа:** `spread_max >> spread_avg` — в минуте был момент низкой ликвидности.

---

## bookTicker — Volatility (3 колонки)

**Колонки:** `mid_price_range`, `mid_price_std`, `price_momentum`

### mid_price_range `DECIMAL(20,8)`

**Диапазон mid_price за минуту** = `max(mid_price) - min(mid_price)`.

Информация, скрытая внутри 1-минутной свечи.

### mid_price_std `DECIMAL(20,8)`

**Стандартное отклонение mid_price** за минуту.

### price_momentum `DECIMAL(10,6)`

**Направление движения** = `(last_mid - first_mid) / first_mid`.

---

## bookTicker — Activity (3 колонки)

**Колонки:** `best_bid_changes`, `best_ask_changes`, `tick_count`

### best_bid_changes, best_ask_changes `INT`

**Количество изменений** лучшей bid/ask цены за минуту. Высокие значения — активная борьба за цену.

### tick_count `INT`

**Общее количество тиков** bookTicker за минуту (~4500 для BTCUSDT).

---

## bookTicker — Quantity Stats (4 колонки)

**Колонки:** `best_bid_qty_avg`, `best_bid_qty_max`, `best_ask_qty_avg`, `best_ask_qty_max`

Средние и максимальные объёмы на лучших ценах за минуту. Показывают глубину ликвидности у цены.

---

## bookDepth — Bid/Ask Depth (10 колонок)

**Колонки:**
- `bid_depth_1pct`, `bid_depth_2pct`, `bid_depth_3pct`, `bid_depth_4pct`, `bid_depth_5pct`
- `ask_depth_1pct`, `ask_depth_2pct`, `ask_depth_3pct`, `ask_depth_4pct`, `ask_depth_5pct`

Доступны с 2023-01-01.

### bid_depth_Npct `DECIMAL(20,8)`

**Кумулятивный объём в базовой валюте** (BTC) на bid-стороне в пределах N% от текущей цены.

**Пример:** `bid_depth_5pct = 450.5` — при цене $100K, покупатели готовы купить 450.5 BTC в диапазоне $95K-$100K.

### ask_depth_Npct `DECIMAL(20,8)`

Аналогично для ask-стороны (продавцы).

**Для анализа:**
- `bid_depth_5pct >> ask_depth_5pct` → бычий сигнал (больше покупателей)
- `ask_depth_5pct >> bid_depth_5pct` → медвежий сигнал (больше продавцов)

---

## bookDepth — Notional (2 колонки)

**Колонки:** `bid_notional_5pct`, `ask_notional_5pct`

### bid_notional_5pct, ask_notional_5pct `DECIMAL(20,4)`

**Объём в USD** на bid/ask стороне в пределах 5% от цены.

**Пример:** `bid_notional_5pct = 45,500,000` — $45.5M ждут на покупку в пределах 5% от цены.

---

## bookDepth — ±0.2% Levels (2 колонки)

**Колонки:** `bid_depth_02pct`, `ask_depth_02pct`

**NULL до 2026-01-15** — эти уровни добавлены в архивы Binance позже.

Самая «горячая» зона стакана — ордера в пределах 0.2% от цены.

---

## bookDepth — Imbalance (4 колонки)

**Колонки:** `imbalance_1pct`, `imbalance_2pct`, `imbalance_3pct`, `imbalance_5pct`

### imbalance_Npct `DECIMAL(10,6)`

**Дисбаланс** на уровне N%:

```
imbalance_Npct = (bid_depth_Npct - ask_depth_Npct) / (bid_depth_Npct + ask_depth_Npct)
```

Диапазон: от **-1** (только продавцы) до **+1** (только покупатели).

| Значение | Интерпретация |
|---|---|
| +0.3 ... +1.0 | Сильное давление покупателей |
| +0.05 ... +0.3 | Умеренное давление покупателей |
| -0.05 ... +0.05 | Баланс |
| -0.3 ... -0.05 | Умеренное давление продавцов |
| -1.0 ... -0.3 | Сильное давление продавцов |

**Для ML:** Imbalance на разных уровнях показывает разную «дальность» сигнала:
- `imbalance_1pct` — краткосрочный (ближняя ликвидность)
- `imbalance_5pct` — среднесрочный (общая поддержка/сопротивление)

---

## bookDepth — Pressure & Liquidity (3 колонки)

**Колонки:** `depth_ratio`, `buy_pressure`, `liquidity_score`

### depth_ratio `DECIMAL(20,8)`

```
depth_ratio = bid_depth_5pct / ask_depth_5pct
```

- `> 1.0` → покупатели доминируют
- `< 1.0` → продавцы доминируют
- Типичный диапазон: 0.5 — 2.5 (медиана ~1.16)

### buy_pressure `DECIMAL(20,8)`

```
buy_pressure = bid_depth_1pct / ask_depth_1pct    (cap: 9999)
```

Более «тактический» сигнал (ближняя зона, 1% от цены).

**Примечание:** Значения ограничены сверху 9999. В экстремальных случаях (напр. `ask_depth_1pct = 0.001 BTC`) реальный ratio может достигать миллионов — cap предотвращает аномальные выбросы в данных.

### liquidity_score `DECIMAL(20,4)`

```
liquidity_score = bid_notional_5pct + ask_notional_5pct
```

Общий показатель ликвидности в USD.

---

## bookDepth — Activity (1 колонка)

### snapshot_count `INT`

**Количество снимков** bookDepth за минуту. Обычно ~2 (каждые ~30 секунд).

---

## Сравнение с Bybit Orderbook

| Метрика | Bybit (`orderbook_bybit_futures_1m`) | Binance (`orderbook_binance_futures_1m`) |
|---------|--------------------------------------|------------------------------------------|
| Колонок | 60 | 46 |
| JSONB уровни | Да (top-50 bid + ask) | Нет |
| Walls (стены) | Да (bid/ask wall price/volume/distance) | Нет |
| Slippage | Да ($10K/$50K/$100K) | Нет |
| Concentration | Да (bid/ask concentration) | Нет |
| Depth по % уровням | Нет | Да (1%, 2%, 3%, 4%, 5%, 0.2%) |
| Imbalance | 1 общий + 1 (0.1%) | 4 (1%, 2%, 3%, 5%) |
| Spread stats | MIN/MAX/AVG/STD | MIN/MAX/AVG/STD |
| Первые данные | 2023-01-18 | 2023-01-01 |
| Размер/строка | ~1.6 KB | ~0.4 KB |

### Уникальные колонки каждой таблицы

**Только в Bybit:** JSONB levels (top-50), walls, slippage, concentration, VWAP bid/ask, volume stats (01/05/10pct)

**Только в Binance:** Depth по % уровням (1-5% + 0.2%), imbalance на 4 уровнях, notional, microprice, tick_count

---

## Применение для ML

### Кросс-биржевые фичи

```sql
-- Binance vs Bybit imbalance divergence
SELECT
    b.timestamp,
    b.imbalance_5pct as binance_imbalance,
    y.imbalance as bybit_imbalance,
    b.imbalance_5pct - y.imbalance as cross_exchange_divergence
FROM orderbook_binance_futures_1m b
JOIN orderbook_bybit_futures_1m y
    ON b.timestamp = y.timestamp AND b.symbol = y.symbol
WHERE b.symbol = 'BTCUSDT';
```

### Профиль глубины стакана

```sql
-- Gradient глубины (как меняется ликвидность по уровням)
SELECT
    timestamp,
    bid_depth_1pct,
    bid_depth_2pct - bid_depth_1pct as bid_depth_1to2pct,
    bid_depth_3pct - bid_depth_2pct as bid_depth_2to3pct,
    bid_depth_5pct - bid_depth_3pct as bid_depth_3to5pct
FROM orderbook_binance_futures_1m
WHERE symbol = 'BTCUSDT';
```

### Рекомендуемые фичи для ML

| Фича | Формула | Что показывает |
|------|---------|----------------|
| `cross_imbalance` | binance_imbalance - bybit_imbalance | Расхождение между биржами |
| `depth_gradient` | depth_5pct / depth_1pct | Насколько равномерна ликвидность |
| `spread_stability` | spread_std / spread_avg | Волатильность ликвидности |
| `microprice_signal` | microprice - mid_price | Краткосрочное давление |
| `tick_intensity` | tick_count / 4500 | Нормализованная активность |
| `notional_imbalance` | (bid_not - ask_not) / (bid_not + ask_not) | USD-взвешенный дисбаланс |

---

## SQL примеры

### Базовая статистика

```sql
-- Количество записей по дням
SELECT DATE(timestamp) as day, COUNT(*) as rows
FROM orderbook_binance_futures_1m
WHERE symbol = 'BTCUSDT'
GROUP BY DATE(timestamp) ORDER BY day;
```

### Depth profile за период

```sql
-- Средняя глубина по уровням за последнюю неделю
SELECT
    ROUND(AVG(bid_depth_1pct)::numeric, 2) as avg_bid_1pct,
    ROUND(AVG(bid_depth_2pct)::numeric, 2) as avg_bid_2pct,
    ROUND(AVG(bid_depth_5pct)::numeric, 2) as avg_bid_5pct,
    ROUND(AVG(ask_depth_1pct)::numeric, 2) as avg_ask_1pct,
    ROUND(AVG(ask_depth_2pct)::numeric, 2) as avg_ask_2pct,
    ROUND(AVG(ask_depth_5pct)::numeric, 2) as avg_ask_5pct
FROM orderbook_binance_futures_1m
WHERE symbol = 'BTCUSDT'
  AND timestamp >= NOW() - INTERVAL '7 days';
```

### Кросс-биржевой анализ

```sql
-- Сравнение спредов Binance vs Bybit
SELECT
    b.timestamp,
    b.spread_pct as binance_spread_pct,
    y.spread_pct as bybit_spread_pct,
    b.spread_pct - y.spread_pct as spread_diff
FROM orderbook_binance_futures_1m b
JOIN orderbook_bybit_futures_1m y
    ON b.timestamp = y.timestamp AND b.symbol = y.symbol
WHERE b.symbol = 'BTCUSDT'
ORDER BY ABS(b.spread_pct - y.spread_pct) DESC
LIMIT 20;
```

### JOIN с candles и indicators

```sql
-- Полная картина: свеча + индикаторы + orderbook Bybit + orderbook Binance
SELECT
    c.timestamp, c.close, c.volume,
    i.rsi_14, i.ema_50,
    yob.imbalance as bybit_imbalance,
    bob.imbalance_5pct as binance_imbalance,
    bob.liquidity_score as binance_liquidity
FROM candles_bybit_futures_1m c
JOIN indicators_bybit_futures_1m i
    ON c.timestamp = i.timestamp AND c.symbol = i.symbol
LEFT JOIN orderbook_bybit_futures_1m yob
    ON c.timestamp = yob.timestamp AND c.symbol = yob.symbol
LEFT JOIN orderbook_binance_futures_1m bob
    ON c.timestamp = bob.timestamp AND c.symbol = bob.symbol
WHERE c.symbol = 'BTCUSDT'
  AND c.timestamp >= '2025-01-01'
  AND c.timestamp < '2025-01-02'
ORDER BY c.timestamp;
```

---

## Технические детали

| Параметр | Значение |
|---|---|
| Таблица | `orderbook_binance_futures_1m` |
| Primary Key | `(timestamp, symbol)` |
| Индексы | `(symbol, timestamp)`, `(timestamp)` |
| Колонок | 46 (2 ключевых + 44 числовых) |
| Строк/день | 1,440 |
| Скрипт | `indicators/orderbook_binance_loader.py` |

### Источники данных

| Источник | URL | Доступен с | Размер/день |
|----------|-----|-----------|-------------|
| bookDepth | `data.binance.vision/.../bookDepth/` | 2023-01-01 | ~500 KB |
| bookTicker | `data.binance.vision/.../bookTicker/` | 2023-05-16 | 50-130 MB |

### NULL-периоды

| Колонки | NULL до | Причина |
|---------|---------|---------|
| Все bookTicker (22 колонки) | 2023-05-16 | bookTicker архивы начинаются позже |
| bid_depth_02pct, ask_depth_02pct | 2026-01-15 | ±0.2% уровни добавлены позже |

**Переход bookTicker** (проверено на реальных данных):

| Дата | bookTicker заполнен | Примечание |
|------|---------------------|------------|
| 2023-05-15 | 0% | Последний день без bookTicker |
| 2023-05-16 | 42% | Данные начинаются с 11:49 UTC |
| 2023-05-17 | 62% | Неполный день |
| 2023-05-18+ | 100% | Полные данные |

### Типичные значения (проверено на 200K+ строк, 2023-01-01 → 2023-05-29)

| Метрика | Диапазон | Типичное значение |
|---------|----------|-------------------|
| Строк/день | 1397-1438 | ~1428 |
| snapshot_count | 2 | 2 (снимки ~30 сек) |
| imbalance_1pct | [-1.0, +1.0] | ~0 (нейтральный рынок) |
| imbalance_5pct | [-0.30, +0.44] | ~0 |
| depth_ratio | 0.54 — 2.54 | ~1.16 |
| buy_pressure | 0.0001 — 9999 (cap) | ~1.10 |
| spread_pct | — | ~0.00037% |
| tick_count | 2689 — 8511 | ~4500 |

### Прогноз размера

| Конфигурация | Размер/год |
|---|---|
| 1 символ (BTCUSDT) | ~0.20 GB |
| 17 символов (все futures) | ~3.4 GB |
| 17 символов × 3 года | ~10 GB |

### Связь с другими таблицами

```
candles_bybit_futures_1m          (OHLCV свечи)
        │
        │ timestamp + symbol
        ▼
indicators_bybit_futures_1m       (технические индикаторы)
        │
        │ timestamp + symbol
        ├──────────────────────────┐
        ▼                          ▼
orderbook_bybit_futures_1m    orderbook_binance_futures_1m
(стакан Bybit)                (стакан Binance)
```

Все таблицы имеют **одинаковый Primary Key**: `(timestamp, symbol)` — можно JOIN без проблем.
