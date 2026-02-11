# Orderbook Data Collection Planning

## Overview

Документ для планирования сбора и хранения данных orderbook (стакана) с Bybit.

**Дата создания:** 2026-02-03
**Последнее обновление:** 2026-02-05 (v4 — загрузка в процессе, 741 день)

---

## Data Source

### URL Pattern
```
https://quote-saver.bycsi.com/orderbook/linear/{SYMBOL}/{DATE}_{SYMBOL}_ob{DEPTH}.data.zip
```

Пример:
```
https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2026-02-01_BTCUSDT_ob200.data.zip
```

### Directory Listing

Полный список файлов доступен по URL:
```
https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/
```
Возвращает HTML с ссылками на все доступные ZIP архивы.

### Важно: orderbook vs trading data

| URL | Содержимое | Формат |
|-----|-----------|--------|
| `quote-saver.bycsi.com/orderbook/linear/` | **Orderbook** (стакан лимитных ордеров) | JSON Lines в ZIP |
| `public.bybit.com/trading/` | **Trades** (исполненные сделки) | CSV в GZIP |

`public.bybit.com/trading/` — это **НЕ orderbook**. Это тиковые данные о сделках (timestamp, side, size, price, tickDirection).

### Data Availability (BTCUSDT)

| Период | Формат | Файлов | Уровней глубины |
|--------|--------|--------|-----------------|
| 2023-01-18 — 2025-08-20 | ob500 | 946 | 500 bid + 500 ask |
| 2025-08-21 — текущая дата | ob200 | 167+ | 200 bid + 200 ask |
| **Итого** | | **1,113+** | |

- **Первый доступный архив:** 2023-01-18
- **Последний доступный:** вчерашняя дата (сегодняшний день формируется)
- **Переход ob500 → ob200:** 2025-08-20 → 2025-08-21

### File Size Estimates (BTCUSDT, per day)

| Depth | Compressed (ZIP) | Uncompressed | Период |
|-------|-------------------|--------------|--------|
| ob500 | 84-284 MB | ~2-4 GB | 2023-01 — 2025-08 |
| ob200 | 98-362 MB | ~1.6 GB | 2025-08+ |

Размер зависит от волатильности рынка (больше движений = больше delta сообщений).

### Total Download Estimate (BTCUSDT, all history)

```
ob500: 946 дней × ~170 MB avg = ~161 GB compressed
ob200: 167 дней × ~230 MB avg = ~38 GB compressed
Итого: ~200 GB для скачивания (ZIP)
```

**Обработка в RAM** — ZIP скачивается в оперативную память (BytesIO), обрабатывается, на диск ничего не пишется.

---

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

**Формат одинаковый** для ob500 и ob200 — отличается только `topic` (`orderbook.500.BTCUSDT` vs `orderbook.200.BTCUSDT`) и количество уровней в snapshot.

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
- ~600 сообщений в минуту
- ~864,000 сообщений в день на символ

### Важно: Snapshot vs Delta

**Snapshot** — полный снимок стакана (все 200/500 уровней bids + asks)
**Delta** — только изменения (обновлённые уровни)

Для восстановления стакана в любой момент:
1. Взять последний snapshot
2. Последовательно применить все delta

**При применении delta:**
- Если `size > 0` → обновить или добавить уровень
- Если `size = 0` → удалить уровень

### Важно: Orderbook ≠ Trades

Orderbook содержит **лимитные ордера** (ещё НЕ исполненные). Это стакан заявок.
Trades (сделки) — это отдельный поток данных, когда рыночный ордер исполняется.

| Метрика | Orderbook | Trades (candles) |
|---------|:---------:|:----------------:|
| Лимитные ордера (кто ждёт) | да | нет |
| Исполненные сделки | нет | частично |
| Кол-во trades/мин | нет | нет |
| Buy/Sell volume раздельно | нет | нет |
| Общий volume сделок | нет | да |
| Стакан глубины | да | нет |

---

## Принятые решения

### 1. Интервал агрегации: 1 минута
Синхронизация с candles_bybit_futures_1m и indicators_bybit_futures_1m.

### 2. Отдельная таблица
`orderbook_bybit_futures_1m` — не добавляем колонки в indicators (113 GB, 206 колонок).

### 3. Начальный символ: BTCUSDT
После загрузки и валидации — добавляем остальные пары.

### 4. Хранение уровней глубины: JSONB (top 50)
Формат `{"p": price, "v": volume}`, сортировка по цене (ближайшие к mid сначала).

Top 50 уровней покрывает 96% объёма стакана (реальная статистика из sample data).

### 5. Обработка в RAM (без записи на диск)
ZIP скачивается в оперативную память (`requests.get()` → `BytesIO`), распаковывается через `zipfile.ZipFile(BytesIO)`, строки читаются итеративно. На диск ничего не пишется.
- RAM: ~300-400 MB (ZIP в памяти + буфер распаковки + orderbook dict)
- VPS: 16 GB RAM — 2.5% памяти, нет проблем

### 6. Расположение скрипта: `indicators/orderbook_bybit_loader.py`
Рядом с другими loaders. После тестов — добавим в `start_all_loaders.py`.

### 7. Конфигурация: хардкод
URL-паттерн и параметры захардкожены в скрипте (не в отдельном YAML).

### 8. Resume-логика: MAX(timestamp) из БД
- При запуске: `SELECT MAX(timestamp) FROM orderbook_bybit_futures_1m WHERE symbol = %s`
- Если данных нет → начинаем с 2023-01-18 (первый архив)
- Если MAX(timestamp) < 23:59 за свой день → перезагружаем этот день (partial day)
- Если MAX(timestamp) = 23:59 → начинаем со следующего дня
- `INSERT...ON CONFLICT DO UPDATE` — повторная обработка дня безопасна (идемпотентно)
- **Без checkpoint файлов** — состояние определяется только из БД

### 9. Обработка ошибок: 3 retry + crash
- 3 попытки с exponential backoff при ошибке скачивания
- Если все 3 попытки провалились → скрипт падает (не пропускаем дни)
- Это гарантирует, что пропущенные данные будут замечены

### 10. Graceful shutdown (Ctrl+C)
- Первый Ctrl+C → завершает текущий день и выходит
- Второй Ctrl+C → force exit

### 11. Batch = 1 день
- Скачали ZIP → обработали в RAM → записали 1440 строк в БД → COMMIT → следующий день
- COMMIT после каждого дня — checkpoint для безопасного прерывания

### 12. CLI флаги (минимальный набор)
```bash
python3 orderbook_bybit_loader.py                        # Обычный запуск (continue from last)
python3 orderbook_bybit_loader.py --symbol BTCUSDT       # Конкретный символ
python3 orderbook_bybit_loader.py --force-reload          # Перезагрузить всё с 2023-01-18
```
Нет `--date-range`, `--backfill` — добавим при необходимости.

---

## Выбранная реализация: Метрики (56 колонок) + JSONB уровни (2 колонки)

### Правила агрегации ~600 сообщений → 1 строка

За 1 минуту приходит ~600 orderbook обновлений (~100ms каждое). Для каждого обновления мы поддерживаем **живое состояние стакана в памяти** и извлекаем метрики:

| Правило | Колонки | Описание |
|---------|---------|----------|
| **LAST** | best_bid, best_ask, mid_price, spread, imbalance, bid_volume, ask_volume, bid/ask_levels (JSONB) | Состояние стакана из **последнего** сообщения минуты (аналог close в свечах) |
| **MIN/MAX** | spread_min/max, imbalance_min/max, imbalance_range, mid_price_range | Экстремумы за минуту — показывают что происходило **внутри** минуты |
| **AVG/STD** | spread_avg/std, imbalance_avg/std, bid_vol_avg/std, ask_vol_avg/std | Средние и разброс — стабильность метрик за минуту |
| **COUNT** | ob_update_count, best_bid_changes, best_ask_changes | Количество событий за минуту |
| **CALC** | microprice, vwap, slippage, walls, pressure, concentration | Вычисляются из LAST состояния стакана |

#### Визуальная схема

```
Raw файл (1.6 GB/день)
│
│  00:00:01.070  msg #1   snapshot  (200 bid + 200 ask)
│  00:00:01.168  msg #2   delta     (обновление уровней)
│  00:00:01.269  msg #3   delta
│  ...каждые ~100ms...
│  00:00:59.969  msg #590 delta
│                                    ┌──────────────────────────────────┐
│  ──── конец минуты ──────────────→ │ ОДНА СТРОКА В БД                │
│                                    │  timestamp: 2026-02-01 00:00:00 │
│                                    │  LAST:  best_bid, levels JSONB  │
│                                    │  MIN/MAX: spread_min/max        │
│                                    │  AVG/STD: spread_avg/std        │
│                                    │  COUNT: ob_update_count         │
│                                    └──────────────────────────────────┘
│
│  00:01:00.069  msg #591 delta
│  ...
│  00:01:59.968  msg #1190 delta
│                                    ┌──────────────────────────────────┐
│  ──── конец минуты ──────────────→ │ ОДНА СТРОКА В БД                │
│                                    │  timestamp: 2026-02-01 00:01:00 │
│                                    └──────────────────────────────────┘
│
│  ... × 1440 минут = 1440 строк в БД за день
```

#### Пример: что происходит внутри 1 минуты (реальные данные)

```
Минута: 2026-02-01 00:00:00  (590 сообщений)

  #1   00:00:01.070  best_bid=78704.30  ask=78704.40  spread=0.10  imb=-0.103  ← ПЕРВОЕ
  #2   00:00:01.168  best_bid=78704.30  ask=78704.40  spread=0.10  imb=-0.125
  ...
  #148 00:00:15.769  best_bid=78708.80  ask=78708.90  spread=0.10  imb=+0.052  ← цена выросла
  ...
  #296 00:00:30.570  best_bid=78658.60  ask=78658.70  spread=0.10  imb=-0.202  ← цена упала на $50
  ...
  #590 00:00:59.969  best_bid=78645.50  ask=78645.60  spread=0.10  imb=-0.180  ← ПОСЛЕДНЕЕ

В БД записывается:
  best_bid      = 78645.50     (LAST)
  spread_min    = 0.10         (MIN за 590 значений)
  spread_max    = 4.90         (MAX — был момент расширения спреда!)
  imbalance_min = -0.4359      (MIN — макс давление продавцов)
  imbalance_max = +0.5533      (MAX — макс давление покупателей)
  mid_price_range = 94.20      (max_mid - min_mid — цена прошла $94 внутри минуты)
  ob_update_count = 590        (активность рынка)
  best_bid_changes = 117       (best_bid менялся 117 раз)
```

### Полный список колонок (58 колонок)

| # | Группа | Колонка | Агрегация | Описание |
|---|--------|---------|-----------|----------|
| | **PRICE** | | | Ценовые данные |
| 1 | | best_bid | LAST | Лучшая цена покупки |
| 2 | | best_ask | LAST | Лучшая цена продажи |
| 3 | | mid_price | LAST | (best_bid + best_ask) / 2 |
| 4 | | microprice | LAST | Взвешенная по объёму на лучших уровнях |
| 5 | | vwap_bid | LAST | VWAP для bid стороны (все уровни) |
| 6 | | vwap_ask | LAST | VWAP для ask стороны |
| | **SPREAD** | | | Спред и его динамика |
| 7 | | spread | LAST | Спред на конец минуты |
| 8 | | spread_pct | LAST | Спред в % от mid |
| 9 | | spread_min | MIN | Минимальный за минуту |
| 10 | | spread_max | MAX | Максимальный за минуту |
| 11 | | spread_avg | AVG | Средний за минуту |
| 12 | | spread_std | STD | Стд. отклонение за минуту |
| | **BID VOLUME** | | | Объёмы на стороне покупки |
| 13 | | bid_vol_01pct | LAST | Объём в пределах 0.1% от mid |
| 14 | | bid_vol_05pct | LAST | Объём в пределах 0.5% от mid |
| 15 | | bid_vol_10pct | LAST | Объём в пределах 1.0% от mid |
| 16 | | bid_volume | LAST | Полный объём (все уровни) |
| 17 | | bid_vol_avg | AVG | Средний за минуту |
| 18 | | bid_vol_std | STD | Стд. отклонение за минуту |
| | **ASK VOLUME** | | | Объёмы на стороне продажи |
| 19 | | ask_vol_01pct | LAST | Объём в пределах 0.1% от mid |
| 20 | | ask_vol_05pct | LAST | Объём в пределах 0.5% от mid |
| 21 | | ask_vol_10pct | LAST | Объём в пределах 1.0% от mid |
| 22 | | ask_volume | LAST | Полный объём (все уровни) |
| 23 | | ask_vol_avg | AVG | Средний за минуту |
| 24 | | ask_vol_std | STD | Стд. отклонение за минуту |
| | **IMBALANCE** | | | Дисбаланс стакана |
| 25 | | imbalance | LAST | (bid - ask) / (bid + ask) |
| 26 | | imbalance_01pct | LAST | Imbalance в пределах 0.1% от mid |
| 27 | | imbalance_min | MIN | Минимум за минуту |
| 28 | | imbalance_max | MAX | Максимум за минуту |
| 29 | | imbalance_avg | AVG | Среднее за минуту |
| 30 | | imbalance_std | STD | Стд. отклонение |
| 31 | | imbalance_range | CALC | max - min |
| | **PRESSURE** | | | Давление покупателей/продавцов |
| 32 | | buy_pressure | LAST | bid_vol_01pct / ask_vol_01pct |
| 33 | | depth_ratio | LAST | bid_vol_10pct / ask_vol_10pct |
| | **WALLS** | | | "Стены" в стакане (крупные ордера) |
| 34 | | bid_wall_price | LAST | Цена с максимальным bid объёмом |
| 35 | | bid_wall_volume | LAST | Объём на этом уровне |
| 36 | | bid_wall_distance_pct | LAST | Расстояние от mid в % |
| 37 | | ask_wall_price | LAST | Цена с максимальным ask объёмом |
| 38 | | ask_wall_volume | LAST | Объём на этом уровне |
| 39 | | ask_wall_distance_pct | LAST | Расстояние от mid в % |
| | **SLIPPAGE** | | | Проскальзывание (в $) |
| 40 | | slippage_buy_10k | LAST | Проскальзывание при покупке на $10K |
| 41 | | slippage_buy_50k | LAST | Проскальзывание при покупке на $50K |
| 42 | | slippage_buy_100k | LAST | Проскальзывание при покупке на $100K |
| 43 | | slippage_sell_10k | LAST | Проскальзывание при продаже на $10K |
| 44 | | slippage_sell_50k | LAST | Проскальзывание при продаже на $50K |
| 45 | | slippage_sell_100k | LAST | Проскальзывание при продаже на $100K |
| | **LIQUIDITY** | | | Метрики ликвидности |
| 46 | | liquidity_score | LAST | (bid_volume + ask_volume) / 2 |
| 47 | | bid_concentration | LAST | Топ-3 уровня / total |
| 48 | | ask_concentration | LAST | Топ-3 уровня / total |
| | **VOLATILITY** | | | Волатильность внутри минуты |
| 49 | | mid_price_range | CALC | max(mid) - min(mid) за минуту |
| 50 | | mid_price_std | STD | Стд. отклонение mid за минуту |
| 51 | | price_momentum | CALC | (last_mid - first_mid) / first_mid |
| | **ACTIVITY** | | | Активность обновлений стакана |
| 52 | | ob_bid_levels | LAST | Количество ненулевых уровней bid |
| 53 | | ob_ask_levels | LAST | Количество ненулевых уровней ask |
| 54 | | ob_update_count | COUNT | Количество обновлений стакана за минуту |
| 55 | | best_bid_changes | COUNT | Сколько раз менялся best_bid |
| 56 | | best_ask_changes | COUNT | Сколько раз менялся best_ask |
| | **DEPTH LEVELS** | | | Уровни глубины стакана (JSONB) |
| 57 | | bid_levels | LAST | Top-50 bid уровней `[{"p":price,"v":vol}, ...]` |
| 58 | | ask_levels | LAST | Top-50 ask уровней `[{"p":price,"v":vol}, ...]` |

---

### Описание групп колонок

#### PRICE (цены) — LAST
- **best_bid/best_ask** — базовые цены стакана на конец минуты
- **mid_price** — "справедливая" цена: `(best_bid + best_ask) / 2`
- **microprice** — более точная оценка: `(best_bid * ask_vol + best_ask * bid_vol) / (bid_vol + ask_vol)`
- **vwap_bid/vwap_ask** — средневзвешенные цены по всем уровням

#### SPREAD (спред) — LAST + MIN/MAX/AVG/STD
- Низкий спред = высокая ликвидность
- **spread_max** — детектор моментов низкой ликвидности (если `spread_max >> spread`, был "пустой" стакан)
- **spread_std** — стабильность ликвидности за минуту

#### VOLUME (объёмы) — LAST + AVG/STD
- **vol_01pct** — ликвидность "у цены" (самая важная для исполнения)
- **vol_05pct/vol_10pct** — глубина стакана
- **vol_avg/vol_std** — стабильность объёмов за минуту

#### IMBALANCE (дисбаланс) — LAST + MIN/MAX/AVG/STD
- Формула: `(bid_volume - ask_volume) / (bid_volume + ask_volume)`
- Диапазон: от -1 (только продавцы) до +1 (только покупатели)
- **imbalance_range** = `max - min` — волатильность давления за минуту

#### WALLS (стены) — LAST
- Крупные ордера, которые могут служить поддержкой/сопротивлением
- **wall_distance_pct** — насколько далеко стена от текущей цены

#### SLIPPAGE (проскальзывание) — LAST
- Используем **долларовые значения** ($10K, $50K, $100K) для универсальности
- Позволяет сравнивать ликвидность между разными парами

#### LIQUIDITY (ликвидность) — LAST
- **liquidity_score** — общий показатель ликвидности
- **concentration** — насколько объём сконцентрирован на топ-уровнях (высокая = "стена" или хрупкий стакан)

#### VOLATILITY (волатильность) — CALC from all messages
- **mid_price_range** — диапазон цены внутри минуты
- **price_momentum** — направление движения за минуту

#### ACTIVITY (активность) — COUNT
- **ob_update_count** — ~600 в норме, меньше = низкая активность
- **best_bid/ask_changes** — частота изменения лучших цен

#### DEPTH LEVELS (уровни глубины) — LAST, JSONB

Top-50 ценовых уровней на конец минуты, отсортированных **по цене** (ближайшие к mid сначала).

**Формат:**
```json
[
  {"p": 78704.30, "v": 2.038},
  {"p": 78704.20, "v": 0.002},
  {"p": 78704.00, "v": 0.001},
  ...
  {"p": 78679.70, "v": 2.222}
]
```

**Почему top 50?**
Из реальных данных (2026-02-01 00:00, BTCUSDT):

| Top-N по объёму | % от общего объёма (bid) | % от общего объёма (ask) |
|:---:|:---:|:---:|
| Top 5 | 30.9% | 25.8% |
| Top 10 | 52.8% | 47.6% |
| Top 20 | 79.9% | 75.0% |
| **Top 50** | **96.1%** | **95.7%** |
| Top 100 | 99.5% | 99.5% |
| All 200 | 100% | 100% |

Top 50 покрывает **96% объёма** — оптимальная точка. Остальные 150 уровней содержат пыль (<0.01 BTC каждый).

**Сортировка по цене (не по объёму):**
- Сохраняет топологию стакана (видно где пусто между уровнями)
- Легко взять "первые N ближайших уровней" для ML
- Стены находятся простым фильтром по volume

**Реальный пример bid_levels (стакан BTCUSDT):**
```
Уровень  Цена       Объём    $Value    Dist от mid
  1     78704.30    2.038   $160,399   0.001%  ← best bid
  2     78704.20    0.002   $157       0.002%
  ...
  21    78694.50    1.683   $132,443   0.013%  ← крупный
  23    78693.80    1.713   $134,802   0.013%  ← крупный
  ...
  33    78687.80    4.111   $323,486   0.021%  ← WALL (стена!)
  ...
  46    78681.60    4.098   $322,437   0.029%  ← WALL
  50    78679.70    2.222   $174,826   0.031%  ← крупный
```

---

### Пример данных в таблице (реальные значения)

| Группа | Колонка | 00:00:00 | 00:01:00 |
|--------|---------|----------|----------|
| **PRICE** | best_bid | 78645.50 | 78640.70 |
| | best_ask | 78645.60 | 78640.80 |
| | mid_price | 78645.55 | 78640.75 |
| **SPREAD** | spread | 0.10 | 0.10 |
| | spread_min | 0.10 | 0.10 |
| | spread_max | 4.90 | 3.00 |
| | spread_avg | 0.1107 | 0.1060 |
| | spread_std | 0.2000 | 0.1217 |
| **BID VOL** | bid_volume | 58.713 | 104.380 |
| | bid_vol_avg | 73.101 | 87.538 |
| **ASK VOL** | ask_volume | 84.503 | 79.105 |
| | ask_vol_avg | 82.125 | 72.545 |
| **IMBALANCE** | imbalance | -0.1801 | 0.1377 |
| | imbalance_min | -0.4359 | -0.4725 |
| | imbalance_max | 0.5533 | 0.6440 |
| | imbalance_range | 0.9892 | 1.1165 |
| | imbalance_avg | -0.0592 | 0.0951 |
| **VOLATILITY** | mid_price_range | 94.20 | 34.50 |
| | price_momentum | -0.000747 | -0.000061 |
| **ACTIVITY** | ob_update_count | 590 | 600 |
| | best_bid_changes | 117 | 80 |
| **JSONB** | bid_levels | [{"p":78645.50,"v":...}, ...] | [...] |
| | ask_levels | [{"p":78645.60,"v":...}, ...] | [...] |

**Интерпретация 00:00:**
- `spread_max = 4.90` — был момент, когда спред раздулся в 49 раз (низкая ликвидность)
- `imbalance_range = 0.9892` — давление качнулось от -0.44 до +0.55 (борьба)
- `mid_price_range = 94.20` — цена прошла $94 внутри этой одной минуты
- `price_momentum = -0.0007` — итоговое движение вниз

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

    -- SLIPPAGE (6 колонок)
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
    ob_bid_levels INT,
    ob_ask_levels INT,
    ob_update_count INT,
    best_bid_changes INT,
    best_ask_changes INT,

    -- DEPTH LEVELS (2 JSONB колонки)
    bid_levels JSONB,    -- Top-50 bid: [{"p":price,"v":volume}, ...] sorted by price DESC
    ask_levels JSONB,    -- Top-50 ask: [{"p":price,"v":volume}, ...] sorted by price ASC

    PRIMARY KEY (timestamp, symbol)
);

-- Индексы
CREATE INDEX idx_ob_symbol_ts ON orderbook_bybit_futures_1m (symbol, timestamp);
CREATE INDEX idx_ob_timestamp ON orderbook_bybit_futures_1m (timestamp);
```

### Оценка размера данных

| Метрика | Значение |
|---------|----------|
| Колонок | 56 числовых + 2 JSONB = 58 |
| Байт на запись (метрики) | ~500 bytes |
| Байт на запись (JSONB) | ~2,500 bytes (2 × 50 уровней) |
| **Байт на запись (итого)** | **~3,000 bytes** |
| Записей в день (1 символ) | 1,440 |
| **Размер в год (1 символ)** | **~1.6 GB** |
| Размер в год (17 символов) | ~27 GB |

**Сравнение с другими таблицами:**

| Таблица | Bytes/row | Колонок | Назначение |
|---------|-----------|---------|------------|
| candles_bybit_futures_1m | ~90 | 8 | OHLCV свечи |
| indicators_bybit_futures_1m | ~4,700 | 206 | Индикаторы |
| **orderbook_bybit_futures_1m** | **~3,000** | **58** | **Стакан** |

### Почему отдельная таблица?

- indicators_bybit_futures_1m уже 113 GB и 206 колонок
- Независимая загрузка данных (из ZIP архивов, не из candles)
- Можно удалить/пересоздать без влияния на indicators
- Проще управлять и отлаживать

---

## Алгоритм обработки

### Пошаговый алгоритм работы `orderbook_bybit_loader.py`

#### Шаг 1: Инициализация
- Подключение к PostgreSQL (`trading_admin`)
- Создание таблицы `orderbook_bybit_futures_1m`, если не существует

#### Шаг 2: Определение диапазона дат
- `SELECT MAX(timestamp) FROM orderbook_bybit_futures_1m WHERE symbol = 'BTCUSDT'`
- Если данных нет → start_date = `2023-01-18`
- Если MAX < 23:59 за свой день → start_date = дата MAX (перезагрузка partial day)
- Если MAX = 23:59 → start_date = дата MAX + 1 день
- end_date = вчера (сегодняшний файл ещё формируется)

#### Шаг 3: Получение списка файлов
- HTTP GET к `https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/`
- Парсинг HTML → список ZIP файлов → фильтрация по диапазону дат

#### Шаг 4: Цикл по дням (tqdm progress bar)

Для каждого дня:

**4a. Скачивание ZIP в RAM**
```python
response = requests.get(url)
zip_data = io.BytesIO(response.content)  # ~300 MB в RAM
```

**4b. Чтение из ZIP без распаковки на диск**
```python
with zipfile.ZipFile(zip_data) as zf:
    with zf.open(inner_filename) as f:
        for line in f:  # итеративное чтение строк
            ...
```

**4c. Потоковая обработка (~864K строк)**
- Держим в памяти один `dict` стакана + числовые аккумуляторы
- Для каждого сообщения: apply snapshot/delta → вычислить метрики → накопить
- При смене минуты: `finalize_minute()` → сохранить строку

**4d. Запись в БД**
- ~1440 строк за день
- `INSERT...ON CONFLICT (timestamp, symbol) DO UPDATE`
- COMMIT после каждого дня

**4e. Очистка RAM**
- `zip_data` и `response` уходят из scope → GC освобождает ~300 MB

### Реализация: `indicators/orderbook_bybit_loader.py`

Скрипт написан и готов к тестированию. Полная реализация — **580 строк**.

#### Структура скрипта

| Компонент | Строк | Описание |
|-----------|-------|----------|
| `OrderbookLoader` | ~150 | Основной класс: ensure_table, get_start_date, get_available_files, download_to_ram, save_day_to_db, run |
| `process_day_from_zip()` | ~100 | ZIP(BytesIO) → iterate JSON lines → apply snapshot/delta → accumulate → finalize |
| `finalize_minute()` | ~120 | Orderbook state + accumulators → 1 tuple (60 fields) для INSERT |
| `calc_*()` | ~80 | Утилиты: volume_in_range, vwap, slippage, concentration |
| CLI + logging | ~80 | setup_logging, parse_args, main, signal_handler |
| SQL + constants | ~100 | CREATE TABLE, UPSERT, ALL_COLUMNS |

#### Ключевые функции

**`process_day_from_zip(zip_bytes, symbol)`** — основной цикл обработки:
```python
with zipfile.ZipFile(zip_bytes) as zf:
    with zf.open(inner_name) as f:
        for raw_line in f:          # итеративное чтение (~864K строк)
            msg = json.loads(line)
            # apply snapshot/delta → accumulate metrics
            # при смене минуты → finalize_minute() → results.append(row)
```

**`finalize_minute(bids, asks, ...)`** — формирование строки БД:
- PRICE: best_bid, best_ask, mid_price, microprice, vwap_bid, vwap_ask
- SPREAD: spread + min/max/avg/std
- VOLUME: bid/ask vol at 0.1%/0.5%/1.0% + total + avg/std
- IMBALANCE: full + 0.1% + min/max/avg/std + range
- PRESSURE: buy_pressure, depth_ratio
- WALLS: bid/ask wall price/volume/distance
- SLIPPAGE: buy/sell at $10K/$50K/$100K
- LIQUIDITY: score, bid/ask concentration (top-3)
- VOLATILITY: mid_price_range, mid_price_std, price_momentum
- ACTIVITY: ob_bid/ask_levels, ob_update_count, best_bid/ask_changes
- JSONB: bid_levels, ask_levels (top 50, sorted by price)

**Защита от crossed book**: пропускает сообщения где `best_bid >= best_ask`

#### Зависимости (все уже установлены)
```
requests, psycopg2, tqdm
```
Не требует pandas, numpy, yaml — минимальный набор.

---

## Ценность orderbook данных

### Для ML

Orderbook даёт фичи, которые **невозможно получить** из OHLCV свечей:

| Фича | Что показывает | Почему важна |
|------|---------------|--------------|
| **Imbalance** | Давление bid vs ask | Предсказывает краткосрочное направление ДО движения цены |
| **Spread dynamics** | Ликвидность в реальном времени | Резкое расширение = надвигающийся шок |
| **Walls** | Где стоят крупные игроки | Уровни поддержки/сопротивления |
| **Slippage** | Стоимость исполнения | Оптимальный размер позиции |
| **Concentration** | Хрупкость стакана | Высокая = одна отмена и цена улетит |
| **Depth profile** (JSONB) | Полная картина ликвидности | Кластерный анализ, паттерны стакана |

### Для бэктестинга

- Реалистичный slippage (не фиксированный %, а по реальному стакану)
- Учёт ликвидности при входе/выходе
- Market impact — как ваш ордер двинул бы цену

---

## Next Steps

1. [x] Определить источник данных и формат → **quote-saver.bycsi.com, JSON Lines**
2. [x] Проверить доступность данных → **2023-01-18 — текущая дата, 1,113 файлов**
3. [x] Определить вариант реализации → **1m агрегация + JSONB top 50 уровней**
4. [x] Создать схему БД → **orderbook_bybit_futures_1m (58 колонок)**
5. [x] Описать правила агрегации → **LAST / MIN-MAX / AVG-STD / COUNT**
6. [x] Финализировать решения по реализации → **RAM, resume из БД, 3 retry + crash, хардкод конфиг**
7. [x] Написать `indicators/orderbook_bybit_loader.py` → **580 строк, все 58 колонок, syntax verified**
8. [x] Создать таблицу на VPS → **2026-02-04, 60 колонок, 2 индекса**
9. [x] Протестировать на данных за 1 день → **✅ Работает корректно**
10. [x] Запустить массовую загрузку исторических данных → **В процессе: 741 день загружено (2023-01-18 → 2025-01-27), 1,065,199 записей**
11. [ ] Добавить в `start_all_loaders.py` (после завершения загрузки)
12. [ ] Добавить остальные торговые пары (ETHUSDT, SOLUSDT, ...)

---

## References

- [Bybit WebSocket API - Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
- [Bybit Historical Data - Orderbook](https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/)
- [Bybit Historical Data - Trading](https://public.bybit.com/trading/) (это trades, НЕ orderbook!)
- Sample data: `data_samples/orderbook/2026-02-01_BTCUSDT_ob200.data.zip`
