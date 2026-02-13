# Options Data Research

**Дата исследования:** 2026-02-09
**Статус:** Исследование завершено, готово к реализации

## Содержание

1. [Теория оценки опционов](#1-теория-оценки-опционов)
2. [Источники данных: Deribit vs Bybit](#2-источники-данных-deribit-vs-bybit)
3. [Доступные исторические данные](#3-доступные-исторические-данные)
4. [Онлайн-данные для сбора](#4-онлайн-данные-для-сбора)
5. [Недоступные исторические данные](#5-недоступные-исторические-данные)
6. [Расчётные метрики для ML](#6-расчётные-метрики-для-ml)
7. [Архитектура таблиц БД](#7-архитектура-таблиц-бд)
8. [План реализации](#8-план-реализации)
9. [API Reference](#9-api-reference)

---

## 1. Теория оценки опционов

### Black-Scholes-Merton (адаптированная для крипто)

```
Call = S * N(d1) - K * e^(-rT) * N(d2)
Put  = K * e^(-rT) * N(-d2) - S * N(-d1)

d1 = [ln(S/K) + (r + sigma^2/2) * T] / (sigma * sqrt(T))
d2 = d1 - sigma * sqrt(T)
```

Где:
- **S** = цена базового актива (BTC spot)
- **K** = страйк (strike price)
- **T** = время до экспирации (в годах)
- **sigma** = implied volatility (IV)
- **r** = безрисковая ставка
- **N()** = функция нормального распределения

### Greeks (чувствительность цены опциона)

| Greek | Формула | Что измеряет | Практическое значение |
|-------|---------|--------------|----------------------|
| **Delta** (D) | dV/dS | Изменение цены опциона при изменении BTC на $1 | Вероятность исполнения опциона |
| **Gamma** (G) | d2V/dS2 | Скорость изменения Delta | Ускорение; важно для маркетмейкеров |
| **Theta** (T) | dV/dT | Временной распад за 1 день | Сколько теряет опцион каждый день |
| **Vega** (v) | dV/dsigma | Чувствительность к IV (за 1%) | Реакция на изменение волатильности |
| **Rho** (r) | dV/dr | Чувствительность к процентной ставке | Минимальное влияние в крипто |

### Implied Volatility (IV)

IV — это "обратная" величина из Black-Scholes: зная рыночную цену опциона, решаем уравнение относительно sigma.

```
Рыночная цена опциона = Black-Scholes(S, K, T, r, sigma=?)
                         Решаем относительно sigma → это и есть IV
```

**Ключевое отличие от Historical Volatility:**

```
Historical Volatility (HV)     Implied Volatility (IV)
-------------------------------  ----------------------------
Считается ИЗ свечей             Считается ИЗ цен опционов
Смотрит НАЗАД                   Смотрит ВПЕРЁД
"Что было"                      "Что рынок ожидает"
Запаздывающий индикатор          Опережающий индикатор
У нас есть (hv_loader.py)        Нужны данные опционов
```

### DVOL (Deribit Volatility Index)

DVOL — аналог VIX для криптовалют. Рассчитывается Deribit из IV всех активных опционов.
- Показывает ожидаемую 30-дневную волатильность BTC
- Запущен в марте 2021
- Единица измерения: annualized % (аналогично нашему HV)

---

## 2. Источники данных: Deribit vs Bybit

### Общее сравнение бирж

| Параметр | Bybit | Deribit |
|----------|-------|---------|
| Доля рынка опционов | ~5-10% | ~85-90% |
| Базовые активы | BTC, ETH | BTC, ETH, USDC, USDT, EURR |
| API версия | v5 | v2 |
| API ключ нужен | Нет (public endpoints) | Нет (public endpoints) |
| Rate limit | ~10 req/s | ~10 req/s (без ключа), ~20 (с ключом) |
| WebSocket | Есть | Есть (рекомендуется) |
| Base URL | `https://api.bybit.com` | `https://www.deribit.com/api/v2` |

### Детальное сравнение данных

| Метрика | Bybit | Deribit | Рекомендация |
|---------|-------|---------|--------------|
| **DVOL (IV Index)** | Нет | 5 лет истории, свечи 1m-1D | **Deribit** |
| **Historical Volatility** | 2 года, периоды 7/14/21/30d | 15 дней | **Bybit** |
| **Ticker + Greeks** | Real-time, все контракты | Real-time, все контракты | **Оба** (Deribit приоритет — 90% рынка) |
| **IV per strike** | markIv, bid1Iv, ask1Iv | mark_iv, bid_iv, ask_iv | **Оба** |
| **Open Interest** | По контрактам | По контрактам + агрегат по валюте | **Deribit** (больше объём) |
| **Trades** | Recent trades | Recent trades + IV в каждой сделке | **Deribit** (IV в трейдах) |
| **Instruments** | Все + expired | Все + expired | **Оба** |

### Почему Deribit — основной источник

Deribit — крупнейшая криптобиржа опционов (~90% объёма BTC/ETH опционов):
- Более глубокая ликвидность → более точные IV и Greeks
- DVOL (аналог VIX) — уникальная метрика
- IV включена в данные о сделках
- Больше экспираций и страйков

---

## 3. Доступные исторические данные

Данные, которые можно загрузить прямо сейчас с глубокой историей.

| # | Метрика | Источник | Глубина истории | Детализация | Endpoint | Описание |
|---|---------|----------|----------------|-------------|----------|----------|
| 1 | **DVOL BTC** | Deribit | ~5 лет (март 2021) | 1m, 1h, 12h, 1D | `public/get_volatility_index_data` | Ожидаемая волатильность BTC на 30 дней |
| 2 | **DVOL ETH** | Deribit | ~5 лет (март 2021) | 1m, 1h, 12h, 1D | `public/get_volatility_index_data` | Ожидаемая волатильность ETH на 30 дней |

> **Historical Volatility:** HV уже рассчитывается нашим `hv_loader.py` из futures свечей (периоды 7/14/30/60/90). Отдельная загрузка Bybit/Deribit HV не нужна — методология идентична, корреляция futures/spot ~0.999.

### Особенности загрузки

**DVOL (Deribit):**
- Пагинация через поле `continuation` в ответе
- Доступные резолюции: `1` (1s), `60` (1m), `3600` (1h), `43200` (12h), `1D`
- Хранение: 1h (полная история с 2021) + 1m (скользящее окно ~186 дней)
- Валюты: BTC, ETH

---

## 4. Онлайн-данные для сбора

Данные, которые нужно начать собирать с момента запуска. История будет накапливаться.

### 4.1 Запрос аналитика (17 метрик)

Аналитик запросил следующие категории данных:

| Категория | Метрики | Нужны Greeks? | Наш этап |
|-----------|---------|:---:|:---:|
| **IV** | IV ATM, IV Skew, IV Term Structure | Нет | Этап 4→5 |
| **IV** | IV Percentile/Rank, IV vs RV Spread | Нет | **Этап 3** (уже есть данные) |
| **Объёмы/OI** | Put/Call Ratio (volume), Put/Call Ratio (OI), Options Total Volume, OI по страйкам, Max Pain | Нет | Этап 4→5 |
| **Greeks** | Net Delta, Net Gamma, GEX, Vega Exposure | **Да** | Этап 4→5 |
| **События** | Дни до экспирации, Notional expiring, Large trades/blocks | Частично | Этап 4→5 |

**Итого:** 2 из 17 можно рассчитать сейчас (Этап 3), 15 требуют онлайн-сбора.

### 4.2 Источники данных (Deribit API)

#### REST API endpoints

| Endpoint | Что возвращает | 1 вызов = | Нагрузка |
|----------|---------------|-----------|----------|
| `public/get_book_summary_by_currency` | OI, Volume, mark_iv, mark_price, bid/ask | **ВСЕ контракты** (~778 BTC / ~804 ETH) | 2 вызова за снапшот |
| `public/ticker` | Всё из book_summary **+ Greeks + bid_iv/ask_iv** | **1 контракт** | ~1582 вызова за снапшот |

**Проблема REST для Greeks:** 1582 последовательных вызова `ticker` = ~160 секунд. Данные по первому и последнему контракту разнесены во времени → несогласованный снапшот (BTC мог двинуться на $100-500 за это время).

#### WebSocket (рекомендуемый подход)

**Тестирование WebSocket (2026-02-12):**

```
Подключение: wss://www.deribit.com/ws/api/v2
Подписка: public/subscribe → ticker.{instrument_name}.100ms

Результат:
  BTC: 778/778 контрактов (100%) — полный снапшот за 0.3s
  ETH: 804/804 контрактов (100%) — полный снапшот за 0.4s
  ВСЕГО: 1582/1582 (100%) за 0.4s
  Greeks: 1582/1582 (100%) — ВСЕ сообщения содержат Greeks
  Скорость: ~4400 сообщений/сек
```

**WebSocket возвращает ВСЕ данные включая Greeks (23 поля на контракт):**

```
instrument_name          BTC-27MAR26-100000-C
mark_iv                  52.81                  ← IV
bid_iv                   52.23                  ← bid IV (нет в book_summary!)
ask_iv                   53.40                  ← ask IV (нет в book_summary!)
open_interest            4931.1                 ← OI
mark_price               0.0013                 ← цена опциона
best_bid_price           0.0012                 ← bid
best_ask_price           0.0014                 ← ask
underlying_price         68017.32               ← цена BTC
greeks.delta             0.02063                ← Greeks (нет в book_summary!)
greeks.gamma             0.00000                ← Greeks
greeks.theta             -7.14315               ← Greeks
greeks.vega              11.57992               ← Greeks
greeks.rho               1.54243                ← Greeks
stats.volume             151.2                  ← объём 24h
stats.volume_usd         13351.58               ← объём USD
settlement_price         0.00138855
interest_rate            0.0
state                    open
timestamp                1770900153972          ← timestamp от Deribit
```

#### Сравнение подходов

| | REST book_summary | REST ticker | **WebSocket** |
|--|:---:|:---:|:---:|
| Все контракты за 1 вызов | **Да** | Нет (1 за вызов) | **Да** |
| mark_iv | Да | Да | **Да** |
| bid_iv / ask_iv | Нет | Да | **Да** |
| Greeks (delta, gamma, theta, vega, rho) | Нет | Да | **Да** |
| Время полного снапшота | 0.5s | ~160s | **0.4s** |
| Консистентность данных | 100% | ~95% | **~100%** |
| API вызовов / снапшот | 2 | ~1582 | 0 (push) |
| Покрытие метрик аналитика | 10/15 | 15/15 | **15/15** |

**Вывод: WebSocket — оптимальный подход.** Один daemon-процесс, подписка на все контракты, получаем ВСЕ данные включая Greeks за 0.4 секунды с идеальной консистентностью.

### 4.3 Схема сбора данных

**Архитектура (WebSocket daemon):**

```
Deribit WebSocket (push каждые 100ms при изменениях)
       │
       ▼
Daemon на VPS: подписка на ticker.*.100ms для всех BTC+ETH опционов
       │
       ├── Буфер в памяти: собираем обновления
       │
       └── Каждые 15 минут: записываем снапшот в БД
           └── ~1582 строки (778 BTC + 804 ETH) → options_deribit_raw
```

**Частота записи в БД: каждые 15 минут** (выбрано как баланс хранение/детализация):

| Частота | Строк/год | GB/год | Статус |
|---------|-----------|--------|--------|
| 1 мин | 831M | 162 GB | Слишком много (47 GB свободно) |
| 5 мин | 166M | 32 GB | Тесновато |
| **15 мин** | **55M** | **~11 GB** | **Выбрано** (комфортно на 4+ года) |
| 30 мин | 28M | 5 GB | Возможно, но теряем детализацию |
| 1 час | 14M | 3 GB | Маловато для ML |

### 4.4 Текущее количество контрактов (замер 2026-02-12)

| | BTC | ETH | Всего |
|--|:---:|:---:|:---:|
| Контрактов | 778 | 804 | 1,582 |
| Calls | 389 | — | — |
| Puts | 389 | — | — |
| Экспираций | 12 | — | — |
| Страйков | 85 (20K—380K) | — | — |
| Общий OI | 436,377 BTC (~$29.7B) | — | — |

**Экспирации BTC (на 2026-02-12):**
- Ближайшие (daily): 13FEB, 14FEB, 15FEB, 16FEB
- Недельная: 20FEB
- Месячные: 27FEB, 6MAR, 27MAR, 24APR
- Квартальные: 26JUN, 25SEP, 25DEC

### 4.5 Объём данных (оценка)

- 1 снапшот: ~1,582 строки × ~210 байт = ~324 KB
- 1 день: ~152K строк (~24 MB)
- 1 месяц: ~4.6M строк (~730 MB)
- 1 год: ~55M строк (~11 GB)

---

## 5. Недоступные исторические данные

Метрики, которые невозможно загрузить за прошлые периоды и невозможно рассчитать из свечей.

| # | Метрика | Почему нет истории | Можно ли рассчитать из свечей? | Альтернатива |
|---|---------|-------------------|-------------------------------|--------------|
| 1 | **IV Surface** | Контракты истекают, API не хранит | Нет — IV считается ИЗ цен опционов | Собирать с сегодня / купить Tardis.dev |
| 2 | **Greeks по контрактам** | Контракты истекают | Нет — нужны цены опционов | Собирать с сегодня |
| 3 | **Put/Call Ratio** | Нужны OI/volume отдельно по puts и calls | Нет — данные только на рынке опционов | Собирать с сегодня |
| 4 | **Max Pain** | Нужен OI по каждому страйку | Нет | Собирать с сегодня |
| 5 | **GEX (Gamma Exposure)** | Нужны gamma + OI по контрактам | Нет | Собирать с сегодня |
| 6 | **IV Skew** | Нужна IV по разным страйкам | Нет | Собирать с сегодня |
| 7 | **Large option trades** | API хранит ограниченную историю | Нет | Собирать с сегодня |
| 8 | **Notional expiring** | Нужен OI по экспирациям | Нет | Собирать с сегодня |

### Платные источники исторических данных

| Сервис | Данные с | Содержимое | Стоимость |
|--------|----------|------------|-----------|
| [Tardis.dev](https://docs.tardis.dev/historical-data-details/deribit) | 2019-03-30 | Все данные (trades, orderbook, ticker, Greeks) | Платная подписка |
| [Amberdata](https://www.amberdata.io/deribit-market-data) | 2021-05-21 | IV, Greeks, OI, trades | Платная подписка |
| [CryptoDataDownload](https://www.cryptodatadownload.com/data/deribit/) | 2022-09 | OHLCV по страйкам | Plus+ подписка |

### Почему нельзя рассчитать из свечей

Implied Volatility — это "обратная задача" из модели Black-Scholes. Для расчёта IV нужна **рыночная цена опциона**, которая формируется на рынке опционов. Из свечей базового актива (BTC futures) можно рассчитать только Historical Volatility — это принципиально другая метрика:

- **HV** = что было (backward-looking)
- **IV** = что ожидается (forward-looking)

Аналогия: нельзя узнать прогноз погоды (IV), анализируя вчерашнюю температуру (HV).

---

## 6. Расчётные метрики для ML

### Метрики из исторических данных (DVOL + HV)

Доступны для обучения ML сразу после загрузки.

| # | Метрика | Формула | Ценность | Что даёт для ML |
|---|---------|---------|----------|-----------------|
| 1 | `dvol` | DVOL close (прямое значение) | ★★★★★ | Ожидаемая волатильность — единственный forward-looking индикатор |
| 2 | `dvol_sma_7` | SMA(DVOL, 7) | ★★★☆☆ | Сглаженный тренд ожиданий волатильности |
| 3 | `dvol_change_24h` | DVOL_now - DVOL_24h_ago | ★★★★☆ | Скорость изменения ожиданий; резкий рост = событие |
| 4 | `iv_hv_spread` | DVOL - HV_30 | ★★★★★ | **Variance Premium** — главный сигнал; >0 = рынок ждёт движение |
| 5 | `iv_hv_ratio` | DVOL / HV_30 | ★★★★☆ | Нормализованный спред; >1.5 = экстремальные ожидания |
| 6 | `dvol_percentile_90d` | Percentile(DVOL, 90 дней) | ★★★★☆ | Относительный уровень страха за 3 месяца |

### Метрики из онлайн-данных (после 3-6 месяцев сбора)

| # | Метрика | Формула | Ценность | Что даёт для ML |
|---|---------|---------|----------|-----------------|
| 7 | `put_call_ratio_volume` | Volume_puts / Volume_calls | ★★★★★ | Сентимент через реальные деньги (надёжнее Fear&Greed) |
| 8 | `put_call_ratio_oi` | OI_puts / OI_calls | ★★★★☆ | Баланс накопленных позиций |
| 9 | `iv_skew_25d` | IV(25D put) - IV(25D call) | ★★★★★ | Направление страха: >0 = боятся падения |
| 10 | `max_pain` | argmin(total_pain(K)) для всех K | ★★★★☆ | Целевая цена к ближайшей экспирации |
| 11 | `max_pain_distance` | (spot - max_pain) / spot * 100 | ★★★★☆ | Отклонение цены от "магнита" в % |
| 12 | `gex` | sum(gamma * OI * 100 * S^2) | ★★★★★ | Режим рынка: >0 = стабилизация, <0 = усиление движений |
| 13 | `iv_atm` | IV при strike ~ spot price | ★★★★☆ | Чистая IV текущего момента (без skew) |
| 14 | `iv_term_structure` | IV_7d - IV_30d | ★★★★☆ | Ожидания краткосрочно vs долгосрочно; инверсия = стресс |
| 15 | `net_gamma_exposure` | GEX_calls - GEX_puts | ★★★★☆ | Направление давления маркетмейкеров |
| 16 | `large_trade_flow` | sum(buy_amount) - sum(sell_amount) для сделок > 1 BTC | ★★★☆☆ | Направление smart money |
| 17 | `notional_expiring_7d` | sum(OI * strike) для экспираций в ближайшие 7 дней | ★★★☆☆ | Потенциальная волатильность от экспирации |

### Ценность опционных данных для ML

Все наши текущие 261 индикатор — **запаздывающие или синхронные**. Опционные метрики добавляют **принципиально новый тип информации**:

| Вопрос | Наши 261 индикатор | Опционные метрики |
|--------|-------------------|-------------------|
| Какая волатильность сейчас? | ATR, HV, Bollinger | — |
| Какая волатильность БУДЕТ? | Не могут ответить | DVOL, IV, iv_hv_spread |
| Куда цена движется? | EMA, MACD, ADX | — |
| Куда цена ОЖИДАЕТСЯ? | Не могут ответить | IV Skew, Max Pain |
| Какой сентимент? | Fear&Greed, L/S Ratio | Put/Call Ratio (надёжнее) |
| Будет тренд или флэт? | Bollinger Squeeze (косвенно) | GEX (прямой сигнал) |
| Где уровни? | SMA, Ichimoku | Max Pain, strike walls |

---

## 7. Архитектура таблиц БД

### Таблицы: options_deribit_dvol_1h, options_deribit_dvol_1m

Хранение raw DVOL OHLC свечей. Расчётные метрики — в отдельном скрипте (Этап 3).

```sql
-- 1h таблица (✅ создана, заполнена)
CREATE TABLE options_deribit_dvol_1h (
    timestamp       TIMESTAMPTZ NOT NULL,
    currency        VARCHAR(10) NOT NULL,    -- BTC, ETH
    open            DECIMAL(20, 8),
    high            DECIMAL(20, 8),
    low             DECIMAL(20, 8),
    close           DECIMAL(20, 8),
    PRIMARY KEY (timestamp, currency)
);
CREATE INDEX idx_dvol_1h_currency_timestamp ON options_deribit_dvol_1h (currency, timestamp);

-- 1m таблица (✅ создана)
CREATE TABLE options_deribit_dvol_1m (
    timestamp       TIMESTAMPTZ NOT NULL,
    currency        VARCHAR(10) NOT NULL,    -- BTC, ETH
    open            DECIMAL(20, 8),
    high            DECIMAL(20, 8),
    low             DECIMAL(20, 8),
    close           DECIMAL(20, 8),
    PRIMARY KEY (timestamp, currency)
);
CREATE INDEX idx_dvol_1m_currency_timestamp ON options_deribit_dvol_1m (currency, timestamp);
```

**Объём данных:**
- 1h: ~87,600 записей (5 лет × 365 × 24 × 2 валюты), ~2 MB
- 1m: ~536K записей при первом запуске (180 дней × 1440 мин × 2 валюты), растёт ~40K/2 недели, ~30 MB


### Таблица: options_deribit_raw (Этап 4 — основная raw таблица)

Снапшоты всех активных опционов каждые 15 минут. Источник: WebSocket `ticker.{instrument}.100ms`.
Содержит **все данные** включая IV, Greeks, OI, Volume — достаточно для расчёта всех 15 метрик аналитика.

**Привязка к таймфреймам:** Timestamp = начало периода (Bybit standard). Снапшот берётся на 59-й секунде последней минуты периода (end-of-bar), что обеспечивает консистентность с close ценой свечи.

```sql
CREATE TABLE options_deribit_raw (
    timestamp           TIMESTAMPTZ NOT NULL,
    instrument_name     VARCHAR(50) NOT NULL,     -- BTC-27MAR26-100000-C
    -- Парсятся из instrument_name
    currency            VARCHAR(10) NOT NULL,     -- BTC, ETH
    expiration          DATE NOT NULL,            -- 2026-03-27
    strike              DECIMAL(20, 2) NOT NULL,  -- 100000.00
    option_type         VARCHAR(4) NOT NULL,      -- call, put
    -- Implied Volatility
    mark_iv             DECIMAL(10, 4),           -- Mark IV (%)
    bid_iv              DECIMAL(10, 4),           -- Bid IV (%)
    ask_iv              DECIMAL(10, 4),           -- Ask IV (%)
    -- Greeks (рассчитываются Deribit по Black-Scholes)
    delta               DECIMAL(20, 10),
    gamma               DECIMAL(20, 10),
    theta               DECIMAL(20, 10),
    vega                DECIMAL(20, 10),
    rho                 DECIMAL(20, 10),
    -- Volume & OI
    open_interest       DECIMAL(20, 8),           -- В базовой валюте (BTC/ETH)
    volume_24h          DECIMAL(20, 8),
    volume_usd_24h      DECIMAL(20, 2),
    -- Цены
    mark_price          DECIMAL(20, 10),          -- Расчётная цена опциона
    last_price          DECIMAL(20, 10),          -- Цена последней сделки
    best_bid_price      DECIMAL(20, 10),
    best_ask_price      DECIMAL(20, 10),
    best_bid_amount     DECIMAL(20, 8),           -- Объём лучшего bid
    best_ask_amount     DECIMAL(20, 8),           -- Объём лучшего ask
    underlying_price    DECIMAL(20, 2),           -- Цена фьючерса (BTC/ETH)
    index_price         DECIMAL(20, 2),           -- Индексная цена (среднее с 5 бирж)
    settlement_price    DECIMAL(20, 10),          -- Цена settlement
    high_24h            DECIMAL(20, 10),          -- Максимум 24h
    low_24h             DECIMAL(20, 10),          -- Минимум 24h
    -- Метаданные
    interest_rate       DECIMAL(10, 6),
    PRIMARY KEY (timestamp, instrument_name)
);

CREATE INDEX idx_raw_currency_timestamp ON options_deribit_raw (currency, timestamp);
CREATE INDEX idx_raw_timestamp ON options_deribit_raw (timestamp);
CREATE INDEX idx_raw_expiration ON options_deribit_raw (expiration);
```

**Колонки:** 29 (2 PK + 4 parsed + 3 IV + 5 Greeks + 3 Volume/OI + 10 Prices + 1 Meta + 1 settlement)

**Объём данных (оценка):**
- ~1,482 контрактов × 96 снапшотов/день × 365 дней = ~52M записей/год
- ~11 GB/год (29 колонок × ~210 байт/строка)
- При 47 GB свободно — хватит на 4+ года

**Примечание:** Таблицы `options_deribit_instruments` и `options_deribit_trades` из предыдущей версии плана **не нужны** — вся необходимая информация (strike, expiration, type, OI, volume) уже содержится в `options_deribit_raw`. При необходимости trades можно добавить позже отдельной таблицей.

### Таблица: options_aggregated (Этап 5, расчётная)

Агрегированные метрики, рассчитанные из собранных данных. Одна строка на timestamp.

```sql
CREATE TABLE options_aggregated (
    timestamp               TIMESTAMPTZ NOT NULL,
    currency                VARCHAR(10) NOT NULL,    -- BTC, ETH
    -- Put/Call Ratio
    put_call_ratio_volume   DECIMAL(10, 4),
    put_call_ratio_oi       DECIMAL(10, 4),
    -- IV метрики
    iv_atm                  DECIMAL(10, 4),          -- ATM Implied Volatility
    iv_skew_25d             DECIMAL(10, 4),          -- 25-delta skew
    iv_term_structure       DECIMAL(10, 4),          -- 7d IV - 30d IV
    -- Max Pain
    max_pain                DECIMAL(20, 2),
    max_pain_distance_pct   DECIMAL(10, 4),
    -- Gamma Exposure
    gex                     DECIMAL(30, 4),
    net_gamma_exposure      DECIMAL(30, 4),
    -- Large trades
    large_trade_flow        DECIMAL(20, 8),          -- Net buy-sell >1 BTC
    -- Expiration
    notional_expiring_7d    DECIMAL(30, 2),          -- USD value expiring in 7 days
    PRIMARY KEY (timestamp, currency)
);

CREATE INDEX idx_aggregated_currency ON options_aggregated (currency, timestamp);
```

---

## 8. План реализации

### Этап 1: DVOL raw свечи 1h (историческая загрузка)

**Приоритет:** Высший
**Сложность:** Низкая (1 лоадер, 1 таблица)
**Статус:** ✅ Завершён (2026-02-12)

Загрузка raw OHLC свечей DVOL с резолюцией 1h. Аналог нашей таблицы `candles_bybit_futures_1m` — только чистые данные из API, без расчётных метрик.

| Шаг | Действие | Статус |
|-----|----------|--------|
| 1.1 | Создать таблицу `options_deribit_dvol_1h` (raw OHLC) | ✅ |
| 1.2 | Создать `options_dvol_loader.py` — загрузка 1h DVOL свечей | ✅ |
| 1.3 | Загрузить историю BTC DVOL (2021-03-24 → сегодня, 42,874 записи) | ✅ |
| 1.4 | Загрузить историю ETH DVOL (42,874 записи) | ✅ |
| 1.5 | Добавить в cron на VPS для автообновления | |

**Таблица:** `options_deribit_dvol_1h` — 6 колонок (timestamp, currency, open, high, low, close)
**Инкрементальная загрузка:** MAX(timestamp) + 1h → now (Подход B)
**Пагинация:** API возвращает max 1000 записей от новых к старым, поле `continuation` для следующей страницы

**Результаты верификации (2026-02-12):**

| Метрика | BTC | ETH |
|---------|-----|-----|
| Записей | 42,874 | 42,874 |
| Покрытие | 100% | 100% |
| Пропуски | 0 | 0 |
| NULL | 0 | 0 |
| OHLC ошибки | 0 | 0 |
| DVOL диапазон | 31.41–167.83% | 30.12–206.44% |
| Spot-check vs API | 4/4 совпали | — |

### Этап 2: DVOL raw свечи 1m (расширение)

**Приоритет:** Средний
**Сложность:** Низкая (доработка существующего лоадера)
**Зависимости:** Этап 1
**Статус:** ✅ Завершён (2026-02-12)

Добавлен флаг `--timeframe 1m|1h` в `options_dvol_loader.py`. 1m данные доступны только за последние ~186 дней (скользящее окно Deribit), поэтому важно запустить регулярный сбор — данные будут накапливаться.

| Шаг | Действие | Статус |
|-----|----------|--------|
| 2.1 | Добавить `--timeframe` флаг в `options_dvol_loader.py` | ✅ |
| 2.2 | Создать таблицу `options_deribit_dvol_1m` | ✅ |
| 2.3 | Загрузить доступные 1m данные BTC (259,168 записей) | ✅ |
| 2.4 | Загрузить доступные 1m данные ETH (259,166 записей) | ✅ |
| 2.5 | Автосбор через cron (start_all_loaders.py, ежедневно 01:00) | ✅ |

**Ограничение API:** 1m данные доступны только за последние ~186 дней. Старые данные удаляются Deribit. Консервативный порог: 180 дней.

**Использование:**
```bash
python3 options_dvol_loader.py                          # BTC, 1m + 1h (оба)
python3 options_dvol_loader.py --timeframe 1m           # только 1m
python3 options_dvol_loader.py --timeframe 1h           # только 1h
python3 options_dvol_loader.py --currency ETH --timeframe 1m
```

**Схема накопления 1m данных:**
```
Запуск 1 (сейчас):    ~259K записей (180 дней × 1440 мин/день)
Каждые 2 недели:      +~20K новых записей (14 дней × 1440 мин)
Через год:            ~619K записей (180 дней начальных + 12 мес накопленных)
```

### Этап 3: Расчёт метрик из DVOL свечей

**Приоритет:** Высокий (сразу после Этапа 1)
**Сложность:** Средняя (отдельный скрипт-калькулятор)
**Зависимости:** Этап 1

Создание отдельного скрипта `options_dvol_indicators.py`, который рассчитывает метрики из raw DVOL свечей. Архитектура аналогична нашим indicator loaders: читает raw данные → считает → записывает в ту же или отдельную таблицу.

| Шаг | Действие | Статус |
|-----|----------|--------|
| 3.1 | Определить финальный список метрик для расчёта | |
| 3.2 | Создать `options_dvol_indicators.py` | |
| 3.3 | Рассчитать метрики по всей истории | |

**Планируемые группы метрик:**

**Трендовые:** dvol_sma_24, dvol_sma_168, dvol_ema_12, dvol_ema_26
**Momentum:** dvol_change_1h, dvol_change_24h, dvol_change_pct_24h, dvol_roc_24
**Уровни:** dvol_percentile_30d, dvol_percentile_90d, dvol_zscore_30d, dvol_range_24h
**IV vs HV спред:** iv_hv_spread (DVOL - HV_30), iv_hv_ratio, iv_hv_spread_zscore
**Корреляция BTC/ETH:** dvol_btc_eth_spread, dvol_btc_eth_ratio, dvol_btc_eth_corr_24h, dvol_btc_eth_corr_168h

> **Корреляция BTC/ETH DVOL** — уникальная метрика. Обычно ETH DVOL > BTC DVOL (ETH более волатильный). Когда спред резко сужается, инвертируется, или корреляция падает — это сигнал стресса или смены режима рынка. Расхождение ожиданий волатильности между двумя главными криптоактивами может предсказывать развороты.

> **Примечание:** Бывший Этап 4 (Bybit HV) удалён — дублирует наш `hv_loader.py`, который уже считает HV из futures свечей. Для `iv_hv_spread` используем существующий `hv_30` из `indicators_bybit_futures_1h`.

#### Хранение метрик

Метрики добавляются как колонки в существующие таблицы `options_deribit_dvol_1h` и `options_deribit_dvol_1m`:
```
options_deribit_dvol_1h: timestamp, currency, open, high, low, close, dvol_sma_24, dvol_ema_12, iv_hv_spread, ...
```

Данных немного (~86K записей в 1h, ~536K в 1m), поэтому отдельные таблицы не нужны. Для 4h/1d — агрегация из 1h на лету (мгновенно на 42K записях).

#### Детальный список метрик (~15 колонок)

**Группа 1 — Трендовые** (считаются из DVOL close):

| Метрика | Формула | Зачем |
|---------|---------|-------|
| `dvol_sma_24` | SMA(close, 24h) | Дневной тренд ожиданий волатильности |
| `dvol_sma_168` | SMA(close, 168h = 7 дней) | Недельный тренд |
| `dvol_ema_12` | EMA(close, 12h) | Быстрая реакция на изменения |
| `dvol_ema_26` | EMA(close, 26h) | Медленная реакция |

**Группа 2 — Momentum** (скорость изменения DVOL):

| Метрика | Формула | Зачем |
|---------|---------|-------|
| `dvol_change_24h` | close - close[24h ago] | Абсолютное изменение за сутки; резкий рост = событие |
| `dvol_change_pct_24h` | (close - close[24h ago]) / close[24h ago] × 100 | Относительное изменение в % |
| `dvol_roc_24` | Rate of Change за 24h | Momentum ожиданий волатильности |

**Группа 3 — Уровни** (где DVOL относительно истории):

| Метрика | Формула | Зачем |
|---------|---------|-------|
| `dvol_percentile_30d` | Percentile(close, 30 дней) | Относительный уровень за месяц (0-100) |
| `dvol_percentile_90d` | Percentile(close, 90 дней) | Относительный уровень за квартал |
| `dvol_zscore_30d` | (close - mean_30d) / std_30d | Отклонение от нормы в стандартных отклонениях |

**Группа 4 — IV vs HV спред** (главная метрика, уникальна для ML):

| Метрика | Формула | Зачем |
|---------|---------|-------|
| `iv_hv_spread` | DVOL_close - HV_30 (из `indicators_bybit_futures_1h`) | **Variance Premium** — >0 рынок ждёт движение больше чем обычно; ключевой предиктор |
| `iv_hv_ratio` | DVOL_close / HV_30 | Нормализованный спред; >1.5 = экстремальные ожидания |

> **iv_hv_spread** — это cross-table JOIN между `options_deribit_dvol_1h` и `indicators_bybit_futures_1h` (колонка `hv_30`). Требует маппинга currency→symbol (BTC→BTCUSDT, ETH→ETHUSDT).

**Группа 5 — Корреляция BTC/ETH DVOL** (уникальная метрика):

| Метрика | Формула | Зачем |
|---------|---------|-------|
| `dvol_btc_eth_spread` | DVOL_ETH - DVOL_BTC | Обычно >0 (ETH волатильнее). Инверсия = стресс |
| `dvol_btc_eth_ratio` | DVOL_ETH / DVOL_BTC | Нормализованный спред между активами |
| `dvol_btc_eth_corr_24h` | Correlation(BTC_close, ETH_close, 24h) | Падение корреляции = расхождение ожиданий = смена режима |

> **Корреляция BTC/ETH** — требует JOIN двух рядов (currency='BTC' и currency='ETH') по timestamp. Записывается для обеих валют одинаково.

#### Реализация

- Скрипт: `options_dvol_indicators.py`
- Архитектура: читает raw DVOL → считает метрики → UPDATE в ту же таблицу
- Инкрементальная загрузка: находит NULL колонки, считает только для них
- `--force-reload`: пересчёт всех метрик

### Этап 4: Онлайн-сбор снапшотов опционов (WebSocket)

**Приоритет:** Высокий (чем раньше запустим — тем больше данных накопим)
**Сложность:** Средняя (1 daemon-скрипт, 1 таблица)
**Данных:** Накапливается с момента запуска
**Оценка хранения:** ~55M записей/год, ~11 GB/год
**Статус:** В процессе (скрипт создан, тестируется)

#### Почему только онлайн?

Когда контракт истекает (каждую пятницу!), его исторические данные **исчезают навсегда** из API. Единственный способ — **собирать в реальном времени**.

> Платные источники (Tardis.dev от $200/мес) имеют архивы с 2019 года, но для начала достаточно собирать самим.

#### Реализация: WebSocket daemon

**Скрипт:** `data_collectors/deribit/options/options_deribit_raw_ws_collector.py`

Размещён в `data_collectors/` (не в `indicators/`), потому что это 24/7 daemon с WebSocket-подключением и автоматическим reconnect — принципиально отличается от batch-loaders. Требует systemd/Docker для auto-restart.

```
Deribit WebSocket: wss://www.deribit.com/ws/api/v2
  │
  ├── Подписка: ticker.{instrument}.100ms для всех BTC+ETH опционов
  │   (подтверждение: 1482 каналов за 0.3s)
  │
  ├── Push-обновления: ~400-700 сообщений/сек
  │   (каждое содержит: IV, Greeks, OI, Volume, цены — 29 полей)
  │
  └── Буфер → снапшот на 59-й секунде каждого периода → INSERT в БД
      └── ~1482 строки за 0.3 секунды (100% покрытие, 100% Greeks)
```

**Запуск:**
```bash
# Продакшн
python3 data_collectors/deribit/options/options_deribit_raw_ws_collector.py

# Тестирование (без записи в БД)
python3 data_collectors/deribit/options/options_deribit_raw_ws_collector.py --dry-run

# Другой интервал
python3 data_collectors/deribit/options/options_deribit_raw_ws_collector.py --interval 5

# Один снапшот и выход
python3 data_collectors/deribit/options/options_deribit_raw_ws_collector.py --single-snapshot --dry-run
```

#### Привязка снапшотов к таймфреймам свечей

Снапшоты привязаны к **wall clock**, а не ко времени запуска скрипта. Это обеспечивает консистентность с данными свечей для ML.

**Подход:** Снапшот берётся на **59-й секунде последней минуты периода** (end-of-bar snapshot). Timestamp в БД = начало периода (Bybit standard).

```
Пример для interval=15m:
  14:00:00 — начало периода
  14:14:59 — берём снапшот буфера (мгновенно, ~микросекунды)
  14:14:59 → сохраняем с timestamp = 14:00:00
  14:15:00...14:29:59 — следующий период
  14:29:59 → сохраняем с timestamp = 14:15:00
```

Для других интервалов:

| Интервал | Снапшот в | Timestamp в БД |
|----------|-----------|----------------|
| 1m | XX:XX:59 | XX:XX:00 |
| 5m | XX:04:59, XX:09:59, ... | XX:00, XX:05, ... |
| 15m | XX:14:59, XX:29:59, XX:44:59, XX:59:59 | XX:00, XX:15, XX:30, XX:45 |
| 1h | XX:59:59 | XX:00 |

**Почему end-of-bar:** IV и Greeks в момент close свечи совпадают с close ценой — данные консистентны для ML. Так работают Bloomberg, CBOE, Deribit собственные агрегации.

#### Консольный мониторинг

Каждые 60 секунд выводится однострочный статус:

```
09:58:54 2026-02-13 | BTC $96,647 IV:53.3% Contr:724 | ETH $2,681 IV:65.3% Contr:758 | Exp:22h01m(62ct) | 396/s | ▸snap:1m04s | Up:1m | saved:0 | rc:0
```

| Поле | Описание |
|------|----------|
| `09:58:54 2026-02-13` | Текущее время UTC |
| `BTC $96,647 IV:53.3%` | Index price (средневзвешенная с 5 бирж) и ATM Implied Volatility |
| `Contr:724` | Количество активных контрактов |
| `Exp:22h01m(62ct)` | Countdown до ближайшей экспирации и количество истекающих контрактов |
| `396/s` | Входящие WebSocket-сообщения в секунду |
| `▸snap:1m04s` | Countdown до следующего снапшота |
| `Up:1m` | Uptime процесса |
| `saved:0` | Количество записанных снапшотов |
| `rc:0` | Количество reconnects |

При снапшоте:
```
>>> [DRY RUN] 09:59:59 SNAPSHOT period:09:45 2026-02-13 | 1484 rows (BTC:726 ETH:758) | saved:1
```

При reconnect:
```
!!! RECONNECT 11:52:38 2026-02-13 | Error: connection closed | Retry in 5s | rc:1
```

#### Пояснение ключевых метрик в консоли

**Index Price (BTC $96,647):** Индексная цена Deribit — средневзвешенная с 5 спотовых бирж (Coinbase, Bitstamp, Gemini, Kraken, LMAX). Deribit — чисто деривативная биржа (нет спот-торговли), поэтому использует внешний индекс для расчёта опционных премий и settlement. Отличие от Bybit futures ~$10-50 (0.01-0.07%).

**ATM IV (53.3%):** Implied Volatility "at-the-money" — подразумеваемая волатильность опциона с ближайшим к текущей цене страйком и ближайшей экспирацией. Означает, что рынок ожидает колебания BTC ±53.3% за год. IV извлекается обратным путём из цены опционов через формулу Black-Scholes — это forward-looking метрика (что рынок ОЖИДАЕТ), в отличие от HV (что БЫЛО).

| Уровень IV BTC | Интерпретация |
|----------------|---------------|
| 20-30% | Очень спокойный рынок (редко) |
| 40-50% | Нормальный |
| 50-60% | Умеренно повышенный |
| 70-80% | Высокий страх |
| 100%+ | Паника (крах FTX, COVID) |

#### Экспирация контрактов

Все контракты на Deribit экспирируются в **08:00 UTC**. Типы экспираций:

| Тип | Расписание | Контрактов |
|-----|-----------|------------|
| Daily | Каждый день (ближайшие 4 дня) | 18-28 |
| Weekly | Каждую пятницу | 30-82 |
| Monthly | Последняя пятница месяца | 62-124 |
| Quarterly | Последняя пятница квартала | 68-88 |

Daemon отслеживает экспирации и автоматически переподписывается:
- Раз в час: REST `get_book_summary_by_currency` → обновляет список инструментов
- Новые контракты → подписка через WebSocket
- Истёкшие контракты → отписка + удаление из буфера

#### Автоматический reconnect

Exponential backoff при потере WebSocket-соединения:

```
Попытка 1: 5 секунд
Попытка 2: 10 секунд
Попытка 3: 20 секунд
...
Максимум: 300 секунд (5 минут)
При успешном подключении: сброс на 5 секунд
```

При каждом reconnect: полная переинициализация (загрузка инструментов → подключение → подписка → сбор данных).

Встроенный heartbeat: `ping_interval=30s`, `ping_timeout=10s` — автоматическое определение "мёртвого" соединения.

#### Преимущества WebSocket vs REST polling

- **Консистентность:** все данные в одном снапшоте (~0.3s vs ~160s для REST)
- **Полнота:** Greeks включены (REST book_summary не содержит Greeks)
- **Нагрузка:** 0 API вызовов (push от биржи)
- **Один скрипт** вместо трёх (instruments + ticker + trades)

#### Что получаем за один снапшот (29 колонок на контракт)

```
BTC-27MAR26-100000-C:
  mark_iv:    52.81%           ← IV конкретного страйка
  bid_iv:     52.23%           ← bid IV
  ask_iv:     53.40%           ← ask IV
  delta:       0.021           ← вероятность исполнения
  gamma:       0.00000         ← ускорение delta
  theta:      -7.143           ← временной распад ($/день)
  vega:       11.580           ← чувствительность к IV
  rho:         1.542           ← чувствительность к ставке
  open_interest: 4931.1 BTC    ← открытые позиции
  volume_24h:  151.2 BTC       ← объём торгов
  volume_usd:  13351.58        ← объём в USD
  mark_price:  0.0013          ← цена опциона (BTC)
  last_price:  0.0012          ← последняя сделка
  best_bid:    0.0012          ← лучший bid
  best_ask:    0.0014          ← лучший ask
  bid_amount:  50.0            ← объём bid
  ask_amount:  30.0            ← объём ask
  underlying:  $68,017         ← цена фьючерса BTC
  index_price: $67,950         ← индексная цена (5 бирж)
  settlement:  0.00139         ← цена settlement
  high_24h:    0.0015          ← максимум 24h
  low_24h:     0.0010          ← минимум 24h
  interest_rate: 0.0           ← процентная ставка
```

**Данные хранятся в таблице `options_deribit_raw`** (29 колонок, schema в секции 7).

#### Объём данных

| Период | Строк | Размер |
|--------|-------|--------|
| 1 снапшот (15 мин) | ~1,482 | ~324 KB |
| 1 день | ~142K | ~24 MB |
| 1 месяц | ~4.3M | ~730 MB |
| 1 год | ~52M | ~11 GB |

| Шаг | Действие | Статус |
|-----|----------|--------|
| 4.1 | Создать таблицу `options_deribit_raw` | ✅ |
| 4.2 | Создать `options_deribit_raw_ws_collector.py` (WebSocket daemon) | ✅ |
| 4.3 | Тестирование (локально: dry-run) | ✅ |
| 4.4 | Тестирование (реальная запись в БД) | |
| 4.5 | Деплой на VPS (tmux + wrapper + cron @reboot) | |

#### Деплой на VPS: tmux + wrapper + cron @reboot

Выбран подход **tmux + wrapper script** вместо systemd/Docker. Причины:
- Живая консоль: `tmux attach` показывает однострочные статусы в реальном времени
- Auto-restart: wrapper script перезапускает Python при падении (через 10 секунд)
- Auto-start: cron `@reboot` создаёт tmux сессию при перезагрузке сервера
- Масштабируемость: при 5+ daemon'ов — отдельные tmux окна в одной сессии

**Файлы:**
```
data_collectors/deribit/options/
├── options_deribit_raw_ws_collector.py   # Основной WebSocket collector
├── run_ws_collector.sh                   # Wrapper script (auto-restart)
└── logs/                                 # Лог-файлы (автосоздание)
```

**Схема работы:**
```
Сервер перезагрузился
  └── cron @reboot
      └── tmux new-session -d -s data_collector_options_raw_deribit
          └── run_ws_collector.sh (while true)
              └── options_deribit_raw_ws_collector.py
                  ├── WebSocket → буфер → снапшот на :59 секунде → БД
                  ├── Упал → wrapper ждёт 10с → перезапуск
                  └── tmux attach → живая консоль
```

**Установка на VPS:**
```bash
# 1. Обновить код
cd /root/TradingCharts && git pull origin main

# 2. Сделать wrapper исполняемым
chmod +x data_collectors/deribit/options/run_ws_collector.sh

# 3. Добавить в cron (автозапуск при reboot)
crontab -e
# Добавить строку:
@reboot /usr/bin/tmux new-session -d -s data_collector_options_raw_deribit '/root/TradingCharts/data_collectors/deribit/options/run_ws_collector.sh'

# 4. Запустить сейчас (без перезагрузки)
tmux new-session -d -s data_collector_options_raw_deribit '/root/TradingCharts/data_collectors/deribit/options/run_ws_collector.sh'

# 5. Проверить
tmux attach -t data_collector_options_raw_deribit
# Ctrl+B, D — отключиться (скрипт продолжает работать)
```

**Мониторинг:**
```bash
tmux attach -t data_collector_options_raw_deribit   # живая консоль
tmux ls                                              # список всех сессий
```

### Этап 5: Агрегированные метрики из онлайн-данных

**Приоритет:** Средний (можно начать сразу после запуска Этапа 4 — не нужно ждать месяцы)
**Сложность:** Средняя
**Зависимости:** Этап 4 (нужны raw данные по контрактам)

#### Метрики для аналитика (15 из 17)

| Метрика | Формула | Откуда из raw | Что даёт для ML |
|---------|---------|---------------|-----------------|
| `put_call_ratio_volume` | Volume(puts) / Volume(calls) | volume_24h + option_type | Сентимент: >1 = страх, <1 = жадность |
| `put_call_ratio_oi` | OI(puts) / OI(calls) | open_interest + option_type | Баланс накопленных позиций |
| `options_total_volume` | sum(volume_24h) | volume_24h | Общая активность рынка опционов |
| `iv_atm` | IV при страйке ≈ spot | mark_iv + strike + underlying_price | Чистая IV текущего момента |
| `iv_skew_25d` | IV(25d put) - IV(25d call) | mark_iv + delta | >0 = боятся падения; инверсия = разворот |
| `iv_term_structure` | IV_7d - IV_30d | mark_iv + expiration | Инверсия = стресс |
| `max_pain` | argmin(total_pain(K)) | open_interest + strike + option_type | "Магнит" для цены к экспирации |
| `max_pain_distance` | (spot - max_pain) / spot × 100 | max_pain + underlying_price | Отклонение от магнита в % |
| `gex` | sum(gamma × OI × 100 × S²) | gamma + open_interest + underlying_price | >0 = стабильность, <0 = хаос |
| `net_delta` | sum(delta × OI) calls - puts | delta + open_interest + option_type | Направление позиций маркетмейкеров |
| `net_gamma` | sum(gamma × OI) | gamma + open_interest | Составная часть GEX |
| `vega_exposure` | sum(vega × OI) | vega + open_interest | Чувствительность рынка к IV |
| `days_to_expiry_nearest` | min(expiration) - now | expiration | Ближайшая экспирация |
| `notional_expiring_7d` | sum(OI × strike) для exp < 7д | open_interest + strike + expiration | Потенциальная волатильность |
| `large_trade_flow` | — | Требует отдельной таблицы trades | Направление smart money |

> **Примечание:** 14 из 15 метрик считаются из `options_deribit_raw`. Только `large_trade_flow` требует отдельного сбора trades — можно добавить позже.

#### Реализация

- Скрипт: `options_analytics_loader.py`
- Таблица: `options_aggregated` (одна строка на timestamp × currency)
- Интервал: каждые 15 мин (сразу после записи raw снапшота)
- Можно запустить сразу после старта Этапа 4 (не нужно ждать месяцы для базовых метрик)
- Для ML моделей нужна история: 1-3 месяца минимум

| Шаг | Действие | Статус |
|-----|----------|--------|
| 5.1 | Создать `options_analytics_loader.py` | |
| 5.2 | Интеграция с ML pipeline | |

### Сравнение этапов и приоритеты

| | Этап 3 (метрики DVOL) | Этап 4 (WebSocket сбор) | Этап 5 (агрегация) |
|--|----------------------|------------------------|-------------------|
| Данные | **Уже есть** (DVOL raw) | Накапливаются с момента запуска | Считаются из Этапа 4 |
| Время до результата | **Дни** | Мгновенно (raw), 1-3 мес для ML | Сразу после Этапа 4 |
| Сложность | Низкая (1 скрипт) | Средняя (1 WebSocket daemon) | Средняя (1 скрипт) |
| Ценность для ML | Высокая (iv_hv_spread) | Нет (сырые данные) | Очень высокая (15 метрик) |
| Хранение | ~0 (колонки в существующей таблице) | ~11 GB/год | ~10 MB/год |
| Зависимости | Нет | Нет | Этап 4 |
| Метрик аналитика | 2/17 | — | 15/17 |

**Рекомендация:** Этап 3 + Этап 4 параллельно. Этап 3 даёт быстрый результат (2 метрики), Этап 4 начинает копить данные. Этап 5 запускаем сразу после Этапа 4.

---

## 9. API Reference

### Deribit API v2

**Base URL:** `https://www.deribit.com/api/v2`
**Test URL:** `https://test.deribit.com/api/v2`
**Документация:** https://docs.deribit.com/

#### Endpoints для Этапа 1-2 (DVOL)

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `public/get_volatility_index_data` | GET | DVOL свечи (OHLC) с историей |

Параметры:
- `currency` (required): BTC, ETH
- `start_timestamp` (required): ms since epoch
- `end_timestamp` (required): ms since epoch
- `resolution` (required): "1", "60", "3600", "43200", "1D"

Ответ:
```json
{
  "result": {
    "data": [[1617235200000, 72.5, 74.2, 71.8, 73.1], ...],
    "continuation": 1617321600000
  }
}
```

#### REST Endpoints для Этапа 4

| Endpoint | Метод | Описание | 1 вызов = |
|----------|-------|----------|-----------|
| `public/get_book_summary_by_currency` | GET | OI, Volume, mark_iv для всех контрактов | Все (~778 BTC) |
| `public/ticker` | GET | Всё + Greeks + bid_iv/ask_iv | 1 контракт |
| `public/get_instruments` | GET | Список всех инструментов | Все |
| `public/get_last_trades_by_currency` | GET | Последние сделки | Все |

#### WebSocket для Этапа 4 (рекомендуемый)

**URL:** `wss://www.deribit.com/ws/api/v2`

| Канал | Описание | Данные |
|-------|----------|--------|
| `ticker.{instrument_name}.100ms` | Push-обновления тикера | IV, Greeks, OI, Volume, цены (23 поля) |

**Подписка:**
```json
{
  "jsonrpc": "2.0",
  "method": "public/subscribe",
  "params": {
    "channels": ["ticker.BTC-27MAR26-100000-C.100ms", "..."]
  }
}
```

**Тестирование (2026-02-12):** 1582 контракта (BTC+ETH), 100% покрытие за 0.4s, все содержат Greeks.

---

## Ссылки

- [Deribit API Documentation](https://docs.deribit.com/)
- [Deribit public/ticker](https://docs.deribit.com/api-reference/market-data/public-ticker)
- [Deribit public/get_instruments](https://docs.deribit.com/api-reference/market-data/public-get_instruments)
- [Deribit public/get_book_summary_by_instrument](https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_instrument)
- [Deribit DVOL Index](https://insights.deribit.com/exchange-updates/dvol-deribit-implied-volatility-index/)
- [Deribit Options Greeks](https://insights.deribit.com/education/introduction-to-option-greeks/)
- [Bybit Historical Volatility](https://bybit-exchange.github.io/docs/v5/market/iv)
- [Bybit Get Tickers](https://bybit-exchange.github.io/docs/v5/market/tickers)
- [Tardis.dev Deribit Historical Data](https://docs.tardis.dev/historical-data-details/deribit)
