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
| 3 | **Historical Volatility** | Deribit | 15 дней | Почасовая | `public/get_historical_volatility` | Realized volatility от Deribit |
| 4 | **Historical Volatility** | Bybit | 2 года (~2024-02 - сегодня) | Почасовая | `/v5/market/historical-volatility` | Realized volatility, периоды 7/14/21/30d |

### Особенности загрузки

**DVOL (Deribit):**
- Пагинация через поле `continuation` в ответе
- Доступные резолюции: `1` (1s), `60` (1m), `3600` (1h), `43200` (12h), `1D`
- Рекомендуемая резолюция для хранения: 1h (баланс между детализацией и объёмом)
- Валюты: BTC, ETH

**Bybit HV:**
- Ограничение: `endTime - startTime <= 30 дней` за запрос
- Загрузка батчами по 30 дней (аналог premium_index_loader)
- Периоды: 7, 14, 21, 30 дней
- Данные: почасовые

---

## 4. Онлайн-данные для сбора

Данные, которые нужно начать собирать с момента запуска. История будет накапливаться.

| # | Метрика | Источник | Endpoint | Частота | Что получаем |
|---|---------|----------|----------|---------|--------------|
| 1 | **Greeks** (delta, gamma, theta, vega, rho) | Deribit | `public/ticker` | 15 мин | Для каждого активного контракта |
| 2 | **Greeks** (delta, gamma, theta, vega) | Bybit | `/v5/market/tickers?category=option` | 15 мин | Для каждого активного контракта |
| 3 | **IV per strike** (bid_iv, ask_iv, mark_iv) | Deribit | `public/ticker` | 15 мин | IV для каждого страйка/экспирации |
| 4 | **IV per strike** (bid1Iv, ask1Iv, markIv) | Bybit | `/v5/market/tickers?category=option` | 15 мин | IV для каждого страйка/экспирации |
| 5 | **Open Interest per strike** | Deribit | `public/get_book_summary_by_currency` | 15 мин | OI отдельно puts/calls по страйкам |
| 6 | **Open Interest per strike** | Bybit | `/v5/market/tickers?category=option` | 15 мин | OI по контрактам |
| 7 | **Options trades** | Deribit | `public/get_last_trades_by_currency` | 5 мин | Сделки с IV, amount, direction |
| 8 | **Options trades** | Bybit | `/v5/market/recent-trade?category=option` | 5 мин | Сделки по опционам |
| 9 | **Instruments list** | Deribit | `public/get_instruments` | 1 час | Все контракты: strike, expiry, type |
| 10 | **Instruments list** | Bybit | `/v5/market/instruments-info?category=option` | 1 час | Все контракты: strike, expiry, type |

### Объём данных (оценка)

- Активных BTC опционов одновременно: ~500-1000 контрактов (Deribit)
- Снапшот каждые 15 минут: ~1000 записей × 96 раз/день = ~96,000 записей/день
- За месяц: ~2.9M записей
- За год: ~35M записей

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

### Таблица: options_deribit_dvol

Хранение DVOL (Implied Volatility Index). Аналог нашей таблицы indicators, но для опционных данных.

```sql
CREATE TABLE options_deribit_dvol (
    timestamp       TIMESTAMPTZ NOT NULL,
    currency        VARCHAR(10) NOT NULL,    -- BTC, ETH
    open            DECIMAL(20, 8),
    high            DECIMAL(20, 8),
    low             DECIMAL(20, 8),
    close           DECIMAL(20, 8),
    -- Расчётные метрики
    dvol_sma_7      DECIMAL(20, 8),          -- SMA(close, 7)
    dvol_change_24h DECIMAL(20, 8),          -- close - close[24h ago]
    dvol_percentile_90d DECIMAL(10, 4),      -- Percentile за 90 дней
    PRIMARY KEY (timestamp, currency)
);

CREATE INDEX idx_dvol_currency_timestamp ON options_deribit_dvol (currency, timestamp);
```

**Объём данных (оценка):**
- 5 лет × 365 дней × 24 часа × 2 валюты = ~87,600 записей (1h)
- ~10 MB

### Таблица: options_bybit_hv

Хранение Historical Volatility от Bybit (опционная метрика).

```sql
CREATE TABLE options_bybit_hv (
    timestamp       TIMESTAMPTZ NOT NULL,
    currency        VARCHAR(10) NOT NULL,    -- BTC, ETH
    period          INTEGER NOT NULL,        -- 7, 14, 21, 30
    volatility      DECIMAL(20, 10),         -- Annualized HV
    PRIMARY KEY (timestamp, currency, period)
);

CREATE INDEX idx_bybit_hv_currency_timestamp ON options_bybit_hv (currency, timestamp);
```

**Объём данных (оценка):**
- 2 года × 365 × 24 × 2 валюты × 4 периода = ~140,160 записей
- ~15 MB

### Таблица: options_deribit_instruments (Фаза 3)

```sql
CREATE TABLE options_deribit_instruments (
    instrument_name     VARCHAR(50) PRIMARY KEY,  -- BTC-28MAR26-100000-C
    currency            VARCHAR(10) NOT NULL,     -- BTC, ETH
    strike              DECIMAL(20, 2) NOT NULL,
    option_type         VARCHAR(4) NOT NULL,      -- call, put
    expiration_timestamp TIMESTAMPTZ NOT NULL,
    settlement_period   VARCHAR(20),              -- month, week, day
    is_active           BOOLEAN DEFAULT TRUE,
    min_trade_amount    DECIMAL(20, 8),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_instruments_currency ON options_deribit_instruments (currency);
CREATE INDEX idx_instruments_expiration ON options_deribit_instruments (expiration_timestamp);
CREATE INDEX idx_instruments_active ON options_deribit_instruments (is_active);
```

### Таблица: options_deribit_ticker (Фаза 3)

Снапшоты всех активных опционов каждые 15 минут.

```sql
CREATE TABLE options_deribit_ticker (
    timestamp           TIMESTAMPTZ NOT NULL,
    instrument_name     VARCHAR(50) NOT NULL,
    -- Цены
    mark_price          DECIMAL(20, 8),
    last_price          DECIMAL(20, 8),
    best_bid_price      DECIMAL(20, 8),
    best_ask_price      DECIMAL(20, 8),
    underlying_price    DECIMAL(20, 2),
    -- Implied Volatility
    mark_iv             DECIMAL(10, 4),           -- Mark IV (%)
    bid_iv              DECIMAL(10, 4),
    ask_iv              DECIMAL(10, 4),
    -- Greeks
    delta               DECIMAL(20, 10),
    gamma               DECIMAL(20, 10),
    theta               DECIMAL(20, 10),
    vega                DECIMAL(20, 10),
    rho                 DECIMAL(20, 10),
    -- Volume & OI
    open_interest       DECIMAL(20, 8),           -- В базовой валюте (BTC)
    volume_24h          DECIMAL(20, 8),
    turnover_24h        DECIMAL(20, 8),
    -- Метаданные
    interest_rate       DECIMAL(10, 6),
    PRIMARY KEY (timestamp, instrument_name)
);

CREATE INDEX idx_ticker_instrument ON options_deribit_ticker (instrument_name, timestamp);
CREATE INDEX idx_ticker_timestamp ON options_deribit_ticker (timestamp);
```

**Объём данных (оценка):**
- ~1000 контрактов × 96 снапшотов/день × 365 дней = ~35M записей/год
- ~5-7 GB/год

### Таблица: options_deribit_trades (Фаза 3)

```sql
CREATE TABLE options_deribit_trades (
    trade_id            VARCHAR(50) PRIMARY KEY,
    timestamp           TIMESTAMPTZ NOT NULL,
    instrument_name     VARCHAR(50) NOT NULL,
    direction           VARCHAR(4) NOT NULL,      -- buy, sell
    amount              DECIMAL(20, 8) NOT NULL,  -- В BTC
    price               DECIMAL(20, 8),
    iv                  DECIMAL(10, 4),           -- IV на момент сделки
    mark_price          DECIMAL(20, 8),
    index_price         DECIMAL(20, 2),
    PRIMARY KEY (trade_id)
);

CREATE INDEX idx_trades_timestamp ON options_deribit_trades (timestamp);
CREATE INDEX idx_trades_instrument ON options_deribit_trades (instrument_name, timestamp);
CREATE INDEX idx_trades_amount ON options_deribit_trades (amount);  -- Для фильтрации large trades
```

### Таблица: options_aggregated (Фаза 4, расчётная)

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

и### Этап 1: DVOL raw свечи 1h (историческая загрузка)

**Приоритет:** Высший
**Сложность:** Низкая (1 лоадер, 1 таблица)
**Статус:** В работе

Загрузка raw OHLC свечей DVOL с резолюцией 1h. Аналог нашей таблицы `candles_bybit_futures_1m` — только чистые данные из API, без расчётных метрик.

| Шаг | Действие | Статус |
|-----|----------|--------|
| 1.1 | Создать таблицу `options_deribit_dvol` (raw OHLC) | |
| 1.2 | Создать `options_dvol_loader.py` — загрузка 1h DVOL свечей | |
| 1.3 | Загрузить историю BTC DVOL (2021-03-24 → сегодня, ~42,864 записи) | |
| 1.4 | Загрузить историю ETH DVOL (~42,864 записи) | |
| 1.5 | Добавить в cron на VPS для автообновления | |

**Таблица:** `options_deribit_dvol` — 6 колонок (timestamp, currency, open, high, low, close)
**Инкрементальная загрузка:** MAX(timestamp) + 1h → now (Подход B)
**Пагинация:** API возвращает max 1000 записей от новых к старым, поле `continuation` для следующей страницы

**API:**
```
GET https://www.deribit.com/api/v2/public/get_volatility_index_data
    ?currency=BTC
    &start_timestamp=1616544000000   (2021-03-24 00:00 UTC)
    &end_timestamp=<now_ms>
    &resolution=3600                  (1h свечи)
```

**Данные из API (пример):**
```
timestamp            open    high    low     close
2024-03-01 00:00     65.54   65.54   65.26   65.36    ← annualized IV в %
2024-03-01 01:00     65.36   65.36   64.53   64.53
```

### Этап 2: DVOL raw свечи 1m (расширение)

**Приоритет:** Средний
**Сложность:** Низкая (доработка существующего лоадера)
**Зависимости:** Этап 1

Добавление режима `--resolution 1m` в `options_dvol_loader.py`. 1m данные доступны только за последние ~6 месяцев (скользящее окно Deribit), поэтому важно запустить сбор как можно раньше — данные будут накапливаться.

| Шаг | Действие | Статус |
|-----|----------|--------|
| 2.1 | Добавить `--resolution` флаг в `options_dvol_loader.py` | |
| 2.2 | Загрузить доступные 1m данные (~6 месяцев назад, ~268K записей/валюту) | |
| 2.3 | Настроить cron для регулярного сбора 1m (данные исчезают через ~6 мес) | |

**Ограничение API:** 1m данные доступны только за последние ~186 дней. Старые данные удаляются Deribit. После запуска сбора — история будет накапливаться.

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

**Финальный список метрик будет определён после Этапа 1 (когда увидим реальные данные).**

### Этап 4: Bybit HV (историческая загрузка)

**Приоритет:** Средний
**Сложность:** Низкая (1 лоадер, 1 таблица)
**Данных:** ~2 года

| Шаг | Действие | Статус |
|-----|----------|--------|
| 4.1 | Создать таблицу `options_bybit_hv` | |
| 4.2 | Создать `options_hv_loader.py` — загрузка HV через `/v5/market/historical-volatility` | |
| 4.3 | Загрузить историю BTC/ETH HV (батчами по 30 дней) | |

**API:**
```
GET https://api.bybit.com/v5/market/historical-volatility
    ?category=option&baseCoin=BTC&period=30
    &startTime=1707350400000&endTime=1709942400000   (max 30 дней)
```

### Этап 5: Онлайн-сбор снапшотов опционов

**Приоритет:** Низкий (запустить после Этапов 1-3)
**Сложность:** Средняя (2-3 лоадера, 3 таблицы, daemon mode)
**Данных:** Накапливается с момента запуска

| Шаг | Действие | Статус |
|-----|----------|--------|
| 5.1 | Создать таблицы: instruments, ticker, trades | |
| 5.2 | Создать `options_instruments_loader.py` — список контрактов (раз в час) | |
| 5.3 | Создать `options_ticker_loader.py` — снапшоты Greeks + IV (каждые 15 мин) | |
| 5.4 | Создать `options_trades_loader.py` — сделки (каждые 5 мин) | |
| 5.5 | Настроить daemon mode на VPS | |

### Этап 6: Агрегированные метрики из онлайн-данных

**Приоритет:** Низкий (после 3-6 месяцев сбора в Этапе 5)
**Сложность:** Средняя
**Зависимости:** Этап 5

| Шаг | Действие | Статус |
|-----|----------|--------|
| 6.1 | Создать `options_analytics_loader.py` — Put/Call Ratio, IV Skew, Max Pain, GEX | |
| 6.2 | Интеграция с ML pipeline | |

---

## 9. API Reference

### Deribit API v2

**Base URL:** `https://www.deribit.com/api/v2`
**Test URL:** `https://test.deribit.com/api/v2`
**Документация:** https://docs.deribit.com/

#### Endpoints для Фазы 1 (DVOL)

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

#### Endpoints для Фазы 3 (онлайн-сбор)

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `public/ticker` | GET | Текущий ticker + Greeks + IV для контракта |
| `public/get_instruments` | GET | Список всех инструментов |
| `public/get_book_summary_by_currency` | GET | Агрегат OI/volume по валюте |
| `public/get_last_trades_by_currency` | GET | Последние сделки |
| `public/get_historical_volatility` | GET | HV (15 дней) |

### Bybit API v5

**Base URL:** `https://api.bybit.com`
**Документация:** https://bybit-exchange.github.io/docs/v5/intro

#### Endpoints для Фазы 2 (HV)

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `/v5/market/historical-volatility` | GET | HV данные (2 года, батчи 30 дней) |

Параметры:
- `category` (required): option
- `baseCoin`: BTC, ETH
- `period`: 7, 14, 21, 30
- `startTime`, `endTime`: ms since epoch (max 30 дней разница)

#### Endpoints для Фазы 3 (онлайн-сбор)

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `/v5/market/tickers?category=option` | GET | Ticker + Greeks + IV для всех контрактов |
| `/v5/market/instruments-info?category=option` | GET | Список опционных контрактов |
| `/v5/market/recent-trade?category=option` | GET | Последние сделки по опционам |

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
