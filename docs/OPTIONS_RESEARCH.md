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


### Таблица: options_deribit_instruments (Этап 4)

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

### Таблица: options_deribit_ticker (Этап 4)

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

### Таблица: options_deribit_trades (Этап 4)

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

### Этап 4: Онлайн-сбор снапшотов опционов

**Приоритет:** Средний (запустить параллельно с Этапом 3 — чем раньше, тем больше данных накопим)
**Сложность:** Высокая (3 daemon-скрипта, 3 таблицы, ~96K записей/день)
**Данных:** Накапливается с момента запуска
**Оценка хранения:** ~35M записей/год, ~5-7 GB/год

#### Почему только онлайн?

Это **принципиально другой тип данных**, чем DVOL. DVOL — один индекс, одно число в час. Здесь речь о данных **по каждому отдельному опционному контракту**:

- На Deribit одновременно **~500-1000 активных BTC опционов**
- Каждый контракт: `BTC-28MAR26-100000-C` (актив-экспирация-страйк-тип)
- У каждого контракта свой IV, Greeks, цена, Open Interest

**Критическая проблема:** когда контракт истекает (каждую пятницу!), его исторические данные **исчезают навсегда** из API. Нет endpoint'а "дай мне ticker контракта, который истёк 3 месяца назад". Единственный способ получить эти данные — **собирать в реальном времени** и накапливать в нашей БД.

> Платные источники исторических данных (Tardis.dev от $200/мес) имеют архивы с 2019 года, но для начала достаточно собирать самим.

#### Что собираем (3 типа данных)

**1. Instruments** (список контрактов) — **раз в час:**
```
BTC-28MAR26-100000-C  →  strike=100000, type=call, expiry=2026-03-28
BTC-28MAR26-100000-P  →  strike=100000, type=put,  expiry=2026-03-28
BTC-28MAR26-95000-C   →  strike=95000,  type=call, expiry=2026-03-28
... ещё ~997 контрактов для BTC
```
Скрипт: `options_instruments_loader.py`
Таблица: `options_deribit_instruments`
Объём: ~1000 записей на обновление, обновляется когда появляются/истекают контракты

**2. Ticker + Greeks** (снапшот каждого контракта) — **каждые 15 мин:**
```
BTC-28MAR26-100000-C:
  mark_price: 0.0145 BTC       ← цена опциона
  mark_iv: 58.2%               ← IV конкретного страйка
  delta: 0.42                  ← вероятность исполнения
  gamma: 0.00003               ← ускорение delta
  theta: -12.5                 ← временной распад ($/день)
  vega: 85.3                   ← чувствительность к IV
  open_interest: 125 BTC       ← открытые позиции
  volume_24h: 15 BTC           ← объём торгов
```
Скрипт: `options_ticker_loader.py`
Таблица: `options_deribit_ticker`
Объём: **~96,000 записей/день** (~1000 контрактов × 96 снапшотов/день)

**3. Trades** (сделки) — **каждые 5 мин:**
```
trade_id: 12345
instrument: BTC-28MAR26-100000-C
direction: buy                 ← покупка/продажа
amount: 2.5 BTC               ← размер сделки
price: 0.0148                 ← цена
iv: 58.5%                     ← IV в момент сделки
```
Скрипт: `options_trades_loader.py`
Таблица: `options_deribit_trades`
Объём: Зависит от активности рынка, ~5-20K сделок/день

#### Реализация на VPS

Daemon-процессы (аналог `monitor.py` для свечей):
- 3 независимых скрипта запускаются через systemd или cron
- Каждый работает в бесконечном цикле с паузой (15 мин / 5 мин / 1 час)
- Graceful shutdown, логирование, обработка ошибок

#### Объём данных (оценка)

| Таблица | Записей/день | Записей/мес | Записей/год | Размер/год |
|---------|-------------|-------------|-------------|------------|
| instruments | ~1,000 | ~30K | ~365K | ~50 MB |
| ticker | ~96,000 | ~2.9M | ~35M | ~5-7 GB |
| trades | ~5-20K | ~300-600K | ~4-7M | ~0.5-1 GB |

| Шаг | Действие | Статус |
|-----|----------|--------|
| 4.1 | Создать таблицы: instruments, ticker, trades | |
| 4.2 | Создать `options_instruments_loader.py` — список контрактов (раз в час) | |
| 4.3 | Создать `options_ticker_loader.py` — снапшоты Greeks + IV (каждые 15 мин) | |
| 4.4 | Создать `options_trades_loader.py` — сделки (каждые 5 мин) | |
| 4.5 | Настроить daemon mode на VPS | |

### Этап 5: Агрегированные метрики из онлайн-данных

**Приоритет:** Низкий (после 3-6 месяцев сбора в Этапе 4)
**Сложность:** Средняя
**Зависимости:** Этап 4 (нужны данные по каждому контракту)

#### Почему нужны данные Этапа 4

Каждая метрика ниже требует знать IV / OI / Greeks **по отдельным контрактам** (puts vs calls, разные страйки, разные экспирации). Без поконтрактных данных из Этапа 4 эти метрики **невозможно рассчитать** — ни из свечей, ни из DVOL.

#### Метрики

| Метрика | Формула | Откуда данные | Что даёт для ML |
|---------|---------|---------------|-----------------|
| `put_call_ratio_volume` | Volume(puts) / Volume(calls) | ticker: volume по type | Сентимент через реальные деньги; >1 = страх, <1 = жадность |
| `put_call_ratio_oi` | OI(puts) / OI(calls) | ticker: open_interest по type | Баланс накопленных позиций |
| `iv_skew_25d` | IV(25-delta put) - IV(25-delta call) | ticker: mark_iv + delta | >0 = рынок боится падения; инверсия = разворот |
| `iv_atm` | IV при страйке ≈ spot price | ticker: mark_iv ближайшего к spot | Чистая IV текущего момента без skew |
| `iv_term_structure` | IV_7d - IV_30d | ticker: mark_iv по экспирациям | Инверсия = стресс; >0 = краткосрочный страх |
| `max_pain` | argmin(total_pain(K)) для всех K | ticker: OI по страйкам | "Магнит" для цены к экспирации |
| `max_pain_distance` | (spot - max_pain) / spot × 100 | max_pain + spot price | Отклонение от магнита в % |
| `gex` | sum(gamma × OI × 100 × S²) | ticker: gamma + OI | >0 = стабилизация рынка, <0 = усиление движений |
| `net_gamma_exposure` | GEX(calls) - GEX(puts) | ticker: gamma + OI по type | Направление давления маркетмейкеров |
| `large_trade_flow` | sum(buy) - sum(sell) для amount > 1 BTC | trades: direction + amount | Направление smart money |

#### Реализация

- Скрипт: `options_analytics_loader.py`
- Таблица: `options_aggregated` (одна строка на timestamp × currency)
- Интервал расчёта: каждые 15 мин (после каждого ticker-снапшота)
- Данные доступны для ML после 3-6 месяцев сбора

| Шаг | Действие | Статус |
|-----|----------|--------|
| 5.1 | Создать `options_analytics_loader.py` — Put/Call Ratio, IV Skew, Max Pain, GEX | |
| 5.2 | Интеграция с ML pipeline | |

### Сравнение этапов и приоритеты

| | Этап 3 (метрики DVOL) | Этап 4 (онлайн-сбор) | Этап 5 (агрегация) |
|--|----------------------|---------------------|-------------------|
| Данные | **Уже есть** (DVOL raw) | Нужно собирать с нуля | Нужны данные Этапа 4 |
| Время до результата | **Дни** | Мгновенно (сбор), но данные копятся | 3-6 месяцев после старта Этапа 4 |
| Сложность | Низкая (1 скрипт) | Высокая (3 daemon скрипта) | Средняя (1 скрипт) |
| Ценность для ML | Высокая (iv_hv_spread) | Нет (сырые данные) | Очень высокая (Put/Call, GEX, Skew) |
| Зависимости | Нет | Нет | Этап 4 |

**Рекомендация:** Этап 3 первый (быстрый результат), затем запустить Этап 4 в фоне (чем раньше — тем больше данных накопим к моменту реализации Этапа 5).

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

#### Endpoints для Этапа 4 (онлайн-сбор)

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `public/ticker` | GET | Текущий ticker + Greeks + IV для контракта |
| `public/get_instruments` | GET | Список всех инструментов |
| `public/get_book_summary_by_currency` | GET | Агрегат OI/volume по валюте |
| `public/get_last_trades_by_currency` | GET | Последние сделки |

### Bybit API v5

**Base URL:** `https://api.bybit.com`
**Документация:** https://bybit-exchange.github.io/docs/v5/intro

#### Endpoints для Этапа 4 (онлайн-сбор)

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
