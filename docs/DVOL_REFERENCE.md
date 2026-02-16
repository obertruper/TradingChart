# DVOL Data Reference

Документация по всем данным Deribit Volatility Index (DVOL) и опционным данным.

DVOL — крипто-аналог VIX: ожидаемая 30-дневная волатильность рынка в % годовых. Значение 55 означает, что рынок ожидает 55% годовой волатильности в ближайшие 30 дней.

---

## 1. Обзор таблиц

| Таблица | Размер | Строки | Период | Описание |
|---------|--------|--------|--------|----------|
| `options_deribit_dvol_1h` | 12 MB | 85,748 | 2021-03-24 — now | DVOL OHLC свечи (1h) |
| `options_deribit_dvol_1m` | 73 MB | 518,334 | rolling ~186 дней | DVOL OHLC свечи (1m) |
| `options_deribit_dvol_indicators_1h` | 28 MB | 85,748 | 2021-03-24 — now | 22 расчётных индикатора из DVOL |
| `options_deribit_raw` | 134 MB | 427,212+ | 2026-02-13 — now | Сырые снапшоты опционов (каждые 15 мин) |

Валюты: **BTC**, **ETH** (для всех таблиц кроме raw, где ~1500 контрактов на снапшот).

---

## 2. Таблица `options_deribit_dvol_1h` — DVOL свечи

**Источник**: Deribit API (`public/get_volatility_index_data`)
**Скрипт**: `indicators/options_dvol_loader.py`
**PK**: (timestamp, currency)

| Колонка | Тип | Описание |
|---------|-----|----------|
| timestamp | TIMESTAMPTZ | Время начала свечи (UTC) |
| currency | VARCHAR(10) | BTC или ETH |
| open | DECIMAL(20,8) | DVOL open (%) |
| high | DECIMAL(20,8) | DVOL high (%) |
| low | DECIMAL(20,8) | DVOL low (%) |
| close | DECIMAL(20,8) | DVOL close (%) |

**Данные**: ~43K строк на валюту, с 2021-03-24 (запуск DVOL на Deribit).

### Полезные запросы

```sql
-- Последние значения DVOL
SELECT timestamp, currency, close
FROM options_deribit_dvol_1h
WHERE timestamp > NOW() - INTERVAL '24 hours'
ORDER BY currency, timestamp DESC;

-- Средний DVOL по месяцам
SELECT currency,
       DATE_TRUNC('month', timestamp) AS month,
       ROUND(AVG(close)::numeric, 2) AS avg_dvol,
       ROUND(MIN(close)::numeric, 2) AS min_dvol,
       ROUND(MAX(close)::numeric, 2) AS max_dvol
FROM options_deribit_dvol_1h
GROUP BY currency, month
ORDER BY currency, month DESC
LIMIT 12;
```

---

## 3. Таблица `options_deribit_dvol_1m` — DVOL свечи (минутные)

**Скрипт**: `indicators/options_dvol_loader.py --timeframe 1m`
**PK**: (timestamp, currency)

Та же структура, что и `_1h`. Deribit хранит 1m данные ~186 дней rolling window. Нужно запускать загрузку каждые 2 недели для накопления истории.

---

## 4. Таблица `options_deribit_dvol_indicators_1h` — расчётные индикаторы

**Источник данных**: `options_deribit_dvol_1h` (DVOL close) + `indicators_bybit_futures_1h` (HV_30)
**Скрипт**: `indicators/options_dvol_indicators_loader.py`
**PK**: (timestamp, currency)

### 4.1 Группа Trend (4 колонки) — тренд волатильности

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_sma_24` | DECIMAL(10,4) | SMA 24h — средняя DVOL за сутки |
| `dvol_sma_168` | DECIMAL(10,4) | SMA 168h (7 дней) — среднесрочный тренд |
| `dvol_ema_12` | DECIMAL(10,4) | EMA 12h — быстрая экспоненциальная средняя |
| `dvol_ema_26` | DECIMAL(10,4) | EMA 26h — медленная экспоненциальная средняя |

**Зачем**: определить направление тренда волатильности. DVOL выше SMA 168 = повышенная волатильность. Пересечение EMA 12 выше EMA 26 = начало роста волатильности. Пересечение вниз = волатильность снижается.

```sql
-- BTC: текущий DVOL vs средние (тренд)
SELECT timestamp, close AS dvol,
       dvol_sma_24, dvol_sma_168,
       CASE WHEN close > dvol_sma_168 THEN 'above weekly avg' ELSE 'below weekly avg' END AS trend
FROM options_deribit_dvol_1h d
JOIN options_deribit_dvol_indicators_1h i USING (timestamp, currency)
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 10;
```

### 4.2 Группа Momentum (3 колонки) — скорость изменения

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_change_24h` | DECIMAL(10,4) | Абсолютное изменение DVOL за 24h (п.п.) |
| `dvol_change_pct_24h` | DECIMAL(10,4) | Процентное изменение DVOL за 24h (%) |
| `dvol_roc_24h` | DECIMAL(10,4) | Rate of Change за 24h (%) |

**Зачем**: отловить резкие всплески волатильности. `change_pct_24h > 15%` = паника / шок на рынке. `change_pct_24h < -15%` = быстрое успокоение после события.

```sql
-- Топ-10 самых сильных всплесков волатильности BTC
SELECT timestamp::date AS date, dvol_change_pct_24h, dvol_change_24h
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND dvol_change_pct_24h IS NOT NULL
ORDER BY dvol_change_pct_24h DESC LIMIT 10;
```

### 4.3 Группа Levels (3 колонки) — статистические уровни

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_percentile_30d` | DECIMAL(10,4) | Перцентиль за 30 дней (0-100) |
| `dvol_percentile_90d` | DECIMAL(10,4) | Перцентиль за 90 дней (0-100) |
| `dvol_zscore_30d` | DECIMAL(10,4) | Z-score за 30 дней (отклонение от средней в σ) |

**Зачем**: определить, насколько текущая волатильность аномальна. `percentile > 90` = экстремально высокая (верхние 10% за период). `zscore > 2` = на 2σ выше нормы (вероятен возврат к среднему). `percentile < 10` = аномально спокойный рынок (затишье перед бурей).

```sql
-- Экстремальные значения DVOL (верхний 5-й перцентиль за 90 дней)
SELECT timestamp::date AS date, currency,
       dvol_percentile_90d, dvol_zscore_30d
FROM options_deribit_dvol_indicators_1h
WHERE dvol_percentile_90d > 95
ORDER BY timestamp DESC LIMIT 20;
```

### 4.4 Группа IV vs HV (2 колонки) — implied vs historical volatility

| Колонка | Тип | Описание |
|---------|-----|----------|
| `iv_hv_spread_30` | DECIMAL(10,4) | DVOL - HV_30: разница implied и historical volatility (п.п.) |
| `iv_hv_ratio_30` | DECIMAL(10,4) | DVOL / HV_30: соотношение implied к historical |

**Источник HV_30**: `indicators_bybit_futures_1h.hv_30` (BTCUSDT, ETHUSDT)

**Зачем**: ключевая метрика для опционной торговли.
- `spread > 0` — рынок ожидает бОльшую волатильность, чем была (премия за страх)
- `ratio > 1.5` — опционы переоценены (можно продавать волатильность)
- `ratio < 0.8` — опционы дёшевы (можно покупать волатильность)
- Средний `ratio` для BTC ~ 1.1-1.3 (IV обычно чуть выше HV)

```sql
-- IV vs HV: поиск моментов когда опционы были дёшевы
SELECT timestamp::date AS date, currency,
       iv_hv_spread_30, iv_hv_ratio_30
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND iv_hv_ratio_30 < 0.8
ORDER BY iv_hv_ratio_30 ASC LIMIT 20;
```

### 4.5 Группа BTC/ETH Cross (3 колонки) — кросс-валютные метрики

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_btc_eth_spread` | DECIMAL(10,4) | ETH DVOL - BTC DVOL (п.п.) |
| `dvol_btc_eth_ratio` | DECIMAL(10,4) | ETH DVOL / BTC DVOL (обычно > 1) |
| `dvol_btc_eth_corr_24h` | DECIMAL(10,4) | Корреляция BTC и ETH DVOL за 24h (-1 до +1) |

**Зачем**: ETH обычно волатильнее BTC (ratio ~1.2-1.4). Расширение spread = ETH реагирует сильнее на событие. Падение `corr_24h < 0.5` = декорреляция (специфическое событие для одного актива). Эти значения записаны в обе строки (BTC и ETH) для удобства.

```sql
-- Моменты декорреляции BTC и ETH волатильности
SELECT timestamp::date AS date,
       dvol_btc_eth_spread, dvol_btc_eth_ratio, dvol_btc_eth_corr_24h
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND dvol_btc_eth_corr_24h < 0.3
ORDER BY dvol_btc_eth_corr_24h ASC LIMIT 20;
```

### 4.6 Группа RSI (1 колонка) — перекупленность/перепроданность

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_rsi_14` | DECIMAL(10,4) | RSI 14 от DVOL close (0-100, Wilder smoothing) |

**Зачем**: RSI применённый к волатильности, а не к цене.
- `rsi > 70` — волатильность перекуплена, вероятен откат (рынок успокоится)
- `rsi < 30` — волатильность перепродана (затишье перед бурей)
- Дивергенция RSI vs DVOL = ранний сигнал разворота тренда волатильности

```sql
-- Экстремальные RSI DVOL (перекупленность/перепроданность волатильности)
SELECT timestamp::date AS date, currency, dvol_rsi_14,
       CASE WHEN dvol_rsi_14 > 70 THEN 'overbought'
            WHEN dvol_rsi_14 < 30 THEN 'oversold'
            ELSE 'neutral' END AS signal
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND (dvol_rsi_14 > 70 OR dvol_rsi_14 < 30)
ORDER BY timestamp DESC LIMIT 20;
```

### 4.7 Группа Bollinger Bands (3 колонки) — полосы волатильности

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_bb_upper_20_2` | DECIMAL(10,4) | Верхняя полоса (SMA 20 + 2σ) |
| `dvol_bb_lower_20_2` | DECIMAL(10,4) | Нижняя полоса (SMA 20 - 2σ) |
| `dvol_bb_pct_b_20_2` | DECIMAL(10,4) | %B: позиция внутри полос (0 = нижняя, 1 = верхняя) |

**Зачем**: Bollinger Bands применённые к DVOL показывают «волатильность волатильности» (vol-of-vol).
- `%B > 1` — DVOL выше верхней полосы (экстремальный рост)
- `%B < 0` — DVOL ниже нижней полосы (аномальное затишье)
- Сужение полос (upper - lower уменьшается) = squeeze — сжатие перед резким движением

```sql
-- Bollinger Bands: пробои верхней полосы (экстремальная волатильность)
SELECT timestamp::date AS date, currency,
       dvol_bb_lower_20_2, dvol_bb_upper_20_2, dvol_bb_pct_b_20_2
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND dvol_bb_pct_b_20_2 > 1.0
ORDER BY dvol_bb_pct_b_20_2 DESC LIMIT 20;

-- Bollinger squeeze: сужение полос (потенциальный взрыв волатильности)
SELECT timestamp::date AS date, currency,
       (dvol_bb_upper_20_2 - dvol_bb_lower_20_2) AS bandwidth
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC' AND dvol_bb_upper_20_2 IS NOT NULL
ORDER BY bandwidth ASC LIMIT 20;
```

### 4.8 Группа MACD (3 колонки) — тренд и моментум

| Колонка | Тип | Описание |
|---------|-----|----------|
| `dvol_macd_line_12_26` | DECIMAL(10,4) | MACD линия (EMA 12 - EMA 26) |
| `dvol_macd_signal_12_26_9` | DECIMAL(10,4) | Сигнальная линия (EMA 9 от MACD) |
| `dvol_macd_hist_12_26_9` | DECIMAL(10,4) | Гистограмма (MACD line - Signal) |

**Зачем**: MACD от волатильности показывает моментум тренда.
- MACD line пересекает signal вверх = начало роста волатильности
- Гистограмма > 0 и растёт = ускорение роста волатильности
- Дивергенция MACD vs DVOL = ранний сигнал разворота

```sql
-- MACD: смена тренда волатильности (пересечения)
SELECT timestamp, currency,
       dvol_macd_line_12_26, dvol_macd_signal_12_26_9, dvol_macd_hist_12_26_9
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 24;
```

---

## 5. Таблица `options_deribit_raw` — сырые снапшоты опционов

**Источник**: Deribit WebSocket API (подписка на `ticker.{instrument}`)
**Скрипт**: `data_collectors/deribit/options/options_deribit_raw_ws_collector.py`
**PK**: (timestamp, instrument_name)
**Частота**: снапшот всех активных контрактов каждые 15 минут

| Колонка | Тип | Описание |
|---------|-----|----------|
| timestamp | TIMESTAMPTZ | Время снапшота (UTC) |
| instrument_name | VARCHAR | Название контракта (напр. `BTC-28MAR26-120000-C`) |
| currency | VARCHAR | BTC или ETH |
| expiration | DATE | Дата экспирации |
| strike | NUMERIC | Страйк-цена |
| option_type | VARCHAR | `call` или `put` |
| mark_iv | NUMERIC | Mark implied volatility (%) |
| bid_iv | NUMERIC | Bid implied volatility (%) |
| ask_iv | NUMERIC | Ask implied volatility (%) |
| delta | NUMERIC | Дельта (-1 до +1) |
| gamma | NUMERIC | Гамма |
| theta | NUMERIC | Тета (временной распад) |
| vega | NUMERIC | Вега (чувствительность к IV) |
| rho | NUMERIC | Ро |
| open_interest | NUMERIC | Открытый интерес (кол-во контрактов) |
| volume_24h | NUMERIC | Объём за 24h (в базовой валюте) |
| volume_usd_24h | NUMERIC | Объём за 24h (USD) |
| mark_price | NUMERIC | Расчётная цена |
| last_price | NUMERIC | Последняя сделка |
| best_bid_price | NUMERIC | Лучший bid |
| best_ask_price | NUMERIC | Лучший ask |
| best_bid_amount | NUMERIC | Объём лучшего bid |
| best_ask_amount | NUMERIC | Объём лучшего ask |
| underlying_price | NUMERIC | Цена базового актива |
| index_price | NUMERIC | Индексная цена |
| settlement_price | NUMERIC | Расчётная цена для экспирации |
| high_24h | NUMERIC | Максимум за 24h |
| low_24h | NUMERIC | Минимум за 24h |
| interest_rate | NUMERIC | Процентная ставка |

**Данные**: ~1,500 контрактов на снапшот, 96 снапшотов/день, ~144K строк/день.

### Полезные запросы

```sql
-- Сколько снапшотов собрано
SELECT DATE(timestamp) AS date, COUNT(DISTINCT timestamp) AS snapshots,
       COUNT(*) AS total_rows
FROM options_deribit_raw
GROUP BY date ORDER BY date;

-- Топ контракты по open interest (BTC)
SELECT instrument_name, strike, expiration, option_type,
       open_interest, mark_iv, delta
FROM options_deribit_raw
WHERE currency = 'BTC' AND timestamp = (SELECT MAX(timestamp) FROM options_deribit_raw)
ORDER BY open_interest DESC LIMIT 20;

-- IV smile: все страйки для конкретной экспирации
SELECT strike, option_type, mark_iv, delta, open_interest
FROM options_deribit_raw
WHERE currency = 'BTC'
  AND expiration = '2026-03-28'
  AND timestamp = (SELECT MAX(timestamp) FROM options_deribit_raw)
ORDER BY strike;
```

---

## 6. Скрипты и команды

### Загрузка DVOL свечей
```bash
python3 options_dvol_loader.py                          # BTC 1m + 1h
python3 options_dvol_loader.py --currency ETH           # ETH
python3 options_dvol_loader.py --timeframe 1h           # Только 1h
python3 options_dvol_loader.py --force-reload           # Полная перезагрузка
```

### Расчёт DVOL индикаторов
```bash
python3 options_dvol_indicators_loader.py                              # Все группы, BTC + ETH
python3 options_dvol_indicators_loader.py --currency BTC               # Только BTC
python3 options_dvol_indicators_loader.py --group rsi                  # Только группа RSI
python3 options_dvol_indicators_loader.py --group macd --currency ETH  # MACD для ETH
python3 options_dvol_indicators_loader.py --force-reload               # Полная перезагрузка
```

**Доступные группы** (`--group`): `trend`, `momentum`, `levels`, `iv_hv`, `cross`, `rsi`, `bollinger`, `macd`

### Сбор сырых опционов (WebSocket)
```bash
# Работает в tmux на VPS, автоматический restart
tmux attach -t data_collector_options_raw_deribit
```

---

## 7. Комплексные аналитические запросы

### Полная картина по BTC на текущий момент
```sql
SELECT
    d.timestamp,
    d.close AS dvol,
    i.dvol_sma_24,
    i.dvol_sma_168,
    i.dvol_rsi_14,
    i.dvol_percentile_90d,
    i.iv_hv_spread_30,
    i.iv_hv_ratio_30,
    i.dvol_btc_eth_ratio,
    i.dvol_bb_pct_b_20_2,
    i.dvol_macd_hist_12_26_9
FROM options_deribit_dvol_1h d
JOIN options_deribit_dvol_indicators_1h i USING (timestamp, currency)
WHERE d.currency = 'BTC'
ORDER BY d.timestamp DESC
LIMIT 1;
```

### Дневная сводка: средние индикаторы по дням
```sql
SELECT
    DATE(timestamp) AS date,
    ROUND(AVG(dvol_sma_24)::numeric, 1) AS avg_dvol,
    ROUND(AVG(dvol_rsi_14)::numeric, 1) AS avg_rsi,
    ROUND(AVG(dvol_percentile_30d)::numeric, 1) AS avg_pctl,
    ROUND(AVG(iv_hv_ratio_30)::numeric, 2) AS avg_iv_hv_ratio,
    ROUND(AVG(dvol_btc_eth_ratio)::numeric, 2) AS avg_cross_ratio
FROM options_deribit_dvol_indicators_1h
WHERE currency = 'BTC'
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

### DVOL + цена BTC: корреляция волатильности и цены
```sql
SELECT
    d.timestamp,
    d.close AS dvol,
    c.close AS btc_price,
    i.dvol_rsi_14,
    i.dvol_change_pct_24h
FROM options_deribit_dvol_1h d
JOIN options_deribit_dvol_indicators_1h i USING (timestamp, currency)
JOIN candles_bybit_futures_1m c ON c.timestamp = d.timestamp AND c.symbol = 'BTCUSDT'
WHERE d.currency = 'BTC'
ORDER BY d.timestamp DESC
LIMIT 24;
```

---

## 8. Интерпретация сигналов

### Шпаргалка

| Сигнал | Условие | Интерпретация |
|--------|---------|---------------|
| Высокая волатильность | `percentile_90d > 90` | Рынок в стрессе, экстремальная IV |
| Низкая волатильность | `percentile_90d < 10` | Затишье, возможен взрыв |
| Опционы дороги | `iv_hv_ratio > 1.5` | Можно продавать волатильность |
| Опционы дёшевы | `iv_hv_ratio < 0.8` | Можно покупать волатильность |
| RSI перекуплен | `rsi > 70` | Волатильность вероятно снизится |
| RSI перепродан | `rsi < 30` | Вероятен рост волатильности |
| BB пробой вверх | `bb_pct_b > 1` | Экстремальный рост IV |
| BB пробой вниз | `bb_pct_b < 0` | Аномально низкая IV |
| Декорреляция | `btc_eth_corr < 0.3` | Специфическое событие для одного актива |
| Тренд вверх | `ema_12 > ema_26` | Волатильность растёт |
| Тренд вниз | `ema_12 < ema_26` | Волатильность снижается |
