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
| `options_deribit_aggregated_15m` | growing | growing | 2026-02-13 — now | 24 агрегированных метрики из raw данных |

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

## 5.5 Таблица `options_deribit_aggregated_15m` — агрегированные метрики

**Источник данных**: `options_deribit_raw` (снапшоты ~1500 контрактов каждые 15 мин)
**Скрипт**: `indicators/options_aggregated_loader.py`
**PK**: (timestamp, currency)

### 5.5.1 Группа Volume/OI (5 колонок) — объёмы и открытый интерес

| Колонка | Тип | Описание |
|---------|-----|----------|
| `put_call_ratio_volume` | DECIMAL(12,4) | Volume(puts) / Volume(calls) — сентимент через реальные деньги |
| `put_call_ratio_oi` | DECIMAL(12,4) | OI(puts) / OI(calls) — баланс накопленных позиций |
| `total_volume_24h` | DECIMAL(20,4) | Суммарный объём всех опционов за 24h (в базовой валюте) |
| `total_open_interest` | DECIMAL(20,4) | Суммарный OI всех контрактов (в базовой валюте) |
| `oi_change_pct_24h` | DECIMAL(12,4) | Изменение OI за 24h (%) — рост = новые позиции, падение = закрытие |

**Зачем**: Put/Call Ratio — один из самых надёжных индикаторов сентимента. `pcr_volume > 1` = страх (больше покупают путов), `pcr_volume < 0.7` = жадность. OI change показывает приток/отток капитала в опционы.

```sql
-- Текущий сентимент через Put/Call Ratio
SELECT timestamp, currency,
       put_call_ratio_volume, put_call_ratio_oi,
       total_open_interest, oi_change_pct_24h
FROM options_deribit_aggregated_15m
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 10;
```

### 5.5.2 Группа IV Metrics (6 колонок) — implied volatility с 30d интерполяцией

| Колонка | Тип | Описание |
|---------|-----|----------|
| `iv_atm_30d` | DECIMAL(12,4) | ATM IV с постоянной дюрацией 30 дней (VIX-style интерполяция) |
| `iv_25d_put_30d` | DECIMAL(12,4) | IV 25-delta put (30d) — страх падения |
| `iv_25d_call_30d` | DECIMAL(12,4) | IV 25-delta call (30d) — ожидание роста |
| `iv_skew_25d_30d` | DECIMAL(12,4) | IV(25d put) - IV(25d call) — направление страха |
| `iv_smile_steepness_30d` | DECIMAL(12,4) | (IV_put + IV_call)/2 - IV_ATM — крутизна улыбки |
| `iv_term_structure_7d_30d` | DECIMAL(12,4) | IV_ATM_7d - IV_ATM_30d — краткосрочные vs долгосрочные ожидания |

**Методология**:
- **VIX-style интерполяция**: Находим две экспирации T1 ≤ 30d и T2 > 30d, линейно интерполируем метрику к точке 30 дней
- **25-delta**: Находим два контракта с delta, окружающими ±0.25, линейно интерполируем IV

**Зачем**: `iv_skew > 0` = рынок боится падения (путы дороже коллов). Инверсия skew = потенциальный разворот. `term_structure < 0` = инверсия (краткосрочная IV выше долгосрочной = стресс).

```sql
-- IV Surface: ATM, Skew, Term Structure
SELECT timestamp, currency,
       iv_atm_30d, iv_skew_25d_30d, iv_term_structure_7d_30d,
       CASE WHEN iv_skew_25d_30d > 5 THEN 'fear of dump'
            WHEN iv_skew_25d_30d < -5 THEN 'fear of pump'
            ELSE 'neutral' END AS skew_signal
FROM options_deribit_aggregated_15m
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 10;
```

### 5.5.3 Группа Max Pain (4 колонки) — ценовой магнит

| Колонка | Тип | Описание |
|---------|-----|----------|
| `max_pain_nearest` | DECIMAL(20,2) | Max Pain ближайшей экспирации ($) |
| `max_pain_nearest_distance_pct` | DECIMAL(12,4) | Расстояние от цены до Max Pain (%) |
| `max_pain_monthly` | DECIMAL(20,2) | Max Pain месячной экспирации ($) |
| `max_pain_monthly_distance_pct` | DECIMAL(12,4) | Расстояние от цены до месячного Max Pain (%) |

**Формула**: Max Pain = страйк, при котором суммарная «боль» всех держателей опционов минимальна. Цена стремится к Max Pain ближе к экспирации (маркетмейкеры хеджируют позиции).

**Зачем**: `distance_pct > 5%` = цена далеко от магнита, возможен возврат. Месячный Max Pain — более стабильный ориентир.

```sql
-- Max Pain vs текущая цена
SELECT timestamp, currency,
       max_pain_nearest, max_pain_nearest_distance_pct,
       max_pain_monthly, max_pain_monthly_distance_pct
FROM options_deribit_aggregated_15m
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 10;
```

### 5.5.4 Группа Greeks Exposure (4 колонки) — экспозиция греков

| Колонка | Тип | Описание |
|---------|-----|----------|
| `gex` | DECIMAL(30,4) | Gamma Exposure: GEX_calls - GEX_puts (dealer convention) |
| `net_delta` | DECIMAL(30,4) | Чистая дельта: направление позиций |
| `net_gamma` | DECIMAL(30,4) | Суммарная гамма × OI |
| `vega_exposure` | DECIMAL(30,4) | Суммарная вега × OI — чувствительность рынка к IV |

**GEX (Gamma Exposure)**:
- Формула: `GEX = Σ(gamma × OI × S² × 0.01)` для calls минус puts
- Дилерская конвенция: дилеры обычно short options → long gamma от calls, short gamma от puts
- `GEX > 0` = стабилизация (дилеры покупают на падении, продают на росте)
- `GEX < 0` = усиление (дилеры продают на падении, покупают на росте)

```sql
-- GEX режим рынка
SELECT timestamp, currency, gex, net_delta, vega_exposure,
       CASE WHEN gex > 0 THEN 'stabilization (pin risk)'
            ELSE 'amplification (breakout risk)' END AS market_regime
FROM options_deribit_aggregated_15m
WHERE currency = 'BTC'
ORDER BY timestamp DESC LIMIT 10;
```

### 5.5.5 Группа Expiration (2 колонки) — экспирации

| Колонка | Тип | Описание |
|---------|-----|----------|
| `days_to_expiry_nearest` | DECIMAL(8,2) | Дней до ближайшей экспирации |
| `notional_expiring_7d` | DECIMAL(30,2) | Номинал экспирирующих в ближайшие 7 дней (OI × strike, $) |

**Зачем**: Крупные экспирации (quarterly) часто сопровождаются повышенной волатильностью. `notional_expiring_7d` показывает масштаб предстоящего события.

### 5.5.6 Группа Liquidity (2 колонки) — ликвидность

| Колонка | Тип | Описание |
|---------|-----|----------|
| `bid_ask_spread_avg_atm` | DECIMAL(20,10) | Средний bid-ask spread ATM контрактов (±5% от spot) |
| `max_oi_strike` | DECIMAL(20,2) | Страйк с максимальным Open Interest ($) |

**Зачем**: Широкий spread = низкая ликвидность = потенциально резкие движения. `max_oi_strike` — уровень, где сконцентрированы позиции (часто работает как поддержка/сопротивление).

### 5.5.7 Группа Positioning (1 колонка) — позиционирование

| Колонка | Тип | Описание |
|---------|-----|----------|
| `gamma_flip_level` | DECIMAL(20,2) | Цена, где net GEX меняет знак ($) |

**Методология**: Итерируем все страйки, считаем net GEX для каждого уровня цены, находим пересечение нуля с линейной интерполяцией.

**Зачем**: Выше gamma flip = стабильный рынок (дилеры стабилизируют). Ниже = нестабильный (дилеры усиливают движения). Ключевой уровень для определения режима рынка.

```sql
-- Gamma Flip Level: ключевой уровень рынка
SELECT a.timestamp, a.currency,
       a.gamma_flip_level, a.gex,
       CASE WHEN a.gamma_flip_level IS NOT NULL THEN
           CASE WHEN d.close > a.gamma_flip_level THEN 'above flip (stable)'
                ELSE 'below flip (volatile)' END
       END AS regime
FROM options_deribit_aggregated_15m a
LEFT JOIN options_deribit_dvol_1h d ON d.timestamp = date_trunc('hour', a.timestamp)
    AND d.currency = a.currency
WHERE a.currency = 'BTC'
ORDER BY a.timestamp DESC LIMIT 10;
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

### Расчёт агрегированных метрик из raw данных
```bash
python3 options_aggregated_loader.py                                # Все группы, BTC + ETH
python3 options_aggregated_loader.py --currency BTC                 # Только BTC
python3 options_aggregated_loader.py --group iv                     # Только IV Metrics
python3 options_aggregated_loader.py --group maxpain --currency ETH # Max Pain для ETH
python3 options_aggregated_loader.py --force-reload                 # Полная перезагрузка
```

**Доступные группы** (`--group`): `volume`, `iv`, `maxpain`, `greeks`, `expiry`, `liquidity`, `positioning`

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

### Полная картина: DVOL + агрегированные метрики + цена BTC
```sql
SELECT
    a.timestamp,
    c.close AS btc_price,
    d.close AS dvol,
    i.iv_hv_ratio_30,
    a.put_call_ratio_volume AS pcr,
    a.iv_atm_30d,
    a.iv_skew_25d_30d AS skew,
    a.max_pain_nearest,
    a.gex,
    a.gamma_flip_level,
    CASE WHEN a.gex > 0 THEN 'stable' ELSE 'volatile' END AS regime
FROM options_deribit_aggregated_15m a
LEFT JOIN options_deribit_dvol_1h d
    ON d.timestamp = date_trunc('hour', a.timestamp) AND d.currency = a.currency
LEFT JOIN options_deribit_dvol_indicators_1h i
    ON i.timestamp = date_trunc('hour', a.timestamp) AND i.currency = a.currency
LEFT JOIN candles_bybit_futures_1m c
    ON c.timestamp = a.timestamp AND c.symbol = 'BTCUSDT'
WHERE a.currency = 'BTC'
ORDER BY a.timestamp DESC
LIMIT 10;
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

### Шпаргалка: агрегированные метрики (`options_deribit_aggregated_15m`)

| Сигнал | Условие | Интерпретация |
|--------|---------|---------------|
| Страх (puts) | `put_call_ratio_volume > 1.0` | Больше покупают путов — хеджирование от падения |
| Жадность (calls) | `put_call_ratio_volume < 0.7` | Больше покупают коллов — бычий сентимент |
| IV Skew — страх падения | `iv_skew_25d_30d > 5` | Путы дороже коллов — рынок боится падения |
| IV Skew — инверсия | `iv_skew_25d_30d < -5` | Коллы дороже — необычно, возможен разворот |
| Term Structure — стресс | `iv_term_structure_7d_30d > 5` | Краткосрочная IV выше долгосрочной (инверсия) |
| Max Pain — магнит | `max_pain_nearest_distance_pct > 5%` | Цена далеко от магнита, вероятен возврат |
| GEX стабилизация | `gex > 0` | Дилеры стабилизируют рынок (pin risk) |
| GEX усиление | `gex < 0` | Дилеры усиливают движения (breakout risk) |
| Выше Gamma Flip | `price > gamma_flip_level` | Стабильный режим рынка |
| Ниже Gamma Flip | `price < gamma_flip_level` | Нестабильный, возможны резкие движения |
| Рост OI | `oi_change_pct_24h > 5%` | Приток нового капитала в опционы |
| Падение OI | `oi_change_pct_24h < -5%` | Массовое закрытие позиций |
