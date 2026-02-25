# Future Indicators - Ideas for Implementation

> Дата создания: 2026-01-26
> Последнее обновление: 2026-02-25

## Обзор

Этот документ содержит список индикаторов и данных для системы. Реализованные помечены ✅, нереализованные — кандидаты на добавление.

---

## Реализованные индикаторы

### Технические индикаторы (23 загрузчика)

| # | Индикатор | Загрузчик | Колонки | Статус |
|---|-----------|-----------|---------|--------|
| 1 | SMA | `sma_loader.py` | 5 (10, 30, 50, 100, 200) | ✅ |
| 2 | EMA | `ema_loader.py` | 7 (9, 12, 21, 26, 50, 100, 200) | ✅ |
| 3 | RSI | `rsi_loader.py` | 5 (7, 9, 14, 21, 25) | ✅ |
| 4 | VMA | `vma_loader.py` | 5 (10, 20, 50, 100, 200) | ✅ |
| 5 | ATR + NATR | `atr_loader.py` | 12 (6 ATR + 6 NATR) | ✅ |
| 6 | ADX | `adx_loader.py` | 24 (8 периодов × 3) | ✅ |
| 7 | MACD | `macd_loader.py` | 24 (8 конфигураций × 3) | ✅ |
| 8 | Bollinger Bands | `bollinger_bands_loader.py` | 78 (13 конф. × 6, вкл. %B) | ✅ |
| 9 | VWAP | `vwap_loader.py` | 16 (daily + 15 rolling) | ✅ |
| 10 | MFI | `mfi_loader.py` | 5 (7, 10, 14, 20, 25) | ✅ |
| 11 | Stochastic + Williams %R | `stochastic_williams_loader.py` | 21 (16 Stoch + 5 W%R) | ✅ |
| 12 | OBV | `obv_loader.py` | 1 | ✅ |
| 13 | Ichimoku Cloud | `ichimoku_loader.py` | 16 (2 конф. × 8) | ✅ |
| 14 | HV | `hv_loader.py` | 8 (5 HV + ratio + 2 percentile) | ✅ |
| 15 | SuperTrend | `supertrend_loader.py` | 21 (5 конф. × 4 + consensus) | ✅ |

### Рыночные данные (API)

| # | Данные | Загрузчик | Колонки | Статус |
|---|--------|-----------|---------|--------|
| 16 | Open Interest | `open_interest_loader.py` | 1 (`open_interest`) | ✅ |
| 17 | Funding Rate | `funding_fee_loader.py` | 2 (`funding_rate_next`, `funding_time_next`) | ✅ |
| 18 | Long/Short Ratio | `long_short_ratio_loader.py` | 3 | ✅ |
| 19 | Premium Index | `premium_index_loader.py` | 1 | ✅ |
| 20 | Fear & Greed (Alternative.me) | `fear_and_greed_loader_alternative.py` | 2 | ✅ |
| 21 | Fear & Greed (CoinMarketCap) | `fear_and_greed_coinmarketcap_loader.py` | 2+ | ✅ |

### Orderbook (отдельные таблицы)

| # | Данные | Загрузчик | Колонки | Статус |
|---|--------|-----------|---------|--------|
| 22 | Bybit Orderbook | `orderbook_bybit_loader.py` | 60 | ✅ |
| 23 | Binance Orderbook | `orderbook_binance_loader.py` | 46 | ✅ |

### Опционы (отдельные таблицы)

| # | Данные | Загрузчик | Колонки | Статус |
|---|--------|-----------|---------|--------|
| 24 | DVOL (Deribit) | `options_dvol_loader.py` | 6 (OHLC) | ✅ |
| 25 | DVOL Indicators | `options_dvol_indicators_loader.py` | 22 (8 групп) | ✅ |
| 26 | Options Aggregated | `options_aggregated_loader.py` | 24 (7 групп) | ✅ |

**Итого:** 26 загрузчиков, 261 колонка в indicators таблицах + 3 отдельные таблицы.

---

## Нереализованные индикаторы

### Bybit API данные (Высокий приоритет)

#### Taker Buy/Sell Volume
- **Источник:** Bybit API
- **Описание:** Объём рыночных ордеров на покупку vs продажу
- **Польза:** Показывает агрессивность покупателей/продавцов
- **Колонки:** `taker_buy_volume`, `taker_sell_volume`, `taker_buy_sell_ratio`

#### Liquidations
- **Источник:** Bybit WebSocket или CoinGlass API ($29/мес)
- **Описание:** Объём ликвидаций long/short позиций
- **Польза:** Каскадные ликвидации вызывают резкие движения
- **Колонки:** `liquidations_long_usd`, `liquidations_short_usd`, `liquidations_ratio`

---

### Классические технические индикаторы (Средний приоритет)

#### CCI (Commodity Channel Index)
- **Формула:** `CCI = (Typical Price - SMA(TP)) / (0.015 × Mean Deviation)`
- **Typical Price:** `(High + Low + Close) / 3`
- **Периоды:** 14, 20
- **Сигналы:** CCI > 100: перекупленность, CCI < -100: перепроданность
- **Колонки:** `cci_14`, `cci_20`

#### ROC (Rate of Change)
- **Формула:** `ROC = (Close - Close_n) / Close_n × 100`
- **Периоды:** 9, 14, 25
- **Описание:** Простой индикатор моментума
- **Колонки:** `roc_9`, `roc_14`, `roc_25`

#### CMF (Chaikin Money Flow)
- **Формула:** `CMF = Σ(MFV × Volume) / Σ(Volume)` за N периодов
- **MFV:** `((Close - Low) - (High - Close)) / (High - Low)`
- **Периоды:** 20, 21
- **Сигналы:** CMF > 0: давление покупки, CMF < 0: давление продажи
- **Колонки:** `cmf_20`, `cmf_21`

#### Aroon
- **Формула:**
  - `Aroon Up = ((N - Days since N-day High) / N) × 100`
  - `Aroon Down = ((N - Days since N-day Low) / N) × 100`
  - `Aroon Oscillator = Aroon Up - Aroon Down`
- **Периоды:** 14, 25
- **Колонки:** `aroon_14_up`, `aroon_14_down`, `aroon_14_oscillator`

#### Parabolic SAR
- **Формула:** Trailing stop с ускорением
- **Параметры:** AF start=0.02, AF max=0.2
- **Сигналы:** SAR ниже цены = восходящий тренд, выше = нисходящий
- **Колонки:** `psar`, `psar_trend` (1=up, -1=down)

#### Keltner Channels
- **Формула:** Middle = EMA(Close, N), Upper/Lower = EMA ± ATR × multiplier
- **Параметры:** EMA 20, ATR 10, multiplier 2.0
- **Отличие от Bollinger:** Использует ATR вместо стандартного отклонения
- **Колонки:** `keltner_20_upper`, `keltner_20_middle`, `keltner_20_lower`

#### Donchian Channels
- **Формула:** Upper = Max(High, N), Lower = Min(Low, N), Middle = (Upper + Lower) / 2
- **Периоды:** 20, 55
- **Сигналы:** Пробой канала = начало тренда
- **Колонки:** `donchian_20_upper`, `donchian_20_lower`, `donchian_20_middle`

---

### Производные фичи для ML (Высокий приоритет)

#### Price Change Percentage
- **Описание:** Изменение цены за разные периоды
- **Периоды:** 5m, 15m, 1h, 4h, 24h, 7d
- **Колонки:** `price_change_5m`, `price_change_15m`, `price_change_1h`, `price_change_4h`, `price_change_24h`, `price_change_7d`

#### Returns Volatility
- **Описание:** Стандартное отклонение доходности
- **Периоды:** 24h, 7d
- **Колонки:** `returns_volatility_24h`, `returns_volatility_7d`

#### Volume Ratio
- **Формула:** `Volume / VMA(Volume, N)`
- **Описание:** Показывает аномальный объём
- **Колонки:** `volume_ratio_20`, `volume_ratio_50`

#### RSI Divergence
- **Описание:** Расхождение между ценой и RSI
- **Типы:** Бычья (цена новый минимум, RSI нет) / Медвежья (цена новый максимум, RSI нет)
- **Колонки:** `rsi_divergence` (-1, 0, 1)

#### Multi-Timeframe Trend
- **Описание:** Согласованность тренда на разных таймфреймах
- **Логика:** Если тренд совпадает на 15m, 1h, 4h — сильный сигнал
- **Колонки:** `mtf_trend_score` (-3 to +3)

> **Примечание:** ATR Ratio (NATR) и Bollinger Band Position (%B) уже реализованы в `atr_loader.py` и `bollinger_bands_loader.py` соответственно.

---

### Альтернативные данные (Низкий приоритет)

#### On-Chain Metrics (требует платный API)
- **Источники:** Glassnode, CryptoQuant, Santiment, **CoinGlass ($29/мес)**
- **Данные:** Exchange Inflow/Outflow, Active Addresses, NVT Ratio, MVRV Ratio, Miner Revenue, NUPL, SOPR, Pi Cycle, Puell Multiple, Stock-to-Flow

> **См. также:** [CoinGlass API Analysis](./coinglass_api_analysis.md)

#### Social Sentiment
- **Источники:** LunarCrush, Santiment
- **Данные:** Social Volume, Weighted Sentiment, Social Dominance

---

## Приоритеты реализации

### Следующие кандидаты
1. Price Change % — простая реализация, полезно для ML
2. Volume Ratio — простая реализация
3. CCI — классическая формула
4. Taker Buy/Sell Volume — Bybit API

### По необходимости
5. ROC, CMF, Aroon, Parabolic SAR
6. Keltner Channels, Donchian Channels
7. RSI Divergence, Multi-Timeframe Trend

---

## Заметки

- Все новые индикаторы должны иметь загрузчик по шаблону существующих
- Необходимо добавлять в `indicators_config.yaml`
- Не забывать про поддержку `--force-reload` и `--check-nulls`
- Документировать в `INDICATORS_REFERENCE.md`
- Таймфреймы: 1m, 15m, 1h, 4h, 1d

---

## Внешние API источники

### CoinGlass API ($29+/месяц)

**Полный анализ:** [coinglass_api_analysis.md](./coinglass_api_analysis.md)

**Ключевые данные (недоступны бесплатно):**
- Агрегированный Open Interest со всех бирж
- OI-weighted Funding Rate
- Тепловые карты ликвидаций
- CVD (Cumulative Volume Delta)
- On-chain индикаторы (Pi Cycle, NUPL, SOPR, Puell Multiple)
- ETF Flows (BTC, ETH, SOL)
- Whale tracking (Hyperliquid)

**Минимальная цена:** $29/месяц (Hobbyist план)

### CoinMarketCap API (бесплатно)

**Полный анализ:** [coinmarketcap_api_analysis.md](./coinmarketcap_api_analysis.md)

**Доступно бесплатно:**
- Fear & Greed Index (2.7 лет истории) ✅ Реализовано
- Global Market Metrics (только текущие, без истории)
- Текущие цены и market cap криптовалют

**Недоступно через API (только на сайте):**
- CMC20, CMC100 индексы
- Pi Cycle, Puell Multiple, Rainbow Chart
- Altcoin Season Index

---

*Последнее обновление: 2026-02-25*
