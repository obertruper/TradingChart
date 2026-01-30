# Coinglass API Reference

Справочник по данным, доступным через Coinglass API, и сравнение с нашей системой.

**Дата создания:** 2026-01-29
**API версия:** v4
**Base URL:** `https://open-api-v4.coinglass.com`
**Документация:** https://docs.coinglass.com

---

## Тарифные планы

| План | Цена/мес | Цена/год | Запросов/мин | Endpoints | Коммерческое |
|------|----------|----------|--------------|-----------|--------------|
| Hobbyist | $29 | $348 | 30 | 70+ | ❌ |
| Startup | $79 | $948 | 80 | 80+ | ❌ |
| Standard | $299 | $3,588 | 300 | 90+ | ✅ |
| Professional | $699 | $8,388 | 1,200 | 100+ | ✅ |
| Enterprise | Custom | Custom | 6,000 | 100+ | ✅ |

**Бесплатный план отсутствует.**

---

## Сводная таблица данных

### Легенда статусов

| Статус | Описание |
|--------|----------|
| ✅ Есть | Данные уже есть в нашей БД |
| ⚡ Можем | Можем получить бесплатно (Bybit API, Alternative.me и др.) |
| 💰 Coinglass | Доступно только через Coinglass (платно) |
| 🔥 Уникально | Уникальные данные, нет бесплатных альтернатив |

---

## 1. Фьючерсы — Рыночные данные

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/coins` | Список поддерживаемых монет | ⚡ Можем | Bybit API бесплатно |
| `/supported-exchanges` | Список бирж | 💰 Coinglass | Агрегация 20+ бирж |
| `/instruments` | Торговые пары по биржам | ⚡ Можем | Bybit API бесплатно |
| `/coins-markets` | Рыночные данные монет | ⚡ Можем | Bybit API бесплатно |
| `/pairs-markets` | Рыночные данные пар | ⚡ Можем | Bybit API бесплатно |
| `/coins-price-change` | Изменение цены | ⚡ Можем | Рассчитываем из OHLC |
| `/price-ohlc-history` | История цен OHLC | ✅ Есть | `candles_bybit_futures_1m` |
| `/delisted-exchange-and-pair` | Делистинг пар | 💰 Coinglass | — |
| `/exchange-list` | Рейтинг бирж по объёму | 💰 Coinglass | Агрегация |

---

## 2. Open Interest (Открытый интерес)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/oi-ohlc-history` | OI История OHLC | ✅ Есть | `open_interest` колонка, Bybit |
| `/oi-ohlc-aggregated-history` | OI агрегированный со всех бирж | 💰 Coinglass | Сумма по 20+ биржам |
| `/oi-ohlc-aggregated-stablecoin-margin-history` | OI Stablecoin Margin | 💰 Coinglass | USDT/USDC контракты |
| `/oi-ohlc-aggregated-coin-margin-history` | OI Coin Margin | 💰 Coinglass | BTC/ETH контракты |
| `/oi-exchange-list` | OI по биржам (текущий) | 💰 Coinglass | Сравнение бирж |
| `/oi-exchange-history-chart` | OI история по биржам | 💰 Coinglass | Динамика по биржам |

**Наши данные:** Open Interest только с Bybit, с октября 2023.

---

## 3. Funding Rate (Ставка финансирования)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/fr-ohlc-history` | FR История OHLC | ✅ Есть | `funding_rate` колонка, Bybit |
| `/oi-weight-ohlc-history` | FR взвешенный по OI | 💰 Coinglass | Более репрезентативный |
| `/vol-weight-ohlc-history` | FR взвешенный по Volume | 💰 Coinglass | Альтернативное взвешивание |
| `/fr-exchange-list` | FR по биржам (текущий) | 💰 Coinglass | Сравнение бирж |
| `/cumulative-exchange-list` | Кумулятивный FR | 💰 Coinglass | Накопленный FR |
| `/fr-arbitrage` | FR Арбитражные возможности | 💰 Coinglass | Спреды между биржами |

**Наши данные:** Funding Rate только с Bybit, с марта 2020.

---

## 4. Long/Short Ratio

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/global-longshort-account-ratio` | Глобальный L/S по аккаунтам | ✅ Есть | `long_short_ratio`, Bybit |
| `/top-longshort-account-ratio` | L/S топ трейдеров | 💰 Coinglass | Позиции китов |
| `/top-longshort-position-ratio` | L/S топ позиций | 💰 Coinglass | По размеру позиций |
| `/taker-buysell-volume-exchange-list` | Taker Buy/Sell по биржам | 💰 Coinglass | Агрессивные ордера |
| `/net-position` | Net Long/Short Position | 💰 Coinglass | Чистая позиция |
| `/net-position-v2` | Net L/S v2 | 💰 Coinglass | Улучшенная версия |

**Наши данные:** Long/Short Ratio с Bybit, 15m и 1h таймфреймы.

---

## 5. Liquidations (Ликвидации) — 🔥 УНИКАЛЬНЫЕ

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/liquidation-history` | История ликвидаций пары | 🔥 Уникально | — |
| `/aggregated-liquidation-history` | История ликвидаций монеты | 🔥 Уникально | Агрегация по биржам |
| `/liquidation-coin-list` | Список ликвидаций | 🔥 Уникально | Топ ликвидаций |
| `/liquidation-exchange-list` | Ликвидации по биржам | 🔥 Уникально | Сравнение бирж |
| `/liquidation-order` | Ордера ликвидаций | 🔥 Уникально | Real-time ордера |
| `/liquidation-heatmap` | Heatmap ликвидаций Model 1 | 🔥 Уникально | Визуализация уровней |
| `/liquidation-heatmap-model2` | Heatmap Model 2 | 🔥 Уникально | Альтернативная модель |
| `/liquidation-heatmap-model3` | Heatmap Model 3 | 🔥 Уникально | Третья модель |
| `/liquidation-aggregate-heatmap` | Агрегированный Heatmap | 🔥 Уникально | По всем биржам |
| `/liquidation-aggregate-heatmap-model2` | Агрег. Heatmap Model 2 | 🔥 Уникально | — |
| `/liquidation-aggregated-heatmap-model3` | Агрег. Heatmap Model 3 | 🔥 Уникально | — |
| `/liquidation-map` | Карта ликвидаций пары | 🔥 Уникально | Уровни ликвидаций |
| `/liquidation-aggregated-map` | Карта ликвидаций монеты | 🔥 Уникально | Агрегированная |
| `/liquidation-max-pain` | Max Pain ликвидаций | 🔥 Уникально | Точка макс. боли |

**Наши данные:** ❌ Отсутствуют. Это главная уникальная ценность Coinglass.

---

## 6. Order Book (Стакан ордеров)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/futures-orderbook-history` | История стакана пары | 💰 Coinglass | Исторические данные |
| `/futures-aggregated-orderbook-history` | Агрегированный стакан | 💰 Coinglass | По всем биржам |
| `/orderbook-heatmap` | Heatmap стакана | 💰 Coinglass | Визуализация |
| `/large-orderbook` | Крупные ордера | 💰 Coinglass | Whale orders |
| `/large-orderbook-history` | История крупных ордеров | 💰 Coinglass | Tracking китов |

**Наши данные:** ❌ Отсутствуют.

---

## 7. Hyperliquid Positions (Позиции на Hyperliquid)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/hyperliquid-whale-alert` | Алерты китов | 🔥 Уникально | DEX прозрачность |
| `/hyperliquid-whale-position` | Позиции китов | 🔥 Уникально | On-chain данные |
| `/hyperliquid-position` | Позиции по монете | 🔥 Уникально | — |
| `/hyperliquid-user-position` | Позиции по адресу | 🔥 Уникально | Tracking кошельков |
| `/hyperliquid-wallet-position-distribution` | Распределение позиций | 🔥 Уникально | Статистика |
| `/hyperliquid-wallet-pnl-distribution` | Распределение PnL | 🔥 Уникально | Прибыль/убытки |

**Наши данные:** ❌ Отсутствуют.

---

## 8. Taker Buy/Sell (Агрессивные ордера)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/taker-buysell-volume` | Taker B/S пары | ⚡ Можем | Bybit API доступен |
| `/aggregated-taker-buysell-volume-history` | Агрегированный Taker B/S | 💰 Coinglass | По всем биржам |
| `/futures-footprint` | Footprint Chart (90 дней) | 💰 Coinglass | Volume profile |
| `/futures-cvd-history` | Cumulative Volume Delta | 💰 Coinglass | CVD индикатор |
| `/futures-aggregated-cvd-history` | Агрегированный CVD | 💰 Coinglass | По всем биржам |
| `/futures-netflow-list` | NetFlow монеты | 💰 Coinglass | Приток/отток |

**Наши данные:** ❌ Отсутствуют. Можем добавить Taker Buy/Sell с Bybit.

---

## 9. Options (Опционы)

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/option-max-pain` | Max Pain опционов | ⚡ Можем | Deribit API бесплатно |
| `/info` | Информация по опционам | ⚡ Можем | Deribit API |
| `/exchange-open-interest-history` | OI опционов | ⚡ Можем | Deribit API |
| `/exchange-volume-history` | Volume опционов | ⚡ Можем | Deribit API |

**Наши данные:** ❌ Отсутствуют. Можем получить бесплатно с Deribit.

---

## 10. On-Chain данные

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/exchange-assets` | Активы бирж | 💰 Coinglass | Прозрачность бирж |
| `/exchange-balance-list` | Балансы бирж | 💰 Coinglass | Резервы |
| `/exchange-balance-chart` | График балансов | 💰 Coinglass | Динамика |
| `/exchange-onchain-transfers` | On-chain переводы ERC-20 | 💰 Coinglass | Крупные транзакции |
| `/whale-transfer` | Переводы китов | 💰 Coinglass | Whale tracking |
| `/coin-unlock-list` | Token Unlock | 💰 Coinglass | Разлоки токенов |
| `/token-vesting` | Token Vesting | 💰 Coinglass | Графики вестинга |

**Наши данные:** ❌ Отсутствуют. Альтернатива: Glassnode, CryptoQuant (тоже платно).

---

## 11. ETF данные

### Bitcoin ETF

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/bitcoin-etfs` | Список BTC ETF | ⚡ Можем | Публичные данные |
| `/hong-kong-bitcoin-etf-flow-history` | HK ETF потоки | 💰 Coinglass | — |
| `/bitcoin-etf-netassets-history` | ETF Net Assets | 💰 Coinglass | AUM история |
| `/etf-flows-history` | ETF потоки | 💰 Coinglass | Inflow/Outflow |
| `/bitcoin-etf-premium-discount-history` | ETF Premium/Discount | 💰 Coinglass | К NAV |
| `/etf-history` | ETF история | 💰 Coinglass | — |
| `/etf-price-ohlc-history` | ETF цены OHLC | ⚡ Можем | Yahoo Finance |
| `/etf-detail` | Детали ETF | ⚡ Можем | Публичные данные |
| `/etf-aum` | ETF AUM | 💰 Coinglass | — |

### Ethereum, Solana, XRP ETF

| Endpoint | Описание | Статус |
|----------|----------|--------|
| `/ethereum-etf-*` | Ethereum ETF данные | 💰 Coinglass |
| `/solana-etf-flows-history` | Solana ETF потоки | 💰 Coinglass |
| `/xrp-etf-flows-history` | XRP ETF потоки | 💰 Coinglass |

### Grayscale

| Endpoint | Описание | Статус |
|----------|----------|--------|
| `/grayscale-holding-list` | Grayscale Holdings | 💰 Coinglass |
| `/grayscale-premium-history` | Grayscale Premium | 💰 Coinglass |

**Наши данные:** ❌ Отсутствуют.

---

## 12. Технические индикаторы — Фьючерсы

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/futures-rsi-list` | RSI по монетам | ✅ Есть | Свой расчёт, 5 периодов |
| `/futures-indicators-rsi` | RSI пары | ✅ Есть | `rsi_7/9/14/21/25` |
| `/futures-indicators-ma` | Moving Average | ✅ Есть | `sma_10/30/50/100/200` |
| `/futures-indicators-ema` | EMA | ✅ Есть | `ema_9/12/21/26/50/100/200` |
| `/futures-indicators-boll` | Bollinger Bands | ✅ Есть | 13 конфигураций |
| `/futures-indicators-macd` | MACD | ✅ Есть | 8 конфигураций |
| `/futures-indicators-avg-true-range` | ATR | ✅ Есть | `atr_7/14/21/30/50/100` |
| `/basis` | Futures Basis | ❌ Нет | Спред фьючерс-спот |
| `/whale-index` | Whale Index | 💰 Coinglass | Активность китов |
| `/cgdi-index` | CGDI Index | 💰 Coinglass | Coinglass индекс |
| `/cdri-index` | CDRI Index | 💰 Coinglass | Coinglass индекс |

**Наши данные:** Основные индикаторы рассчитываем сами.

---

## 13. Технические индикаторы — Spot

| Endpoint | Описание | Статус | Примечание |
|----------|----------|--------|------------|
| `/coinbase-premium-index` | Coinbase Premium | ⚡ Можем | Coinbase vs Binance |
| `/bitfinex-margin-long-short` | Bitfinex Margin L/S | 💰 Coinglass | — |
| `/borrow-interest-rate` | Ставки заимствования | 💰 Coinglass | DeFi ставки |

**Наши данные:** ❌ Отсутствуют.

---

## 14. Индикаторы Bitcoin (On-Chain & Macro)

| Endpoint | Описание | Статус | Тип |
|----------|----------|--------|-----|
| `/ahr999` | AHR999 Index | 💰 Coinglass | Накопление |
| `/bull-market-peak-indicator` | Индикатор пика | 💰 Coinglass | Цикл |
| `/puell-multiple` | Puell Multiple | 💰 Coinglass | Майнеры |
| `/stock-flow` | Stock-to-Flow | 💰 Coinglass | Дефицит |
| `/pi` | Pi Cycle Top | 💰 Coinglass | Цикл |
| `/golden-ratio-multiplier` | Golden Ratio | 💰 Coinglass | Уровни |
| `/bitcoin-profitable-days` | Profitable Days | 💰 Coinglass | Статистика |
| `/bitcoin-rainbow-chart` | Rainbow Chart | 💰 Coinglass | Визуализация |
| `/cryptofear-greedindex` | Fear & Greed | ✅ Есть | Alternative.me |
| `/stablecoin-marketcap-history` | Stablecoin MCap | 💰 Coinglass | Ликвидность |
| `/bitcoin-bubble-index` | Bubble Index | 💰 Coinglass | Перегрев |
| `/tow-year-ma-multiplier` | 2Y MA Multiplier | 💰 Coinglass | Цикл |
| `/tow-hundred-week-moving-avg-heatmap` | 200W MA Heatmap | 💰 Coinglass | Тренд |
| `/altcoin-season-index` | Altcoin Season | 💰 Coinglass | Ротация |
| `/bitcoin-short-term-holder-sopr` | STH SOPR | 💰 Coinglass | On-chain |
| `/bitcoin-long-term-holder-sopr` | LTH SOPR | 💰 Coinglass | On-chain |
| `/bitcoin-*-realized-price` | Realized Price | 💰 Coinglass | On-chain |
| `/bitcoin-*-supply` | Holder Supply | 💰 Coinglass | On-chain |
| `/bitcoin-rhodl-ratio` | RHODL Ratio | 💰 Coinglass | On-chain |
| `/bitcoin-reserve-risk` | Reserve Risk | 💰 Coinglass | On-chain |
| `/bitcoin-active-addresses` | Active Addresses | 💰 Coinglass | On-chain |
| `/bitcoin-new-addresses` | New Addresses | 💰 Coinglass | On-chain |
| `/bitcoin-nupl` | Net Unrealized PnL | 💰 Coinglass | On-chain |
| `/btc-correlations-*` | Корреляции | 💰 Coinglass | Macro |
| `/bitcoin-macro-oscillator-bmo` | Macro Oscillator | 💰 Coinglass | Macro |
| `/optionsfutures-oi-ratio` | Options/Futures OI | 💰 Coinglass | Деривативы |
| `/bitcoin-vs-*-m2-supply` | BTC vs M2 | 💰 Coinglass | Macro |
| `/bitcoin-dominance` | BTC Dominance | ⚡ Можем | CoinMarketCap |
| `/futures-spot-volume-ratio` | Futures/Spot Volume | 💰 Coinglass | Соотношение |

**Наши данные:** Только Fear & Greed Index.

---

## 15. WebSocket (Real-time)

| Endpoint | Описание | Статус |
|----------|----------|--------|
| `/ws-liquidation-order` | Ликвидации real-time | 🔥 Уникально |
| `/futures-trade-orders` | Сделки real-time | 💰 Coinglass |

---

## Итоговая статистика

| Категория | Всего endpoints | У нас есть | Можем бесплатно | Только Coinglass |
|-----------|-----------------|------------|-----------------|------------------|
| Фьючерсы рынок | 9 | 1 | 6 | 2 |
| Open Interest | 6 | 1 | 0 | 5 |
| Funding Rate | 6 | 1 | 0 | 5 |
| Long/Short | 6 | 1 | 0 | 5 |
| **Liquidations** | **14** | **0** | **0** | **14** |
| Order Book | 5 | 0 | 0 | 5 |
| Hyperliquid | 6 | 0 | 0 | 6 |
| Taker Buy/Sell | 6 | 0 | 1 | 5 |
| Options | 4 | 0 | 4 | 0 |
| On-Chain | 7 | 0 | 0 | 7 |
| ETF | 15 | 0 | 3 | 12 |
| Индикаторы фьючерсы | 11 | 7 | 0 | 4 |
| Индикаторы прочие | 30+ | 1 | 2 | 27+ |
| WebSocket | 2 | 0 | 0 | 2 |
| **ИТОГО** | **~127** | **~12** | **~16** | **~99** |

---

## Рекомендации

### Что стоит добавить бесплатно (без Coinglass):

1. **Taker Buy/Sell Ratio** — Bybit API
2. **Options Max Pain** — Deribit API
3. **Coinbase Premium** — расчёт из цен
4. **BTC Dominance** — CoinMarketCap API
5. **Futures Basis** — расчёт спот vs фьючерс

### Когда стоит платить за Coinglass ($29+/мес):

1. **Liquidation Heatmaps** — уникальные данные, нет альтернатив
2. **Агрегированные данные** — если нужны данные со всех бирж, не только Bybit
3. **On-chain метрики BTC** — SOPR, RHODL, Realized Price
4. **Hyperliquid позиции** — уникальная прозрачность DEX

### Альтернативы Coinglass для On-Chain:

| Сервис | Цена | Специализация |
|--------|------|---------------|
| Glassnode | от $29/мес | On-chain метрики |
| CryptoQuant | от $29/мес | On-chain + биржи |
| Santiment | от $44/мес | Social + On-chain |
| IntoTheBlock | от $10/мес | ML + On-chain |

---

## Что можем сами vs Что только Coinglass

### Сводная таблица по источникам данных

| Индикатор/Данные | Свои расчёты | Бесплатный API | Coinglass | Примечание |
|------------------|--------------|----------------|-----------|------------|
| **Тренд** |
| SMA, EMA, MACD | ✅ | — | есть | Считаем сами |
| ADX, Ichimoku | ✅ | — | есть | Считаем сами |
| Supertrend | ✅ | — | — | Считаем из ATR |
| Parabolic SAR | ✅ | — | — | Считаем сами |
| Donchian Channels | ✅ | — | — | Считаем сами |
| Aroon | ✅ | — | — | Считаем сами |
| **Моментум** |
| RSI, Stochastic | ✅ | — | есть | Считаем сами |
| MFI, Williams %R | ✅ | — | — | Считаем сами |
| CCI, ROC | ✅ | — | — | Считаем сами |
| **Волатильность** |
| ATR, Bollinger | ✅ | — | есть | Считаем сами |
| Historical Volatility | ✅ | — | — | Считаем сами |
| NATR, Garman-Klass | ✅ | — | — | Считаем сами |
| Implied Volatility (DVOL) | — | ✅ Deribit | — | Deribit API бесплатно |
| **Объём** |
| OBV, VWAP, VMA | ✅ | — | — | Считаем сами |
| CMF, CVD | ✅ | — | — | Считаем сами |
| Taker Buy/Sell | — | ✅ Bybit | есть | Bybit API бесплатно |
| **Сентимент** |
| Fear & Greed | — | ✅ Alternative.me | есть | Уже есть в БД |
| Long/Short Ratio | — | ✅ Bybit | есть | Уже есть в БД |
| Coinbase Premium | — | ✅ Coinbase+Binance | есть | Можем считать |
| **Деривативы** |
| Open Interest | — | ✅ Bybit | есть | Уже есть в БД |
| Funding Rate | — | ✅ Bybit | есть | Уже есть в БД |
| Futures Basis | ✅ | — | есть | Считаем (F-S)/S |
| Options Max Pain | — | ✅ Deribit | есть | Deribit бесплатно |
| **Macro** |
| BTC Dominance | — | ⚠️ CMC (текущие) | есть | См. раздел ниже |
| Stablecoin MarketCap | — | ⚠️ CMC (текущие) | есть | Только текущие бесплатно |
| **🔥 УНИКАЛЬНОЕ Coinglass** |
| Liquidation History | — | — | 🔥 только | Нет альтернатив |
| Liquidation Heatmap | — | — | 🔥 только | Нет альтернатив |
| Liquidation Map | — | — | 🔥 только | Нет альтернатив |
| Aggregated OI (все биржи) | — | — | 🔥 только | Нужно собирать с 20+ бирж |
| Aggregated FR (все биржи) | — | — | 🔥 только | Нужно собирать с 20+ бирж |
| Whale Index | — | — | 🔥 только | Проприетарный |
| Hyperliquid Positions | — | — | 🔥 только | DEX прозрачность |
| Large Orders Tracking | — | — | 🔥 только | Orderbook analysis |
| **On-Chain (все платно)** |
| SOPR (STH/LTH) | — | — | 💰 | Или Glassnode $29+ |
| Realized Price | — | — | 💰 | Или Glassnode $29+ |
| NUPL | — | — | 💰 | Или Glassnode $29+ |
| Exchange Balances | — | — | 💰 | Или Glassnode $29+ |
| Active Addresses | — | — | 💰 | Или Glassnode $29+ |

---

## BTC Dominance — детали

### Источник: CoinMarketCap API

**Endpoint:** `GET /v1/global-metrics/quotes/latest`

**Доступные данные:**
```json
{
  "btc_dominance": 57.34,
  "eth_dominance": 12.50,
  "total_market_cap": 3320000000000,
  "total_volume_24h": 180000000000,
  "stablecoin_market_cap": 150000000000,
  "defi_market_cap": 80000000000,
  "active_cryptocurrencies": 2400000,
  "active_exchanges": 750
}
```

### Тарифы CoinMarketCap

| План | Цена | Вызовов/мес | Текущие данные | Исторические |
|------|------|-------------|----------------|--------------|
| **Basic (Free)** | $0 | 10,000 | ✅ Да | ❌ Нет |
| Hobbyist | $29 | 40,000 | ✅ Да | ✅ Да |
| Startup | $79 | 120,000 | ✅ Да | ✅ Да |

### Наша ситуация

- ✅ **API ключ уже есть** в `indicators_config.yaml`
- ✅ **Текущие данные бесплатно** — можем получать btc_dominance, eth_dominance
- ❌ **Исторические данные платно** — нужна подписка $29+/мес

### Рекомендация по BTC Dominance

**Вариант 1: Накопление с нуля (бесплатно)**
- Создать `btc_dominance_loader.py`
- Получать данные каждый час
- История будет накапливаться со временем
- Старые данные = NULL

**Вариант 2: Coinglass ($29/мес)**
- Исторические данные доступны
- Endpoint: `/bitcoin-dominance`

**Вариант 3: CoinMarketCap Hobbyist ($29/мес)**
- Endpoint: `/v1/global-metrics/quotes/historical`
- Полная история с 2013 года

---

## 🔥 Уникальная ценность Coinglass (нет альтернатив)

### Критически важные данные (нигде больше нет):

| Данные | Почему важно | Применение |
|--------|--------------|------------|
| **Liquidation Heatmap** | Показывает уровни массовых ликвидаций | Определение S/R, точки разворота |
| **Liquidation History** | Объём и направление ликвидаций | Sentiment, подтверждение трендов |
| **Aggregated OI** | OI со всех бирж (не только Bybit) | Полная картина рынка |
| **Aggregated Funding** | FR взвешенный по OI/Volume | Более точный sentiment |
| **Hyperliquid Whale Positions** | Позиции крупных трейдеров на DEX | Следование за smart money |
| **Large Orders** | Крупные ордера в стакане | Определение intent крупных игроков |

### Важные, но есть альтернативы:

| Данные Coinglass | Альтернатива | Цена альтернативы |
|------------------|--------------|-------------------|
| On-chain метрики | Glassnode | $29+/мес |
| BTC Dominance (история) | CoinMarketCap | $29+/мес |
| Options данные | Deribit API | Бесплатно |

---

## Итоговая рекомендация

### Что добавить БЕСПЛАТНО (приоритет):

1. ✅ **Волатильность** — HV, NATR, Garman-Klass (из OHLCV)
2. ✅ **Тренд** — Supertrend, Parabolic SAR, Donchian, Aroon (из OHLCV)
3. ✅ **Taker Buy/Sell** — Bybit API
4. ✅ **BTC Dominance** — CoinMarketCap (текущие, накапливать)
5. ✅ **DVOL** — Deribit API (implied volatility)
6. ✅ **Futures Basis** — расчёт из наших данных

### Когда платить за Coinglass ($29/мес):

**Стоит, если нужны:**
- 🔥 Liquidation данные (heatmap, history, levels)
- 🔥 Агрегированные данные со всех бирж
- 🔥 Whale tracking (Hyperliquid, Large Orders)

**Не стоит, если:**
- Достаточно данных только с Bybit
- On-chain метрики не критичны
- Можем накапливать историю постепенно

---

## Источники

- [Coinglass Pricing](https://www.coinglass.com/pricing)
- [Coinglass API Documentation v4](https://docs.coinglass.com)
- [Bybit API Documentation](https://bybit-exchange.github.io/docs/)
- [Deribit API Documentation](https://docs.deribit.com/)
