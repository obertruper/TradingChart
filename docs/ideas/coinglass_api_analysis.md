# CoinGlass API - Анализ и возможности

> Дата анализа: 2026-01-29
> Статус: Исследование завершено

## Обзор

CoinGlass - профессиональный провайдер данных по криптодеривативам. API предоставляет агрегированные данные с множества бирж, on-chain метрики и уникальные индикаторы.

**Официальные ссылки:**
- Pricing: https://www.coinglass.com/pricing
- API Docs: https://docs.coinglass.com/
- Endpoints: https://docs.coinglass.com/reference/endpoint-overview

---

## Тарифные планы

> **ВАЖНО:** Бесплатного плана НЕТ!

| План | Цена/мес | Цена/год | Endpoints | Rate Limit | Использование |
|------|----------|----------|-----------|------------|---------------|
| **Hobbyist** | $29 | $348 | 70+ | 30/мин | Personal only |
| **Startup** | $79 | $948 | 80+ | 80/мин | Personal only |
| **Standard** | $299 | $3,588 | 90+ | 300/мин | Commercial |
| **Professional** | $699 | $8,388 | 100+ | 1200/мин | Commercial |
| **Enterprise** | Custom | Custom | 100+ | 6000/мин | Commercial |

**Особенности:**
- Update frequency: ≤ 1 минута для всех планов
- Commercial use требует план Standard ($299+)
- При оплате за год скидка ~20%

---

## Полный список endpoints (100+)

### Futures Trading

#### Trading Market
| Endpoint | Описание |
|----------|----------|
| `/futures/supported-coins` | Доступные фьючерсные монеты |
| `/futures/supported-exchange-pairs` | Поддерживаемые биржи и пары |
| `/api/futures/pairs-markets` | Рыночные данные по парам |
| `/api/futures/coins-markets` | Рыночные данные по монетам |
| `/futures/price-change-list` | Изменение цены |
| `/api/price/ohlc-history` | OHLC история |

#### Open Interest (уникальные данные!)
| Endpoint | Описание |
|----------|----------|
| `/api/futures/openInterest/ohlc-history` | OI OHLC история |
| `/api/futures/openInterest/ohlc-aggregated-history` | **Агрегированный OI со всех бирж** |
| `/api/futures/openInterest/ohlc-aggregated-stablecoin` | OI по стейблкоин-маржинальным контрактам |
| `/api/futures/openInterest/ohlc-aggregated-coin-margin-history` | OI по коин-маржинальным контрактам |
| `/api/futures/openInterest/exchange-list` | OI по биржам |
| `/api/futures/openInterest/exchange-history-chart` | OI история по биржам |

#### Funding Rate (уникальные данные!)
| Endpoint | Описание |
|----------|----------|
| `/api/futures/fundingRate/ohlc-history` | Funding rate OHLC |
| `/api/futures/fundingRate/oi-weight-ohlc-history` | **OI-взвешенный funding rate** |
| `/api/futures/fundingRate/vol-weight-ohlc-history` | Volume-взвешенный funding rate |
| `/api/futures/fundingRate/exchange-list` | Funding rate по биржам |
| `/api/futures/fundingRate/accumulated-exchange-list` | Накопленный funding rate |
| `/api/futures/fundingRate/arbitrage` | Арбитражные возможности |

#### Long-Short Ratio
| Endpoint | Описание |
|----------|----------|
| `/api/futures/global-long-short-account-ratio/history` | Глобальное соотношение аккаунтов |
| `/api/futures/top-long-short-account-ratio/history` | Топ трейдеры (аккаунты) |
| `/api/futures/top-long-short-position-ratio/history` | Топ трейдеры (позиции) |
| `/api/futures/taker-buy-sell-volume/exchange-list` | Taker объёмы по биржам |
| `/api/futures/net-position` | Чистые позиции |

#### Liquidation (уникальные данные!)
| Endpoint | Описание | История |
|----------|----------|---------|
| `/api/futures/liquidation/history` | Ликвидации по паре | ? |
| `/api/futures/liquidation/aggregated-history` | Ликвидации по монете | ? |
| `/api/futures/liquidation/coin-list` | Доступные монеты | - |
| `/api/futures/liquidation/exchange-list` | Ликвидации по биржам | - |
| `/api/futures/liquidation/order` | **Ордера ликвидаций** | 7 дней |
| `/api/futures/liquidation/heatmap/model1-3` | **Тепловые карты** | - |
| `/api/futures/liquidation/aggregated-heatmap/model1-3` | Агрегированные тепловые карты | - |
| `/api/futures/liquidation/map` | Карта ликвидаций по паре | - |
| `/api/futures/liquidation/aggregated-map` | Карта ликвидаций по монете | - |
| `/api/futures/liquidation/max-pain` | Уровни Max Pain | - |

#### Order Book
| Endpoint | Описание |
|----------|----------|
| `/api/futures/orderbook/ask-bids-history` | История стакана по паре |
| `/api/futures/orderbook/aggregated-ask-bids-history` | Агрегированный стакан |
| `/api/futures/orderbook/history` | Тепловая карта стакана |
| `/api/futures/orderbook/large-limit-order` | **Крупные лимитные ордера** |
| `/api/futures/orderbook/large-limit-order-history` | История крупных ордеров |

#### Hyperliquid Positions (уникальные данные!)
| Endpoint | Описание |
|----------|----------|
| `/api/hyperliquid/whale-alert` | Алерты по китам |
| `/api/hyperliquid/whale-position` | **Позиции китов** |
| `/api/hyperliquid/position` | Позиции по монете |
| `/api/hyperliquid/user-position` | Позиции по адресу |
| `/api/hyperliquid/wallet-position-distribution` | Распределение позиций |
| `/api/hyperliquid/wallet-pnl-distribution` | Распределение PnL |

#### Taker Buy/Sell
| Endpoint | Описание |
|----------|----------|
| `/api/futures/taker-buy-sell-volume/history` | Taker объёмы по паре |
| `/api/futures/aggregated-taker-buy-sell-volume/history` | Агрегированные taker объёмы |
| `/api/futures/footprint` | Footprint данные (90 дней) |
| `/api/futures/cvd-history` | Cumulative Volume Delta |
| `/api/futures/aggregated-cvd-history` | Агрегированный CVD |
| `/api/futures/netflow-list` | Net Flow по монете |

---

### Spot Trading

| Endpoint | Описание |
|----------|----------|
| `/api/spot/supported-coins` | Доступные спот монеты |
| `/api/spot/supported-exchange-pairs` | Спот пары по биржам |
| `/api/spot/coins-markets` | Рынки по монетам |
| `/api/spot/pairs-markets` | Рынки по парам |
| `/api/spot/price/history` | OHLC история |
| `/api/spot/orderbook/*` | Стакан (аналогично futures) |
| `/api/spot/taker-buy-sell-volume/*` | Taker объёмы |
| `/api/spot/cvd-history` | CVD |

---

### Options

| Endpoint | Описание |
|----------|----------|
| `/api/option/max-pain` | Max Pain анализ |
| `/api/option/info` | Информация по опционам |
| `/api/option/exchange-oi-history` | OI по биржам |
| `/api/option/exchange-vol-history` | Объёмы по биржам |

---

### On-Chain Data

| Endpoint | Описание |
|----------|----------|
| `/api/exchange/assets` | Активы бирж |
| `/api/exchange/balance/list` | Балансы бирж |
| `/api/exchange/balance/chart` | График балансов |
| `/api/exchange/chain/tx/list` | ERC-20 транзакции |
| `/api/whale-transfer` | **Крупные переводы** |
| `/api/coin-unlock-list` | Расписание разблокировок |
| `/api/token-vesting` | Vesting данные |

---

### ETF Data

#### Bitcoin ETF
| Endpoint | Описание |
|----------|----------|
| `/api/etf/bitcoin/list` | Список BTC ETF |
| `/api/etf/bitcoin/flow-history` | **История потоков** |
| `/api/etf/bitcoin/net-assets/history` | Чистые активы |
| `/api/etf/bitcoin/premium-discount/history` | Премия/дисконт |
| `/api/hk-etf/bitcoin/flow-history` | Гонконгские ETF |

#### Ethereum/Solana/XRP ETF
- Аналогичные endpoints для ETH, SOL, XRP

#### Grayscale
| Endpoint | Описание |
|----------|----------|
| `/api/grayscale/holdings-list` | Holdings |
| `/api/grayscale/premium-history` | История премии |

---

### Indicators (On-Chain метрики - САМОЕ ЦЕННОЕ!)

| Endpoint | Описание | Доступно бесплатно в CMC? |
|----------|----------|---------------------------|
| `/api/index/pi-cycle-indicator` | **Pi Cycle Top Indicator** | Нет |
| `/api/index/puell-multiple` | **Puell Multiple** | Нет |
| `/api/index/stock-flow` | **Stock-to-Flow Model** | Нет |
| `/api/index/bitcoin/rainbow-chart` | **Rainbow Chart** | Нет |
| `/api/index/bitcoin/bubble-index` | **Bubble Index** | Нет |
| `/api/index/altcoin-season` | **Altcoin Season Index** | Нет |
| `/api/index/bitcoin/nupl` | **NUPL (Net Unrealized P/L)** | Нет |
| `/api/index/bitcoin/sopr-short-term` | SOPR (Short-Term Holders) | Нет |
| `/api/index/bitcoin/sopr-long-term` | SOPR (Long-Term Holders) | Нет |
| `/api/index/bitcoin/reserve-risk` | Reserve Risk | Нет |
| `/api/index/bitcoin/rhodl-ratio` | RHODL Ratio | Нет |
| `/api/index/bitcoin/active-addresses` | Active Addresses | Нет |
| `/api/index/bitcoin/new-addresses` | New Addresses | Нет |
| `/api/index/ahr999` | AHR999 Index | Нет |
| `/api/index/golden-ratio-multiplier` | Golden Ratio Multiplier | Нет |
| `/api/index/bitcoin/profitable-days` | BTC Profitable Days | Нет |
| `/api/index/fear-greed-history` | Fear & Greed Index | Да (2.7 лет) |
| `/api/index/stableCoin-marketCap-history` | Stablecoin Market Cap | Нет |
| `/api/bull-market-peak-indicator` | Bull Market Peak | Нет |
| `/api/index/2-year-ma-multiplier` | 2-Year MA Multiplier | Нет |
| `/api/index/200-week-moving-average-heatmap` | 200-Week MA Heatmap | Нет |
| `/api/index/bitcoin/dominance` | Bitcoin Dominance | Нет |
| `/api/index/bitcoin-macro-oscillator` | Macro Oscillator | Нет |
| `/api/index/bitcoin-vs-m2` | Bitcoin vs M2 Supply | Нет |
| `/api/index/futures-spot-volume-ratio` | Futures/Spot Ratio | Нет |
| `/api/index/options-futures-oi-ratio` | Options/Futures OI Ratio | Нет |
| `/api/coinbase-premium-index` | Coinbase Premium Index | Нет |
| `/api/bitfinex-margin-long-short` | Bitfinex Margin L/S | Нет |

---

## Сравнение с текущими источниками

| Данные | Bybit API (бесплатно) | CoinMarketCap (бесплатно) | CoinGlass ($29+) |
|--------|----------------------|---------------------------|------------------|
| **Funding Rate** | 1 биржа | - | **Все биржи + OI-weighted** |
| **Open Interest** | 1 биржа | - | **Агрегированный** |
| **Premium Index** | 1 биржа | - | **Все биржи** |
| **Long/Short Ratio** | 1 биржа | - | **Все биржи** |
| **Liquidations** | - | - | **Heatmap + Orders (7 дней)** |
| **Whale Tracking** | - | - | **Hyperliquid позиции** |
| **Fear & Greed** | - | 2.7 лет | Да |
| **Pi Cycle** | - | - | **Да** |
| **Puell Multiple** | - | - | **Да** |
| **Stock-to-Flow** | - | - | **Да** |
| **Rainbow Chart** | - | - | **Да** |
| **NUPL, SOPR** | - | - | **Да** |
| **ETF Flows** | - | - | **Да** |
| **CVD (Cumulative Volume Delta)** | - | - | **Да** |

---

## Рекомендации

### Если бюджет позволяет ($29/месяц - Hobbyist)

**Приоритет 1 - Уникальные данные для ML:**
1. **Aggregated Open Interest** - OI со всех бирж вместо только Bybit
2. **OI-weighted Funding Rate** - более точный индикатор настроений
3. **Liquidation Heatmaps** - предсказание уровней ликвидаций
4. **CVD (Cumulative Volume Delta)** - momentum индикатор

**Приоритет 2 - On-Chain метрики:**
1. **NUPL** - Net Unrealized P/L (исторически предсказывает топы/дно)
2. **SOPR** - Spent Output Profit Ratio (прибыльность транзакций)
3. **Pi Cycle Top** - исторически точно определял топы BTC
4. **Puell Multiple** - относительный доход майнеров

**Приоритет 3 - Дополнительные данные:**
1. **ETF Flows** - институциональный интерес
2. **Whale Tracking (Hyperliquid)** - позиции крупных игроков
3. **Coinbase Premium** - американский retail интерес

### Если бюджет ограничен

Продолжаем использовать:
- Bybit API для Funding Rate, OI, Premium Index, Long/Short Ratio (1 биржа)
- CoinMarketCap для Fear & Greed (бесплатно, 2.7 лет истории)

**Альтернатива:** Можно рассчитывать некоторые индикаторы самостоятельно:
- Pi Cycle: `SMA(111) vs SMA(350) × 2` (нужна только цена BTC)
- 2-Year MA Multiplier: `Price / SMA(730)` (нужна только цена BTC)

---

## Ограничения

1. **Минимальная цена $29/месяц** - нет бесплатного плана
2. **Liquidation Orders** - только 7 дней истории
3. **Footprint данные** - только 90 дней
4. **Commercial use** - требует план Standard ($299/месяц)
5. **Rate Limits** - 30/мин на Hobbyist (достаточно для ежечасных обновлений)

---

## Заключение

CoinGlass предоставляет уникальные данные, которые невозможно получить бесплатно:
- Агрегированные деривативные метрики со всех бирж
- On-chain индикаторы (Pi Cycle, NUPL, SOPR и др.)
- Тепловые карты ликвидаций
- Whale tracking

**ROI оценка:** $29/месяц может значительно улучшить качество ML моделей за счёт уникальных фичей, которые недоступны у конкурентов бесплатно.

---

*Последнее обновление: 2026-01-29*
