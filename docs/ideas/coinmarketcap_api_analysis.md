# CoinMarketCap API - Анализ и возможности

> Дата анализа: 2026-01-29
> Статус: Исследование завершено

## Обзор

CoinMarketCap (CMC) - крупнейший агрегатор данных о криптовалютах. API предоставляет информацию о ценах, market cap, объёмах и рыночных метриках.

**Официальные ссылки:**
- API Portal: https://coinmarketcap.com/api/
- Pricing: https://coinmarketcap.com/api/pricing/
- Documentation: https://coinmarketcap.com/api/documentation/v1/

**Наш API ключ:** Настроен в `indicators/indicators_config.yaml` (секция `coinmarketcap_fear_and_greed.api_key`)

---

## Тарифные планы

| План | Цена/мес | API Credits | Историч. данные | Commercial |
|------|----------|-------------|-----------------|------------|
| **Basic** | Бесплатно | 10,000/мес | Ограничено | Нет |
| **Hobbyist** | $29 | 40,000/мес | 1 год | Нет |
| **Startup** | $79 | 120,000/мес | 3 года | Нет |
| **Standard** | $299 | 500,000/мес | 5 лет | Да |
| **Professional** | $699 | 1,500,000/мес | 10 лет | Да |
| **Enterprise** | Custom | Unlimited | **All-time (14+ лет)** | Да |

---

## Бесплатные endpoints (Basic план)

### Работающие endpoints (протестировано)

#### 1. Fear & Greed Index (Historical)
```
GET https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical
```
- **Описание:** Исторический индекс страха и жадности
- **История:** ~2.7 лет (с 2023-06-29)
- **Формат:** Ежедневные значения (0-100)
- **Статус:** ✅ Используется в `fear_and_greed_coinmarketcap_loader.py`

**Пример ответа:**
```json
{
  "data": [
    {
      "timestamp": "2025-01-28T00:00:00.000Z",
      "value": 73,
      "value_classification": "Greed"
    }
  ]
}
```

#### 2. Fear & Greed Index (Latest)
```
GET https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest
```
- **Описание:** Текущее значение Fear & Greed
- **Статус:** ✅ Работает

#### 3. Global Metrics (Latest)
```
GET https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest
```
- **Описание:** Глобальные метрики криптовалютного рынка
- **История:** ❌ Только текущие данные
- **Статус:** ✅ Работает (исследовано, не реализовано)

**Доступные метрики:**
| Метрика | Описание | Пример значения |
|---------|----------|-----------------|
| `total_market_cap` | Общая капитализация | $3.68T |
| `total_volume_24h` | Объём за 24ч | $181.85B |
| `btc_dominance` | Доминация BTC | 59.20% |
| `eth_dominance` | Доминация ETH | 12.63% |
| `altcoin_market_cap` | Капитализация альткоинов | $1.5T |
| `defi_market_cap` | DeFi капитализация | - |
| `defi_volume_24h` | DeFi объём 24ч | - |
| `stablecoin_market_cap` | Капитализация стейблкоинов | - |
| `stablecoin_volume_24h` | Объём стейблкоинов 24ч | - |
| `derivatives_volume_24h` | Объём деривативов 24ч | - |
| `active_cryptocurrencies` | Активных криптовалют | ~10,000 |
| `active_exchanges` | Активных бирж | ~700 |
| `active_market_pairs` | Активных торговых пар | ~80,000 |

**Частота обновления:** Каждые 1 минуту (официально каждые 5 минут)

#### 4. Cryptocurrency Listings
```
GET https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest
```
- **Описание:** Список криптовалют с ценами и метриками
- **Лимит:** 5000 криптовалют
- **Статус:** ✅ Работает

#### 5. Cryptocurrency Quotes
```
GET https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest
```
- **Описание:** Текущие котировки для конкретных криптовалют
- **Параметры:** `id` или `symbol`
- **Статус:** ✅ Работает

---

## Платные endpoints (требуют Hobbyist+)

### Historical Data (требует подписку)

#### Cryptocurrency OHLCV Historical
```
GET https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical
```
- **План:** Hobbyist+ ($29/мес)
- **История:** Зависит от плана (1-14+ лет)
- **Интервалы:** daily, weekly, monthly, yearly

#### Global Metrics Historical
```
GET https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/historical
```
- **План:** Standard+ ($299/мес)
- **Описание:** Историческая глобальная капитализация
- **Статус:** ❌ Недоступно на Basic/Hobbyist

#### Cryptocurrency Quotes Historical
```
GET https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/historical
```
- **План:** Hobbyist+ ($29/мес)
- **Описание:** Исторические котировки

---

## Endpoints которые НЕ работают через API

> **ВАЖНО:** Следующие данные доступны ТОЛЬКО на сайте, НЕ через API!

### Market Cycle Indicators (недоступны)
| Индикатор | URL на сайте | API Endpoint |
|-----------|--------------|--------------|
| **CMC Crypto Market Cap** | /charts/ | ❌ Нет |
| **CMC 20 Index** | /charts/cmc20/ | ❌ Нет |
| **CMC 100 Index** | /charts/cmc100/ | ❌ Нет |
| **Pi Cycle Top** | /charts/crypto-market-cycle-indicators/ | ❌ Нет |
| **Puell Multiple** | /charts/crypto-market-cycle-indicators/ | ❌ Нет |
| **Bitcoin Rainbow Chart** | /charts/crypto-market-cycle-indicators/ | ❌ Нет |
| **Altcoin Season Index** | /charts/altcoin-season-index/ | ❌ Нет |
| **Bitcoin Bubble Index** | /charts/ | ❌ Нет |

**Причина:** Эти индикаторы - проприетарные визуализации CMC, доступные только через веб-интерфейс.

### Trending (ограничено)
```
GET https://pro-api.coinmarketcap.com/v1/cryptocurrency/trending/latest
```
- **Статус:** ❌ Возвращает пустой массив на Basic плане

### Community/Content
- `/v1/content/posts/top` - ❌ Недоступно
- `/v1/content/posts/latest` - ❌ Недоступно
- `/v1/community/trending/topic` - ❌ Недоступно

---

## Использование API credits

| Операция | Credits | Примечание |
|----------|---------|------------|
| Quotes Latest | 1 credit | На запрос |
| Listings Latest | 1 credit | На 200 криптовалют |
| Global Metrics Latest | 1 credit | На запрос |
| Fear & Greed | 1 credit | На запрос |
| OHLCV Historical | 1 credit | На символ/интервал |

**Basic план:** 10,000 credits/месяц
- Ежечасные запросы: 24 × 30 = 720 запросов/месяц (7.2% лимита) ✅
- Ежеминутные запросы: 60 × 24 × 30 = 43,200 запросов/месяц ❌ Превышает лимит

---

## Текущая реализация в проекте

### Реализованные загрузчики

#### 1. fear_and_greed_coinmarketcap_loader.py
- **Endpoint:** `/v3/fear-and-greed/historical`
- **Данные:** Fear & Greed Index
- **История:** ~2.7 лет (с 2023-06-29)
- **Таблица:** `indicators_bybit_futures_*`
- **Колонки:**
  - `coinmarketcap_fear_greed_value` (0-100)
  - `coinmarketcap_fear_greed_classification` (Extreme Fear/Fear/Neutral/Greed/Extreme Greed)

### Нереализованные (возможно добавить)

#### Global Metrics Loader (планируется)
- **Endpoint:** `/v1/global-metrics/quotes/latest`
- **Частота:** Ежечасно
- **Колонки (предложение):**
  - `cmc_total_market_cap`
  - `cmc_btc_dominance`
  - `cmc_eth_dominance`
  - `cmc_altcoin_market_cap`
  - `cmc_defi_market_cap`
  - `cmc_stablecoin_market_cap`
  - `cmc_derivatives_volume_24h`
  - `cmc_total_volume_24h`

**Ограничение:** Только текущие данные, нет истории на бесплатном плане.

---

## Сравнение с другими источниками

| Данные | CMC (бесплатно) | CMC (платно) | CoinGlass ($29+) | Alternative.me |
|--------|-----------------|--------------|------------------|----------------|
| Fear & Greed | 2.7 лет | 2.7 лет | Да | **6+ лет** |
| Global Market Cap | Только текущее | История | - | - |
| BTC/ETH Dominance | Только текущее | История | Да | - |
| Pi Cycle | ❌ | ❌ | **Да** | - |
| Puell Multiple | ❌ | ❌ | **Да** | - |
| NUPL, SOPR | ❌ | ❌ | **Да** | - |
| Altcoin Season | ❌ | ❌ | **Да** | - |
| Цены криптовалют | Текущие | История | - | - |

---

## Рекомендации

### Что уже используем (оставить)
1. **Fear & Greed Index** через `fear_and_greed_coinmarketcap_loader.py`
   - Дополняет данные Alternative.me (более длинная история)

### Что можно добавить (бесплатно)
1. **Global Metrics** - текущие данные о рынке
   - Полезно: BTC/ETH dominance, total market cap
   - Ограничение: нет истории, только текущие значения
   - Стратегия: собирать ежечасно, накапливать историю самостоятельно

### Что НЕ стоит покупать
1. **Hobbyist план ($29/мес)** - лучше потратить на CoinGlass
   - CMC Hobbyist даёт только исторические цены (которые есть у нас с Bybit)
   - CoinGlass за те же $29 даёт уникальные данные (OI, ликвидации, on-chain)

### Альтернативы для недоступных индикаторов
| Индикатор | Альтернатива |
|-----------|--------------|
| Pi Cycle | Рассчитать самостоятельно: `SMA(111)` vs `SMA(350) × 2` |
| 2-Year MA Multiplier | Рассчитать самостоятельно: `Price / SMA(730)` |
| Puell Multiple | CoinGlass API ($29/мес) |
| NUPL, SOPR | CoinGlass API ($29/мес) |
| Altcoin Season | CoinGlass API ($29/мес) |

---

## API Response Examples

### Fear & Greed Historical
```bash
curl -H "X-CMC_PRO_API_KEY: YOUR_KEY" \
  "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical?limit=7"
```

```json
{
  "status": {
    "timestamp": "2026-01-29T10:00:00.000Z",
    "error_code": 0,
    "credit_count": 1
  },
  "data": [
    {"timestamp": "2026-01-28T00:00:00.000Z", "value": 73, "value_classification": "Greed"},
    {"timestamp": "2026-01-27T00:00:00.000Z", "value": 70, "value_classification": "Greed"},
    {"timestamp": "2026-01-26T00:00:00.000Z", "value": 68, "value_classification": "Greed"}
  ]
}
```

### Global Metrics Latest
```bash
curl -H "X-CMC_PRO_API_KEY: YOUR_KEY" \
  "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
```

```json
{
  "data": {
    "active_cryptocurrencies": 10234,
    "total_cryptocurrencies": 2456789,
    "active_market_pairs": 83456,
    "active_exchanges": 756,
    "btc_dominance": 59.20,
    "eth_dominance": 12.63,
    "quote": {
      "USD": {
        "total_market_cap": 3680000000000,
        "total_volume_24h": 181850000000,
        "altcoin_market_cap": 1500000000000,
        "defi_market_cap": 150000000000,
        "stablecoin_market_cap": 200000000000,
        "derivatives_volume_24h": 95000000000
      }
    }
  }
}
```

---

## Заключение

**CoinMarketCap API (бесплатный план) полезен для:**
- Fear & Greed Index (2.7 лет истории)
- Текущие глобальные метрики (market cap, dominance)
- Текущие цены криптовалют

**Ограничения:**
- Нет исторических данных для большинства метрик
- Market cycle индикаторы недоступны через API
- Для уникальных on-chain данных лучше использовать CoinGlass

**Вывод:** Продолжаем использовать бесплатный API для Fear & Greed. Для расширенных данных рассмотреть CoinGlass ($29/мес) вместо CMC Hobbyist.

---

## Связанные документы

- [CoinGlass API Analysis](./coinglass_api_analysis.md) - анализ альтернативного источника данных
- [Future Indicators](./future_indicators.md) - планируемые индикаторы

---

*Последнее обновление: 2026-01-29*
