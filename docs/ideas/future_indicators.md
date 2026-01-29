# Future Indicators - Ideas for Implementation

> Дата создания: 2026-01-26
> Статус: Планирование

## Обзор

Этот документ содержит список индикаторов и данных, которые планируется добавить в систему для улучшения ML моделей.

---

## В процессе реализации

### 1. Open Interest
- **Статус:** Планируется
- **Источник:** Bybit API `/v5/market/open-interest`
- **Описание:** Общее количество открытых фьючерсных контрактов
- **Интервалы:** 5min, 15min, 30min, 1h, 4h, 1d
- **История:** ~27 месяцев (с октября 2023)
- **Польза для ML:**
  - Подтверждение силы тренда
  - Предсказание разворотов (резкое падение OI = ликвидации)
  - Комбинация с ценой даёт 4 сигнала (см. ниже)

| Цена | OI | Интерпретация |
|------|-----|---------------|
| ↑ | ↑ | Сильный бычий тренд |
| ↑ | ↓ | Слабый рост (закрытие шортов) |
| ↓ | ↑ | Сильный медвежий тренд |
| ↓ | ↓ | Капитуляция, возможен разворот |

**Колонки для БД:**
- `open_interest` - OI в базовой валюте (BTC, ETH, etc.)
- `open_interest_change_pct` - изменение OI в % за период

---

### 2. Funding Rate
- **Статус:** Планируется
- **Источник:** Bybit API `/v5/market/funding/history`
- **Описание:** Ставка финансирования для perpetual контрактов
- **Интервалы:** Каждые 8 часов (00:00, 08:00, 16:00 UTC)
- **История:** Доступна с начала торговли парой
- **Польза для ML:**
  - Положительный funding = лонги платят шортам = перегрев бычьего рынка
  - Отрицательный funding = шорты платят лонгам = перегрев медвежьего рынка
  - Экстремальные значения (>0.1% или <-0.1%) часто предшествуют коррекции

**Колонки для БД:**
- `funding_rate` - ставка финансирования (например, 0.0001 = 0.01%)
- `funding_rate_annualized` - годовая ставка (funding_rate × 3 × 365)

---

## Запланированные индикаторы

### Индикаторы из Bybit API (Высокий приоритет)

#### 3. Taker Buy/Sell Volume
- **Источник:** Bybit API
- **Описание:** Объём рыночных ордеров на покупку vs продажу
- **Польза:** Показывает агрессивность покупателей/продавцов
- **Колонки:**
  - `taker_buy_volume`
  - `taker_sell_volume`
  - `taker_buy_sell_ratio`

#### 4. Liquidations
- **Источник:** Bybit WebSocket или сторонние API
- **Описание:** Объём ликвидаций long/short позиций
- **Польза:** Каскадные ликвидации вызывают резкие движения
- **Колонки:**
  - `liquidations_long_usd`
  - `liquidations_short_usd`
  - `liquidations_ratio`

---

### Классические технические индикаторы (Средний приоритет)

#### 5. CCI (Commodity Channel Index)
- **Формула:** `CCI = (Typical Price - SMA(TP)) / (0.015 × Mean Deviation)`
- **Typical Price:** `(High + Low + Close) / 3`
- **Периоды:** 14, 20
- **Сигналы:**
  - CCI > 100: Перекупленность
  - CCI < -100: Перепроданность
- **Колонки:** `cci_14`, `cci_20`

#### 6. ROC (Rate of Change)
- **Формула:** `ROC = (Close - Close_n) / Close_n × 100`
- **Периоды:** 9, 14, 25
- **Описание:** Простой индикатор моментума
- **Колонки:** `roc_9`, `roc_14`, `roc_25`

#### 7. CMF (Chaikin Money Flow)
- **Формула:** `CMF = Σ(MFV × Volume) / Σ(Volume)` за N периодов
- **MFV (Money Flow Multiplier):** `((Close - Low) - (High - Close)) / (High - Low)`
- **Периоды:** 20, 21
- **Сигналы:**
  - CMF > 0: Давление покупки
  - CMF < 0: Давление продажи
- **Колонки:** `cmf_20`, `cmf_21`

#### 8. Aroon
- **Формула:**
  - `Aroon Up = ((N - Days since N-day High) / N) × 100`
  - `Aroon Down = ((N - Days since N-day Low) / N) × 100`
  - `Aroon Oscillator = Aroon Up - Aroon Down`
- **Периоды:** 14, 25
- **Сигналы:**
  - Aroon Up > 70 + Aroon Down < 30: Сильный восходящий тренд
  - Пересечение линий: Возможная смена тренда
- **Колонки:** `aroon_14_up`, `aroon_14_down`, `aroon_14_oscillator`

#### 9. Parabolic SAR
- **Формула:** Trailing stop с ускорением
- **Параметры:** AF start=0.02, AF max=0.2
- **Сигналы:**
  - SAR ниже цены: Восходящий тренд
  - SAR выше цены: Нисходящий тренд
  - Переключение SAR: Сигнал разворота
- **Колонки:** `psar`, `psar_trend` (1=up, -1=down)

#### 10. Keltner Channels
- **Формула:**
  - Middle = EMA(Close, N)
  - Upper = EMA + ATR × multiplier
  - Lower = EMA - ATR × multiplier
- **Параметры:** EMA 20, ATR 10, multiplier 2.0
- **Отличие от Bollinger:** Использует ATR вместо стандартного отклонения
- **Колонки:** `keltner_20_upper`, `keltner_20_middle`, `keltner_20_lower`

#### 11. Donchian Channels
- **Формула:**
  - Upper = Max(High, N)
  - Lower = Min(Low, N)
  - Middle = (Upper + Lower) / 2
- **Периоды:** 20, 55
- **Сигналы:** Пробой канала = начало тренда
- **Колонки:** `donchian_20_upper`, `donchian_20_lower`, `donchian_20_middle`

---

### Производные фичи для ML (Высокий приоритет)

#### 12. Price Change Percentage
- **Описание:** Изменение цены за разные периоды
- **Периоды:** 5m, 15m, 1h, 4h, 24h, 7d
- **Колонки:** `price_change_5m`, `price_change_15m`, `price_change_1h`, `price_change_4h`, `price_change_24h`, `price_change_7d`

#### 13. Returns Volatility
- **Описание:** Стандартное отклонение доходности
- **Периоды:** 24h, 7d
- **Колонки:** `returns_volatility_24h`, `returns_volatility_7d`

#### 14. Volume Ratio
- **Формула:** `Volume / VMA(Volume, N)`
- **Описание:** Показывает аномальный объём
- **Колонки:** `volume_ratio_20`, `volume_ratio_50`

#### 15. ATR Ratio (Normalized ATR)
- **Формула:** `ATR / Close × 100`
- **Описание:** ATR в процентах от цены для сравнения между активами
- **Колонки:** `atr_ratio_14`

#### 16. Bollinger Band Position
- **Формула:** `(Close - BB_Lower) / (BB_Upper - BB_Lower)`
- **Диапазон:** 0-1 (может выходить за пределы)
- **Колонки:** `bb_position_20`

#### 17. RSI Divergence
- **Описание:** Расхождение между ценой и RSI
- **Типы:**
  - Бычья дивергенция: Цена делает новый минимум, RSI - нет
  - Медвежья дивергенция: Цена делает новый максимум, RSI - нет
- **Колонки:** `rsi_divergence` (-1, 0, 1)

#### 18. Multi-Timeframe Trend
- **Описание:** Согласованность тренда на разных таймфреймах
- **Логика:** Если тренд совпадает на 15m, 1h, 4h - сильный сигнал
- **Колонки:** `mtf_trend_score` (-3 to +3)

---

### Альтернативные данные (Низкий приоритет)

#### 19. On-Chain Metrics (требует платный API)
- **Источники:** Glassnode, CryptoQuant, Santiment, **CoinGlass ($29/мес)**
- **Данные:**
  - Exchange Inflow/Outflow
  - Active Addresses
  - NVT Ratio
  - MVRV Ratio
  - Miner Revenue
  - NUPL (Net Unrealized P/L)
  - SOPR (Spent Output Profit Ratio)
  - Pi Cycle Top Indicator
  - Puell Multiple
  - Stock-to-Flow

> **См. также:** [CoinGlass API Analysis](./coinglass_api_analysis.md) - полный анализ API и доступных данных

#### 20. Social Sentiment
- **Источники:** LunarCrush, Santiment
- **Данные:**
  - Social Volume
  - Weighted Sentiment
  - Social Dominance

---

## Приоритеты реализации

### Фаза 1 (Ближайшее время)
1. ✅ Open Interest - Bybit API доступен
2. ✅ Funding Rate - Bybit API доступен

### Фаза 2 (После Фазы 1)
3. Price Change % - простая реализация
4. Volume Ratio - простая реализация
5. ATR Ratio - простая реализация
6. CCI - классическая формула

### Фаза 3 (По необходимости)
7. Taker Buy/Sell Volume
8. ROC
9. CMF
10. Aroon
11. Остальные индикаторы

---

## Заметки

- Все новые индикаторы должны иметь загрузчик по шаблону существующих
- Необходимо добавлять в `indicators_config.yaml`
- Не забывать про поддержку `--force-reload`
- Документировать в `INDICATORS_REFERENCE.md`

---

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

**Требует платный план:**
- Исторические данные по market cap (Standard $299/мес)
- Исторические OHLCV данные (Hobbyist $29/мес)

**Недоступно через API (только на сайте):**
- CMC20, CMC100 индексы
- Pi Cycle, Puell Multiple, Rainbow Chart
- Altcoin Season Index

---

*Последнее обновление: 2026-01-29*
