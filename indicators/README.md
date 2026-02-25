# Indicators Module

Модуль расчёта и хранения технических индикаторов в PostgreSQL. 26 загрузчиков, 261 колонка, 5 таймфреймов (1m, 15m, 1h, 4h, 1d).

## Быстрый старт

```bash
# Запуск всех загрузчиков последовательно (оркестратор)
python3 start_all_loaders.py

# С проверкой и заполнением NULL в середине данных
python3 start_all_loaders.py --check-nulls

# Отдельный загрузчик
python3 sma_loader.py --timeframe 1h --symbol BTCUSDT
```

Конфигурация: `indicators_config.yaml` (символы, таймфреймы, периоды, включение/выключение загрузчиков).

## Загрузчики

### Технические индикаторы (23 загрузчика)

| Загрузчик | Индикатор | Колонок | Категория |
|-----------|-----------|---------|-----------|
| `sma_loader.py` | Simple Moving Average | 5 | Тренд |
| `ema_loader.py` | Exponential Moving Average | 7 | Тренд |
| `macd_loader.py` | MACD (8 конфигураций) | 24 | Тренд |
| `adx_loader.py` | Average Directional Index (8 периодов) | 24 | Тренд |
| `ichimoku_loader.py` | Ichimoku Cloud (2 конфигурации) | 16 | Тренд |
| `rsi_loader.py` | Relative Strength Index | 5 | Моментум |
| `stochastic_williams_loader.py` | Stochastic (8 конфиг.) + Williams %R (5 периодов) | 21 | Моментум |
| `mfi_loader.py` | Money Flow Index | 5 | Моментум |
| `atr_loader.py` | Average True Range + NATR | 12 | Волатильность |
| `bollinger_bands_loader.py` | Bollinger Bands (13 конфигураций) | 78 | Волатильность |
| `hv_loader.py` | Historical Volatility | 8 | Волатильность |
| `supertrend_loader.py` | SuperTrend (5 конфигураций) | 21 | Волатильность |
| `vma_loader.py` | Volume Moving Average | 5 | Объём |
| `obv_loader.py` | On-Balance Volume | 1 | Объём |
| `vwap_loader.py` | VWAP (1 daily + 15 rolling) | 16 | Объём |
| `long_short_ratio_loader.py` | Long/Short Ratio (Bybit API) | 3 | Сентимент |
| `open_interest_loader.py` | Open Interest (Bybit API) | 1 | Сентимент |
| `funding_fee_loader.py` | Funding Rate (Bybit API) | 2 | Сентимент |
| `premium_index_loader.py` | Premium Index (Bybit API) | 1 | Сентимент |
| `fear_and_greed_loader_alternative.py` | Fear & Greed Index (Alternative.me) | 2 | Сентимент |
| `fear_and_greed_coinmarketcap_loader.py` | Market Metrics (CoinMarketCap) | 2 | Сентимент |
| `orderbook_bybit_loader.py` | Orderbook Bybit (отдельная таблица) | 58 | Микроструктура |
| `orderbook_binance_loader.py` | Orderbook Binance (отдельная таблица) | 44 | Микроструктура |

### Опционы (3 загрузчика, отдельные таблицы)

| Загрузчик | Данные | Таблица | Колонок |
|-----------|--------|---------|---------|
| `options_dvol_loader.py` | DVOL (Deribit Volatility Index) | `options_deribit_dvol_1h`, `_1m` | 6 |
| `options_dvol_indicators_loader.py` | Индикаторы на DVOL (8 групп) | `options_deribit_dvol_indicators_1h` | 22 |
| `options_aggregated_loader.py` | Агрегированные метрики опционов (7 групп) | `options_deribit_aggregated_15m` | 24 |

## Таблицы в БД

### Индикаторы (261 колонка, одна схема на все таймфреймы)

| Таблица | Строк | Размер |
|---------|-------|--------|
| `indicators_bybit_futures_1m` | 25.6M | 114 GB |
| `indicators_bybit_futures_15m` | 1.7M | 7.7 GB |
| `indicators_bybit_futures_1h` | 437K | 2 GB |
| `indicators_bybit_futures_4h` | — | в процессе загрузки |
| `indicators_bybit_futures_1d` | — | в процессе загрузки |

### Отдельные таблицы

| Таблица | Назначение |
|---------|------------|
| `orderbook_bybit_futures_1m` | Ордербук Bybit (58 колонок + 2 JSONB) |
| `orderbook_binance_futures_1m` | Ордербук Binance (44 колонки) |
| `options_deribit_dvol_1h` / `_1m` | DVOL OHLC свечи |
| `options_deribit_dvol_indicators_1h` | Индикаторы на DVOL |
| `options_deribit_aggregated_15m` | Агрегированные метрики опционов |

## Документация

### Индикаторы

| Файл | Описание |
|------|----------|
| [INDICATORS_REFERENCE.md](INDICATORS_REFERENCE.md) | Полный справочник: формулы, SQL примеры, торговые стратегии, архитектура загрузчиков |
| [INDICATORS_QUICK_REFERENCE.md](INDICATORS_QUICK_REFERENCE.md) | Краткий справочник: 1 абзац на индикатор |
| [DB_PERFORMANCE_FAQ.md](DB_PERFORMANCE_FAQ.md) | FAQ по производительности PostgreSQL |

### Опционы и деривативы

| Файл | Описание |
|------|----------|
| [docs/DVOL_REFERENCE.md](../docs/DVOL_REFERENCE.md) | DVOL таблицы, 22 индикатора, 24 агрегированных метрики, SQL примеры |
| [docs/OPTIONS_RESEARCH.md](../docs/OPTIONS_RESEARCH.md) | Исследование: Black-Scholes, Greeks, Deribit vs Bybit API |

### Ордербук (микроструктура рынка)

| Файл | Описание |
|------|----------|
| [docs/ORDERBOOK_REFERENCE.md](../docs/ORDERBOOK_REFERENCE.md) | Bybit ордербук: 58 колонок, 12 групп метрик |
| [docs/ORDERBOOK_BINANCE_REFERENCE.md](../docs/ORDERBOOK_BINANCE_REFERENCE.md) | Binance ордербук: 44 колонки, bookDepth + bookTicker |
| [docs/ORDERBOOK_PLANNING.md](../docs/ORDERBOOK_PLANNING.md) | Архитектура сбора данных ордербука |

### Планирование

| Файл | Описание |
|------|----------|
| [docs/INDICATORS_PLANNING.md](../docs/INDICATORS_PLANNING.md) | Реализованные vs планируемые индикаторы, анализ API |
| [docs/COINGLASS_API_REFERENCE.md](../docs/COINGLASS_API_REFERENCE.md) | Исследование Coinglass API |
