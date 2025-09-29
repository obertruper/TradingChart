# Indicators Data Checker with Excel Export

## Описание

`check_indicators_in_db_save_excel.py` - скрипт для проверки наличия и полноты данных технических индикаторов в базе данных PostgreSQL. Создает детальный Excel отчет с анализом по каждому индикатору, таймфрейму и символу.

## Основные возможности

- ✅ Проверка данных индикаторов SMA, EMA, RSI
- ✅ Анализ трех таймфреймов: 1m, 15m, 1h
- ✅ Два режима работы: последние 30 дней или весь период данных
- ✅ Экспорт в Excel с отдельными листами для каждой комбинации
- ✅ Цветовое кодирование статусов в Excel
- ✅ Быстрая работа благодаря оптимизированным запросам

## Требования

### Python зависимости
```bash
pip install psycopg2-binary
pip install pandas
pip install python-dotenv
pip install openpyxl
```

### База данных
- PostgreSQL с таблицами индикаторов:
  - `indicators_bybit_futures_1m`
  - `indicators_bybit_futures_15m`
  - `indicators_bybit_futures_1h`

### Файл окружения (.env)
Создайте файл `.env` со следующими переменными:
```env
DB_HOST=82.25.115.144
DB_PORT=5432
DB_NAME=trading_data
DB_READER_USER=trading_reader
DB_READER_PASSWORD=ваш_пароль
```

## Установка и настройка

1. Клонируйте репозиторий или скопируйте файлы
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Создайте файл `.env` с параметрами подключения к БД
4. Убедитесь, что папка `results/` существует (создается автоматически)

## Использование

### Базовый запуск (последние 30 дней)
```bash
python3 check_indicators_in_db_save_excel.py
```

### Анализ всех данных в БД
Измените в файле параметр:
```python
FULL_DAYS_DATA = True  # Анализировать все данные
```

### Изменение периода анализа
Для анализа другого периода (например, 60 дней):
```python
START_DATE = END_DATE - timedelta(days=60)
```

### Добавление новых символов
Измените список символов:
```python
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
```

## Конфигурация

### Основные параметры

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `SYMBOLS` | Список символов для проверки | `['BTCUSDT']` |
| `TIMEFRAMES` | Таймфреймы для анализа | `['1m', '15m', '1h']` |
| `FULL_DAYS_DATA` | Анализировать весь период | `False` |
| `START_DATE` | Начальная дата (если FULL_DAYS_DATA=False) | 30 дней назад |

### Проверяемые индикаторы

**SMA (Simple Moving Average):**
- sma_10, sma_30, sma_50, sma_100, sma_200

**EMA (Exponential Moving Average):**
- ema_9, ema_12, ema_21, ema_26, ema_50, ema_100, ema_200

**RSI (Relative Strength Index):**
- rsi_7, rsi_9, rsi_14, rsi_21, rsi_25

## Структура выходного Excel файла

### Лист Summary
Сводная статистика по всем индикаторам:
- Общее количество дней
- Количество полных дней (COMPLETE)
- Количество частичных дней (PARTIAL)
- Дни без данных (NO_DATA)
- Средний процент полноты

### Индивидуальные листы
Формат названия: `SYMBOL_TIMEFRAME_INDICATOR`

Примеры:
- `BTCUSDT_1m_sma`
- `BTCUSDT_15m_ema`
- `BTCUSDT_1h_rsi`

Каждый лист содержит:
- Дата
- День недели
- Статус (COMPLETE/PARTIAL/NO_DATA/INSUFFICIENT)
- Процент полноты
- Количество свечей
- Детали по каждому периоду индикатора

## Статусы данных

| Статус | Описание | Цвет в Excel |
|--------|----------|--------------|
| `COMPLETE` | ≥95% данных присутствует | Зеленый |
| `PARTIAL` | 50-94% данных | Желтый |
| `INSUFFICIENT` | <50% данных | Красный |
| `NO_DATA` | Нет данных за день | Красный |
| `NO_INDICATORS` | Есть свечи, но нет индикаторов | Красный |

## Производительность

- **30 дней:** ~1 секунда
- **1 год:** ~5-10 секунд
- **5+ лет (2000+ дней):** ~30 секунд

## Примеры вывода

### Консольный вывод
```
================================================================================
Optimized Indicator Data Checker with Excel Export
Date range: 2025-08-30 to 2025-09-29
Symbols: BTCUSDT
Timeframes: 1m, 15m, 1h
Indicators: SMA, EMA, RSI
================================================================================
Processing symbol: BTCUSDT
  Fetching data for 1m (1/3)
    BTCUSDT_1m_sma: 20/31 complete days
    BTCUSDT_1m_ema: 20/31 complete days
    BTCUSDT_1m_rsi: 20/31 complete days
...
✅ Report saved successfully: results/indicators_analysis_20250929_103822.xlsx
```

### Результирующий файл
`results/indicators_analysis_YYYYMMDD_HHMMSS.xlsx`

## Возможные проблемы и решения

### Ошибка подключения к БД
**Проблема:** `fe_sendauth: no password supplied`
**Решение:** Проверьте файл `.env` и убедитесь, что пароль указан правильно

### Медленная работа при FULL_DAYS_DATA=True
**Проблема:** Скрипт работает долго при анализе всех данных
**Решение:** Это нормально для больших объемов данных (5+ лет). Используйте режим последних 30 дней для быстрой проверки

### Модуль openpyxl не найден
**Проблема:** `ModuleNotFoundError: No module named 'openpyxl'`
**Решение:** Установите модуль: `pip install openpyxl`

## Структура проекта

```
check_indicators_in_db/
├── check_indicators_in_db_save_excel.py  # Основной скрипт
├── .env                                   # Переменные окружения (не коммитить!)
├── README.md                              # Эта документация
├── requirements.txt                       # Python зависимости
└── results/                               # Папка для Excel отчетов
    └── indicators_analysis_*.xlsx        # Результаты анализа
```

## Автор и поддержка

Скрипт разработан для анализа полноты данных индикаторов в торговой системе.
При возникновении вопросов обращайтесь к документации или исходному коду.

## Лицензия

Внутренний проект для анализа торговых данных.