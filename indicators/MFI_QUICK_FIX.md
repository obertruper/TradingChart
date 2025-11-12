# MFI Loader - Быстрая справка

## 🚨 Validation показывает ошибки - что делать?

### Случай 1: Ошибки на последних 1-2 timestamp (99.99% accuracy)

**Это НОРМАЛЬНО!** Data race condition - loader обогнал прибытие данных.

**Решение:** Просто запустите снова
```bash
python3 mfi_loader.py --symbol ETHUSDT --timeframe 1h --batch-days 1
python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h --days 2
```

Должно стать 100% accuracy.

---

### Случай 2: Ошибки НЕ исправляются после 2-х запусков

**Проблема:** Неполные 1m данные в БД

**Диагностика:**
```bash
python3 << 'EOF'
import psycopg2, yaml
with open('indicators_config.yaml') as f: cfg = yaml.safe_load(f)
conn = psycopg2.connect(**cfg['database'])
cur = conn.cursor()

# Замените timestamp на проблемный
cur.execute("""
    SELECT COUNT(*) FROM candles_bybit_futures_1m
    WHERE symbol = 'ETHUSDT'
        AND timestamp >= '2025-11-12 09:00:00'
        AND timestamp < '2025-11-12 10:00:00'
""")
print(f"Candles: {cur.fetchone()[0]}/60")
EOF
```

Если < 60 свечей → проблема в monitor, не в loader.

---

### Случай 3: Множественные ошибки по всей истории

**Решение:** Полный пересчет с --force-reload
```bash
python3 mfi_loader.py --symbol ETHUSDT --timeframe 1h --force-reload --batch-days 3
```

⚠️ Займет 30-40 минут для полной истории!

---

## 🔧 Быстрое исправление конкретного timestamp

Если validation показывает ошибку на конкретном timestamp:

```bash
# 1. Удалить неправильные данные
python3 << 'EOF'
import psycopg2, yaml
with open('indicators_config.yaml') as f: cfg = yaml.safe_load(f)
conn = psycopg2.connect(**cfg['database'])
cur = conn.cursor()

cur.execute("""
    UPDATE indicators_bybit_futures_1h
    SET mfi_7 = NULL, mfi_10 = NULL, mfi_14 = NULL,
        mfi_20 = NULL, mfi_25 = NULL
    WHERE symbol = 'ETHUSDT'
        AND timestamp = '2025-11-12 09:00:00+00'
""")
conn.commit()
print(f"Deleted: {cur.rowcount} rows")
EOF

# 2. Перезапустить loader
python3 mfi_loader.py --symbol ETHUSDT --timeframe 1h --batch-days 1

# 3. Проверить
python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h --days 1
```

---

## 📊 Проверка статуса MFI данных

### Быстрая проверка последних данных:
```bash
python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h --days 2
```

### Полная проверка всей истории:
```bash
python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h
```

### Проверка в SQL:
```sql
-- Последние 10 записей
SELECT timestamp, mfi_7, mfi_10, mfi_14, mfi_20, mfi_25
FROM indicators_bybit_futures_1h
WHERE symbol = 'ETHUSDT'
ORDER BY timestamp DESC
LIMIT 10;

-- Пропуски в данных
SELECT
    timestamp,
    CASE
        WHEN mfi_7 IS NULL THEN '❌ MISSING'
        ELSE '✅ OK'
    END as status
FROM indicators_bybit_futures_1h
WHERE symbol = 'ETHUSDT'
    AND timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY timestamp DESC;
```

---

## ⏰ Настройка автоматических запусков (Cron)

**Рекомендуемая конфигурация:**

```bash
crontab -e
```

Добавить:
```bash
# MFI 1h - каждый час на 5-й минуте
5 * * * * cd /path/to/indicators && python3 mfi_loader.py --timeframe 1h >> logs/mfi_cron_1h.log 2>&1

# MFI 15m - каждые 15 минут
*/15 * * * * cd /path/to/indicators && python3 mfi_loader.py --timeframe 15m >> logs/mfi_cron_15m.log 2>&1

# Валидация - раз в день в 06:00
0 6 * * * cd /path/to/indicators && python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h --days 7 >> logs/mfi_validation.log 2>&1
```

**Проверка работы cron:**
```bash
tail -f logs/mfi_cron_1h.log
```

---

## 🐛 Troubleshooting

### TypeError при расчете MFI
**Симптом:** `TypeError: unsupported operand type(s) for +: 'Decimal' and 'Decimal'`

**Причина:** Старая версия без float64 conversion

**Решение:** Обновить mfi_loader.py до v2.0

---

### В БД попадают данные для текущего часа (незавершенные свечи)
**Симптом:** Validation показывает ошибки на текущем часе

**Причина:** Старая версия без incomplete candle exclusion

**Решение:** Обновить mfi_loader.py до v2.0

---

### Loader не находит новые данные (пропускает часы)
**Симптом:** `✅ ETHUSDT - данные MFI актуальны` но ошибки остаются

**Причина:** MFI данные есть, но некорректные (старые значения)

**Решение:**
```bash
# Удалить проблемные timestamp вручную (см. "Быстрое исправление" выше)
# Или запустить с --force-reload
```

---

### Monitor не загружает 1m свечи
**Симптом:** Постоянно < 60 свечей в часе

**Причина:** Monitor не работает или проблема с API

**Решение:**
```bash
# Проверить статус monitor
ps aux | grep monitor

# Перезапустить monitor
cd data_collectors/bybit/futures
./monitor_manager.sh restart

# Проверить логи monitor
tail -f monitor.log
```

---

## 📈 Ожидаемые результаты

### После первого запуска:
- Validation: 99.9% - 100.0% accuracy
- Возможны 1-2 ошибки на последних timestamp (data race)

### После второго запуска (через час):
- Validation: 100.0% accuracy
- 0 ошибок

### Если accuracy < 99%:
1. Проверить что monitor загружает 1m свечи
2. Проверить наличие пропусков в candles_bybit_futures_1m
3. Запустить с --force-reload если нужно

---

## 🎯 One-liner команды

```bash
# Полная проверка: обновить данные + валидация
python3 mfi_loader.py --symbol ETHUSDT --timeframe 1h --batch-days 1 && python3 tests/check_full_data/check_mfi_data.py --symbol ETHUSDT --timeframe 1h --days 2

# Исправить все ошибки за последние 7 дней
for i in {0..6}; do
  date=$(date -u -v-${i}d +%Y-%m-%d)
  python3 mfi_loader.py --symbol ETHUSDT --timeframe 1h --start-date "$date" --batch-days 1
done

# Быстрая проверка логов cron
tail -20 logs/mfi_cron_1h.log | grep -E "(завершен|ERROR|WARNING)"
```

---

## 📚 Дополнительная документация

- **Полная инструкция:** `DEPLOY_MFI_VPS.md`
- **История изменений:** `MFI_LOADER_CHANGELOG.md`
- **Референс по индикаторам:** `INDICATORS_REFERENCE.md`
