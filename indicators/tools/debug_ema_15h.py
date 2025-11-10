#!/usr/bin/env python3
"""
Детальный анализ EMA расчетов для timestamp 2025-11-03 15:00
Сравнивает результаты loader vs validator
"""

import os
import sys
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(env_path)

# Database connection
def get_connection():
    return psycopg2.connect(
        host="82.25.115.144",
        port=5432,
        database="trading_data",
        user="trading_reader",
        password=os.getenv("DB_READER_PASSWORD")
    )

def analyze_timestamp():
    """Анализ конкретного timestamp 2025-11-03 15:00"""
    
    conn = get_connection()
    cur = conn.cursor()
    
    target_timestamp = '2025-11-03 15:00:00+00'
    symbol = 'BTCUSDT'
    period = 9
    lookback_hours = period * 3  # 27 hours
    
    print("=" * 80)
    print(f"АНАЛИЗ EMA_{period} для {symbol} на {target_timestamp}")
    print("=" * 80)
    
    # 1. Проверяем сохраненное значение EMA в БД
    print("\n1. СОХРАНЕННОЕ ЗНАЧЕНИЕ EMA В БД:")
    print("-" * 80)
    cur.execute("""
        SELECT timestamp, ema_9
        FROM indicators_bybit_futures_1h
        WHERE symbol = %s AND timestamp = %s
    """, (symbol, target_timestamp))
    
    stored_data = cur.fetchone()
    if stored_data:
        stored_timestamp, stored_ema = stored_data
        print(f"Timestamp: {stored_timestamp}")
        print(f"EMA_9 (сохранено): {stored_ema}")
    else:
        print("ОШИБКА: Нет данных в БД!")
        return
    
    # 2. Получаем 1m свечи для агрегации этого часа (14:00-14:59)
    print("\n2. 1M СВЕЧИ ДЛЯ АГРЕГАЦИИ 15:00 ЧАСА:")
    print("-" * 80)
    print("(Час начинается в 14:00 и заканчивается в 14:59)")
    cur.execute("""
        SELECT timestamp, close
        FROM candles_bybit_futures_1m
        WHERE symbol = %s 
          AND timestamp >= '2025-11-03 14:00:00+00'
          AND timestamp < '2025-11-03 15:00:00+00'
        ORDER BY timestamp
    """, (symbol,))
    
    candles_1m = cur.fetchall()
    print(f"Количество 1m свечей: {len(candles_1m)}")
    if len(candles_1m) > 0:
        print(f"Первая свеча: {candles_1m[0]}")
        print(f"Последняя свеча: {candles_1m[-1]}")
        last_close = candles_1m[-1][1]
        print(f"Close последней 1m свечи (14:59): {last_close}")
        print(f"Это значение должно использоваться как close для 1h свечи 15:00")
    
    # 3. Проверяем lookback период - нужно 27 часов назад от 15:00
    print(f"\n3. LOOKBACK ПЕРИОД (нужно {lookback_hours} часов):")
    print("-" * 80)
    
    # Используем stored_timestamp который уже datetime объект
    target_dt = stored_timestamp
    lookback_start = target_dt - timedelta(hours=lookback_hours)
    
    print(f"Target timestamp: {target_dt}")
    print(f"Lookback start: {lookback_start}")
    print(f"Диапазон: {lookback_start} → {target_dt} ({lookback_hours} часов)")
    
    # Метод LOADER: использует adjusted_overlap_start с агрегацией 1m
    print("\n3a. МЕТОД LOADER (агрегация 1m свечей):")
    print("-" * 80)
    
    query_loader = """
        WITH candle_data AS (
            SELECT
                date_trunc('hour', timestamp) +
                INTERVAL '60 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 60) as period_start,
                close,
                symbol,
                timestamp as original_timestamp
            FROM candles_bybit_futures_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
        ),
        last_in_period AS (
            SELECT DISTINCT ON (period_start)
                period_start as timestamp,
                close as price
            FROM candle_data
            ORDER BY period_start, original_timestamp DESC
        )
        SELECT timestamp, price
        FROM last_in_period
        ORDER BY timestamp
    """
    
    cur.execute(query_loader, (symbol, lookback_start, target_dt))
    loader_data = cur.fetchall()
    
    print(f"Количество часовых периодов (LOADER): {len(loader_data)}")
    if len(loader_data) > 0:
        print(f"Первый период: {loader_data[0]}")
        print(f"Последний период: {loader_data[-1]}")
        
        # Рассчитываем EMA используя метод LOADER
        df_loader = pd.DataFrame(loader_data, columns=['timestamp', 'price'])
        df_loader['price'] = df_loader['price'].astype(float)
        df_loader['ema_9'] = df_loader['price'].ewm(span=period, adjust=False, min_periods=period).mean()
        
        # Показываем все рассчитанные EMA
        print(f"\nВсе рассчитанные EMA_9 (LOADER метод) - последние 10:")
        print(df_loader[['timestamp', 'price', 'ema_9']].tail(10).to_string())
        
        # Находим значение для target timestamp
        target_row = df_loader[df_loader['timestamp'] == target_dt]
        if not target_row.empty:
            calculated_ema_loader = target_row['ema_9'].iloc[0]
            calculated_price_loader = target_row['price'].iloc[0]
            print(f"\n✓ РЕЗУЛЬТАТ LOADER для {target_dt}:")
            print(f"  Price (close): {calculated_price_loader:.8f}")
            print(f"  EMA_9 (рассчитано): {calculated_ema_loader:.8f}")
            print(f"  EMA_9 (сохранено в БД): {stored_ema}")
            if stored_ema:
                diff = abs(calculated_ema_loader - float(stored_ema))
                print(f"  Разница: {diff:.10f}")
                if diff < 0.01:
                    print(f"  ✓ СОВПАДЕНИЕ (разница < 0.01)")
                else:
                    print(f"  ✗ РАСХОЖДЕНИЕ (разница >= 0.01)")
    
    # Метод VALIDATOR: прямая JOIN с indicators_table
    print("\n3b. МЕТОД VALIDATOR (агрегация + JOIN):")
    print("-" * 80)
    
    query_validator = """
        WITH aggregated_candles AS (
            SELECT
                date_trunc('hour', timestamp) +
                INTERVAL '60 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 60) as period_start,
                close,
                symbol,
                timestamp as original_timestamp
            FROM candles_bybit_futures_1m
            WHERE symbol = %s
                AND timestamp >= %s
                AND timestamp <= %s
        ),
        last_in_period AS (
            SELECT DISTINCT ON (period_start, symbol)
                period_start as timestamp,
                symbol,
                close
            FROM aggregated_candles
            ORDER BY period_start, symbol, original_timestamp DESC
        )
        SELECT
            c.timestamp,
            c.close,
            i.ema_9 as ema_stored
        FROM last_in_period c
        LEFT JOIN indicators_bybit_futures_1h i
            ON c.timestamp = i.timestamp AND c.symbol = i.symbol
        ORDER BY c.timestamp ASC
    """
    
    cur.execute(query_validator, (symbol, lookback_start, target_dt))
    validator_data = cur.fetchall()
    
    print(f"Количество часовых периодов (VALIDATOR): {len(validator_data)}")
    if len(validator_data) > 0:
        print(f"Первый период: {validator_data[0]}")
        print(f"Последний период: {validator_data[-1]}")
        
        # Рассчитываем EMA используя метод VALIDATOR
        df_validator = pd.DataFrame(validator_data, columns=['timestamp', 'close', 'ema_stored'])
        df_validator['close'] = df_validator['close'].astype(float)
        df_validator['ema_9_calc'] = df_validator['close'].ewm(span=period, adjust=False, min_periods=period).mean()
        
        # Показываем все рассчитанные EMA
        print(f"\nВсе рассчитанные EMA_9 (VALIDATOR метод) - последние 10:")
        print(df_validator[['timestamp', 'close', 'ema_9_calc', 'ema_stored']].tail(10).to_string())
        
        # Находим значение для target timestamp
        target_row = df_validator[df_validator['timestamp'] == target_dt]
        if not target_row.empty:
            calculated_ema_validator = target_row['ema_9_calc'].iloc[0]
            stored_ema_validator = target_row['ema_stored'].iloc[0]
            calculated_close_validator = target_row['close'].iloc[0]
            print(f"\n✓ РЕЗУЛЬТАТ VALIDATOR для {target_dt}:")
            print(f"  Close: {calculated_close_validator:.8f}")
            print(f"  EMA_9 (рассчитано): {calculated_ema_validator:.8f}")
            print(f"  EMA_9 (сохранено в БД): {stored_ema_validator}")
            if stored_ema_validator:
                diff = abs(calculated_ema_validator - float(stored_ema_validator))
                print(f"  Разница: {diff:.10f}")
                if diff < 0.01:
                    print(f"  ✓ СОВПАДЕНИЕ (разница < 0.01)")
                else:
                    print(f"  ✗ РАСХОЖДЕНИЕ (разница >= 0.01)")
    
    # 4. Сравниваем данные LOADER vs VALIDATOR
    print("\n4. СРАВНЕНИЕ ДАННЫХ LOADER vs VALIDATOR:")
    print("-" * 80)
    
    if len(loader_data) != len(validator_data):
        print(f"⚠️  ПРОБЛЕМА: Разное количество периодов!")
        print(f"   LOADER: {len(loader_data)} периодов")
        print(f"   VALIDATOR: {len(validator_data)} периодов")
    else:
        print(f"✓ Количество периодов совпадает: {len(loader_data)}")
    
    # Сравниваем первые 5 и последние 5 записей
    print("\nПервые 5 периодов:")
    print("LOADER vs VALIDATOR (timestamp, price/close):")
    for i in range(min(5, len(loader_data), len(validator_data))):
        loader_ts, loader_price = loader_data[i]
        validator_ts, validator_close, _ = validator_data[i]
        match = "✓" if loader_ts == validator_ts and abs(float(loader_price) - float(validator_close)) < 0.0001 else "✗"
        print(f"  {match} {loader_ts} | LOADER: {float(loader_price):.8f} | VALIDATOR: {float(validator_close):.8f}")
    
    print("\nПоследние 5 периодов:")
    for i in range(max(0, min(len(loader_data), len(validator_data)) - 5), min(len(loader_data), len(validator_data))):
        loader_ts, loader_price = loader_data[i]
        validator_ts, validator_close, _ = validator_data[i]
        match = "✓" if loader_ts == validator_ts and abs(float(loader_price) - float(validator_close)) < 0.0001 else "✗"
        print(f"  {match} {loader_ts} | LOADER: {float(loader_price):.8f} | VALIDATOR: {float(validator_close):.8f}")
    
    print("\n" + "=" * 80)
    print("ИТОГИ:")
    print("=" * 80)
    print(f"1. Сохранено в БД: {stored_ema}")
    if 'calculated_ema_loader' in locals():
        print(f"2. LOADER метод: {calculated_ema_loader:.8f}")
        if stored_ema:
            diff = abs(calculated_ema_loader - float(stored_ema))
            print(f"   Отклонение от БД: {diff:.10f}")
            print(f"   Статус: {'✓ СОВПАДЕНИЕ' if diff < 0.01 else '✗ РАСХОЖДЕНИЕ'}")
    if 'calculated_ema_validator' in locals():
        print(f"3. VALIDATOR метод: {calculated_ema_validator:.8f}")
        if stored_ema_validator:
            diff = abs(calculated_ema_validator - float(stored_ema_validator))
            print(f"   Отклонение от БД: {diff:.10f}")
            print(f"   Статус: {'✓ СОВПАДЕНИЕ' if diff < 0.01 else '✗ РАСХОЖДЕНИЕ'}")
    
    # КРИТИЧЕСКИЙ ВОПРОС
    if 'calculated_ema_loader' in locals() and 'calculated_ema_validator' in locals():
        diff_methods = abs(calculated_ema_loader - calculated_ema_validator)
        print(f"\n4. РАЗНИЦА между LOADER и VALIDATOR: {diff_methods:.10f}")
        if diff_methods < 0.00001:
            print("   ✓ Методы дают ОДИНАКОВЫЙ результат!")
        else:
            print("   ✗ Методы дают РАЗНЫЕ результаты!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    analyze_timestamp()
