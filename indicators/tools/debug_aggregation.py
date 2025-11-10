#!/usr/bin/env python3
"""
Проверка агрегации 1m свечей в 1h
"""

import os
import sys
import psycopg2
from datetime import datetime, timezone
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

def check_aggregation():
    conn = get_connection()
    cur = conn.cursor()
    
    symbol = 'BTCUSDT'
    
    print("=" * 80)
    print("ПРОВЕРКА АГРЕГАЦИИ для 15:00")
    print("=" * 80)
    
    # Проверяем какая формула агрегации используется
    print("\n1. SQL ФОРМУЛА АГРЕГАЦИИ (как в LOADER):")
    print("-" * 80)
    query = """
        WITH candle_data AS (
            SELECT
                timestamp,
                date_trunc('hour', timestamp) +
                INTERVAL '60 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 60) as period_start,
                close
            FROM candles_bybit_futures_1m
            WHERE symbol = %s 
              AND timestamp >= '2025-11-03 14:00:00+00'
              AND timestamp < '2025-11-03 16:00:00+00'
        ),
        last_in_period AS (
            SELECT DISTINCT ON (period_start)
                period_start as timestamp,
                close as price,
                (SELECT original_timestamp FROM (
                    SELECT timestamp as original_timestamp FROM candle_data c2 
                    WHERE c2.period_start = candle_data.period_start 
                    ORDER BY timestamp DESC LIMIT 1
                ) x) as source_timestamp
            FROM candle_data
            ORDER BY period_start, timestamp DESC
        )
        SELECT timestamp, price, source_timestamp
        FROM last_in_period
        ORDER BY timestamp
    """
    
    cur.execute(query, (symbol,))
    results = cur.fetchall()
    
    print(f"Количество агрегированных периодов: {len(results)}")
    for row in results:
        period_ts, price, source_ts = row
        print(f"  Период: {period_ts}, Price: {price}, Источник (last 1m candle): {source_ts}")
    
    # Теперь смотрим на детали для периода 15:00
    print("\n2. ДЕТАЛИ ДЛЯ ПЕРИОДА 15:00:")
    print("-" * 80)
    
    query_detail = """
        SELECT
            timestamp,
            date_trunc('hour', timestamp) +
            INTERVAL '60 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 60) as period_start,
            close,
            EXTRACT(MINUTE FROM timestamp) as minute_part,
            (EXTRACT(MINUTE FROM timestamp)::integer / 60) as division_result
        FROM candles_bybit_futures_1m
        WHERE symbol = %s 
          AND timestamp >= '2025-11-03 14:00:00+00'
          AND timestamp < '2025-11-03 16:00:00+00'
        ORDER BY timestamp
    """
    
    cur.execute(query_detail, (symbol,))
    all_candles = cur.fetchall()
    
    print(f"Всего 1m свечей: {len(all_candles)}")
    print("\nПервые 5 свечей:")
    for i in range(min(5, len(all_candles))):
        ts, period, close, minute, div = all_candles[i]
        print(f"  {ts} → период: {period}, close: {close}, minute: {minute}, minute/60: {div}")
    
    print("\nСвечи 55-60 (конец 14:00 часа):")
    for i in range(55, min(60, len(all_candles))):
        ts, period, close, minute, div = all_candles[i]
        print(f"  {ts} → период: {period}, close: {close}, minute: {minute}, minute/60: {div}")
    
    print("\nСвечи 60-65 (начало 15:00 часа):")
    for i in range(60, min(65, len(all_candles))):
        ts, period, close, minute, div = all_candles[i]
        print(f"  {ts} → период: {period}, close: {close}, minute: {minute}, minute/60: {div}")
    
    # Проверка: какие свечи попадают в период 15:00?
    print("\n3. КАКИЕ 1M СВЕЧИ ОТНОСЯТСЯ К ПЕРИОДУ 15:00?")
    print("-" * 80)
    
    query_period = """
        WITH candle_data AS (
            SELECT
                timestamp,
                date_trunc('hour', timestamp) +
                INTERVAL '60 minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / 60) as period_start,
                close
            FROM candles_bybit_futures_1m
            WHERE symbol = %s 
              AND timestamp >= '2025-11-03 14:00:00+00'
              AND timestamp < '2025-11-03 16:00:00+00'
        )
        SELECT timestamp, close, period_start
        FROM candle_data
        WHERE period_start = '2025-11-03 15:00:00+00'
        ORDER BY timestamp
    """
    
    cur.execute(query_period, (symbol,))
    period_15_candles = cur.fetchall()
    
    print(f"Количество 1m свечей для периода 15:00: {len(period_15_candles)}")
    if len(period_15_candles) > 0:
        print(f"Первая свеча: {period_15_candles[0]}")
        print(f"Последняя свеча: {period_15_candles[-1]}")
        print(f"\nВСЕ свечи периода 15:00:")
        for ts, close, period in period_15_candles:
            print(f"  {ts}: close = {close}")
    else:
        print("⚠️  НЕТ СВЕЧЕЙ ДЛЯ ПЕРИОДА 15:00!")
    
    print("\n4. ПЕРИОД 14:00 (для сравнения):")
    print("-" * 80)
    
    cur.execute(query_period.replace("15:00:00", "14:00:00"), (symbol,))
    period_14_candles = cur.fetchall()
    
    print(f"Количество 1m свечей для периода 14:00: {len(period_14_candles)}")
    if len(period_14_candles) > 0:
        print(f"Первая свеча: {period_14_candles[0]}")
        print(f"Последняя свеча: {period_14_candles[-1]}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_aggregation()
