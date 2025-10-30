#!/usr/bin/env python3
"""
Проверка статуса всех индикаторов в базе данных
"""

import psycopg2
import yaml
from datetime import datetime
from tabulate import tabulate
import os

def main():
    # Загружаем конфигурацию
    config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    db_config = config['database']
    
    # Подключаемся к БД
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
    )
    cur = conn.cursor()
    
    print("="*100)
    print("📊 СТАТУС ИНДИКАТОРОВ В БАЗЕ ДАННЫХ")
    print("="*100)
    
    # Получаем список таблиц
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'indicators_bybit_futures_%'
        ORDER BY table_name
    """)
    
    tables = [row[0] for row in cur.fetchall()]
    
    if not tables:
        print("\n⚠️ Таблицы индикаторов не найдены")
        return
    
    for table in tables:
        timeframe = table.replace('indicators_bybit_futures_', '')
        print(f"\n🕐 Таймфрейм: {timeframe}")
        print("-"*80)
        
        # Получаем колонки индикаторов
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND (column_name LIKE 'sma_%%' OR column_name LIKE 'ema_%%')
            ORDER BY column_name
        """, (table,))
        
        columns = [row[0] for row in cur.fetchall()]
        
        # Группируем по типам
        sma_cols = [c for c in columns if c.startswith('sma_')]
        ema_cols = [c for c in columns if c.startswith('ema_')]
        
        if sma_cols:
            print(f"  📊 SMA периоды: {', '.join([c.replace('sma_', '') for c in sma_cols])}")
        if ema_cols:
            print(f"  📈 EMA периоды: {', '.join([c.replace('ema_', '') for c in ema_cols])}")
        
        # Проверяем заполненность
        for symbol in ['BTCUSDT']:
            # Общая статистика
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    MIN(timestamp) as first_date,
                    MAX(timestamp) as last_date
                FROM {table}
                WHERE symbol = %s
            """, (symbol,))
            
            stats = cur.fetchone()
            if stats[0] > 0:
                print(f"\n  📊 Символ: {symbol}")
                print(f"    Записей: {stats[0]:,}")
                print(f"    Период: {stats[1]} - {stats[2]}")
                
                # Проверяем заполненность индикаторов
                if columns:
                    count_queries = [f"COUNT({col})" for col in columns]
                    cur.execute(f"""
                        SELECT {', '.join(count_queries)}
                        FROM {table}
                        WHERE symbol = %s
                    """, (symbol,))
                    
                    counts = cur.fetchone()
                    
                    print("\n    Заполненность индикаторов:")
                    
                    # SMA
                    if sma_cols:
                        print("    SMA:")
                        for i, col in enumerate(sma_cols):
                            percent = (counts[columns.index(col)] / stats[0]) * 100
                            status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"
                            print(f"      {col}: {counts[columns.index(col)]:,}/{stats[0]:,} ({percent:.1f}%) {status}")
                    
                    # EMA
                    if ema_cols:
                        print("    EMA:")
                        for col in ema_cols:
                            idx = columns.index(col)
                            percent = (counts[idx] / stats[0]) * 100 if stats[0] > 0 else 0
                            status = "✅" if percent > 95 else "⚠️" if percent > 50 else "❌"
                            print(f"      {col}: {counts[idx]:,}/{stats[0]:,} ({percent:.1f}%) {status}")
    
    # Получаем последние значения
    print("\n" + "="*80)
    print("🔄 ПОСЛЕДНИЕ ЗНАЧЕНИЯ (1h таймфрейм, BTCUSDT)")
    print("="*80)
    
    if 'indicators_bybit_futures_1h' in tables:
        cur.execute("""
            SELECT timestamp, sma_50, sma_200, ema_9, ema_21, ema_50
            FROM indicators_bybit_futures_1h
            WHERE symbol = 'BTCUSDT'
            ORDER BY timestamp DESC
            LIMIT 5
        """)
        
        rows = cur.fetchall()
        if rows:
            headers = ['Timestamp', 'SMA_50', 'SMA_200', 'EMA_9', 'EMA_21', 'EMA_50']
            table_data = []
            for row in rows:
                table_data.append([
                    row[0].strftime('%Y-%m-%d %H:%M'),
                    f"{float(row[1]):.2f}" if row[1] else "N/A",
                    f"{float(row[2]):.2f}" if row[2] else "N/A",
                    f"{float(row[3]):.2f}" if row[3] else "N/A",
                    f"{float(row[4]):.2f}" if row[4] else "N/A",
                    f"{float(row[5]):.2f}" if row[5] else "N/A"
                ])
            print("\n" + tabulate(table_data, headers=headers, tablefmt='grid'))
    
    cur.close()
    conn.close()
    
    print("\n💡 Для загрузки индикаторов:")
    print("   SMA: python indicators/sma_loader.py")
    print("   EMA: python indicators/ema_loader.py")
    print("\n📄 Документация:")
    print("   indicators/INDICATORS_REFERENCE.md - Полный справочник по индикаторам")
    print("   indicators/USAGE_GUIDE.md - Руководство по использованию")

if __name__ == "__main__":
    main()