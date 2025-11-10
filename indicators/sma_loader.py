#!/usr/bin/env python3
"""
Universal SMA Loader with Multi-Timeframe Support
==================================================
Универсальный загрузчик SMA индикаторов с поддержкой:
- Любых таймфреймов (1m, 5m, 15m, 30m, 1h, 4h, 1d и т.д.)
- Автоматической агрегации из 1m данных
- Динамического создания таблиц
- Инкрементальных обновлений
- Параллельной обработки символов
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional, Any
import logging
from tqdm import tqdm
import sys
import os
import warnings
import yaml
import argparse
import time

# Игнорируем предупреждение pandas о psycopg2
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование в консоль и файл"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'sma_multi_tf_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename, encoding='utf-8')
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"📝 Логирование настроено. Лог-файл: {log_filename}")

    return logger

logger = setup_logging()


class SMALoader:
    """Загрузчик SMA для разных таймфреймов"""

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.config = self.load_config()
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.force_reload = False  # Флаг принудительного пересчета (устанавливается из main())

        # Динамический мапинг таймфреймов на минуты (после загрузки конфига)
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """
        Динамически парсит таймфреймы из конфигурации
        Поддерживает форматы: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w

        Returns:
            dict: Мапинг таймфрейма на количество минут
        """
        timeframe_map = {}

        # Получаем таймфреймы из конфига или используем стандартные
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        for tf in timeframes:
            # Парсим число и единицу измерения
            import re
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)

                # Конвертируем в минуты
                if unit == 'm':
                    minutes = number
                elif unit == 'h':
                    minutes = number * 60
                elif unit == 'd':
                    minutes = number * 1440
                elif unit == 'w':
                    minutes = number * 10080  # 7 * 24 * 60
                else:
                    logger.warning(f"⚠️ Неизвестный формат таймфрейма: {tf}")
                    continue

                timeframe_map[tf] = minutes
                logger.debug(f"📊 Таймфрейм {tf} = {minutes} минут")
            else:
                logger.warning(f"⚠️ Неправильный формат таймфрейма: {tf} (используйте формат как 1m, 5m, 1h, 1d)")

        if not timeframe_map:
            # Если ничего не распарсили, используем минимальный набор
            logger.warning("⚠️ Не удалось распарсить таймфреймы, использую стандартные")
            timeframe_map = {'1m': 1, '15m': 15, '1h': 60}

        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из indicators_config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"📋 Конфигурация загружена из {config_path}")
                    return config
            except Exception as e:
                logger.error(f"❌ Не удалось загрузить indicators_config.yaml: {e}")
                raise
        else:
            logger.error("❌ indicators_config.yaml не найден")
            raise FileNotFoundError("indicators_config.yaml not found")

    def create_indicators_table(self, timeframe: str) -> bool:
        """
        Создает таблицу для индикаторов если её нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)

        Returns:
            True если таблица создана или уже существует
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Проверяем существование таблицы
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    );
                """, (table_name,))

                exists = cur.fetchone()[0]

                if not exists:
                    logger.info(f"🔨 Создаю таблицу {table_name}")

                    # Создаем таблицу
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        timestamp TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        -- SMA колонки будут добавлены динамически
                        PRIMARY KEY (timestamp, symbol)
                    );

                    -- Создаем индексы
                    CREATE INDEX idx_{timeframe}_symbol_timestamp
                    ON {table_name} (symbol, timestamp);

                    CREATE INDEX idx_{timeframe}_timestamp
                    ON {table_name} (timestamp);
                    """

                    cur.execute(create_query)
                    conn.commit()
                    logger.info(f"✅ Таблица {table_name} создана")
                else:
                    logger.info(f"ℹ️ Таблица {table_name} уже существует")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при создании таблицы: {e}")
                conn.rollback()
                return False
            finally:
                cur.close()

    def create_sma_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для SMA периодов

        Args:
            timeframe: Таймфрейм
            periods: Список периодов SMA
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for period in periods:
                    column_name = f'sma_{period}'

                    # Проверяем существование колонки
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                            AND table_name = %s
                            AND column_name = %s
                        );
                    """, (table_name, column_name))

                    exists = cur.fetchone()[0]

                    if not exists:
                        logger.info(f"  📊 Создаю колонку {column_name} в таблице {table_name}")
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {column_name} DECIMAL(20,8);
                        """)

                conn.commit()
                logger.info(f"✅ Колонки SMA созданы для {table_name}")

            except Exception as e:
                logger.error(f"❌ Ошибка при создании колонок: {e}")
                conn.rollback()
            finally:
                cur.close()

    def clear_sma_columns(self, timeframe: str, periods: List[int]) -> bool:
        """
        Обнуляет (устанавливает NULL) все SMA столбцы для указанного таймфрейма и символа

        Args:
            timeframe: Таймфрейм для очистки (1m, 15m, 1h)
            periods: Список периодов SMA для очистки

        Returns:
            True если очистка успешна, False в случае ошибки
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Формируем SET clause для всех SMA колонок
                set_clauses = [f'sma_{period} = NULL' for period in periods]
                set_clause = ', '.join(set_clauses)

                # Выполняем UPDATE запрос
                query = f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE symbol = %s
                """

                cur.execute(query, (self.symbol,))
                rows_affected = cur.rowcount

                conn.commit()
                logger.info(f"🗑️  Обнулено {rows_affected:,} записей для SMA столбцов в {table_name} (символ: {self.symbol})")
                logger.info(f"   Очищены столбцы: {', '.join([f'sma_{p}' for p in periods])}")

                return True

            except Exception as e:
                logger.error(f"❌ Ошибка при очистке SMA столбцов: {e}")
                conn.rollback()
                return False
            finally:
                cur.close()

    def aggregate_candles(self, start_date: datetime, end_date: datetime,
                         timeframe: str) -> pd.DataFrame:
        """
        Агрегирует 1m свечи в нужный таймфрейм

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            timeframe: Целевой таймфрейм

        Returns:
            DataFrame с агрегированными свечами
        """
        minutes = self.timeframe_minutes[timeframe]

        with self.db.get_connection() as conn:
            # Для 1m просто читаем данные
            if timeframe == '1m':
                query = """
                    SELECT timestamp, symbol, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                # Для остальных таймфреймов агрегируем
                query = f"""
                    WITH time_groups AS (
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start,
                            close,
                            symbol,
                            timestamp as original_timestamp
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                    ),
                    last_in_period AS (
                        SELECT DISTINCT ON (period_start, symbol)
                            period_start as timestamp,
                            symbol,
                            close
                        FROM time_groups
                        ORDER BY period_start, symbol, original_timestamp DESC
                    )
                    SELECT * FROM last_in_period
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            if df.empty:
                logger.warning(f"⚠️ Нет данных для {self.symbol} в период {start_date} - {end_date}")
                return pd.DataFrame()

            # Убеждаемся, что timestamp - это datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            return df

    def calculate_and_save_sma(self, timeframe: str, periods: List[int],
                               batch_days: int = 30):
        """
        Рассчитывает и сохраняет SMA для указанного таймфрейма

        Args:
            timeframe: Таймфрейм
            periods: Список периодов SMA
            batch_days: Размер батча в днях
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Получаем диапазон дат из исходных данных
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Находим диапазон дат в 1m данных
                cur.execute("""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()
                if not result or result[0] is None:
                    logger.error(f"❌ Нет данных для {self.symbol}")
                    return

                min_date, max_date = result
                logger.info(f"📅 Диапазон данных: {min_date} - {max_date}")

                # Проверяем последнюю загруженную дату для КАЖДОГО периода SMA (оптимизированно)
                logger.info(f"\n📊 Анализ заполнения SMA периодов:")

                # Один запрос для всех SMA периодов
                sma_columns = [f'sma_{p}' for p in periods]
                sma_max_queries = ', '.join([f'MAX({col}) as max_{col}' for col in sma_columns])

                cur.execute(f"""
                    SELECT {sma_max_queries}
                    FROM (
                        SELECT timestamp, {', '.join(sma_columns)}
                        FROM {table_name}
                        WHERE symbol = %s
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    ) t
                """, (self.symbol,))

                result_row = cur.fetchone()

                sma_status = {}
                earliest_date = max_date  # Начинаем с максимальной даты

                # Теперь проверяем каждый период
                for idx, period in enumerate(tqdm(periods, desc="   Проверка периодов SMA", unit="период",
                                                  leave=False, bar_format='{desc}: {n}/{total} [{elapsed}]')):
                    column_name = f'sma_{period}'

                    # Получаем последнюю дату для этого SMA
                    cur.execute(f"""
                        SELECT MAX(timestamp)
                        FROM {table_name}
                        WHERE symbol = %s AND {column_name} IS NOT NULL
                    """, (self.symbol,))

                    last_sma_date = cur.fetchone()[0]

                    if last_sma_date:
                        gap_days = (max_date - last_sma_date).days
                        gap_hours = (max_date - last_sma_date).total_seconds() / 3600

                        if gap_hours < 24:
                            logger.info(f"   • SMA_{period}: актуален (отстает на {gap_hours:.1f} часов)")
                        else:
                            logger.info(f"   • SMA_{period}: последняя {last_sma_date.strftime('%Y-%m-%d %H:%M')} (отстает на {gap_days} дней)")

                        sma_status[period] = last_sma_date

                        # Находим самую раннюю дату из всех SMA
                        if last_sma_date < earliest_date:
                            earliest_date = last_sma_date
                    else:
                        # Этот SMA совсем пустой
                        min_start = min_date + timedelta(minutes=period * self.timeframe_minutes[timeframe])
                        sma_status[period] = None
                        logger.info(f"   • SMA_{period}: ПУСТО (нужна полная загрузка)")

                        if min_start < earliest_date:
                            earliest_date = min_start

                # Используем самую раннюю дату для начала загрузки
                if earliest_date < max_date:
                    start_date = earliest_date
                    logger.info(f"\n📍 Начинаю загрузку с {start_date} (самый отстающий период)")
                else:
                    start_date = max_date
                    logger.info(f"\n✅ Все SMA актуальны!")

                # Подсчитываем общее количество батчей для прогресса
                # Учитываем не только дни, но и часы/минуты для точного подсчета
                total_time_diff = max_date - start_date
                total_days = total_time_diff.days
                total_hours = total_time_diff.total_seconds() / 3600

                # Более точный расчет батчей
                if total_days == 0:
                    # Если меньше дня, все равно будет 1 батч
                    total_batches = 1
                else:
                    # Рассчитываем количество батчей
                    total_batches = (total_days + batch_days - 1) // batch_days  # Округление вверх

                # Показываем статистику загрузки
                logger.info(f"\n📊 План загрузки:")

                if total_hours < 1:
                    logger.info(f"   • Период: менее часа (последние минуты)")
                elif total_hours < 24:
                    logger.info(f"   • Период: {total_hours:.1f} часов ({start_date.strftime('%H:%M')} → {max_date.strftime('%H:%M')})")
                else:
                    logger.info(f"   • Период: {total_days} дней ({start_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')})")

                logger.info(f"   • Батчей: {total_batches} по {batch_days} дней")
                logger.info(f"   • Таймфрейм: {timeframe}")
                logger.info(f"   • SMA периоды: {periods}")

                # Оцениваем количество записей
                if timeframe == '1m':
                    est_records = int(total_hours * 60) if total_days == 0 else total_days * 1440
                else:
                    minutes = self.timeframe_minutes[timeframe]
                    if total_days == 0:
                        est_records = int((total_hours * 60) / minutes)
                    else:
                        est_records = (total_days * 1440) // minutes

                if est_records > 0:
                    logger.info(f"   • Ожидается записей: ~{est_records:,}")
                    if total_batches <= 1:
                        logger.info(f"   • Примерное время: менее минуты")
                    else:
                        logger.info(f"   • Примерное время: ~{(total_batches * 1.5)/60:.1f} минут")
                else:
                    logger.info(f"   • Данные полностью актуальны!")

                # Обработка батчами с улучшенным прогресс-баром
                current_date = start_date
                total_records = 0
                processed_batches = 0

                # Создаем прогресс-бар с общим количеством батчей
                sma_list = ','.join([str(p) for p in periods])
                progress_desc = f"{self.symbol} {self.symbol_progress} SMA[{sma_list}] {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} SMA[{sma_list}] {timeframe.upper()}"
                with tqdm(total=total_batches,
                         desc=progress_desc,
                         unit="batch",
                         bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:

                    while current_date <= max_date:
                        batch_end = min(current_date + timedelta(days=batch_days), max_date)

                        # Загружаем данные с запасом для расчета SMA
                        max_period = max(periods)
                        lookback = timedelta(minutes=max_period * self.timeframe_minutes[timeframe])
                        data_start = current_date - lookback

                        # Агрегируем свечи
                        df = self.aggregate_candles(data_start, batch_end, timeframe)

                        if df.empty:
                            logger.warning(f"⚠️ Нет данных для батча {current_date} - {batch_end}")
                            current_date = batch_end
                            pbar.update(1)
                            continue

                        # Рассчитываем SMA
                        for period in periods:
                            df[f'sma_{period}'] = df['close'].rolling(window=period, min_periods=period).mean()

                        # Фильтруем только нужный диапазон (убираем lookback данные)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # Удаляем строки где все SMA = NaN
                        sma_columns = [f'sma_{p}' for p in periods]
                        df_to_save = df_to_save.dropna(subset=sma_columns, how='all')

                        if not df_to_save.empty:
                            # Сохраняем в БД
                            self.save_to_db(df_to_save, table_name, periods)
                            total_records += len(df_to_save)

                        processed_batches += 1
                        pbar.update(1)

                        # Обновляем детальную информацию
                        if not df_to_save.empty:
                            progress_pct = (processed_batches / total_batches) * 100
                            pbar.set_postfix({
                                'записей': f'{total_records:,}',
                                'последняя': df_to_save['timestamp'].max().strftime('%Y-%m-%d %H:%M'),
                                'прогресс': f'{progress_pct:.1f}%'
                            })

                        current_date = batch_end

                        # Защита от бесконечного цикла для актуальных данных
                        if current_date >= max_date:
                            break

                logger.info(f"✅ Загружено {total_records} записей для {timeframe}")

            except Exception as e:
                logger.error(f"❌ Ошибка при расчете SMA: {e}")
                raise
            finally:
                cur.close()

    def save_to_db(self, df: pd.DataFrame, table_name: str, periods: List[int]):
        """
        Сохраняет данные в БД

        Args:
            df: DataFrame с данными
            table_name: Имя таблицы
            periods: Список периодов SMA
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Подготавливаем данные
                sma_columns = [f'sma_{p}' for p in periods]
                columns = ['timestamp', 'symbol'] + sma_columns

                # Формируем данные для вставки
                records = []
                for _, row in df.iterrows():
                    record = [row['timestamp'], self.symbol]
                    for col in sma_columns:
                        value = row[col] if pd.notna(row[col]) else None
                        record.append(value)
                    records.append(tuple(record))

                # Формируем запрос
                placeholders = ','.join(['%s'] * len(columns))
                update_set = ','.join([f"{col} = EXCLUDED.{col}" for col in sma_columns])

                insert_query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT (timestamp, symbol) DO UPDATE SET
                    {update_set};
                """

                # Выполняем вставку батчами
                psycopg2.extras.execute_batch(cur, insert_query, records, page_size=1000)
                conn.commit()

            except Exception as e:
                logger.error(f"❌ Ошибка при сохранении в БД: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def process_timeframe(self, timeframe: str):
        """
        Обрабатывает один таймфрейм

        Args:
            timeframe: Таймфрейм для обработки
        """
        logger.info(f"\n{'='*50}")
        logger.info(f"⏰ Обработка таймфрейма: {timeframe}")
        logger.info(f"{'='*50}")

        # Получаем периоды из конфига
        sma_config = self.config.get('indicators', {}).get('sma', {})
        if not sma_config.get('enabled', False):
            logger.info(f"⏭️ SMA отключен в конфигурации")
            return

        periods = sma_config.get('periods', [])
        if not periods:
            logger.warning(f"⚠️ Не указаны периоды SMA")
            return

        logger.info(f"📊 Периоды SMA: {periods}")

        # Создаем таблицу если нужно
        if not self.create_indicators_table(timeframe):
            logger.error(f"❌ Не удалось создать таблицу для {timeframe}")
            return

        # Создаем колонки для SMA
        self.create_sma_columns(timeframe, periods)

        # Обнуляем существующие данные если включен флаг force-reload
        if self.force_reload:
            logger.info(f"\n🔄 Включен режим force-reload - обнуление существующих SMA данных")
            if not self.clear_sma_columns(timeframe, periods):
                logger.error(f"❌ Не удалось обнулить SMA столбцы для {timeframe}")
                return

        # Рассчитываем и сохраняем SMA
        self.calculate_and_save_sma(timeframe, periods)

    def run(self, timeframes: Optional[List[str]] = None):
        """
        Запускает обработку для всех таймфреймов

        Args:
            timeframes: Список таймфреймов или None для использования из конфига
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"🚀 Запуск обработки для таймфреймов: {timeframes}")
        logger.info(f"📈 Символ: {self.symbol}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe)

        logger.info(f"\n✅ Обработка завершена для всех таймфреймов")


def main():
    """Основная функция с поддержкой обратной совместимости"""
    parser = argparse.ArgumentParser(description='Universal SMA Loader')
    parser.add_argument('--symbol', type=str, default=None,
                      help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                      help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframes', type=str, default=None,
                      help='Таймфреймы через запятую (1m,15m,1h) или пусто для всех из config.yaml')
    parser.add_argument('--timeframe', type=str, default=None,
                      help='Один таймфрейм (для обратной совместимости)')
    parser.add_argument('--batch-days', type=int, default=30,
                      help='Размер батча в днях (по умолчанию 30)')
    parser.add_argument('--force-reload', action='store_true',
                      help='Обнулить все SMA столбцы перед загрузкой (принудительный полный пересчет)')

    args = parser.parse_args()

    # Определяем символы для обработки
    if args.symbols:
        # Если указаны конкретные символы через аргумент --symbols
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        # Если указан один символ через аргумент --symbol
        symbols = [args.symbol]
    else:
        # Читаем символы из config.yaml
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    # Обратная совместимость: если указан --timeframe, используем его
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = [tf.strip() for tf in args.timeframes.split(',')]
    else:
        timeframes = None  # Будут использованы из config.yaml

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        try:
            # Создаем и запускаем загрузчик для текущего символа
            loader = SMALoader(symbol=symbol)
            # Устанавливаем информацию о прогрессе символов
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            # Передаем флаг принудительного пересчета
            loader.force_reload = args.force_reload

            # Если указан конкретный таймфрейм, обрабатываем только его
            if timeframes and len(timeframes) == 1:
                loader.process_timeframe(timeframes[0])
            else:
                loader.run(timeframes=timeframes)

            logger.info(f"\n✅ Символ {symbol} обработан\n")
        except KeyboardInterrupt:
            logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Критическая ошибка для символа {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Вычисляем общее время обработки
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()