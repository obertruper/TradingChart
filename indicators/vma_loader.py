#!/usr/bin/env python3
"""
VMA (Volume Moving Average) Loader with Multi-Timeframe Support
================================================================
Загрузчик VMA индикаторов с поддержкой:
- Множественных периодов (10, 20, 50, 100, 200)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h)
- Инкрементальных обновлений
- Последовательной обработки периодов (можно прервать)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional
import logging
from tqdm import tqdm
import sys
import os
import warnings
import yaml
import argparse
import time

# Игнорируем предупреждения pandas
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indicators.database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование в консоль и файл"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'vma_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

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


class VMALoader:
    """Загрузчик VMA (Volume Moving Average) для разных таймфреймов"""

    def __init__(self, symbol: str = 'BTCUSDT'):
        """
        Инициализация загрузчика

        Args:
            symbol: Торговая пара
        """
        self.db = DatabaseConnection()
        self.symbol = symbol
        self.symbol_progress = ""  # Будет установлено из main() для отображения прогресса
        self.config = self.load_config()

        # Динамический мапинг таймфреймов на минуты
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
                        -- VMA колонки будут добавлены динамически
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

    def create_vma_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для VMA периодов

        Args:
            timeframe: Таймфрейм
            periods: Список периодов VMA
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for period in periods:
                    column_name = f'vma_{period}'

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
                logger.info(f"✅ Колонки VMA созданы для {table_name}")

            except Exception as e:
                logger.error(f"❌ Ошибка при создании колонок: {e}")
                conn.rollback()
            finally:
                cur.close()

    def get_last_vma_date(self, timeframe: str, period: int) -> Optional[datetime]:
        """
        Получает последнюю дату для конкретного периода VMA

        Args:
            timeframe: Таймфрейм
            period: Период VMA

        Returns:
            Последняя дата с данными или None
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        column_name = f'vma_{period}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM {table_name}
                WHERE symbol = %s AND {column_name} IS NOT NULL
            """, (self.symbol,))

            result = cur.fetchone()
            cur.close()

            return result[0] if result and result[0] else None

    def get_data_date_range(self) -> Tuple[datetime, datetime]:
        """
        Получает диапазон дат доступных данных в candles_bybit_futures_1m

        Returns:
            Tuple[min_date, max_date]
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT MIN(timestamp), MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))

            result = cur.fetchone()
            cur.close()

            if not result or result[0] is None:
                raise ValueError(f"Нет данных для {self.symbol} в candles_bybit_futures_1m")

            return result[0], result[1]

    def get_last_complete_period(self, current_time: datetime, timeframe: str) -> datetime:
        """
        Возвращает последний ЗАВЕРШЕННЫЙ период

        Args:
            current_time: Текущее время
            timeframe: Таймфрейм

        Returns:
            Timestamp последнего завершенного периода
        """
        if timeframe == '1m':
            # Последняя завершенная минута
            return current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
        elif timeframe == '15m':
            # Последние завершенные 15 минут
            minute = (current_time.minute // 15) * 15
            result = current_time.replace(minute=minute, second=0, microsecond=0)
            if current_time.minute % 15 == 0 and current_time.second == 0:
                result -= timedelta(minutes=15)
            return result
        elif timeframe == '1h':
            # Последний завершенный час
            result = current_time.replace(minute=0, second=0, microsecond=0)
            if current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=1)
            return result
        elif timeframe == '1d':
            # Вчерашний день
            return (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # По умолчанию - текущее время минус интервал
            minutes = self.timeframe_minutes.get(timeframe, 60)
            return current_time - timedelta(minutes=minutes)

    def aggregate_candles(self, start_date: datetime, end_date: datetime,
                         timeframe: str) -> pd.DataFrame:
        """
        Агрегирует 1m свечи в нужный таймфрейм, суммируя объемы

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            timeframe: Целевой таймфрейм

        Returns:
            DataFrame с агрегированными данными (timestamp, symbol, volume)
        """
        minutes = self.timeframe_minutes[timeframe]

        with self.db.get_connection() as conn:
            # Для 1m просто читаем данные
            if timeframe == '1m':
                query = """
                    SELECT timestamp, symbol, volume
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                # Для остальных таймфреймов агрегируем СУММУ объемов
                # ВАЖНО: Timestamp = НАЧАЛО периода (Bybit standard)
                query = f"""
                    WITH time_groups AS (
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start,
                            volume,
                            symbol
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
                    )
                    SELECT
                        period_start as timestamp,
                        symbol,
                        SUM(volume) as volume
                    FROM time_groups
                    GROUP BY period_start, symbol
                    ORDER BY period_start
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            if df.empty:
                logger.warning(f"⚠️ Нет данных для {self.symbol} в период {start_date} - {end_date}")
                return pd.DataFrame()

            # Убеждаемся, что timestamp - это datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            return df

    def save_single_column_to_db(self, df: pd.DataFrame, table_name: str, period: int):
        """
        Сохраняет одну колонку VMA в БД

        Args:
            df: DataFrame с данными
            table_name: Имя таблицы
            period: Период VMA
        """
        column_name = f'vma_{period}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Подготавливаем данные
                records = []
                for _, row in df.iterrows():
                    value = row[column_name] if pd.notna(row[column_name]) else None
                    records.append((row['timestamp'], self.symbol, value))

                # Формируем запрос
                insert_query = f"""
                    INSERT INTO {table_name} (timestamp, symbol, {column_name})
                    VALUES (%s, %s, %s)
                    ON CONFLICT (timestamp, symbol) DO UPDATE SET
                    {column_name} = EXCLUDED.{column_name};
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

    def calculate_and_save_vma(self, timeframe: str, periods: List[int],
                               batch_days: int = 1):
        """
        Рассчитывает и сохраняет VMA для указанного таймфрейма
        Обрабатывает каждый период последовательно

        Args:
            timeframe: Таймфрейм
            periods: Список периодов VMA
            batch_days: Размер батча в днях
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        # Получаем диапазон дат из исходных данных
        min_date, max_date = self.get_data_date_range()
        logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Определяем последний завершенный период
        end_date = self.get_last_complete_period(max_date, timeframe)
        logger.info(f"🎯 Последний завершенный период: {end_date}")

        # Обрабатываем каждый период ПОСЛЕДОВАТЕЛЬНО
        for idx, period in enumerate(periods):
            logger.info(f"\n{'─'*50}")
            logger.info(f"📈 Обработка VMA_{period} ({idx + 1}/{len(periods)}):")
            logger.info(f"{'─'*50}")

            # Находим последнюю дату для ЭТОГО периода
            last_date = self.get_last_vma_date(timeframe, period)

            if last_date is None:
                # Колонка пустая, начинаем с начала данных
                start_date = min_date
                logger.info(f"  📝 VMA_{period}: нет данных (загрузка с начала)")
            else:
                # Перезаписываем последний день (на случай обрыва)
                start_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)
                days_behind = (end_date - last_date).days
                logger.info(f"  ✅ VMA_{period}: последняя дата {last_date.strftime('%Y-%m-%d %H:%M')} "
                           f"(отстает на {days_behind} дней)")

            # Подсчитываем количество батчей
            total_time_diff = end_date - start_date
            total_days = total_time_diff.days + 1
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"\n📊 План загрузки VMA_{period}:")
            logger.info(f"   • Период: {total_days} дней ({start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')})")
            logger.info(f"   • Батчей: {total_batches} по {batch_days} дней")
            logger.info(f"   • Таймфрейм: {timeframe}")

            # Батчевая обработка
            current_date = start_date
            total_records = 0
            processed_batches = 0

            with tqdm(total=total_batches,
                     desc=f"{self.symbol} {self.symbol_progress} VMA-{period} {timeframe.upper()}",
                     unit="batch",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:

                while current_date <= end_date:
                    batch_end = min(current_date + timedelta(days=batch_days), end_date)

                    # Загружаем данные с запасом для расчета VMA (lookback)
                    lookback = timedelta(minutes=period * self.timeframe_minutes[timeframe])
                    data_start = current_date - lookback

                    # Агрегируем данные
                    df = self.aggregate_candles(data_start, batch_end, timeframe)

                    if df.empty:
                        logger.warning(f"⚠️ Нет данных для батча {current_date} - {batch_end}")
                        current_date = batch_end
                        pbar.update(1)
                        continue

                    # Рассчитываем VMA для этого периода
                    df[f'vma_{period}'] = df['volume'].rolling(window=period, min_periods=period).mean()

                    # Фильтруем только нужный диапазон (убираем lookback данные)
                    df_to_save = df[df['timestamp'] >= current_date].copy()

                    # Удаляем строки где VMA = NaN (недостаточно данных)
                    df_to_save = df_to_save.dropna(subset=[f'vma_{period}'])

                    if not df_to_save.empty:
                        # Сохраняем в БД с retry
                        for attempt in range(3):
                            try:
                                self.save_single_column_to_db(df_to_save, table_name, period)
                                total_records += len(df_to_save)
                                break
                            except Exception as e:
                                if attempt == 2:
                                    logger.error(f"❌ Не удалось сохранить VMA_{period} после 3 попыток: {e}")
                                    raise
                                logger.warning(f"⚠️ Попытка {attempt + 1}/3 не удалась, повтор через {2 ** attempt} сек...")
                                time.sleep(2 ** attempt)

                    processed_batches += 1
                    pbar.update(1)

                    # Обновляем детальную информацию
                    if not df_to_save.empty:
                        progress_pct = (processed_batches / total_batches) * 100
                        pbar.set_postfix({
                            'записей': f'{total_records:,}',
                            'последняя': df_to_save['timestamp'].max().strftime('%Y-%m-%d %H:%M')
                        })

                    current_date = batch_end

                    # Защита от бесконечного цикла
                    if current_date >= end_date:
                        break

            logger.info(f"✅ VMA_{period}: Загружено {total_records:,} записей для {timeframe}")

    def process_timeframe(self, timeframe: str):
        """
        Обрабатывает один таймфрейм

        Args:
            timeframe: Таймфрейм для обработки
        """
        logger.info(f"\n{'═'*50}")
        logger.info(f"⏰ Таймфрейм: {timeframe}")
        logger.info(f"{'═'*50}")

        # Получаем параметры из конфига
        vma_config = self.config.get('indicators', {}).get('vma', {})
        if not vma_config.get('enabled', False):
            logger.info(f"⏭️ VMA отключен в конфигурации")
            return

        periods = vma_config.get('periods', [])
        if not periods:
            logger.warning(f"⚠️ Не указаны периоды VMA")
            return

        batch_days = vma_config.get('batch_days', 1)

        logger.info(f"📊 Периоды VMA: {periods}")
        logger.info(f"📅 Размер батча: {batch_days} день/дней")

        # Создаем таблицу если нужно
        if not self.create_indicators_table(timeframe):
            logger.error(f"❌ Не удалось создать таблицу для {timeframe}")
            return

        # Создаем колонки для VMA
        self.create_vma_columns(timeframe, periods)

        # Рассчитываем и сохраняем VMA
        self.calculate_and_save_vma(timeframe, periods, batch_days)

        logger.info(f"\n✅ Таймфрейм {timeframe} завершен: {len(periods)} периодов")

    def run(self, timeframes: Optional[List[str]] = None):
        """
        Запускает обработку для всех таймфреймов

        Args:
            timeframes: Список таймфреймов или None для использования из конфига
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"\n{'═'*60}")
        logger.info(f"🚀 VMA Loader: Запуск для {self.symbol}")
        logger.info(f"📋 Таймфреймы: {', '.join(timeframes)}")
        logger.info(f"{'═'*60}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe)

        logger.info(f"\n{'═'*60}")
        logger.info(f"✅ Обработка завершена для всех таймфреймов")
        logger.info(f"{'═'*60}")


def main():
    """Основная функция с поддержкой параметров командной строки"""
    parser = argparse.ArgumentParser(description='VMA (Volume Moving Average) Loader')
    parser.add_argument('--symbol', type=str, default=None,
                      help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                      help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframes', type=str, default=None,
                      help='Таймфреймы через запятую (1m,15m,1h) или пусто для всех из config.yaml')
    parser.add_argument('--timeframe', type=str, default=None,
                      help='Один таймфрейм (для обратной совместимости)')
    parser.add_argument('--batch-days', type=int, default=None,
                      help='Размер батча в днях (по умолчанию из config.yaml)')
    parser.add_argument('--periods', type=str, default=None,
                      help='Периоды VMA через запятую (10,20,50)')

    args = parser.parse_args()

    # Определяем символы для обработки
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        symbols = [args.symbol]
    else:
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
            loader = VMALoader(symbol=symbol)
            loader.symbol_progress = f"[{idx}/{total_symbols}] "

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
