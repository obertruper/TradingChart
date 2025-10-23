#!/usr/bin/env python3
"""
EMA Loader - основной загрузчик EMA индикаторов
С батчевой обработкой и системой checkpoint для продолжения загрузки

Использование:
    python ema_loader.py                     # Загрузка всех таймфреймов из config.yaml
    python ema_loader.py --timeframe 1m      # Загрузка конкретного таймфрейма
    python ema_loader.py --batch-days 7      # Размер батча 7 дней
    python ema_loader.py --start-date 2024-01-01  # Начать с конкретной даты
"""

import os
import sys
import re
import logging
import argparse
import yaml
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from tqdm import tqdm
import time

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'ema_{timestamp}.log')

    # Настройка форматирования
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Файловый обработчик
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Настройка логгера
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📝 EMA Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger

logger = setup_logging()

class EMALoader:
    """
    Загрузчик EMA индикаторов с батчевой обработкой и checkpoint системой
    """

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
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """
        Парсит таймфреймы из конфигурации
        Поддерживает форматы: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w

        Returns:
            dict: Мапинг таймфрейма на количество минут
        """
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        for tf in timeframes:
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)

                if unit == 'm':
                    minutes = number
                elif unit == 'h':
                    minutes = number * 60
                elif unit == 'd':
                    minutes = number * 1440
                elif unit == 'w':
                    minutes = number * 10080
                else:
                    continue

                timeframe_map[tf] = minutes

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

    def create_ema_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для EMA периодов в существующей таблице

        Args:
            timeframe: Таймфрейм
            periods: Список периодов EMA
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем существование таблицы
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table_name,))

            if not cur.fetchone()[0]:
                logger.error(f"❌ Таблица {table_name} не существует")
                return False

            # Проверяем существующие колонки
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND column_name LIKE 'ema_%%'
            """, (table_name,))

            existing = {row[0] for row in cur.fetchall()}

            # Создаем недостающие колонки
            created = []
            for period in periods:
                col_name = f'ema_{period}'
                if col_name not in existing:
                    try:
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(20,8)
                        """)
                        created.append(col_name)
                        logger.info(f"  📊 Создана колонка {col_name} в таблице {table_name}")
                    except Exception as e:
                        logger.error(f"  ❌ Ошибка создания колонки {col_name}: {e}")

            conn.commit()

            if created:
                logger.info(f"✅ Созданы колонки EMA для {table_name}: {created}")
            else:
                logger.info(f"ℹ️ Все колонки EMA уже существуют в {table_name}")

            return True

    def get_last_ema_checkpoint(self, timeframe: str, period: int) -> Tuple[Optional[datetime], Optional[float]]:
        """
        Получает последнее сохраненное значение EMA для продолжения

        Args:
            timeframe: Таймфрейм
            period: Период EMA

        Returns:
            (timestamp, ema_value) или (None, None)
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        column_name = f'ema_{period}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем последнее значение
            try:
                cur.execute(f"""
                    SELECT timestamp, {column_name}
                    FROM {table_name}
                    WHERE symbol = %s
                    AND {column_name} IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.symbol,))

                result = cur.fetchone()
                if result:
                    return result[0], float(result[1])
            except Exception as e:
                logger.debug(f"Не удалось получить checkpoint для {column_name}: {e}")

            return None, None

    def calculate_ema_batch(self, df: pd.DataFrame, periods: List[int],
                           initial_emas: Dict[int, float]) -> pd.DataFrame:
        """
        Рассчитывает EMA для батча данных

        Args:
            df: DataFrame с ценами
            periods: Список периодов EMA
            initial_emas: Начальные значения EMA для каждого периода

        Returns:
            DataFrame с рассчитанными EMA
        """
        for period in periods:
            column_name = f'ema_{period}'
            alpha = 2.0 / (period + 1)

            if period in initial_emas and initial_emas[period] is not None:
                # Есть начальное значение - используем рекурсивную формулу
                ema_values = []
                prev_ema = initial_emas[period]

                for price in df['price']:
                    if pd.notna(price):
                        new_ema = float(price) * alpha + prev_ema * (1 - alpha)
                        ema_values.append(new_ema)
                        prev_ema = new_ema
                    else:
                        ema_values.append(None)

                df[column_name] = ema_values
            else:
                # Нет начального значения - используем pandas.ewm
                df[column_name] = df['price'].ewm(span=period, adjust=False).mean()

        return df

    def process_batch(self, timeframe: str, periods: List[int],
                     start_date: datetime, end_date: datetime,
                     initial_emas: Dict[int, float]) -> Dict[int, float]:
        """
        Обрабатывает один батч данных

        Args:
            timeframe: Таймфрейм
            periods: Периоды EMA
            start_date: Начало батча
            end_date: Конец батча
            initial_emas: Начальные значения EMA

        Returns:
            Последние значения EMA для следующего батча
        """
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Загружаем данные
            if timeframe == '1m':
                # Для 1m берем close из свечей
                query = """
                    SELECT timestamp, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                    AND timestamp >= %s
                    AND timestamp <= %s
                    ORDER BY timestamp
                """
                cur.execute(query, (self.symbol, start_date, end_date))
            else:
                # Для других таймфреймов - агрегация
                minutes = self.timeframe_minutes[timeframe]
                query = f"""
                    WITH candle_data AS (
                        SELECT
                            date_trunc('hour', timestamp) +
                            INTERVAL '{minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::integer / {minutes}) as period_start,
                            open,  -- Используем OPEN для старших таймфреймов
                            symbol,
                            timestamp as original_timestamp
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                    )
                    SELECT DISTINCT ON (period_start)
                        period_start + INTERVAL '{minutes} minutes' as timestamp,
                        first_value(open) OVER (PARTITION BY period_start ORDER BY original_timestamp) as price
                    FROM candle_data
                    WHERE symbol = %s
                    ORDER BY period_start
                """
                cur.execute(query, (self.symbol, start_date, end_date, self.symbol))

            rows = cur.fetchall()

            if not rows:
                return initial_emas

            # Создаем DataFrame
            df = pd.DataFrame(rows, columns=['timestamp', 'price'])
            df['price'] = df['price'].astype(float)

            # Рассчитываем EMA
            df = self.calculate_ema_batch(df, periods, initial_emas)

            # Сохраняем в БД
            table_name = f'indicators_bybit_futures_{timeframe}'

            # Подготавливаем данные для batch update
            updates = []
            for _, row in df.iterrows():
                update_values = {'timestamp': row['timestamp'], 'symbol': self.symbol}
                for period in periods:
                    col_name = f'ema_{period}'
                    if col_name in df.columns and pd.notna(row[col_name]):
                        update_values[col_name] = float(row[col_name])

                if len(update_values) > 2:  # Есть хотя бы одно значение EMA
                    updates.append(update_values)

            # Выполняем batch update
            if updates:
                for update in updates:
                    # Формируем динамический UPDATE запрос
                    ema_columns = [k for k in update.keys() if k.startswith('ema_')]
                    if ema_columns:
                        set_clause = ', '.join([f"{col} = %s" for col in ema_columns])
                        values = [update[col] for col in ema_columns]
                        values.extend([update['timestamp'], update['symbol']])

                        update_query = f"""
                            UPDATE {table_name}
                            SET {set_clause}
                            WHERE timestamp = %s AND symbol = %s
                        """

                        try:
                            cur.execute(update_query, values)
                        except Exception as e:
                            logger.error(f"Ошибка обновления: {e}")

            conn.commit()

            # Возвращаем последние значения EMA для следующего батча
            last_emas = {}
            for period in periods:
                col_name = f'ema_{period}'
                if col_name in df.columns:
                    last_value = df[col_name].dropna().iloc[-1] if not df[col_name].dropna().empty else None
                    if last_value is not None:
                        last_emas[period] = float(last_value)
                    elif period in initial_emas:
                        last_emas[period] = initial_emas[period]

            return last_emas

    def calculate_and_save_ema(self, timeframe: str, periods: List[int],
                               batch_days: int = 7,
                               start_date: Optional[datetime] = None):
        """
        Рассчитывает и сохраняет EMA для указанного таймфрейма с батчевой обработкой

        Args:
            timeframe: Таймфрейм
            periods: Список периодов EMA
            batch_days: Размер батча в днях
            start_date: Начальная дата (если None, продолжаем с последней или начинаем сначала)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Обработка EMA для таймфрейма {timeframe}")
        logger.info(f"💹 Периоды EMA: {periods}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        logger.info(f"{'='*60}")

        # Создаем колонки если нужно
        if not self.create_ema_columns(timeframe, periods):
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем checkpoint для каждого периода
            logger.info("\n🔍 Проверка существующих данных (checkpoint):")
            checkpoints = {}
            latest_checkpoint = None

            for period in periods:
                last_timestamp, last_ema = self.get_last_ema_checkpoint(timeframe, period)
                checkpoints[period] = {
                    'last_timestamp': last_timestamp,
                    'last_ema': last_ema
                }

                if last_timestamp:
                    logger.info(f"  ✅ EMA_{period}: продолжение с {last_timestamp} (последнее значение: {last_ema:.2f})")
                    if not latest_checkpoint or last_timestamp > latest_checkpoint:
                        latest_checkpoint = last_timestamp
                else:
                    logger.info(f"  📝 EMA_{period}: нет данных (будет рассчитан с начала)")

            # Определяем начальную точку
            if start_date:
                current_date = start_date
                logger.info(f"\n📅 Используем указанную дату начала: {current_date}")
            elif latest_checkpoint:
                current_date = latest_checkpoint
                logger.info(f"\n♻️ Продолжаем с checkpoint: {current_date}")
            else:
                # Начинаем с самого начала
                cur.execute("""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    current_date = result[0]
                    logger.info(f"\n🚀 Начинаем с самого начала: {current_date}")
                else:
                    logger.error("❌ Нет данных для обработки")
                    return

            # Добавляем буфер для инициализации (если начинаем с начала)
            initial_emas = {}
            if not latest_checkpoint:
                max_period = max(periods)
                buffer_start = current_date - timedelta(minutes=max_period + 50)

                logger.info(f"\n🔧 Инициализация EMA с буферными данными...")
                initial_emas = self.process_batch(
                    timeframe, periods,
                    buffer_start, current_date,
                    {}
                )

                # Используем значения из checkpoints если они есть
                for period, checkpoint in checkpoints.items():
                    if checkpoint['last_ema'] is not None:
                        initial_emas[period] = checkpoint['last_ema']

                logger.info(f"✅ Инициализация завершена")
            else:
                # Используем checkpoint значения как начальные
                for period, checkpoint in checkpoints.items():
                    if checkpoint['last_ema'] is not None:
                        initial_emas[period] = checkpoint['last_ema']

            # Получаем конечную дату
            cur.execute("""
                SELECT MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            max_date = cur.fetchone()[0]

            if not max_date or current_date >= max_date:
                logger.info("✅ Данные уже актуальны")
                return

            # Рассчитываем количество батчей
            total_days = (max_date - current_date).days
            if total_days <= 0:
                total_days = 1
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"\n📊 План загрузки:")
            logger.info(f"   • Период: {total_days} дней ({current_date.strftime('%Y-%m-%d %H:%M')} → {max_date.strftime('%Y-%m-%d %H:%M')})")
            logger.info(f"   • Батчей: {total_batches}")
            logger.info(f"   • Таймфрейм: {timeframe}")
            logger.info(f"   • EMA периоды: {periods}")

            # Обработка батчами с прогресс-баром
            logger.info(f"\n🚀 Начинаю обработку...")

            progress_desc = f"{self.symbol} {self.symbol_progress} EMA {periods} {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} EMA {periods} {timeframe.upper()}"
            with tqdm(total=total_batches,
                     desc=f"📊 {progress_desc}",
                     unit='batch',
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

                batch_count = 0
                total_records = 0

                while current_date < max_date:
                    batch_end = min(current_date + timedelta(days=batch_days), max_date)

                    # Обрабатываем батч
                    try:
                        initial_emas = self.process_batch(
                            timeframe, periods,
                            current_date, batch_end,
                            initial_emas
                        )

                        # Считаем записи в батче
                        if timeframe == '1m':
                            batch_records = batch_days * 1440  # Примерная оценка
                        else:
                            batch_records = batch_days * (1440 // self.timeframe_minutes[timeframe])

                        total_records += batch_records

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча: {e}")
                        logger.info(f"   Батч: {current_date} - {batch_end}")
                        # Продолжаем со следующего батча
                        pass

                    batch_count += 1
                    pbar.update(1)
                    pbar.set_postfix({
                        'текущая_дата': batch_end.strftime('%Y-%m-%d'),
                        'записей': f'~{total_records:,}'
                    })

                    current_date = batch_end + timedelta(minutes=1)

                    # Checkpoint каждые 10 батчей
                    if batch_count % 10 == 0:
                        logger.debug(f"💾 Checkpoint: обработано {batch_count} батчей, последняя дата: {batch_end}")

            logger.info(f"\n✅ Загрузка EMA для {timeframe} завершена!")
            logger.info(f"   • Обработано батчей: {batch_count}")
            logger.info(f"   • Примерно записей: {total_records:,}")

    def process_timeframe(self, timeframe: str, batch_days: int = 7,
                         start_date: Optional[datetime] = None):
        """
        Обрабатывает один таймфрейм

        Args:
            timeframe: Таймфрейм для обработки
            batch_days: Размер батча в днях
            start_date: Начальная дата (опционально)
        """
        # Получаем периоды из конфига
        ema_config = self.config.get('indicators', {}).get('ema', {})
        if not ema_config.get('enabled', False):
            logger.info(f"⏭️ EMA отключен в конфигурации")
            return

        periods = ema_config.get('periods', [])
        if not periods:
            logger.warning(f"⚠️ Не указаны периоды EMA")
            return

        # Рассчитываем и сохраняем EMA
        self.calculate_and_save_ema(timeframe, periods, batch_days, start_date)

    def run(self, timeframes: Optional[List[str]] = None,
            batch_days: int = 7,
            start_date: Optional[datetime] = None):
        """
        Запускает обработку для всех таймфреймов

        Args:
            timeframes: Список таймфреймов или None для использования из конфига
            batch_days: Размер батча в днях
            start_date: Начальная дата (опционально)
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Запуск EMA Loader")
        logger.info(f"📊 Таймфреймы: {timeframes}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        if start_date:
            logger.info(f"📅 Начальная дата: {start_date}")
        else:
            logger.info(f"♻️ Режим: продолжение с последнего checkpoint")
        logger.info(f"{'='*60}")

        for timeframe in timeframes:
            if timeframe not in self.timeframe_minutes:
                logger.warning(f"⚠️ Неподдерживаемый таймфрейм: {timeframe}")
                continue

            self.process_timeframe(timeframe, batch_days, start_date)

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Обработка всех таймфреймов завершена!")
        logger.info(f"{'='*60}")

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='EMA Indicator Loader - основной загрузчик EMA индикаторов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python ema_loader.py                          # Загрузка всех таймфреймов из indicators_config.yaml
  python ema_loader.py --timeframe 1m           # Загрузка только 1m таймфрейма
  python ema_loader.py --timeframes 1m,15m,1h   # Загрузка нескольких таймфреймов
  python ema_loader.py --batch-days 3           # Использовать батчи по 3 дня
  python ema_loader.py --start-date 2024-01-01  # Начать с конкретной даты
  python ema_loader.py --symbol ETHUSDT         # Загрузка для другого символа
        """
    )

    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str,
                       help='Один таймфрейм для обработки')
    parser.add_argument('--timeframes', type=str,
                       help='Несколько таймфреймов через запятую (например: 1m,15m,1h)')
    parser.add_argument('--batch-days', type=int, default=1,
                       help='Размер батча в днях (по умолчанию: 1)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD (если не указана, продолжает с checkpoint)')

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

    # Определяем таймфреймы
    timeframes = None
    if args.timeframes:
        timeframes = args.timeframes.split(',')
    elif args.timeframe:
        timeframes = [args.timeframe]

    # Парсим дату если указана
    start_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"❌ Неверный формат даты: {args.start_date}. Используйте YYYY-MM-DD")
            sys.exit(1)

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        # Создаем загрузчик и запускаем для текущего символа
        try:
            loader = EMALoader(symbol=symbol)
            loader.symbol_progress = f"[{idx}/{total_symbols}]"
            loader.run(timeframes, args.batch_days, start_date)
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