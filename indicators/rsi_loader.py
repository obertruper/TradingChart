#!/usr/bin/env python3
"""
RSI (Relative Strength Index) Loader with Batch Processing
===========================================================
Загрузчик RSI индикаторов с поддержкой:
- Множественных периодов (7, 9, 14, 21, 25)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h и т.д.)
- Инкрементальных обновлений
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm
import argparse
import warnings

warnings.filterwarnings('ignore')

from database import DatabaseConnection

# Настройка логирования
def setup_logging():
    """Настраивает логирование с выводом в файл и консоль"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'rsi_{timestamp}.log')

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

    logger.info(f"📝 RSI Loader: Логирование настроено. Лог-файл: {log_file}")
    return logger

logger = setup_logging()

class RSILoader:
    """
    Загрузчик RSI индикаторов с батчевой обработкой и checkpoint системой
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
        self.timeframe_minutes = self._parse_timeframes()

    def _parse_timeframes(self) -> dict:
        """
        Парсит таймфреймы из конфигурации
        """
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m'])

        for tf in timeframes:
            import re
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

    def create_rsi_columns(self, timeframe: str, periods: List[int]) -> bool:
        """
        Создает колонки для RSI периодов в существующей таблице

        Args:
            timeframe: Таймфрейм
            periods: Список периодов RSI
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
                AND column_name LIKE 'rsi_%%'
            """, (table_name,))

            existing_columns = [row[0] for row in cur.fetchall()]

            # Добавляем недостающие колонки
            columns_added = []
            for period in periods:
                col_name = f'rsi_{period}'
                if col_name not in existing_columns:
                    try:
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(10, 4)
                        """)
                        columns_added.append(col_name)
                    except Exception as e:
                        logger.error(f"❌ Ошибка при добавлении колонки {col_name}: {e}")
                        conn.rollback()
                        return False

            if columns_added:
                conn.commit()
                logger.info(f"✅ Добавлены колонки RSI: {', '.join(columns_added)} в {table_name}")
            else:
                logger.info(f"ℹ️ Все колонки RSI уже существуют в {table_name}")

            return True

    def get_last_rsi_checkpoint(self, timeframe: str, period: int) -> Tuple[Optional[datetime], Dict]:
        """
        Получает последнее сохраненное значение RSI и состояние для продолжения расчетов

        Returns:
            Tuple[последний timestamp, словарь с avg_gain и avg_loss]
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        col_name = f'rsi_{period}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Получаем последнюю запись с RSI
            cur.execute(f"""
                SELECT timestamp, {col_name}
                FROM {table_name}
                WHERE symbol = %s AND {col_name} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
            """, (self.symbol,))

            result = cur.fetchone()
            if result:
                # Для восстановления состояния нам нужны предыдущие avg_gain и avg_loss
                # Их можно приблизительно восстановить из RSI, но лучше хранить отдельно
                # Пока возвращаем просто timestamp
                return result[0], {}

            return None, {}

    def calculate_rsi_batch(self, closes: np.ndarray, period: int,
                           initial_avg_gain: float = None,
                           initial_avg_loss: float = None):
        """
        Рассчитывает RSI для батча данных

        Args:
            closes: Массив цен закрытия
            period: Период RSI
            initial_avg_gain: Начальное среднее значение прироста
            initial_avg_loss: Начальное среднее значение потерь

        Returns:
            Tuple[массив RSI, финальный avg_gain, финальный avg_loss]
        """
        if len(closes) < period + 1:
            return np.full(len(closes), np.nan), None, None

        # Рассчитываем изменения цены
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        rsi_values = np.full(len(closes), np.nan)

        # Инициализация или использование checkpoint
        if initial_avg_gain is None or initial_avg_loss is None:
            # Первый расчет - используем SMA для начальных значений
            avg_gain = np.mean(gains[:period])
            avg_loss = np.mean(losses[:period])
            start_idx = period
        else:
            avg_gain = initial_avg_gain
            avg_loss = initial_avg_loss
            start_idx = 0

        # Рассчитываем RSI для каждой точки
        for i in range(start_idx, len(gains)):
            # Сглаженное среднее (Wilder's smoothing)
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            # Рассчитываем RSI
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))

            rsi_values[i + 1] = rsi  # +1 потому что deltas короче на 1

        return rsi_values, avg_gain, avg_loss

    def process_batch(self, timeframe: str, periods: List[int],
                     start_date: datetime, end_date: datetime,
                     initial_states: Dict[int, Dict]) -> Dict[int, Dict]:
        """
        Обрабатывает батч данных и сохраняет RSI в БД

        Args:
            timeframe: Таймфрейм
            periods: Список периодов RSI
            start_date: Начальная дата батча
            end_date: Конечная дата батча
            initial_states: Начальные состояния для каждого периода

        Returns:
            Финальные состояния для следующего батча
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Определяем какую цену использовать
            price_column = 'close' if timeframe == '1m' else 'open'

            # Загружаем данные
            # Для 1m используем таблицу candles, для остальных - агрегируем из 1m данных
            if timeframe == '1m':
                cur.execute(f"""
                    SELECT timestamp, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                    AND timestamp > %s
                    AND timestamp <= %s
                    ORDER BY timestamp
                """, (self.symbol, start_date, end_date))
            else:
                # Для других таймфреймов агрегируем из 1m данных
                interval_minutes = self.timeframe_minutes[timeframe]
                cur.execute(f"""
                    SELECT
                        date_trunc('hour', timestamp) +
                        INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as period_start,
                        (array_agg(open ORDER BY timestamp))[1] as open_price
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                    AND timestamp > %s
                    AND timestamp <= %s
                    GROUP BY period_start
                    ORDER BY period_start
                """, (self.symbol, start_date, end_date))

            data = cur.fetchall()
            if not data:
                return initial_states

            timestamps = [row[0] for row in data]
            closes = np.array([float(row[1]) for row in data])

            # Рассчитываем RSI для каждого периода
            rsi_results = {}
            final_states = {}

            for period in periods:
                initial_state = initial_states.get(period, {})
                initial_avg_gain = initial_state.get('avg_gain')
                initial_avg_loss = initial_state.get('avg_loss')

                # Если нет начального состояния, нужен буфер данных
                if initial_avg_gain is None:
                    # Загружаем дополнительные данные для инициализации
                    buffer_start = start_date - timedelta(minutes=period * self.timeframe_minutes.get(timeframe, 1) * 2)

                    if timeframe == '1m':
                        cur.execute(f"""
                            SELECT close
                            FROM candles_bybit_futures_1m
                            WHERE symbol = %s
                            AND timestamp > %s
                            AND timestamp <= %s
                            ORDER BY timestamp
                        """, (self.symbol, buffer_start, start_date))
                    else:
                        # Для других таймфреймов агрегируем из 1m данных
                        interval_minutes = self.timeframe_minutes[timeframe]
                        cur.execute(f"""
                            SELECT
                                (array_agg(open ORDER BY timestamp))[1] as open_price
                            FROM (
                                SELECT
                                    date_trunc('hour', timestamp) +
                                    INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as period_start,
                                    timestamp,
                                    open
                                FROM candles_bybit_futures_1m
                                WHERE symbol = %s
                                AND timestamp > %s
                                AND timestamp <= %s
                            ) t
                            GROUP BY period_start
                            ORDER BY period_start
                        """, (self.symbol, buffer_start, start_date))

                    buffer_data = [float(row[0]) for row in cur.fetchall()]
                    if len(buffer_data) >= period:
                        # Объединяем буферные данные с основными
                        all_closes = np.concatenate([buffer_data, closes])
                        rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                            all_closes, period
                        )
                        # Берем только значения для основного батча
                        rsi_results[period] = rsi_values[len(buffer_data):]
                    else:
                        # Недостаточно данных для инициализации
                        logger.warning(f"⚠️ Недостаточно данных для инициализации RSI_{period}")
                        rsi_results[period] = np.full(len(closes), np.nan)
                        avg_gain, avg_loss = None, None
                else:
                    # Используем сохраненное состояние
                    rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                        closes, period, initial_avg_gain, initial_avg_loss
                    )
                    rsi_results[period] = rsi_values

                # Сохраняем финальное состояние
                final_states[period] = {
                    'avg_gain': avg_gain,
                    'avg_loss': avg_loss
                }

            # Сохраняем в БД батчевым UPDATE
            for i, timestamp in enumerate(timestamps):
                set_clause_parts = []
                params = []

                for period in periods:
                    rsi_value = rsi_results[period][i]
                    if not np.isnan(rsi_value):
                        set_clause_parts.append(f"rsi_{period} = %s")
                        params.append(float(rsi_value))

                if set_clause_parts:
                    set_clause = ', '.join(set_clause_parts)
                    params.extend([timestamp, self.symbol])

                    cur.execute(f"""
                        UPDATE {table_name}
                        SET {set_clause}
                        WHERE timestamp = %s AND symbol = %s
                    """, params)

            conn.commit()

        return final_states

    def process_timeframe(self, timeframe: str, batch_days: int = 7,
                         start_date: Optional[datetime] = None):
        """
        Обрабатывает RSI для указанного таймфрейма с батчевой загрузкой

        Args:
            timeframe: Таймфрейм
            batch_days: Размер батча в днях
            start_date: Начальная дата (если None, продолжаем с checkpoint)
        """
        # Получаем периоды из конфига
        periods = self.config.get('indicators', {}).get('rsi', {}).get('periods', [14])
        batch_days = self.config.get('indicators', {}).get('rsi', {}).get('batch_days', batch_days)

        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Обработка RSI для таймфрейма {timeframe}")
        logger.info(f"📈 Периоды RSI: {periods}")
        logger.info(f"🎯 Символ: {self.symbol}")
        logger.info(f"📦 Размер батча: {batch_days} дней")
        logger.info(f"{'='*60}")

        # Создаем колонки если нужно
        if not self.create_rsi_columns(timeframe, periods):
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Определяем начальную точку
            checkpoints = {}
            latest_checkpoint = None

            logger.info("\n🔍 Проверка существующих данных (checkpoint):")
            for period in periods:
                last_timestamp, state = self.get_last_rsi_checkpoint(timeframe, period)
                checkpoints[period] = {
                    'last_timestamp': last_timestamp,
                    'state': state
                }

                if last_timestamp:
                    logger.info(f"  ✅ RSI_{period}: продолжение с {last_timestamp}")
                    if not latest_checkpoint or last_timestamp > latest_checkpoint:
                        latest_checkpoint = last_timestamp
                else:
                    logger.info(f"  📝 RSI_{period}: нет данных (будет рассчитан с начала)")

            # Определяем начальную дату
            if start_date:
                current_date = start_date
            elif latest_checkpoint:
                current_date = latest_checkpoint
            else:
                # Начинаем с самого начала - всегда из таблицы 1m свечей
                cur.execute(f"""
                    SELECT MIN(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))
                result = cur.fetchone()
                if result and result[0]:
                    current_date = result[0]
                else:
                    logger.error("❌ Нет данных для обработки")
                    return

            # Получаем конечную дату - всегда из таблицы 1m свечей
            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            max_date = cur.fetchone()[0]

            if not max_date or current_date >= max_date:
                logger.info("✅ Данные уже актуальны")
                return

            # Инициализируем состояния
            initial_states = {}
            for period, checkpoint in checkpoints.items():
                initial_states[period] = checkpoint.get('state', {})

            # Подсчитываем батчи
            total_days = (max_date - current_date).days
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"\n📊 План загрузки:")
            logger.info(f"   • Период: {total_days} дней ({current_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')})")
            logger.info(f"   • Батчей: {total_batches}")
            logger.info(f"   • Таймфрейм: {timeframe}")
            logger.info(f"   • RSI периоды: {periods}")

            # Обрабатываем батчами
            logger.info(f"\n🚀 Начинаю обработку...")
            current_states = initial_states.copy()

            with tqdm(total=total_batches, desc="Обработка батчей") as pbar:
                batch_num = 0

                while current_date < max_date:
                    batch_end = min(current_date + timedelta(days=batch_days), max_date)

                    try:
                        # Обрабатываем батч
                        current_states = self.process_batch(
                            timeframe, periods,
                            current_date, batch_end,
                            current_states
                        )

                        batch_num += 1
                        pbar.set_description(
                            f"Батч {batch_num}/{total_batches} "
                            f"(до {batch_end.strftime('%Y-%m-%d %H:%M')})"
                        )
                        pbar.update(1)

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча: {e}")
                        import traceback
                        traceback.print_exc()
                        break

                    current_date = batch_end

            logger.info(f"\n✅ Обработка RSI для {timeframe} завершена!")

    def run(self, timeframes: List[str] = None, batch_days: int = 7,
            start_date: Optional[datetime] = None):
        """
        Запускает обработку RSI для указанных таймфреймов

        Args:
            timeframes: Список таймфреймов (если None, берет из конфига)
            batch_days: Размер батча в днях
            start_date: Начальная дата
        """
        if not timeframes:
            timeframes = self.config.get('timeframes', ['1m'])

        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Запуск RSI Loader")
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
        description='RSI Indicator Loader - загрузчик индикатора RSI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python rsi_loader.py                          # Загрузка всех таймфреймов из indicators_config.yaml
  python rsi_loader.py --timeframe 1m           # Загрузка только 1m таймфрейма
  python rsi_loader.py --timeframes 1m,15m,1h   # Загрузка нескольких таймфреймов
  python rsi_loader.py --batch-days 7           # Использовать батчи по 7 дней
  python rsi_loader.py --start-date 2024-01-01  # Начать с конкретной даты
  python rsi_loader.py --symbol ETHUSDT         # Загрузка для другого символа
        """
    )

    parser.add_argument('--symbol', type=str, default='BTCUSDT',
                       help='Торговый символ (по умолчанию: BTCUSDT)')
    parser.add_argument('--timeframe', type=str,
                       help='Один таймфрейм для обработки')
    parser.add_argument('--timeframes', type=str,
                       help='Несколько таймфреймов через запятую (например: 1m,15m,1h)')
    parser.add_argument('--batch-days', type=int, default=7,
                       help='Размер батча в днях (по умолчанию: 7)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD (если не указана, продолжает с checkpoint)')

    args = parser.parse_args()

    # Определяем таймфреймы
    timeframes = None
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = args.timeframes.split(',')

    # Парсим дату если указана
    start_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"❌ Неверный формат даты: {args.start_date}. Используйте YYYY-MM-DD")
            sys.exit(1)

    # Создаем загрузчик и запускаем
    try:
        loader = RSILoader(symbol=args.symbol)
        loader.run(timeframes, args.batch_days, start_date)
    except KeyboardInterrupt:
        logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()