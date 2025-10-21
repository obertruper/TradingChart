#!/usr/bin/env python3
"""
RSI (Relative Strength Index) Loader with Enhanced Batch Processing
===================================================================
Загрузчик RSI индикаторов с поддержкой:
- Автоматического определения пустых столбцов
- Раздельной загрузки для разных уровней заполненности
- Множественных периодов (7, 9, 14, 21, 25)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h и т.д.)
- Инкрементальных обновлений
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime, timedelta, timezone
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
    Улучшенный загрузчик RSI индикаторов с автоматическим определением пустых столбцов
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

    def load_config(self):
        """Загружает конфигурацию из файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"📋 Конфигурация загружена из {config_path}")
        return config

    def _parse_timeframes(self) -> dict:
        """Парсит таймфреймы из конфигурации"""
        timeframe_map = {}
        timeframes = self.config.get('timeframes', ['1m'])

        for tf in timeframes:
            import re
            match = re.match(r'^(\d+)([mhdw])$', tf.lower())
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                if unit == 'm':
                    timeframe_map[tf] = number
                elif unit == 'h':
                    timeframe_map[tf] = number * 60
                elif unit == 'd':
                    timeframe_map[tf] = number * 1440
                elif unit == 'w':
                    timeframe_map[tf] = number * 10080

        return timeframe_map

    def create_rsi_columns(self, timeframe: str, periods: List[int]) -> bool:
        """Создает колонки для RSI если их нет"""
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Проверяем существование таблицы
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table_name,))

            if not cur.fetchone()[0]:
                logger.error(f"❌ Таблица {table_name} не существует!")
                return False

            # Получаем существующие колонки
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """, (table_name,))

            existing_columns = {row[0] for row in cur.fetchall()}

            # Добавляем недостающие колонки
            columns_added = []
            for period in periods:
                col_name = f'rsi_{period}'
                if col_name not in existing_columns:
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {col_name} DECIMAL(10,4)
                    """)
                    columns_added.append(col_name)

            if columns_added:
                conn.commit()
                logger.info(f"✅ Добавлены колонки RSI: {columns_added} в таблицу {table_name}")
            else:
                logger.info(f"ℹ️ Все колонки RSI уже существуют в {table_name}")

            return True

    def analyze_rsi_periods(self, timeframe: str, periods: List[int]) -> Dict[str, List[int]]:
        """
        Анализирует заполненность RSI периодов и группирует их по статусу

        Returns:
            Словарь с группировкой периодов:
            - 'empty': пустые периоды (< 50% заполнения)
            - 'partial': частично заполненные (50-95%)
            - 'complete': почти полные (> 95%)
        """
        table_name = f'indicators_bybit_futures_{timeframe}'
        groups = {
            'empty': [],
            'partial': [],
            'complete': []
        }
        checkpoints = {}

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            logger.info(f"\n🔍 Анализ существующих данных RSI:")

            for period in periods:
                col_name = f'rsi_{period}'

                # Получаем статистику заполнения
                cur.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT({col_name}) as filled,
                        MIN(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as first_rsi,
                        MAX(timestamp) FILTER (WHERE {col_name} IS NOT NULL) as last_rsi
                    FROM {table_name}
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()
                if result:
                    total, filled, first_rsi, last_rsi = result
                    if total > 0:
                        fill_percent = (filled / total) * 100 if filled else 0

                        if fill_percent < 50:
                            groups['empty'].append(period)
                            logger.info(f"  ❌ RSI_{period}: {fill_percent:.1f}% заполнено (будет загружен с начала)")
                        elif fill_percent < 95:
                            groups['partial'].append(period)
                            checkpoints[period] = {'date': last_rsi, 'state': {}}
                            logger.info(f"  ⚠️ RSI_{period}: {fill_percent:.1f}% заполнено (продолжение с {last_rsi.strftime('%Y-%m-%d %H:%M') if last_rsi else 'начала'})")
                        else:
                            groups['complete'].append(period)
                            checkpoints[period] = {'date': last_rsi, 'state': {}}
                            logger.info(f"  ✅ RSI_{period}: {fill_percent:.1f}% заполнено (обновление с {last_rsi.strftime('%Y-%m-%d %H:%M') if last_rsi else 'конца'})")
                    else:
                        groups['empty'].append(period)
                        logger.info(f"  📝 RSI_{period}: нет данных в таблице (будет загружен с начала)")
                else:
                    groups['empty'].append(period)
                    logger.info(f"  📝 RSI_{period}: нет данных (будет загружен с начала)")

        # Сохраняем checkpoints для дальнейшего использования
        self.checkpoints = checkpoints

        return groups

    def calculate_rsi_batch(self, closes: np.ndarray, period: int,
                           initial_avg_gain: float = None,
                           initial_avg_loss: float = None):
        """
        Рассчитывает RSI для батча данных

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

        Returns:
            Финальные состояния для следующего батча
        """
        table_name = f'indicators_bybit_futures_{timeframe}'

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Для RSI всегда используем цену закрытия (close)
            price_column = 'close'  # RSI должен рассчитываться по close для всех таймфреймов

            # Загружаем данные
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
                        (array_agg(close ORDER BY timestamp DESC))[1] as close_price
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
                                (array_agg(close ORDER BY timestamp DESC))[1] as close_price
                            FROM (
                                SELECT
                                    date_trunc('hour', timestamp) +
                                    INTERVAL '{interval_minutes} minutes' * (EXTRACT(MINUTE FROM timestamp)::INTEGER / {interval_minutes}) as period_start,
                                    timestamp,
                                    close
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
                        # Берем только RSI для основных данных
                        rsi_values = rsi_values[len(buffer_data):]
                    else:
                        # Недостаточно данных для инициализации
                        rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                            closes, period
                        )
                else:
                    # Используем checkpoint
                    rsi_values, avg_gain, avg_loss = self.calculate_rsi_batch(
                        closes, period, initial_avg_gain, initial_avg_loss
                    )

                rsi_results[period] = rsi_values
                final_states[period] = {
                    'avg_gain': avg_gain,
                    'avg_loss': avg_loss
                }

            # Обновляем данные в БД
            for i, timestamp in enumerate(timestamps):
                updates = []
                params = []

                for period in periods:
                    if i < len(rsi_results[period]):
                        rsi_value = rsi_results[period][i]
                        if not np.isnan(rsi_value):
                            updates.append(f"rsi_{period} = %s")
                            params.append(float(rsi_value))

                if updates:
                    params.extend([self.symbol, timestamp])
                    update_query = f"""
                        UPDATE {table_name}
                        SET {', '.join(updates)}
                        WHERE symbol = %s AND timestamp = %s
                    """
                    cur.execute(update_query, params)

            conn.commit()

        return final_states

    def process_periods_group(self, timeframe: str, periods: List[int],
                             batch_days: int, start_date: Optional[datetime] = None,
                             from_beginning: bool = False):
        """
        Обрабатывает группу периодов RSI

        Args:
            timeframe: Таймфрейм
            periods: Список периодов для обработки
            batch_days: Размер батча в днях
            start_date: Начальная дата
            from_beginning: Начать с самого начала (для пустых периодов)
        """
        if not periods:
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # Определяем начальную дату
            if start_date:
                current_date = start_date
            elif from_beginning:
                # Начинаем с самого начала - из таблицы 1m свечей
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
            else:
                # Используем минимальный checkpoint среди периодов
                valid_dates = []
                for period in periods:
                    if period in self.checkpoints and self.checkpoints[period].get('date'):
                        valid_dates.append(self.checkpoints[period]['date'])

                if valid_dates:
                    current_date = min(valid_dates)
                else:
                    # Если нет checkpoint'ов, начинаем с начала
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

            # Получаем конечную дату
            cur.execute(f"""
                SELECT MAX(timestamp)
                FROM candles_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))
            max_date = cur.fetchone()[0]

            if not max_date or current_date >= max_date:
                logger.info("✅ Данные уже актуальны")
                return

            # Подсчитываем батчи
            total_days = (max_date - current_date).days
            total_batches = (total_days + batch_days - 1) // batch_days

            logger.info(f"   • Период: {total_days} дней ({current_date.strftime('%Y-%m-%d')} → {max_date.strftime('%Y-%m-%d')})")
            logger.info(f"   • Батчей: {total_batches}")

            # Инициализируем состояния
            current_states = {}
            for period in periods:
                if period in self.checkpoints and not from_beginning:
                    current_states[period] = self.checkpoints[period].get('state', {})
                else:
                    current_states[period] = {}

            action = 'Загрузка' if from_beginning else 'Обновление'
            progress_desc = f"{self.symbol} {self.symbol_progress} RSI {periods} {timeframe.upper()}" if self.symbol_progress else f"{self.symbol} RSI {periods} {timeframe.upper()}"
            with tqdm(total=total_batches, desc=f"📊 {progress_desc} - {action}") as pbar:
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
                        # Добавляем информацию о символе в progress bar
                        symbol_info = f"{self.symbol} {self.symbol_progress} " if self.symbol_progress else f"{self.symbol} "
                        pbar.set_description(
                            f"{symbol_info}Батч {batch_num}/{total_batches} "
                            f"(до {batch_end.strftime('%Y-%m-%d %H:%M')})"
                        )
                        pbar.update(1)

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча: {e}")
                        import traceback
                        traceback.print_exc()
                        break

                    current_date = batch_end

    def process_timeframe(self, timeframe: str, batch_days: int = 7,
                         start_date: Optional[datetime] = None):
        """
        Обрабатывает RSI для указанного таймфрейма с автоматическим определением пустых столбцов

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

        # Анализируем заполненность периодов
        groups = self.analyze_rsi_periods(timeframe, periods)

        # Выводим план загрузки
        if groups['empty'] or groups['partial'] or groups['complete']:
            logger.info(f"\n📋 План загрузки:")
            if groups['empty']:
                logger.info(f"  🔄 Полная загрузка с начала для периодов: {groups['empty']}")
            if groups['partial']:
                logger.info(f"  ⏩ Продолжение загрузки для периодов: {groups['partial']}")
            if groups['complete']:
                logger.info(f"  📊 Обновление последних данных для периодов: {groups['complete']}")

        # Обрабатываем пустые периоды (с начала)
        if groups['empty']:
            logger.info(f"\n🚀 Начинаю загрузку пустых периодов RSI: {groups['empty']}")
            self.process_periods_group(timeframe, groups['empty'], batch_days, start_date, from_beginning=True)

        # Обрабатываем частично заполненные периоды (с checkpoint'а)
        if groups['partial']:
            logger.info(f"\n🚀 Продолжаю загрузку частичных периодов RSI: {groups['partial']}")
            self.process_periods_group(timeframe, groups['partial'], batch_days, start_date, from_beginning=False)

        # Обрабатываем полные периоды (обновление последних данных)
        if groups['complete']:
            logger.info(f"\n🚀 Обновляю полные периоды RSI: {groups['complete']}")
            self.process_periods_group(timeframe, groups['complete'], batch_days, start_date, from_beginning=False)

        if not groups['empty'] and not groups['partial'] and not groups['complete']:
            logger.info(f"\n✅ Все периоды RSI для {timeframe} уже актуальны!")

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
            logger.info(f"♻️ Режим: автоматическое определение пустых столбцов")
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
        description='RSI Indicator Loader - загрузчик с автоматическим определением пустых столбцов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python rsi_loader.py                          # Автоматическое определение и загрузка пустых столбцов
  python rsi_loader.py --timeframe 1m           # Загрузка только 1m таймфрейма
  python rsi_loader.py --timeframes 1m,15m,1h   # Загрузка нескольких таймфреймов
  python rsi_loader.py --batch-days 7           # Использовать батчи по 7 дней
  python rsi_loader.py --start-date 2024-01-01  # Принудительно начать с конкретной даты
  python rsi_loader.py --symbol ETHUSDT         # Загрузка для другого символа
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
    parser.add_argument('--batch-days', type=int, default=7,
                       help='Размер батча в днях (по умолчанию: 7)')
    parser.add_argument('--start-date', type=str,
                       help='Начальная дата в формате YYYY-MM-DD (если не указана, автоматическое определение)')

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
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

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
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"❌ Неверный формат даты: {args.start_date}. Используйте YYYY-MM-DD")
            sys.exit(1)

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        # Создаем загрузчик и запускаем для текущего символа
        try:
            loader = RSILoader(symbol=symbol)
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

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")

if __name__ == "__main__":
    main()