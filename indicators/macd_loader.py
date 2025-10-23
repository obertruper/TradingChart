#!/usr/bin/env python3
"""
MACD (Moving Average Convergence Divergence) Loader with Multi-Timeframe Support
=================================================================================
Загрузчик MACD индикаторов с поддержкой:
- Множественных конфигураций (classic, crypto, aggressive, balanced, scalping, swing, longterm, ultralong)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h)
- Инкрементальных обновлений
- Последовательной обработки конфигураций (можно прервать)
- Независимого расчёта EMA (не зависит от ema_loader.py)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
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

    log_filename = os.path.join(log_dir, f'macd_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

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


class MACDLoader:
    """Загрузчик MACD (Moving Average Convergence Divergence) для разных таймфреймов"""

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

            if not match:
                logger.warning(f"Неизвестный таймфрейм: {tf}, пропускаем")
                continue

            value = int(match.group(1))
            unit = match.group(2)

            # Конвертируем в минуты
            if unit == 'm':
                minutes = value
            elif unit == 'h':
                minutes = value * 60
            elif unit == 'd':
                minutes = value * 60 * 24
            elif unit == 'w':
                minutes = value * 60 * 24 * 7
            else:
                logger.warning(f"Неподдерживаемая единица времени: {unit}")
                continue

            timeframe_map[tf] = minutes

        logger.info(f"Распарсенные таймфреймы: {timeframe_map}")
        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из YAML файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config

    def get_table_name(self, timeframe: str) -> str:
        """Получить имя таблицы индикаторов для таймфрейма"""
        return f"indicators_bybit_futures_{timeframe}"

    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """
        Рассчитывает EMA для массива цен

        Формула: EMA = Price × k + EMA_prev × (1 - k)
        где k = 2 / (period + 1)

        Args:
            prices: Series с ценами
            period: Период EMA

        Returns:
            Series с EMA значениями
        """
        return prices.ewm(span=period, adjust=False).mean()

    def calculate_macd(self, close_prices: pd.Series, fast: int,
                       slow: int, signal: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Рассчитывает MACD, Signal Line, Histogram

        Args:
            close_prices: Series с ценами закрытия
            fast: Период быстрой EMA
            slow: Период медленной EMA
            signal: Период сигнальной линии

        Returns:
            Tuple[macd_line, signal_line, histogram]
        """
        # Шаг 1: Рассчитываем Fast EMA
        ema_fast = self.calculate_ema(close_prices, fast)

        # Шаг 2: Рассчитываем Slow EMA
        ema_slow = self.calculate_ema(close_prices, slow)

        # Шаг 3: MACD Line = Fast EMA - Slow EMA
        macd_line = ema_fast - ema_slow

        # Шаг 4: Signal Line = EMA от MACD Line
        signal_line = self.calculate_ema(macd_line, signal)

        # Шаг 5: Histogram = MACD Line - Signal Line
        histogram = macd_line - signal_line

        return macd_line, signal_line, histogram

    def ensure_macd_columns(self, timeframe: str, configs: List[Dict]):
        """
        Создает колонки для MACD конфигураций если их нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            configs: Список конфигураций MACD
        """
        table_name = self.get_table_name(timeframe)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for config in configs:
                    fast = config['fast']
                    slow = config['slow']
                    signal_period = config['signal']

                    # Формируем имена колонок
                    base_name = f"macd_{fast}_{slow}_{signal_period}"
                    columns = [
                        f"{base_name}_line",
                        f"{base_name}_signal",
                        f"{base_name}_histogram"
                    ]

                    for col_name in columns:
                        # Проверяем существование колонки
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_schema = 'public'
                                AND table_name = %s
                                AND column_name = %s
                            );
                        """, (table_name, col_name))

                        exists = cur.fetchone()[0]

                        if not exists:
                            logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                            cur.execute(f"""
                                ALTER TABLE {table_name}
                                ADD COLUMN {col_name} DECIMAL(20,8);
                            """)
                            logger.info(f"✅ Колонка {col_name} создана")

                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при создании колонок: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def get_data_range(self, timeframe: str) -> Tuple[datetime, datetime]:
        """
        Получает диапазон доступных данных в базовой таблице свечей
        Всегда читает из candles_bybit_futures_1m, т.к. старшие таймфреймы
        агрегируются из минутных данных

        Returns:
            (min_date, max_date)
        """
        # Всегда используем базовую таблицу 1m для получения диапазона
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute("""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                """, (self.symbol,))

                result = cur.fetchone()

                if not result or result[0] is None:
                    raise ValueError(f"Нет данных для {self.symbol} в candles_bybit_futures_1m")

                return result[0], result[1]
            finally:
                cur.close()

    def get_last_macd_date(self, timeframe: str, config: Dict) -> Optional[datetime]:
        """
        Получает последнюю дату с рассчитанным MACD для конфигурации

        Args:
            timeframe: Таймфрейм
            config: Конфигурация MACD

        Returns:
            Последняя дата или None
        """
        table_name = self.get_table_name(timeframe)

        fast = config['fast']
        slow = config['slow']
        signal_period = config['signal']
        col_name = f"macd_{fast}_{slow}_{signal_period}_line"

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                cur.execute(f"""
                    SELECT MAX(timestamp)
                    FROM {table_name}
                    WHERE symbol = %s AND {col_name} IS NOT NULL
                """, (self.symbol,))

                result = cur.fetchone()
                return result[0] if result[0] else None
            finally:
                cur.close()

    def get_last_complete_period(self, current_time: datetime, timeframe: str) -> datetime:
        """
        Получает timestamp последнего ЗАВЕРШЕННОГО периода

        Args:
            current_time: Текущее время (timezone-aware)
            timeframe: Таймфрейм

        Returns:
            Timestamp последнего завершенного периода
        """
        minutes = self.timeframe_minutes[timeframe]

        if timeframe == '1m':
            # Для минут: просто откатываем на 1 минуту назад
            return current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)

        elif timeframe == '15m':
            # Для 15m: округляем вниз до ближайших 15 минут
            minute = (current_time.minute // 15) * 15
            result = current_time.replace(minute=minute, second=0, microsecond=0)

            # Если мы точно на границе периода, проверяем завершенность
            if current_time.minute % 15 == 0 and current_time.second == 0:
                # Отступаем на 15 минут назад
                result -= timedelta(minutes=15)

            return result

        elif timeframe == '1h':
            # Для часов: округляем вниз до начала часа
            result = current_time.replace(minute=0, second=0, microsecond=0)

            # Если мы точно на границе часа
            if current_time.minute == 0 and current_time.second == 0:
                result -= timedelta(hours=1)

            return result

        else:
            # Для других таймфреймов: общая логика
            total_minutes = int(current_time.timestamp() / 60)
            period_start_minutes = (total_minutes // minutes) * minutes
            result = datetime.fromtimestamp(period_start_minutes * 60)

            # Если на границе - отступаем на период назад
            if total_minutes % minutes == 0:
                result -= timedelta(minutes=minutes)

            return result

    def aggregate_candles(self, start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """
        Агрегирует минутные свечи в нужный таймфрейм или читает напрямую

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            timeframe: Целевой таймфрейм

        Returns:
            DataFrame с колонками: timestamp, symbol, close
        """
        with self.db.get_connection() as conn:
            if timeframe == '1m':
                # Для минутных данных читаем напрямую
                query = """
                    SELECT timestamp, symbol, close
                    FROM candles_bybit_futures_1m
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))
            else:
                # Для старших таймфреймов агрегируем из минутных
                minutes = self.timeframe_minutes[timeframe]

                query = f"""
                    WITH time_groups AS (
                        SELECT
                            timestamp,
                            DATE_TRUNC('hour', timestamp) +
                            INTERVAL '1 minute' * (FLOOR(EXTRACT(MINUTE FROM timestamp) / {minutes}) * {minutes}) as period_start,
                            close,
                            symbol
                        FROM candles_bybit_futures_1m
                        WHERE symbol = %s
                          AND timestamp >= %s
                          AND timestamp <= %s
                    )
                    SELECT
                        period_start + INTERVAL '{minutes} minutes' as timestamp,
                        symbol,
                        (ARRAY_AGG(close ORDER BY timestamp DESC))[1] as close
                    FROM time_groups
                    GROUP BY period_start, symbol
                    ORDER BY period_start
                """

                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            return df

    def save_macd_to_db(self, df: pd.DataFrame, table_name: str, config: Dict):
        """
        Сохраняет MACD компоненты в базу данных

        Args:
            df: DataFrame с MACD компонентами
            table_name: Имя таблицы
            config: Конфигурация MACD
        """
        fast = config['fast']
        slow = config['slow']
        signal_period = config['signal']

        base_name = f"macd_{fast}_{slow}_{signal_period}"
        col_line = f"{base_name}_line"
        col_signal = f"{base_name}_signal"
        col_histogram = f"{base_name}_histogram"

        # Фильтруем только строки с не-NULL значениями
        df_to_save = df[df[col_line].notna()].copy()

        if len(df_to_save) == 0:
            # Не выводим warning, чтобы не мешать tqdm
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Используем batch update для производительности
                update_query = f"""
                    UPDATE {table_name}
                    SET {col_line} = %s,
                        {col_signal} = %s,
                        {col_histogram} = %s
                    WHERE timestamp = %s AND symbol = %s
                """

                # Подготавливаем данные для batch update
                data = [
                    (
                        float(row[col_line]) if pd.notna(row[col_line]) else None,
                        float(row[col_signal]) if pd.notna(row[col_signal]) else None,
                        float(row[col_histogram]) if pd.notna(row[col_histogram]) else None,
                        row['timestamp'],
                        row['symbol']
                    )
                    for _, row in df_to_save.iterrows()
                ]

                # Выполняем batch update
                psycopg2.extras.execute_batch(cur, update_query, data, page_size=1000)
                conn.commit()

                # Логирование убрано, чтобы не мешать tqdm progress bar

            except Exception as e:
                logger.error(f"Ошибка при сохранении MACD {config['name']}: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def calculate_and_save_macd(self, timeframe: str, configs: List[Dict], batch_days: int = 1):
        """
        Последовательно рассчитывает и сохраняет MACD для каждой конфигурации

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            configs: Список конфигураций MACD
            batch_days: Размер батча в днях
        """
        table_name = self.get_table_name(timeframe)

        logger.info(f"🚀 Начало расчета MACD для {self.symbol} на таймфрейме {timeframe}")
        logger.info(f"📊 Конфигурации: {[c['name'] for c in configs]}")
        logger.info(f"📦 Размер батча: {batch_days} дней")

        # Получаем диапазон доступных данных
        min_date, max_date = self.get_data_range(timeframe)
        logger.info(f"📅 Диапазон данных в БД: {min_date} - {max_date}")

        # Определяем последний завершенный период (используем max_date из БД как текущее время)
        last_complete_period = self.get_last_complete_period(max_date, timeframe)

        # Ограничиваем max_date последним завершенным периодом
        if max_date > last_complete_period:
            logger.info(f"⏸️  Ограничение max_date до последнего завершенного периода: {last_complete_period}")
            max_date = last_complete_period

        # Получаем lookback multiplier из конфига
        lookback_multiplier = self.config['indicators']['macd'].get('lookback_multiplier', 3)

        # Последовательная обработка каждой конфигурации
        for config in configs:
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 Обработка конфигурации: {config['name']} ({config['fast']}, {config['slow']}, {config['signal']})")
            logger.info(f"{'='*80}")

            # Находим последнюю дату с данными для этой конфигурации
            last_date = self.get_last_macd_date(timeframe, config)

            if last_date:
                # Начинаем с дня после последнего
                start_date = last_date + timedelta(days=1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                logger.info(f"📅 Последняя дата MACD {config['name']}: {last_date}")
                logger.info(f"▶️  Продолжаем с: {start_date}")
            else:
                # Начинаем с самого начала
                start_date = min_date
                logger.info(f"🆕 MACD {config['name']} пуст, начинаем с начала: {start_date}")

            # Если уже все обработано
            if start_date > max_date:
                logger.info(f"✅ MACD {config['name']} уже актуален (до {max_date})")
                continue

            # Рассчитываем количество дней для обработки
            total_days = (max_date.date() - start_date.date()).days + 1
            logger.info(f"📆 Всего дней для обработки: {total_days}")

            # Обрабатываем батчами
            current_date = start_date
            processed_days = 0
            total_records = 0  # Счетчик обработанных записей

            # Lookback для корректного расчета = max(slow, signal) × lookback_multiplier × timeframe_minutes
            lookback_periods = max(config['slow'], config['signal']) * lookback_multiplier
            lookback_minutes = lookback_periods * self.timeframe_minutes[timeframe]
            lookback_delta = timedelta(minutes=lookback_minutes)

            logger.info(f"🔙 Lookback период: {lookback_minutes} минут ({lookback_periods} периодов × {self.timeframe_minutes[timeframe]} мин)")

            with tqdm(total=total_days,
                     desc=f"📊 {self.symbol} {self.symbol_progress}MACD {config['name']} ({config['fast']}, {config['slow']}, {config['signal']}) {timeframe.upper()}",
                     unit="day",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}') as pbar:
                while current_date <= max_date:
                    # Определяем конец батча
                    batch_end = min(
                        current_date + timedelta(days=batch_days) - timedelta(seconds=1),
                        max_date
                    )

                    # Добавляем lookback к началу для корректного расчета
                    data_start = current_date - lookback_delta

                    try:
                        # Загружаем данные с lookback
                        df = self.aggregate_candles(data_start, batch_end, timeframe)

                        if len(df) == 0:
                            # Пропускаем пустые батчи без вывода в консоль (не мешаем tqdm)
                            current_date += timedelta(days=batch_days)
                            processed_days += batch_days
                            pbar.update(min(batch_days, total_days - processed_days + batch_days))
                            continue

                        # Рассчитываем MACD
                        macd_line, signal_line, histogram = self.calculate_macd(
                            df['close'],
                            config['fast'],
                            config['slow'],
                            config['signal']
                        )

                        # Добавляем к DataFrame
                        base_name = f"macd_{config['fast']}_{config['slow']}_{config['signal']}"
                        df[f'{base_name}_line'] = macd_line
                        df[f'{base_name}_signal'] = signal_line
                        df[f'{base_name}_histogram'] = histogram

                        # Фильтруем только целевой диапазон (без lookback)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # Сохраняем с retry логикой
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                self.save_macd_to_db(df_to_save, table_name, config)
                                break
                            except Exception as e:
                                if attempt < max_retries - 1:
                                    wait_time = 2 ** attempt
                                    # Не выводим warning в консоль, чтобы не мешать tqdm
                                    time.sleep(wait_time)
                                else:
                                    # Только при полном провале выводим ошибку (это критично)
                                    logger.error(f"❌ Все {max_retries} попытки не удались для батча {current_date.date()}")
                                    raise

                        # Обновляем прогресс
                        days_in_batch = min(batch_days, (max_date.date() - current_date.date()).days + 1)
                        processed_days += days_in_batch
                        total_records += len(df_to_save)
                        pbar.update(days_in_batch)

                        # Обновляем информацию в progress bar
                        if not df_to_save.empty:
                            latest_timestamp = df_to_save['timestamp'].max()
                            pbar.set_postfix({
                                'всего': f'{total_records:,}',
                                'последняя': latest_timestamp.strftime('%Y-%m-%d %H:%M')
                            })

                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке батча {current_date.date()}: {e}")
                        raise

                    # Переходим к следующему батчу
                    current_date += timedelta(days=batch_days)

            logger.info(f"✅ MACD {config['name']} завершен: {total_records:,} записей обработано за {processed_days} дней")

        logger.info(f"\n{'='*80}")
        logger.info(f"🎉 Все конфигурации MACD для {timeframe} завершены!")
        logger.info(f"{'='*80}")

    def run(self, timeframe: str = None, batch_days: int = None):
        """
        Запуск загрузки MACD

        Args:
            timeframe: Конкретный таймфрейм или None для всех
            batch_days: Размер батча в днях
        """
        # Получаем параметры из конфига
        macd_config = self.config['indicators']['macd']
        configs = macd_config['configurations']

        if batch_days is None:
            batch_days = macd_config.get('batch_days', 1)

        # Определяем таймфреймы для обработки
        if timeframe:
            timeframes = [timeframe]
        else:
            timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        logger.info(f"🚀 Запуск MACD Loader для {self.symbol}")
        logger.info(f"⏰ Таймфреймы: {timeframes}")
        logger.info(f"📊 Конфигурации: {[c['name'] for c in configs]}")
        logger.info(f"📦 Batch size: {batch_days} дней")

        for tf in timeframes:
            logger.info(f"\n{'#'*80}")
            logger.info(f"⏰ Таймфрейм: {tf}")
            logger.info(f"{'#'*80}")

            # Создаем колонки если нужно
            self.ensure_macd_columns(tf, configs)

            # Рассчитываем и сохраняем MACD
            self.calculate_and_save_macd(tf, configs, batch_days)

        logger.info(f"\n{'#'*80}")
        logger.info(f"🎉 Загрузка MACD завершена для всех таймфреймов!")
        logger.info(f"{'#'*80}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='MACD Loader для разных таймфреймов')
    parser.add_argument('--symbol', type=str, default=None,
                       help='Одна торговая пара (например, BTCUSDT)')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Несколько торговых пар через запятую (например, BTCUSDT,ETHUSDT)')
    parser.add_argument('--timeframe', type=str, help='Конкретный таймфрейм (1m, 15m, 1h)')
    parser.add_argument('--batch-days', type=int, help='Размер батча в днях')

    args = parser.parse_args()

    # Определяем символы для обработки
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    elif args.symbol:
        symbols = [args.symbol]
    else:
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')
        if os.path.exists(config_path):
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                symbols = config.get('symbols', ['BTCUSDT'])
        else:
            symbols = ['BTCUSDT']

    logger.info(f"🎯 Обработка символов: {symbols}")

    # Засекаем время начала обработки
    start_time = time.time()

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        loader = MACDLoader(symbol=symbol)
        loader.symbol_progress = f"[{idx}/{total_symbols}] "
        loader.run(timeframe=args.timeframe, batch_days=args.batch_days)

        logger.info(f"\n✅ Символ {symbol} обработан\n")

    # Вычисляем общее время обработки
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")
    logger.info(f"⏱️  Total time: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()
