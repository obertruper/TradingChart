#!/usr/bin/env python3
"""
ATR (Average True Range) Loader with Multi-Timeframe Support
==============================================================
Загрузчик ATR индикаторов с поддержкой:
- Множественных периодов (7, 14, 21, 30, 50, 100)
- Батчевой обработки с checkpoint
- Любых таймфреймов (1m, 15m, 1h)
- Инкрементальных обновлений
- Последовательной обработки периодов (можно прервать)
- Сглаживания Уайлдера для плавности
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

    log_filename = os.path.join(log_dir, f'atr_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

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


class ATRLoader:
    """Загрузчик ATR (Average True Range) для разных таймфреймов"""

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
            if match:
                number = int(match.group(1))
                unit = match.group(2)

                # Конвертируем в минуты
                if unit == 'm':
                    minutes = number
                elif unit == 'h':
                    minutes = number * 60
                elif unit == 'd':
                    minutes = number * 60 * 24
                elif unit == 'w':
                    minutes = number * 60 * 24 * 7
                else:
                    logger.warning(f"Неизвестная единица времени: {unit}")
                    continue

                timeframe_map[tf] = minutes
            else:
                logger.warning(f"Не удалось распарсить таймфрейм: {tf}")

        logger.info(f"Распарсенные таймфреймы: {timeframe_map}")
        return timeframe_map

    def load_config(self) -> dict:
        """Загружает конфигурацию из YAML файла"""
        config_path = os.path.join(os.path.dirname(__file__), 'indicators_config.yaml')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return config

    def get_table_name(self, timeframe: str) -> str:
        """Получить имя таблицы для таймфрейма"""
        return f"indicators_bybit_futures_{timeframe}"

    def get_candles_table_name(self, timeframe: str) -> str:
        """Получить имя таблицы свечей для таймфрейма"""
        return f"candles_bybit_futures_{timeframe}"

    def ensure_atr_columns(self, timeframe: str, periods: List[int]):
        """
        Создает колонки для ATR если их нет

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов ATR
        """
        table_name = self.get_table_name(timeframe)

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                for period in periods:
                    col_name = f'atr_{period}'

                    # Проверяем существование колонки
                    cur.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s AND column_name = %s
                    """, (table_name, col_name))

                    if cur.fetchone() is None:
                        logger.info(f"➕ Создание колонки {col_name} в таблице {table_name}")
                        cur.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col_name} DECIMAL(20,8)
                        """)
                        conn.commit()
                        logger.info(f"✅ Колонка {col_name} создана")

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

    def get_last_atr_date(self, timeframe: str, period: int) -> Optional[datetime]:
        """
        Получает последнюю дату с рассчитанным ATR для конкретного периода

        Args:
            timeframe: Таймфрейм
            period: Период ATR

        Returns:
            Последняя дата или None
        """
        table_name = self.get_table_name(timeframe)
        col_name = f'atr_{period}'

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
            current_time: Текущее время
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
            DataFrame с колонками: timestamp, symbol, high, low, close
        """
        with self.db.get_connection() as conn:
            if timeframe == '1m':
                # Для минутных данных читаем напрямую
                query = """
                    SELECT timestamp, symbol, high, low, close
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
                            high,
                            low,
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
                        MAX(high) as high,
                        MIN(low) as low,
                        (ARRAY_AGG(close ORDER BY period_start DESC))[1] as close
                    FROM time_groups
                    GROUP BY period_start, symbol
                    ORDER BY period_start
                """

                df = pd.read_sql_query(query, conn, params=(self.symbol, start_date, end_date))

            return df

    def calculate_true_range(self, df: pd.DataFrame) -> pd.Series:
        """
        Расчет True Range для каждой свечи
        TR = max(High - Low, |High - Previous Close|, |Low - Previous Close|)

        Args:
            df: DataFrame с колонками high, low, close

        Returns:
            Series с True Range
        """
        # Получаем предыдущее закрытие
        df = df.copy()
        df['prev_close'] = df['close'].shift(1)

        # Три компонента True Range
        df['hl'] = df['high'] - df['low']
        df['hc'] = abs(df['high'] - df['prev_close'])
        df['lc'] = abs(df['low'] - df['prev_close'])

        # True Range = максимум из трех
        df['tr'] = df[['hl', 'hc', 'lc']].max(axis=1)

        # Для первой свечи (где нет prev_close) используем просто hl
        df.loc[df['prev_close'].isna(), 'tr'] = df.loc[df['prev_close'].isna(), 'hl']

        return df['tr']

    def calculate_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Расчет ATR используя сглаживание Уайлдера

        Первое значение = SMA(TR, period)
        Последующие: ATR = (ATR_prev × (period - 1) + TR_current) / period

        Args:
            df: DataFrame с колонкой 'tr' (True Range)
            period: Период ATR

        Returns:
            Series с ATR значениями
        """
        df = df.copy()

        # Проверяем наличие достаточного количества данных
        if len(df) < period:
            logger.warning(f"Недостаточно данных для расчета ATR_{period}: {len(df)} < {period}")
            return pd.Series([np.nan] * len(df), index=df.index)

        # Рассчитываем ATR
        atr_values = []

        # Первое значение = простое среднее первых period значений TR
        first_atr = df['tr'].iloc[:period].mean()

        # Заполняем NaN для первых period-1 значений
        atr_values = [np.nan] * (period - 1)
        atr_values.append(first_atr)

        # Последующие значения с сглаживанием Уайлдера
        for i in range(period, len(df)):
            current_atr = (atr_values[-1] * (period - 1) + df['tr'].iloc[i]) / period
            atr_values.append(current_atr)

        return pd.Series(atr_values, index=df.index)

    def save_single_column_to_db(self, df: pd.DataFrame, table_name: str, period: int):
        """
        Сохраняет одну колонку ATR в базу данных

        Args:
            df: DataFrame с колонкой atr_{period}
            table_name: Имя таблицы
            period: Период ATR
        """
        col_name = f'atr_{period}'

        # Фильтруем только строки с не-NULL значениями
        df_to_save = df[df[col_name].notna()].copy()

        if len(df_to_save) == 0:
            logger.warning(f"Нет данных для сохранения в {col_name}")
            return

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            try:
                # Используем batch update для производительности
                update_query = f"""
                    UPDATE {table_name}
                    SET {col_name} = %s
                    WHERE timestamp = %s AND symbol = %s
                """

                # Подготавливаем данные для batch update
                data = [
                    (float(row[col_name]), row['timestamp'], row['symbol'])
                    for _, row in df_to_save.iterrows()
                ]

                # Выполняем batch update
                psycopg2.extras.execute_batch(cur, update_query, data, page_size=1000)
                conn.commit()

                # Логирование убрано, чтобы не мешать tqdm progress bar
                # Информация о сохранении будет отображаться через pbar.set_postfix()

            except Exception as e:
                logger.error(f"Ошибка при сохранении {col_name}: {e}")
                conn.rollback()
                raise
            finally:
                cur.close()

    def calculate_and_save_atr(self, timeframe: str, periods: List[int], batch_days: int = 1):
        """
        Последовательно рассчитывает и сохраняет ATR для каждого периода

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)
            periods: Список периодов ATR
            batch_days: Размер батча в днях
        """
        table_name = self.get_table_name(timeframe)

        logger.info(f"🚀 Начало расчета ATR для {self.symbol} на таймфрейме {timeframe}")
        logger.info(f"📊 Периоды: {periods}")
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

        # Последовательная обработка каждого периода
        for period in periods:
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 Обработка периода: ATR_{period}")
            logger.info(f"{'='*80}")

            # Находим последнюю дату с данными для этого периода
            last_date = self.get_last_atr_date(timeframe, period)

            if last_date:
                # Начинаем с дня после последнего
                start_date = last_date + timedelta(days=1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                logger.info(f"📅 Последняя дата ATR_{period}: {last_date}")
                logger.info(f"▶️  Продолжаем с: {start_date}")
            else:
                # Начинаем с самого начала
                start_date = min_date
                logger.info(f"🆕 ATR_{period} пуст, начинаем с начала: {start_date}")

            # Если уже все обработано
            if start_date > max_date:
                logger.info(f"✅ ATR_{period} уже актуален (до {max_date})")
                continue

            # Рассчитываем количество дней для обработки
            total_days = (max_date.date() - start_date.date()).days + 1
            logger.info(f"📆 Всего дней для обработки: {total_days}")

            # Обрабатываем батчами
            current_date = start_date
            processed_days = 0
            total_records = 0  # Счетчик обработанных записей

            # Lookback для корректного расчета = period × 2 × timeframe_minutes
            lookback_minutes = period * 2 * self.timeframe_minutes[timeframe]
            lookback_delta = timedelta(minutes=lookback_minutes)

            logger.info(f"🔙 Lookback период: {lookback_minutes} минут ({period} × 2 × {self.timeframe_minutes[timeframe]})")

            with tqdm(total=total_days,
                     desc=f"{self.symbol} {self.symbol_progress} {timeframe.upper()} ATR-{period}",
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

                        # Рассчитываем True Range
                        df['tr'] = self.calculate_true_range(df)

                        # Рассчитываем ATR
                        df[f'atr_{period}'] = self.calculate_atr(df, period)

                        # Фильтруем только целевой диапазон (без lookback)
                        df_to_save = df[df['timestamp'] >= current_date].copy()

                        # Сохраняем с retry логикой
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                self.save_single_column_to_db(df_to_save, table_name, period)
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

            logger.info(f"✅ ATR_{period} завершен: {total_records:,} записей обработано за {processed_days} дней")

        logger.info(f"\n{'='*80}")
        logger.info(f"🎉 Все периоды ATR для {timeframe} завершены!")
        logger.info(f"{'='*80}")

    def run(self, timeframe: str = None, batch_days: int = None):
        """
        Запуск загрузки ATR

        Args:
            timeframe: Конкретный таймфрейм или None для всех
            batch_days: Размер батча в днях
        """
        # Получаем параметры из конфига
        atr_config = self.config['indicators']['atr']
        periods = atr_config['periods']

        if batch_days is None:
            batch_days = atr_config.get('batch_days', 1)

        # Определяем таймфреймы для обработки
        if timeframe:
            timeframes = [timeframe]
        else:
            timeframes = self.config.get('timeframes', ['1m', '15m', '1h'])

        logger.info(f"🚀 Запуск ATR Loader для {self.symbol}")
        logger.info(f"⏰ Таймфреймы: {timeframes}")
        logger.info(f"📊 Периоды: {periods}")
        logger.info(f"📦 Batch size: {batch_days} дней")

        for tf in timeframes:
            logger.info(f"\n{'#'*80}")
            logger.info(f"⏰ Таймфрейм: {tf}")
            logger.info(f"{'#'*80}")

            # Создаем колонки если нужно
            self.ensure_atr_columns(tf, periods)

            # Рассчитываем и сохраняем ATR
            self.calculate_and_save_atr(tf, periods, batch_days)

        logger.info(f"\n{'#'*80}")
        logger.info(f"🎉 Загрузка ATR завершена для всех таймфреймов!")
        logger.info(f"{'#'*80}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='ATR Loader для разных таймфреймов')
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

    # Цикл по всем символам
    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Начинаем обработку символа: {symbol} [{idx}/{total_symbols}]")
        logger.info(f"{'='*80}\n")

        loader = ATRLoader(symbol=symbol)
        loader.symbol_progress = f"[{idx}/{total_symbols}]"
        loader.run(timeframe=args.timeframe, batch_days=args.batch_days)

        logger.info(f"\n✅ Символ {symbol} обработан\n")

    logger.info(f"\n🎉 Все символы обработаны: {symbols}")


if __name__ == "__main__":
    main()
