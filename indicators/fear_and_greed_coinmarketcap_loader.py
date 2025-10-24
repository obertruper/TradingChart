#!/usr/bin/env python3
"""
CoinMarketCap Fear & Greed Index Loader
Загрузчик индекса страха и жадности от CoinMarketCap для всех таймфреймов
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import yaml
import logging
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Dict, List, Tuple
import requests
import json
from tqdm import tqdm
import time

# Добавляем путь к родительской директории для импорта модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class CoinMarketCapFearGreedLoader:
    """Загрузчик Fear & Greed Index от CoinMarketCap"""

    def __init__(self, config_path: str = 'indicators_config.yaml'):
        """
        Инициализация загрузчика

        Args:
            config_path: Путь к файлу конфигурации
        """
        # Загружаем конфигурацию
        self.config_path = config_path
        self.load_config()

        # Настройка логирования
        self.setup_logging()

        # Параметры подключения к БД
        self.db_config = self.config['database']

        # Конфигурация CoinMarketCap API
        self.api_config = self.config.get('indicators', {}).get('coinmarketcap_fear_and_greed', {})
        self.api_key = self.api_config.get('api_key')
        self.base_url = self.api_config.get('base_url', 'https://pro-api.coinmarketcap.com')
        self.batch_size = self.api_config.get('batch_size', 500)
        self.batch_days = self.api_config.get('batch_days', 1)
        self.retry_on_error = self.api_config.get('retry_on_error', 3)

        # Символ для обработки (Fear & Greed применяется только к BTCUSDT)
        self.symbol = 'BTCUSDT'

        # Таймфреймы для обработки
        self.timeframes = self.api_config.get('timeframes', ['1m', '15m', '1h'])

        # Названия колонок в БД
        self.index_column = 'fear_and_greed_index_coinmarketcap'
        self.classification_column = 'fear_and_greed_index_coinmarketcap_classification'

        # Кеш для API данных
        self.api_data_cache = None

        # Кеш для checkpoint'ов (чтобы не запрашивать повторно)
        self.checkpoints_cache = None

        self.logger.info("=" * 60)
        self.logger.info("🚀 Запуск CoinMarketCap Fear & Greed Index Loader")
        self.logger.info(f"🎯 Символ: {self.symbol}")
        self.logger.info(f"⏱️ Таймфреймы: {', '.join(self.timeframes)}")
        self.logger.info("=" * 60)

    def load_config(self):
        """Загрузка конфигурации из файла"""
        config_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(config_dir, self.config_path)

        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Файл конфигурации не найден: {config_file}")

        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

    def setup_logging(self):
        """Настройка логирования"""
        # Создаем папку для логов если её нет
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)

        # Имя файла лога с текущей датой и временем
        log_filename = os.path.join(
            log_dir,
            f"coinmarketcap_fear_greed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📝 CoinMarketCap Fear & Greed Loader: Логирование настроено. Лог-файл: {log_filename}")

        # Загружаем конфигурацию после настройки логирования
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.config_path)
        self.logger.info(f"📋 Конфигурация загружена из {config_file}")

    def connect_db(self):
        """Подключение к базе данных"""
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            return conn
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    def create_columns(self) -> bool:
        """
        Создание колонок для Fear & Greed Index если их нет

        Returns:
            bool: True если колонки созданы или уже существуют
        """
        self.logger.info("🔨 Проверка и создание колонок CoinMarketCap Fear & Greed...")

        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            for timeframe in self.timeframes:
                table_name = f'indicators_bybit_futures_{timeframe}'

                # Проверяем существование таблицы
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """, (table_name,))

                if not cursor.fetchone()[0]:
                    self.logger.error(f"  ❌ Таблица {table_name} не существует")
                    return False

                # Проверяем и создаем колонку для значения индекса
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, self.index_column))

                if not cursor.fetchone():
                    cursor.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN {self.index_column} SMALLINT
                    """)
                    conn.commit()
                    self.logger.info(f"  ✅ Колонка {self.index_column} создана в {table_name}")
                else:
                    self.logger.info(f"  ✅ Колонка {self.index_column} уже существует в {table_name}")

                # Проверяем и создаем колонку для классификации
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, self.classification_column))

                if not cursor.fetchone():
                    cursor.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN {self.classification_column} VARCHAR(20)
                    """)
                    conn.commit()
                    self.logger.info(f"  ✅ Колонка {self.classification_column} создана в {table_name}")
                else:
                    self.logger.info(f"  ✅ Колонка {self.classification_column} уже существует в {table_name}")

            self.logger.info("✅ Все колонки готовы")
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка при создании колонок: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def get_api_data(self) -> Optional[Dict]:
        """
        Получение данных с API CoinMarketCap

        Returns:
            Dict с данными или None при ошибке
        """
        if self.api_data_cache:
            self.logger.info("📦 Используем кешированные данные API")
            return self.api_data_cache

        self.logger.info(f"📡 Загрузка данных с CoinMarketCap API")

        if not self.api_key:
            self.logger.error("❌ API ключ не указан в конфигурации")
            return None

        headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        }

        all_data = []
        total_credits_used = 0

        try:
            # Первый батч - последние 500 записей
            url = f'{self.base_url}/v3/fear-and-greed/historical'
            params = {'limit': self.batch_size}

            self.logger.info(f"  📥 Запрос батча 1 (limit={self.batch_size})...")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                status = data.get('status', {})
                credits = status.get('credit_count', 0)
                total_credits_used += credits

                if 'data' in data and data['data']:
                    batch1 = data['data']
                    all_data.extend(batch1)
                    self.logger.info(f"  ✅ Батч 1: получено {len(batch1)} записей (использовано {credits} кредитов)")
            else:
                self.logger.error(f"  ❌ Ошибка API (батч 1): {response.status_code}")
                if response.status_code == 429:
                    self.logger.error("  ⚠️ Превышен лимит запросов API")
                elif response.status_code == 401:
                    self.logger.error("  ⚠️ Неверный API ключ")
                return None

            # Второй батч - следующие записи
            params = {'start': 500, 'limit': self.batch_size}
            self.logger.info(f"  📥 Запрос батча 2 (start=500, limit={self.batch_size})...")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                status = data.get('status', {})
                credits = status.get('credit_count', 0)
                total_credits_used += credits

                if 'data' in data and data['data']:
                    batch2 = data['data']
                    all_data.extend(batch2)
                    self.logger.info(f"  ✅ Батч 2: получено {len(batch2)} записей (использовано {credits} кредитов)")
            else:
                self.logger.warning(f"  ⚠️ Батч 2: нет данных или ошибка {response.status_code}")

            if all_data:
                # Преобразуем данные в словарь по датам
                data_by_date = {}

                for record in all_data:
                    # Преобразуем timestamp в дату
                    ts = int(record['timestamp'])
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    date_key = dt.date()

                    data_by_date[date_key] = {
                        'value': record['value'],
                        'classification': record['value_classification']
                    }

                # Сортируем по датам
                sorted_dates = sorted(data_by_date.keys())

                self.logger.info(f"✅ Загружено {len(data_by_date)} дней данных")
                self.logger.info(f"📅 Период: {sorted_dates[0]} - {sorted_dates[-1]}")
                self.logger.info(f"💳 Использовано кредитов: {total_credits_used}")

                # Кешируем данные
                self.api_data_cache = data_by_date
                return data_by_date
            else:
                self.logger.error("❌ Не удалось получить данные с API")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Ошибка при запросе к API: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Неожиданная ошибка: {e}")
            return None

    def get_all_checkpoints(self) -> Dict[str, Optional[datetime]]:
        """
        Получение checkpoint для ВСЕХ таймфреймов одним запросом (оптимизация)

        Returns:
            Dict с checkpoint'ами: {'1m': datetime, '15m': datetime, '1h': datetime}
        """
        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            # ОДИН запрос для всех 3 таймфреймов с UNION ALL
            query = """
                SELECT '1m' as timeframe, DATE(MAX(timestamp)) as max_date
                FROM indicators_bybit_futures_1m
                WHERE symbol = %s AND fear_and_greed_index_coinmarketcap IS NOT NULL

                UNION ALL

                SELECT '15m' as timeframe, DATE(MAX(timestamp)) as max_date
                FROM indicators_bybit_futures_15m
                WHERE symbol = %s AND fear_and_greed_index_coinmarketcap IS NOT NULL

                UNION ALL

                SELECT '1h' as timeframe, DATE(MAX(timestamp)) as max_date
                FROM indicators_bybit_futures_1h
                WHERE symbol = %s AND fear_and_greed_index_coinmarketcap IS NOT NULL
            """

            cursor.execute(query, (self.symbol, self.symbol, self.symbol))
            results = cursor.fetchall()

            # Преобразуем в словарь
            checkpoints = {}
            for timeframe, max_date in results:
                if max_date:
                    checkpoints[timeframe] = datetime.combine(
                        max_date,
                        datetime.min.time()
                    ).replace(tzinfo=timezone.utc)
                else:
                    checkpoints[timeframe] = None

            # Заполняем отсутствующие таймфреймы
            for tf in self.timeframes:
                if tf not in checkpoints:
                    checkpoints[tf] = None

            return checkpoints

        except Exception as e:
            self.logger.error(f"❌ Ошибка при получении checkpoints: {e}")
            return {tf: None for tf in self.timeframes}
        finally:
            cursor.close()
            conn.close()

    def get_checkpoint(self, timeframe: str) -> Optional[datetime]:
        """
        Получение checkpoint для таймфрейма

        DEPRECATED: Рекомендуется использовать get_all_checkpoints() для лучшей производительности

        Args:
            timeframe: Таймфрейм (1m, 15m, 1h)

        Returns:
            Последняя обработанная дата или None
        """
        # Если есть кеш - берем оттуда
        if self.checkpoints_cache and timeframe in self.checkpoints_cache:
            return self.checkpoints_cache[timeframe]

        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            table_name = f'indicators_bybit_futures_{timeframe}'

            # ОПТИМИЗИРОВАНО: DATE(MAX()) вместо MAX(DATE()) - намного быстрее!
            # DATE() вызывается только 1 раз вместо вызова на каждой строке
            cursor.execute(f"""
                SELECT DATE(MAX(timestamp))
                FROM {table_name}
                WHERE symbol = %s
                  AND {self.index_column} IS NOT NULL
            """, (self.symbol,))

            result = cursor.fetchone()
            if result and result[0]:
                # Возвращаем как datetime с временем 00:00:00
                return datetime.combine(result[0], datetime.min.time()).replace(tzinfo=timezone.utc)
            return None

        except Exception as e:
            self.logger.error(f"❌ Ошибка при получении checkpoint: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def update_batch(self, timeframe: str, date: date, value: int, classification: str, conn=None) -> int:
        """
        Обновление данных Fear & Greed для одного дня

        Args:
            timeframe: Таймфрейм
            date: Дата для обновления
            value: Значение индекса
            classification: Классификация
            conn: Опциональное DB соединение (для оптимизации)

        Returns:
            Количество обновленных записей
        """
        # Если соединение не передано - создаем свое (обратная совместимость)
        own_connection = False
        if conn is None:
            conn = self.connect_db()
            own_connection = True

        cursor = conn.cursor()

        try:
            table_name = f'indicators_bybit_futures_{timeframe}'

            # Создаем временные метки для начала и конца дня (UTC)
            start_ts = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_ts = start_ts + timedelta(days=1)

            # Обновляем все записи за день используя BETWEEN для эффективного использования индекса
            cursor.execute(f"""
                UPDATE {table_name}
                SET
                    {self.index_column} = %s,
                    {self.classification_column} = %s
                WHERE symbol = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                  AND {self.index_column} IS NULL
            """, (value, classification, self.symbol, start_ts, end_ts))

            updated_count = cursor.rowcount

            # Коммитим только если мы создали свое соединение
            if own_connection:
                conn.commit()

            return updated_count

        except Exception as e:
            self.logger.error(f"❌ Ошибка при обновлении данных: {e}")
            if own_connection:
                conn.rollback()
            return 0
        finally:
            cursor.close()
            # Закрываем только если мы создали свое соединение
            if own_connection:
                conn.close()

    def validate_day_consistency(self, date: date) -> bool:
        """
        Проверка консистентности данных за день между таймфреймами

        Args:
            date: Дата для проверки

        Returns:
            True если данные консистентны
        """
        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            values = {}

            for timeframe in self.timeframes:
                table_name = f'indicators_bybit_futures_{timeframe}'

                cursor.execute(f"""
                    SELECT DISTINCT {self.index_column}, {self.classification_column}
                    FROM {table_name}
                    WHERE symbol = %s
                      AND DATE(timestamp) = %s
                      AND {self.index_column} IS NOT NULL
                """, (self.symbol, date))

                results = cursor.fetchall()
                if results:
                    if len(results) > 1:
                        self.logger.warning(f"  ⚠️ Несколько значений для {timeframe}: {results}")
                        return False
                    values[timeframe] = results[0]

            # Проверяем что все значения одинаковые
            unique_values = set(values.values())
            if len(unique_values) > 1:
                self.logger.warning(f"  ⚠️ Разные значения между таймфреймами: {values}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка при проверке консистентности: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def process_timeframe(self, timeframe: str, start_date: datetime, end_date: datetime) -> bool:
        """
        Обработка одного таймфрейма

        Args:
            timeframe: Таймфрейм
            start_date: Начальная дата
            end_date: Конечная дата

        Returns:
            True если обработка успешна
        """
        self.logger.info(f"\n📊 Обработка таймфрейма: {timeframe}")
        self.logger.info(f"📅 Период: {start_date.date()} - {end_date.date()}")

        # Используем закешированный checkpoint (не делаем новый запрос к БД!)
        checkpoint = self.checkpoints_cache.get(timeframe) if self.checkpoints_cache else None
        if checkpoint and checkpoint >= start_date:
            self.logger.info(f"⏩ Продолжаю с checkpoint: {checkpoint.date()}")
            current_date = checkpoint + timedelta(days=1)
        else:
            current_date = start_date

        # Подсчитываем количество дней для обработки
        total_days = (end_date - current_date).days + 1
        if total_days <= 0:
            self.logger.info("✅ Нет новых данных для обработки")
            return True

        self.logger.info(f"📦 Дней для обработки: {total_days}")

        # Получаем данные API если еще не загружены
        api_data = self.api_data_cache or self.get_api_data()
        if not api_data:
            self.logger.error("❌ Нет данных API для обработки")
            return False

        # Открываем ОДНО соединение для всех дней (оптимизация)
        conn = self.connect_db()

        try:
            # Обрабатываем по дням с прогресс-баром
            processed_days = 0
            updated_records = 0
            commit_batch_size = 10  # Коммитим каждые 10 дней

            with tqdm(total=total_days, desc=f"{timeframe}", unit="день") as pbar:
                while current_date <= end_date:
                    date_key = current_date.date()

                    # Проверяем есть ли данные за этот день
                    if date_key in api_data:
                        data = api_data[date_key]
                        value = data['value']
                        classification = data['classification']

                        # Обновляем данные (передаем существующее соединение)
                        count = self.update_batch(timeframe, date_key, value, classification, conn=conn)
                        updated_records += count

                        # Обновляем прогресс-бар
                        pbar.set_description(f"{timeframe}: {date_key} (CMC={value}, обновлено={count})")
                        processed_days += 1

                        # Batch commit каждые N дней
                        if processed_days % commit_batch_size == 0:
                            conn.commit()
                            self.logger.debug(f"  💾 Commit после {processed_days} дней")
                    else:
                        # Нет данных за этот день
                        pbar.set_description(f"{timeframe}: {date_key} (нет данных API)")
                        self.logger.debug(f"  ⏩ Нет данных API для {date_key}, пропускаю")

                    current_date += timedelta(days=1)
                    pbar.update(1)

            # Финальный commit
            conn.commit()
            self.logger.debug(f"  💾 Финальный commit")

            self.logger.info(f"✅ Обработано {processed_days} дней, обновлено {updated_records} записей")
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка при обработке таймфрейма: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def run(self):
        """Основной метод запуска загрузчика"""
        try:
            # Создаем колонки если их нет
            if not self.create_columns():
                self.logger.error("❌ Не удалось создать колонки")
                return False

            # Получаем данные с API
            api_data = self.get_api_data()
            if not api_data:
                self.logger.error("❌ Не удалось получить данные с API")
                return False

            # Получаем checkpoint'ы для ВСЕХ таймфреймов ОДНИМ запросом (оптимизация)
            self.logger.info("📍 Получение checkpoint'ов для всех таймфреймов...")
            self.checkpoints_cache = self.get_all_checkpoints()
            for tf in self.timeframes:
                checkpoint_info = self.checkpoints_cache.get(tf)
                if checkpoint_info:
                    self.logger.info(f"   • {tf}: {checkpoint_info.date()}")
                else:
                    self.logger.info(f"   • {tf}: нет данных")

            # Определяем период обработки
            conn = self.connect_db()
            cursor = conn.cursor()

            # ОПТИМИЗИРОВАНО: DATE(MIN/MAX()) вместо MIN/MAX(DATE()) - намного быстрее!
            # DATE() вызывается только 2 раза вместо вызова на каждой строке
            cursor.execute("""
                SELECT
                    DATE(MIN(timestamp)) as min_date,
                    DATE(MAX(timestamp)) as max_date
                FROM indicators_bybit_futures_1m
                WHERE symbol = %s
            """, (self.symbol,))

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result or not result[0]:
                self.logger.error("❌ Нет данных в БД для обработки")
                return False

            db_min_date = result[0]
            db_max_date = result[1]

            # Определяем даты из API данных
            api_dates = sorted(api_data.keys())
            api_min_date = api_dates[0]
            api_max_date = api_dates[-1]

            # Начинаем с максимума между минимальной датой БД и минимальной датой API
            start_date = max(db_min_date, api_min_date)
            # Заканчиваем минимумом между максимальной датой БД и максимальной датой API
            end_date = min(db_max_date, api_max_date)

            self.logger.info(f"\n📅 Общий период обработки:")
            self.logger.info(f"   БД: {db_min_date} - {db_max_date}")
            self.logger.info(f"   API: {api_min_date} - {api_max_date}")
            self.logger.info(f"   Обработка: {start_date} - {end_date}")

            # Преобразуем в datetime
            start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_datetime = datetime.combine(end_date, datetime.min.time()).replace(tzinfo=timezone.utc)

            # Обрабатываем каждый таймфрейм
            for timeframe in self.timeframes:
                if not self.process_timeframe(timeframe, start_datetime, end_datetime):
                    self.logger.error(f"❌ Ошибка при обработке таймфрейма {timeframe}")
                    return False

            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ CoinMarketCap Fear & Greed Index успешно загружен!")
            self.logger.info("=" * 60)
            return True

        except KeyboardInterrupt:
            self.logger.info("\n⚠️ Прервано пользователем. Можно продолжить позже с этого места.")
            sys.exit(0)
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}")
            return False


def main():
    """Основная функция"""
    loader = CoinMarketCapFearGreedLoader()
    success = loader.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()