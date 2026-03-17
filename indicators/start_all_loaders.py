#!/usr/bin/env python3
"""
Start All Loaders - Orchestrator для автоматического последовательного запуска всех loader'ов
==========================================================================================

Этот скрипт автоматизирует загрузку индикаторов, запуская loader'ы последовательно
в порядке, определенном в indicators_config.yaml.

Особенности:
- Читает настройки из indicators_config.yaml (секция orchestrator.loaders)
- Запускает только те loader'ы, у которых установлен флаг true
- Порядок выполнения = порядок в секции indicators в config файле
- Умная логика для stochastic + williams_r (один файл для обоих индикаторов)
- Останавливается при первой ошибке
- Логирует в консоль + файл logs/run_YYYYMMDD_HHMMSS.log
- Показывает статистику времени выполнения
- Поддержка --symbol для фильтрации по символу (пробрасывается в каждый загрузчик)
- Автоматическая трансляция --symbol → --currency для Options-загрузчиков (BTCUSDT→BTC, ETHUSDT→ETH)
- Поддержка --timeframe для обработки конкретного таймфрейма (например, 4h или 1d)
- Поддержка --force-reload для полного пересчёта данных (пробрасывается в каждый загрузчик)

Использование:
    cd indicators
    python3 start_all_loaders.py                          # Все символы, все таймфреймы
    python3 start_all_loaders.py --symbol BTCUSDT         # Только BTCUSDT
    python3 start_all_loaders.py --timeframe 4h           # Только 4h таймфрейм
    python3 start_all_loaders.py --force-reload           # Полный пересчёт всех данных
    python3 start_all_loaders.py --check-nulls            # Заполнение NULL
    python3 start_all_loaders.py --symbol BTCUSDT --timeframe 1h  # Комбинация

Автор: Trading System
Дата: 2025-10-23
"""

import subprocess
import sys
import os
import yaml
import logging
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Цвета для консоли
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Mapping индикаторов на скрипты
LOADER_MAPPING = {
    'sma': 'sma_loader.py',
    'ema': 'ema_loader.py',
    'rsi': 'rsi_loader.py',
    'vma': 'vma_loader.py',
    'atr': 'atr_loader.py',
    'macd': 'macd_loader.py',
    'bollinger_bands': 'bollinger_bands_loader.py',
    'adx': 'adx_loader.py',
    'vwap': 'vwap_loader.py',
    'mfi': 'mfi_loader.py',
    'obv': 'obv_loader.py',
    'long_short_ratio': 'long_short_ratio_loader.py',
    'open_interest': 'open_interest_loader.py',
    'funding_rate': 'funding_fee_loader.py',
    'premium_index': 'premium_index_loader.py',
    'ichimoku': 'ichimoku_loader.py',
    'hv': 'hv_loader.py',
    'supertrend': 'supertrend_loader.py',
    'bybit_orderbook': 'orderbook_bybit_loader.py',
    'binance_orderbook': 'orderbook_binance_loader.py',
    'options_dvol': 'options_dvol_loader.py',
    'options_dvol_indicators': 'options_dvol_indicators_loader.py',
    'options_aggregated': 'options_aggregated_loader.py',

    # Специальные случаи (не стандартное название файла)
    'fear_and_greed': 'fear_and_greed_loader_alternative.py',
    'coinmarketcap_fear_and_greed': 'fear_and_greed_coinmarketcap_loader.py',

    # Stochastic + Williams (один файл для обоих)
    'stochastic': 'stochastic_williams_loader.py',
    'williams_r': 'stochastic_williams_loader.py',
}

# Загрузчики, поддерживающие флаг --check-nulls
LOADERS_WITH_CHECK_NULLS = {
    'sma', 'ema', 'rsi', 'vma', 'atr', 'adx', 'macd', 'bollinger_bands', 'vwap', 'mfi',
    'stochastic', 'williams_r', 'premium_index', 'ichimoku', 'hv', 'supertrend',
    'fear_and_greed', 'coinmarketcap_fear_and_greed', 'binance_orderbook',
}

# Загрузчики, поддерживающие флаг --symbol
LOADERS_WITH_SYMBOL = {
    'sma', 'ema', 'rsi', 'vma', 'atr', 'adx', 'macd', 'obv', 'bollinger_bands',
    'vwap', 'mfi', 'stochastic', 'williams_r', 'ichimoku', 'hv', 'supertrend',
    'long_short_ratio', 'open_interest', 'funding_rate', 'premium_index',
    'bybit_orderbook', 'binance_orderbook',
}

# Загрузчики, использующие --currency вместо --symbol (Options/Deribit)
# Все три Options-загрузчика — глобальные индексы, всегда загружают обе валюты (BTC+ETH)
# При запуске через orchestrator --currency НЕ передаётся (аналогично Fear & Greed)
LOADERS_WITH_CURRENCY = set()

# Маппинг symbol → currency для Options-загрузчиков
SYMBOL_TO_CURRENCY = {
    'BTCUSDT': 'BTC',
    'ETHUSDT': 'ETH',
}

# Загрузчики, поддерживающие флаг --timeframe
LOADERS_WITH_TIMEFRAME = {
    'sma', 'ema', 'rsi', 'vma', 'atr', 'adx', 'macd', 'bollinger_bands',
    'vwap', 'mfi', 'stochastic', 'williams_r', 'obv',
    'long_short_ratio', 'open_interest', 'funding_rate', 'premium_index',
    'ichimoku', 'hv', 'supertrend',
    'fear_and_greed', 'coinmarketcap_fear_and_greed',
    'options_dvol',
}

# Загрузчики, поддерживающие флаг --force-reload
LOADERS_WITH_FORCE_RELOAD = {
    'sma', 'ema', 'rsi', 'vma', 'atr', 'adx', 'macd', 'bollinger_bands',
    'vwap', 'mfi', 'stochastic', 'williams_r', 'obv',
    'long_short_ratio', 'open_interest', 'funding_rate', 'premium_index',
    'ichimoku', 'hv', 'supertrend',
    'fear_and_greed', 'coinmarketcap_fear_and_greed',
    'bybit_orderbook', 'binance_orderbook',
    'options_dvol', 'options_dvol_indicators', 'options_aggregated',
}


def setup_logging() -> logging.Logger:
    """Настраивает логирование в консоль и файл"""
    # Создаем директорию для логов
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)

    # Имя файла лога с timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'start_all_loaders_{timestamp}.log'

    # Настройка логгера
    logger = logging.getLogger('start_all_loaders')
    logger.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Добавляем handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger, log_file


def load_config() -> Dict:
    """Загружает конфигурацию из indicators_config.yaml"""
    config_path = Path(__file__).parent / 'indicators_config.yaml'

    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def get_execution_order(config: Dict) -> List[str]:
    """
    Возвращает порядок выполнения индикаторов из YAML файла

    Порядок = порядок ключей в секции indicators
    """
    indicators_section = config.get('indicators', {})
    return list(indicators_section.keys())


def get_enabled_loaders(config: Dict) -> Dict[str, bool]:
    """Возвращает словарь {indicator_name: enabled_flag} из orchestrator.loaders"""
    orchestrator = config.get('orchestrator', {})
    loaders = orchestrator.get('loaders', {})

    if not loaders:
        raise ValueError("Секция orchestrator.loaders не найдена в конфигурации")

    return loaders


def get_stochastic_williams_args(config: Dict) -> List[str]:
    """
    Умная логика для определения аргументов stochastic_williams_loader.py

    Returns:
        Список аргументов для --indicator флага
    """
    loaders = get_enabled_loaders(config)

    stoch_enabled = loaders.get('stochastic', False)
    williams_enabled = loaders.get('williams_r', False)

    if stoch_enabled and williams_enabled:
        return ['--indicator', 'both']
    elif stoch_enabled:
        return ['--indicator', 'stochastic']
    elif williams_enabled:
        return ['--indicator', 'williams']
    else:
        return []


def format_duration(seconds: float) -> str:
    """Форматирует длительность в человекочитаемый вид (1d 4h 15m 30s)"""
    total = int(seconds)
    days = total // 86400
    hours = (total % 86400) // 3600
    minutes = (total % 3600) // 60
    secs = total % 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def run_loader(indicator_name: str, script_name: str, extra_args: List[str],
               logger: logging.Logger, index: int, total: int) -> Tuple[bool, float]:
    """
    Запускает один loader через subprocess

    Args:
        indicator_name: Название индикатора (для логов)
        script_name: Имя скрипта для запуска
        extra_args: Дополнительные аргументы командной строки
        logger: Logger для вывода
        index: Номер текущего loader'а
        total: Общее количество loader'ов

    Returns:
        Tuple[success: bool, duration: float]
    """
    script_path = Path(__file__).parent / script_name

    if not script_path.exists():
        logger.error(f"❌ Скрипт не найден: {script_path}")
        return False, 0.0

    # Формируем команду
    cmd = [sys.executable, str(script_path)] + extra_args

    # Логируем начало
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"{index}/{total} [{indicator_name.upper()}] Запуск {script_name}...")
    logger.info("=" * 80)

    # Запускаем
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=str(Path(__file__).parent),
            check=True,  # Raises CalledProcessError if return code != 0
            text=True
        )

        duration = time.time() - start_time

        logger.info("")
        logger.info(f"{Colors.OKGREEN}✅ [{indicator_name.upper()}] Завершено за {format_duration(duration)}{Colors.ENDC}")

        return True, duration

    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time

        logger.error("")
        logger.error(f"{Colors.FAIL}❌ [{indicator_name.upper()}] Ошибка выполнения{Colors.ENDC}")
        logger.error(f"Return code: {e.returncode}")
        logger.error(f"Длительность до ошибки: {format_duration(duration)}")

        return False, duration

    except Exception as e:
        duration = time.time() - start_time

        logger.error("")
        logger.error(f"{Colors.FAIL}❌ [{indicator_name.upper()}] Неожиданная ошибка: {e}{Colors.ENDC}")

        return False, duration


def main():
    """Основная функция"""
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Orchestrator: последовательный запуск всех indicator loaders')
    parser.add_argument('--check-nulls', action='store_true',
                       help='Передать --check-nulls каждому загрузчику (заполнение NULL в середине данных)')
    parser.add_argument('--symbol', type=str, default=None,
                       help='Обработать только указанный символ (например, BTCUSDT). '
                            'Пробрасывается как --symbol для индикаторов и --currency для Options-загрузчиков.')
    parser.add_argument('--timeframe', type=str, default=None,
                       help='Обработать только указанный таймфрейм (например, 1h, 4h, 1d). '
                            'Пробрасывается как --timeframe в каждый загрузчик.')
    parser.add_argument('--force-reload', action='store_true',
                       help='Полный пересчёт всех данных. '
                            'Пробрасывается как --force-reload в каждый загрузчик.')
    args = parser.parse_args()

    # Настраиваем логирование
    logger, log_file = setup_logging()

    # Заголовок
    print(f"\n{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}🚀 ЗАПУСК START ALL LOADERS - Автоматическое обновление индикаторов{Colors.ENDC}")
    print(f"{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")

    logger.info(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Лог файл: {log_file}")
    if args.check_nulls:
        logger.info(f"🔍 Режим CHECK NULLS: будет передан --check-nulls поддерживающим загрузчикам")
    if args.symbol:
        currency = SYMBOL_TO_CURRENCY.get(args.symbol)
        currency_info = f" (→ --currency {currency} для Options)" if currency else " (Options-загрузчики будут пропущены)"
        logger.info(f"🎯 Фильтр по символу: {args.symbol}{currency_info}")
    if args.timeframe:
        logger.info(f"🕐 Фильтр по таймфрейму: {args.timeframe}")
    if args.force_reload:
        logger.info(f"🔄 Режим FORCE RELOAD: будет передан --force-reload поддерживающим загрузчикам")
    logger.info("")

    # Подтверждение --force-reload
    if args.force_reload:
        print(f"\n{Colors.WARNING}{'=' * 70}{Colors.ENDC}")
        print(f"{Colors.WARNING}{Colors.BOLD}⚠️  ВНИМАНИЕ: --force-reload полностью перезапишет ВСЕ данные индикаторов!{Colors.ENDC}")
        print(f"{Colors.WARNING}⏱️  Полная перезапись всех индикаторов может занять несколько часов.{Colors.ENDC}")
        print(f"{Colors.WARNING}{'=' * 70}{Colors.ENDC}")
        response = input("\nПродолжить? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("❌ Отменено пользователем.")
            sys.exit(0)
        # Устанавливаем переменную окружения, чтобы дочерние процессы
        # (загрузчики) не спрашивали подтверждение повторно
        os.environ['FORCE_RELOAD_CONFIRMED'] = '1'
        print()

    # Загружаем конфигурацию
    try:
        logger.info("📖 Загрузка конфигурации из indicators_config.yaml...")
        config = load_config()
        logger.info("✅ Конфигурация загружена успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

    # Получаем порядок выполнения и enabled флаги
    execution_order = get_execution_order(config)
    enabled_loaders = get_enabled_loaders(config)

    # Фильтруем только enabled=true
    loaders_to_run = [
        indicator for indicator in execution_order
        if enabled_loaders.get(indicator, False)
    ]

    # Статистика
    total_indicators = len(execution_order)
    enabled_count = len(loaders_to_run)
    disabled_count = total_indicators - enabled_count

    logger.info("")
    logger.info(f"📊 Всего индикаторов в конфиге: {total_indicators}")
    logger.info(f"✅ Включено (orchestrator.loaders = true): {enabled_count}")
    logger.info(f"⏸️  Пропущено (orchestrator.loaders = false): {disabled_count}")
    logger.info("")

    if enabled_count == 0:
        logger.warning("⚠️  Нет включенных loader'ов. Завершение работы.")
        return

    logger.info(f"🎯 Порядок выполнения:")
    for idx, indicator in enumerate(loaders_to_run, 1):
        logger.info(f"  {idx}. {indicator}")
    logger.info("")

    # Подготавливаем общие аргументы для --symbol
    symbol_args = []
    currency_args = []
    skip_currency_loaders = False

    if args.symbol:
        symbol_args = ['--symbol', args.symbol]
        currency = SYMBOL_TO_CURRENCY.get(args.symbol)
        if currency:
            currency_args = ['--currency', currency]
        else:
            skip_currency_loaders = True
            logger.info(f"⚠️  Символ {args.symbol} не имеет маппинга на currency — Options-загрузчики будут пропущены")

    # Подготавливаем общие аргументы для --timeframe
    timeframe_args = []
    if args.timeframe:
        timeframe_args = ['--timeframe', args.timeframe]

    # Получаем аргументы для stochastic+williams
    stochastic_williams_args = get_stochastic_williams_args(config)
    if args.check_nulls:
        stochastic_williams_args += ['--check-nulls']
    if args.force_reload:
        stochastic_williams_args += ['--force-reload']
    if symbol_args:
        stochastic_williams_args += symbol_args
    if timeframe_args:
        stochastic_williams_args += timeframe_args

    # Запускаем loader'ы последовательно
    results = []
    total_start_time = time.time()

    # Флаг для обработки stochastic+williams
    stochastic_williams_processed = False

    for idx, indicator_name in enumerate(loaders_to_run, 1):
        # Специальная обработка stochastic + williams_r
        if indicator_name == 'stochastic':
            # Запускаем один раз для обоих индикаторов
            script_name = LOADER_MAPPING[indicator_name]
            success, duration = run_loader(
                'stochastic+williams_r',
                script_name,
                stochastic_williams_args,
                logger,
                idx,
                enabled_count
            )
            results.append((indicator_name, success, duration))
            stochastic_williams_processed = True

            if not success:
                logger.error("")
                logger.error(f"{Colors.FAIL}❌ ОСТАНОВКА: Ошибка при выполнении {indicator_name}{Colors.ENDC}")
                break

        elif indicator_name == 'williams_r':
            # Пропускаем, т.к. уже обработан в stochastic
            if stochastic_williams_processed:
                logger.info("")
                logger.info(f"⏭️  [{indicator_name.upper()}] Пропущено (обработан вместе со stochastic)")
                continue
            else:
                # Если stochastic отключен, но williams включен - запускаем отдельно
                script_name = LOADER_MAPPING[indicator_name]
                success, duration = run_loader(
                    indicator_name,
                    script_name,
                    stochastic_williams_args,
                    logger,
                    idx,
                    enabled_count
                )
                results.append((indicator_name, success, duration))

                if not success:
                    logger.error("")
                    logger.error(f"{Colors.FAIL}❌ ОСТАНОВКА: Ошибка при выполнении {indicator_name}{Colors.ENDC}")
                    break
        else:
            # Обычный loader
            script_name = LOADER_MAPPING.get(indicator_name)

            if not script_name:
                logger.error(f"❌ Неизвестный индикатор: {indicator_name}")
                logger.error(f"Добавьте mapping в LOADER_MAPPING")
                break

            # Пропускаем Options-загрузчики если символ не маппится на currency
            if args.symbol and indicator_name in LOADERS_WITH_CURRENCY and skip_currency_loaders:
                logger.info("")
                logger.info(f"⏭️  [{indicator_name.upper()}] Пропущено (символ {args.symbol} не поддерживается)")
                continue

            extra_args = []
            if args.check_nulls and indicator_name in LOADERS_WITH_CHECK_NULLS:
                extra_args.append('--check-nulls')

            # Пробрасываем --symbol или --currency
            if args.symbol:
                if indicator_name in LOADERS_WITH_SYMBOL:
                    extra_args += symbol_args
                elif indicator_name in LOADERS_WITH_CURRENCY:
                    extra_args += currency_args
                # Fear & Greed и другие без поддержки symbol — запускаются без фильтра

            # Пробрасываем --timeframe
            if args.timeframe and indicator_name in LOADERS_WITH_TIMEFRAME:
                extra_args += timeframe_args

            # Пробрасываем --force-reload
            if args.force_reload and indicator_name in LOADERS_WITH_FORCE_RELOAD:
                extra_args.append('--force-reload')

            success, duration = run_loader(
                indicator_name,
                script_name,
                extra_args,
                logger,
                idx,
                enabled_count
            )
            results.append((indicator_name, success, duration))

            if not success:
                logger.error("")
                logger.error(f"{Colors.FAIL}❌ ОСТАНОВКА: Ошибка при выполнении {indicator_name}{Colors.ENDC}")
                break

    # Итоговая статистика
    total_duration = time.time() - total_start_time

    successful = sum(1 for _, success, _ in results if success)
    failed = len(results) - successful

    logger.info("")
    logger.info("=" * 80)
    logger.info(f"{Colors.BOLD}📈 ИТОГОВАЯ СТАТИСТИКА{Colors.ENDC}")
    logger.info("=" * 80)
    logger.info("")

    # Детализация по каждому loader'у
    logger.info("Выполненные loader'ы:")
    for indicator, success, duration in results:
        status = f"{Colors.OKGREEN}✅{Colors.ENDC}" if success else f"{Colors.FAIL}❌{Colors.ENDC}"
        logger.info(f"  {status} {indicator:25s} - {format_duration(duration)}")

    logger.info("")
    logger.info(f"✅ Успешно выполнено: {successful}")
    logger.info(f"❌ Ошибки: {failed}")
    logger.info(f"⏱️  Общее время выполнения: {format_duration(total_duration)}")
    logger.info(f"📝 Лог сохранен: {log_file}")
    logger.info("")

    if failed == 0:
        logger.info(f"{Colors.OKGREEN}{Colors.BOLD}🎉 ВСЕ ИНДИКАТОРЫ ОБНОВЛЕНЫ УСПЕШНО!{Colors.ENDC}")
    else:
        logger.error(f"{Colors.FAIL}{Colors.BOLD}⚠️  ОБНОВЛЕНИЕ ЗАВЕРШЕНО С ОШИБКАМИ{Colors.ENDC}")

    logger.info("=" * 80)
    logger.info("")

    # Exit code
    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
