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

Использование:
    cd indicators
    python3 start_all_loaders.py

Автор: Trading System
Дата: 2025-10-23
"""

import subprocess
import sys
import os
import yaml
import logging
import time
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

    # Специальные случаи (не стандартное название файла)
    'fear_and_greed': 'fear_and_greed_loader_alternative.py',
    'coinmarketcap_fear_and_greed': 'fear_and_greed_coinmarketcap_loader.py',

    # Stochastic + Williams (один файл для обоих)
    'stochastic': 'stochastic_williams_loader.py',
    'williams_r': 'stochastic_williams_loader.py',
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
    """Форматирует длительность в человекочитаемый вид"""
    minutes = int(seconds // 60)
    secs = int(seconds % 60)

    if minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


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
    # Настраиваем логирование
    logger, log_file = setup_logging()

    # Заголовок
    print(f"\n{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}🚀 ЗАПУСК START ALL LOADERS - Автоматическое обновление индикаторов{Colors.ENDC}")
    print(f"{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")

    logger.info(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Лог файл: {log_file}")
    logger.info("")

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

    # Получаем аргументы для stochastic+williams
    stochastic_williams_args = get_stochastic_williams_args(config)

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

            success, duration = run_loader(
                indicator_name,
                script_name,
                [],
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
