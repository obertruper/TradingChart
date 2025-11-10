#!/usr/bin/env python3
"""
Утилита для просмотра логов SMA загрузчика
"""

import os
import sys
from datetime import datetime
import argparse


def list_logs():
    """Показывает список всех лог-файлов"""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')

    if not os.path.exists(log_dir):
        print("❌ Папка logs не найдена")
        return []

    log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
    log_files.sort(reverse=True)  # Сначала новые

    if not log_files:
        print("❌ Лог-файлы не найдены")
        return []

    print("=" * 60)
    print("📝 ДОСТУПНЫЕ ЛОГ-ФАЙЛЫ")
    print("=" * 60)

    for i, log_file in enumerate(log_files[:10], 1):  # Показываем последние 10
        # Получаем размер файла
        log_path = os.path.join(log_dir, log_file)
        size_kb = os.path.getsize(log_path) / 1024

        # Извлекаем дату из имени файла
        try:
            date_str = log_file.replace('sma_loader_', '').replace('.log', '')
            date_obj = datetime.strptime(date_str, '%Y%m%d_%H%M%S')
            date_formatted = date_obj.strftime('%Y-%m-%d %H:%M:%S')
        except:
            date_formatted = 'неизвестно'

        print(f"{i:2}. {log_file:<40} {size_kb:>8.1f} KB  {date_formatted}")

    if len(log_files) > 10:
        print(f"\n   ... и еще {len(log_files) - 10} файлов")

    return log_files


def view_log(log_file=None, lines=50, follow=False):
    """
    Просматривает содержимое лог-файла

    Args:
        log_file: Имя файла или None для последнего
        lines: Количество строк для показа
        follow: Следить за обновлениями (как tail -f)
    """
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')

    if not log_file:
        # Берем последний лог
        log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
        if not log_files:
            print("❌ Лог-файлы не найдены")
            return
        log_file = sorted(log_files)[-1]

    log_path = os.path.join(log_dir, log_file)

    if not os.path.exists(log_path):
        print(f"❌ Файл {log_file} не найден")
        return

    print("=" * 80)
    print(f"📋 ЛОГ: {log_file}")
    print("=" * 80)

    if follow:
        # Режим слежения за файлом
        print("📡 Режим слежения (Ctrl+C для выхода)")
        print("-" * 80)

        import time
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                # Переходим в конец файла
                f.seek(0, 2)

                while True:
                    line = f.readline()
                    if line:
                        # Форматируем вывод
                        if 'ERROR' in line:
                            print(f"❌ {line.strip()}")
                        elif 'WARNING' in line:
                            print(f"⚠️  {line.strip()}")
                        elif '🔍' in line or '📈' in line or '✅' in line:
                            print(line.strip())
                        else:
                            print(f"   {line.strip()}")
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n⚠️ Слежение прервано")
    else:
        # Обычный просмотр
        with open(log_path, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()

            if lines == -1:
                # Показать весь файл
                for line in all_lines:
                    print(line.strip())
            else:
                # Показать последние N строк
                for line in all_lines[-lines:]:
                    print(line.strip())

        print("-" * 80)
        print(f"Показано последних {min(lines, len(all_lines))} строк из {len(all_lines)}")


def analyze_log(log_file=None):
    """Анализирует лог-файл и показывает статистику"""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')

    if not log_file:
        log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
        if not log_files:
            print("❌ Лог-файлы не найдены")
            return
        log_file = sorted(log_files)[-1]

    log_path = os.path.join(log_dir, log_file)

    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    print("=" * 60)
    print(f"📊 АНАЛИЗ ЛОГА: {log_file}")
    print("=" * 60)

    # Собираем статистику
    stats = {
        'total': len(lines),
        'errors': 0,
        'warnings': 0,
        'gaps_found': 0,
        'records_processed': 0,
        'dates_processed': set()
    }

    for line in lines:
        if 'ERROR' in line:
            stats['errors'] += 1
        elif 'WARNING' in line:
            stats['warnings'] += 1
        elif 'пробелы для периодов' in line:
            stats['gaps_found'] += 1
        elif 'Обработано' in line and 'записей' in line:
            # Извлекаем количество записей
            try:
                parts = line.split('записей')[0].split(':')[-1].strip()
                records = int(parts.replace(',', ''))
                stats['records_processed'] += records

                # Извлекаем даты
                date_part = line.split('Обработано')[1].split(':')[0].strip()
                stats['dates_processed'].add(date_part)
            except:
                pass

    print(f"\n📈 Статистика:")
    print(f"   Всего строк: {stats['total']}")
    print(f"   Ошибок: {stats['errors']}")
    print(f"   Предупреждений: {stats['warnings']}")
    print(f"   Обнаружено пробелов: {stats['gaps_found']}")
    print(f"   Обработано записей: {stats['records_processed']:,}")
    print(f"   Обработано дней: {len(stats['dates_processed'])}")

    if stats['dates_processed']:
        print(f"\n📅 Обработанные периоды:")
        dates = sorted(stats['dates_processed'])[:5]
        for date in dates:
            print(f"   {date}")
        if len(stats['dates_processed']) > 5:
            print(f"   ... и еще {len(stats['dates_processed']) - 5} дней")


def main():
    parser = argparse.ArgumentParser(description='Просмотр логов SMA загрузчика')
    parser.add_argument('--list', action='store_true',
                       help='Показать список логов')
    parser.add_argument('--view', type=str, nargs='?', const='latest',
                       help='Просмотреть лог (по умолчанию последний)')
    parser.add_argument('--lines', type=int, default=50,
                       help='Количество строк для показа (по умолчанию 50, -1 для всех)')
    parser.add_argument('--follow', action='store_true',
                       help='Следить за обновлениями (как tail -f)')
    parser.add_argument('--analyze', type=str, nargs='?', const='latest',
                       help='Анализировать лог')

    args = parser.parse_args()

    if args.list:
        list_logs()
    elif args.view:
        if args.view == 'latest':
            view_log(None, args.lines, args.follow)
        else:
            view_log(args.view, args.lines, args.follow)
    elif args.analyze:
        if args.analyze == 'latest':
            analyze_log(None)
        else:
            analyze_log(args.analyze)
    else:
        # По умолчанию показываем последний лог
        view_log(None, 30, False)


if __name__ == "__main__":
    main()