#!/bin/bash
# =============================================================================
# Wrapper script for options_deribit_raw_ws_collector.py
# =============================================================================
#
# Автоматический restart при падении.
# Запускается внутри tmux сессии через cron @reboot.
#
# Установка на VPS:
#   1. Скопировать файлы на VPS
#   2. chmod +x /root/TradingCharts/data_collectors/deribit/options/run_ws_collector.sh
#   3. crontab -e → добавить:
#      @reboot /usr/bin/tmux new-session -d -s data_collector_options_raw_deribit '/root/TradingCharts/data_collectors/deribit/options/run_ws_collector.sh'
#
# Мониторинг:
#   tmux attach -t data_collector_options_raw_deribit   # подключиться
#   Ctrl+B, D                                           # отключиться
#
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python3"
SCRIPT="$SCRIPT_DIR/options_deribit_raw_ws_collector.py"

echo "========================================"
echo "Options Deribit Raw WS Collector Wrapper"
echo "========================================"
echo "Project: $PROJECT_DIR"
echo "Python:  $VENV_PYTHON"
echo "Script:  $SCRIPT"
echo "Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "========================================"
echo ""

while true; do
    echo ">>> $(date -u '+%Y-%m-%d %H:%M:%S UTC') Starting collector..."
    echo ""

    cd "$PROJECT_DIR" && "$VENV_PYTHON" "$SCRIPT"

    EXIT_CODE=$?
    echo ""
    echo ">>> $(date -u '+%Y-%m-%d %H:%M:%S UTC') Collector exited with code $EXIT_CODE"
    echo ">>> Restarting in 10 seconds..."
    echo ""
    sleep 10
done
