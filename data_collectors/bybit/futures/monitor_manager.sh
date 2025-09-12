#!/bin/bash

# Monitor Manager Script
# Управление continuous_monitor.py процессом

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/continuous_monitor.py"
PID_FILE="$SCRIPT_DIR/.monitor.pid"
LOG_FILE="$SCRIPT_DIR/logs/monitor_daemon.log"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Создаем директорию для логов если не существует
mkdir -p "$SCRIPT_DIR/logs"

start() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${YELLOW}Monitor already running with PID $PID${NC}"
            return 1
        else
            echo -e "${YELLOW}Removing stale PID file${NC}"
            rm "$PID_FILE"
        fi
    fi
    
    echo -e "${GREEN}Starting continuous monitor...${NC}"
    nohup python3 "$PYTHON_SCRIPT" --daemon >> "$LOG_FILE" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    
    sleep 2
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${GREEN}Monitor started successfully with PID $PID${NC}"
        echo -e "${GREEN}Log file: $LOG_FILE${NC}"
    else
        echo -e "${RED}Failed to start monitor${NC}"
        rm "$PID_FILE"
        return 1
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo -e "${YELLOW}Monitor is not running (no PID file)${NC}"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${YELLOW}Stopping monitor with PID $PID...${NC}"
        kill $PID
        
        # Ждем завершения процесса
        for i in {1..10}; do
            if ! ps -p $PID > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${RED}Process didn't stop gracefully, forcing...${NC}"
            kill -9 $PID
        fi
        
        rm "$PID_FILE"
        echo -e "${GREEN}Monitor stopped${NC}"
    else
        echo -e "${YELLOW}Monitor is not running (process not found)${NC}"
        rm "$PID_FILE"
    fi
}

restart() {
    echo -e "${YELLOW}Restarting monitor...${NC}"
    stop
    sleep 2
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}Monitor is running with PID $PID${NC}"
            
            # Показываем последние логи
            if [ -f "$LOG_FILE" ]; then
                echo -e "\n${YELLOW}Last 10 log lines:${NC}"
                tail -n 10 "$LOG_FILE"
            fi
            
            # Показываем информацию о процессе
            echo -e "\n${YELLOW}Process info:${NC}"
            ps -fp $PID
            
            # Показываем использование памяти
            echo -e "\n${YELLOW}Memory usage:${NC}"
            ps -o pid,vsz,rss,comm -p $PID
        else
            echo -e "${RED}Monitor is not running (process not found)${NC}"
            rm "$PID_FILE"
        fi
    else
        echo -e "${YELLOW}Monitor is not running${NC}"
    fi
}

logs() {
    if [ -f "$LOG_FILE" ]; then
        echo -e "${YELLOW}Showing live logs (Ctrl+C to exit)...${NC}"
        tail -f "$LOG_FILE"
    else
        echo -e "${RED}Log file not found${NC}"
    fi
}

check_once() {
    SYMBOL=${2:-BTCUSDT}
    echo -e "${GREEN}Running single check for $SYMBOL...${NC}"
    python3 "$PYTHON_SCRIPT" --check-once --symbol "$SYMBOL"
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    check-once)
        check_once "$@"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|check-once [SYMBOL]}"
        echo ""
        echo "Commands:"
        echo "  start       - Start the monitor in daemon mode"
        echo "  stop        - Stop the running monitor"
        echo "  restart     - Restart the monitor"
        echo "  status      - Check if monitor is running"
        echo "  logs        - Show live logs"
        echo "  check-once  - Run single check for symbol (default: BTCUSDT)"
        echo ""
        echo "Examples:"
        echo "  $0 start"
        echo "  $0 check-once ETHUSDT"
        echo "  $0 status"
        exit 1
        ;;
esac