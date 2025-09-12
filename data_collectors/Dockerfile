FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копирование requirements
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY bybit/futures/ /app/

# Создание директорий для логов и чекпоинтов
RUN mkdir -p /app/logs /app/checkpoints

# Пользователь для безопасности
RUN useradd -m -u 1000 monitor && chown -R monitor:monitor /app
USER monitor

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import psycopg2; psycopg2.connect('host=postgres dbname=trading_db user=macbook')" || exit 1

# Запуск по умолчанию
CMD ["python", "continuous_monitor.py", "--daemon"]