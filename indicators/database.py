#!/usr/bin/env python3
"""
Database connection manager for indicators
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Управление подключением к PostgreSQL для индикаторов"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Инициализация подключения к БД

        Args:
            config: Словарь с параметрами подключения. Если не указан,
                   используются значения по умолчанию для VPS БД
        """
        if config is None:
            # Параметры по умолчанию для VPS БД
            config = {
                'host': '82.25.115.144',
                'port': 5432,
                'database': 'trading_data',
                'user': 'trading_admin',
                'password': 'TrAdm!n2025$Kx9Lm'
            }

        self.config = config
        self.connection = None

    @contextmanager
    def get_connection(self):
        """Context manager для безопасной работы с подключением"""
        try:
            self.connection = psycopg2.connect(**self.config)
            yield self.connection
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()

    def execute_query(self, query: str, params=None):
        """Выполнить запрос и вернуть результат"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                conn.commit()
                return None

    def execute_many(self, query: str, data):
        """Выполнить множественную вставку"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, query, data, page_size=1000)
                conn.commit()
                return cur.rowcount

    def check_table_exists(self, table_name: str) -> bool:
        """Проверить существование таблицы"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else False

    def get_last_timestamp(self, table_name: str, symbol: str = 'BTCUSDT'):
        """Получить последний timestamp для символа"""
        query = f"""
            SELECT MAX(timestamp)
            FROM {table_name}
            WHERE symbol = %s;
        """
        result = self.execute_query(query, (symbol,))
        return result[0][0] if result and result[0][0] else None