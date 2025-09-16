#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Management for Candle Data Collection
===============================================

This module provides database operations for storing historical candle data.
Optimized for PostgreSQL with connection pooling and bulk operations.
"""

import os
import sys
import yaml
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras
from typing import List, Dict, Any, Optional, Tuple, Union
from decimal import Decimal
import datetime
from contextlib import contextmanager


class DatabaseManager:
    """
    Manages PostgreSQL database connections and table creation/management.

    Features:
    - PostgreSQL connection pooling
    - Automatic table creation with indexes
    - Database health checks
    - Schema validation
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database manager.

        Args:
            config: Configuration dictionary containing database settings
        """
        self.config = config
        self.db_config = config.get("database", {})
        self.db_type = "postgres"  # Fixed to PostgreSQL only
        self.table_name = self.db_config.get("table_name", "candles_bybit_futures_1m")

        self.logger = logging.getLogger("DatabaseManager")
        self.connection_pool = None

        self._initialize_database()

    def _initialize_database(self):
        """Initialize PostgreSQL database connection and create tables if needed."""
        try:
            self._init_postgres()
            self._create_tables()
            self.logger.info("PostgreSQL database initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def _init_postgres(self):
        """Initialize PostgreSQL connection pool."""
        try:
            pool_config = self.db_config.get("connection_pool", {})

            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=pool_config.get("min_connections", 2),
                maxconn=pool_config.get("max_connections", 10),
                host=self.db_config.get("host", "localhost"),
                port=self.db_config.get("port", 5432),
                database=self.db_config.get("database", "trading_db"),
                user=self.db_config.get("user", "postgres"),
                password=self.db_config.get("password", "postgres"),
                connect_timeout=pool_config.get("timeout", 30),
            )

            self.logger.info("PostgreSQL connection pool created")

        except Exception as e:
            self.logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Get PostgreSQL database connection with automatic cleanup.

        Yields:
            PostgreSQL connection object
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def _create_tables(self):
        """Check if table exists, create only if database is empty."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Check if table already exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                    """,
                    (self.table_name,)
                )

                table_exists = cursor.fetchone()[0]

                if table_exists:
                    self.logger.info(f"Table '{self.table_name}' already exists, skipping creation")
                    return

                # Create table only if it doesn't exist
                # Using the correct structure that matches our VPS database
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open DECIMAL(20,8) NOT NULL,
                    high DECIMAL(20,8) NOT NULL,
                    low DECIMAL(20,8) NOT NULL,
                    close DECIMAL(20,8) NOT NULL,
                    volume DECIMAL(20,8),
                    turnover DECIMAL(20,8),
                    PRIMARY KEY (timestamp, symbol)
                );
                """

                # Create indexes
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON {self.table_name}(symbol, timestamp);",
                    f"CREATE INDEX IF NOT EXISTS idx_timestamp ON {self.table_name}(timestamp);",
                ]

                # Execute table creation
                cursor.execute(create_table_sql)

                # Execute index creation
                for index_sql in indexes:
                    cursor.execute(index_sql)

                conn.commit()
                self.logger.info(f"Table '{self.table_name}' and indexes created successfully")

        except Exception as e:
            # If error is about permissions, just log and continue
            if "permission denied" in str(e).lower() or "must be owner" in str(e).lower():
                self.logger.info(f"Table '{self.table_name}' exists (permission check), continuing...")
            else:
                self.logger.error(f"Failed to create tables: {e}")
                raise

    def check_table_exists(self) -> bool:
        """
        Check if the PostgreSQL candles table exists.

        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """,
                    (self.table_name,),
                )

                result = cursor.fetchone()
                return bool(result and result[0])

        except Exception as e:
            self.logger.error(f"Failed to check table existence: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the candles table.

        Returns:
            Dictionary with table statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_rows = cursor.fetchone()[0]

                # Get symbol count
                cursor.execute(f"SELECT COUNT(DISTINCT symbol) FROM {self.table_name}")
                symbol_count = cursor.fetchone()[0]

                # Get date range
                cursor.execute(f"SELECT MIN(open_time), MAX(open_time) FROM {self.table_name}")
                time_range = cursor.fetchone()

                min_time = None
                max_time = None
                if time_range[0] and time_range[1]:
                    min_time = datetime.datetime.fromtimestamp(time_range[0] / 1000)
                    max_time = datetime.datetime.fromtimestamp(time_range[1] / 1000)

                return {
                    "table_name": self.table_name,
                    "total_rows": total_rows,
                    "symbol_count": symbol_count,
                    "min_time": min_time,
                    "max_time": max_time,
                    "db_type": self.db_type,
                }

        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            return {"error": str(e)}

    def close(self):
        """Close database connections."""
        try:
            if self.connection_pool:
                self.connection_pool.closeall()
                self.logger.info("PostgreSQL connection pool closed")

        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")


class CandleDataManager:
    """
    Manages PostgreSQL candle data operations including insertion, querying, and validation.

    Features:
    - Bulk data insertion with duplicate handling using PostgreSQL execute_values
    - Data validation and cleanup
    - Progress tracking
    - Memory-efficient batch processing
    - Smart gap detection and collection planning
    """

    def __init__(self, db_manager: DatabaseManager, config: Dict[str, Any]):
        """
        Initialize candle data manager.

        Args:
            db_manager: DatabaseManager instance
            config: Configuration dictionary
        """
        self.db_manager = db_manager
        self.config = config
        self.logger = logging.getLogger("CandleDataManager")

        self.table_name = db_manager.table_name
        self.skip_duplicates = config.get("collection", {}).get("skip_duplicates", True)
        self.use_bulk_insert = config.get("advanced", {}).get("use_bulk_insert", True)
        self.commit_frequency = config.get("advanced", {}).get("commit_frequency", 1000)

    def _prepare_candle_data(self, candle: List[Any], symbol: str, interval: str) -> Tuple:
        """
        Prepare single candle data for database insertion.

        Args:
            candle: Raw candle data [timestamp, open, high, low, close, volume, turnover]
            symbol: Trading pair symbol
            interval: Timeframe interval

        Returns:
            Tuple ready for database insertion
        """
        try:
            timestamp = int(candle[0])
            open_price = Decimal(str(candle[1]))
            high_price = Decimal(str(candle[2]))
            low_price = Decimal(str(candle[3]))
            close_price = Decimal(str(candle[4]))
            volume = Decimal(str(candle[5]))
            turnover = Decimal(str(candle[6])) if len(candle) > 6 and candle[6] else None

            # Convert timestamp from milliseconds to PostgreSQL timestamp
            from datetime import datetime, timezone
            timestamp_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)

            # Return in the order matching our table: timestamp, symbol, open, high, low, close, volume, turnover
            return (
                timestamp_dt,   # timestamp as datetime object
                symbol,         # symbol
                open_price,     # open
                high_price,     # high
                low_price,      # low
                close_price,    # close
                volume,         # volume
                turnover or Decimal("0"),  # turnover
            )

        except (ValueError, TypeError, IndexError) as e:
            self.logger.error(f"Failed to prepare candle data: {e}")
            raise

    def insert_candles_batch(self, candles: List[List[Any]], symbol: str, interval: str = "1") -> int:
        """
        Insert a batch of candles into the database.

        Args:
            candles: List of candle data
            symbol: Trading pair symbol
            interval: Timeframe interval

        Returns:
            Number of candles successfully inserted
        """
        if not candles:
            return 0

        try:
            # Prepare data
            prepared_data = []
            for candle in candles:
                try:
                    prepared_candle = self._prepare_candle_data(candle, symbol, interval)
                    prepared_data.append(prepared_candle)
                except Exception as e:
                    self.logger.warning(f"Skipping invalid candle for {symbol}: {e}")
                    continue

            if not prepared_data:
                self.logger.warning(f"No valid candles to insert for {symbol}")
                return 0

            # Insert data
            inserted_count = 0

            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                if self.use_bulk_insert:
                    inserted_count = self._bulk_insert_postgres(cursor, prepared_data)
                else:
                    inserted_count = self._insert_one_by_one(cursor, prepared_data)

                conn.commit()

            # Only log in debug mode to reduce console clutter
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Inserted {inserted_count} candles for {symbol}")
            return inserted_count

        except Exception as e:
            self.logger.error(f"Failed to insert candles for {symbol}: {e}")
            raise

    def _bulk_insert_postgres(self, cursor, prepared_data: List[Tuple]) -> int:
        """
        Perform bulk insert for PostgreSQL using execute_values.

        Args:
            cursor: Database cursor
            prepared_data: List of prepared candle tuples

        Returns:
            Number of inserted rows
        """
        try:
            if self.skip_duplicates:
                insert_sql = f"""
                INSERT INTO {self.table_name}
                (timestamp, symbol, open, high, low, close, volume, turnover)
                VALUES %s
                ON CONFLICT (timestamp, symbol) DO NOTHING
                """
            else:
                insert_sql = f"""
                INSERT INTO {self.table_name}
                (timestamp, symbol, open, high, low, close, volume, turnover)
                VALUES %s
                """

            # Use execute_values for bulk insert
            psycopg2.extras.execute_values(cursor, insert_sql, prepared_data, template=None, page_size=1000)

            # Get number of affected rows
            return cursor.rowcount

        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            raise

    def _insert_one_by_one(self, cursor, prepared_data: List[Tuple]) -> int:
        """
        Insert candles one by one (PostgreSQL fallback method).

        Args:
            cursor: Database cursor
            prepared_data: List of prepared candle tuples

        Returns:
            Number of inserted rows
        """
        inserted_count = 0

        if self.skip_duplicates:
            insert_sql = f"""
            INSERT INTO {self.table_name}
            (timestamp, symbol, open, high, low, close, volume, turnover)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, symbol) DO NOTHING
            """
        else:
            insert_sql = f"""
            INSERT INTO {self.table_name}
            (timestamp, symbol, open, high, low, close, volume, turnover)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

        for i, candle_data in enumerate(prepared_data):
            try:
                cursor.execute(insert_sql, candle_data)
                if cursor.rowcount > 0:
                    inserted_count += 1

                # Commit periodically for large batches
                if (i + 1) % self.commit_frequency == 0:
                    cursor.connection.commit()

            except Exception as e:
                self.logger.warning(f"Failed to insert single candle: {e}")
                continue

        return inserted_count

    def get_existing_data_range(self, symbol: str, interval: str = "1") -> Optional[Tuple[int, int]]:
        """
        Get the time range of existing data for a symbol.

        Args:
            symbol: Trading pair symbol
            interval: Timeframe interval (not used, kept for compatibility)

        Returns:
            Tuple of (min_timestamp, max_timestamp) in milliseconds or None if no data exists
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    f"""
                    SELECT
                        EXTRACT(EPOCH FROM MIN(timestamp))::bigint * 1000,
                        EXTRACT(EPOCH FROM MAX(timestamp))::bigint * 1000
                    FROM {self.table_name}
                    WHERE symbol = %s
                """,
                    (symbol,),
                )

                result = cursor.fetchone()

                if result and result[0] and result[1]:
                    return (result[0], result[1])

                return None

        except Exception as e:
            self.logger.error(f"Failed to get existing data range for {symbol}: {e}")
            return None

    def count_candles(self, symbol: Optional[str] = None, interval: str = "1") -> int:
        """
        Count candles in the database.

        Args:
            symbol: Optional symbol to filter by
            interval: Timeframe interval (not used, kept for compatibility)

        Returns:
            Number of candles
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                if symbol:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*) FROM {self.table_name}
                        WHERE symbol = %s
                    """,
                        (symbol,),
                    )
                else:
                    cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")

                result = cursor.fetchone()
                return result[0] if result else 0

        except Exception as e:
            self.logger.error(f"Failed to count candles: {e}")
            return 0

    def get_symbols_list(self) -> List[str]:
        """
        Get list of all symbols in the database.

        Returns:
            List of symbol strings
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(f"SELECT DISTINCT symbol FROM {self.table_name} ORDER BY symbol")

                results = cursor.fetchall()
                return [row[0] for row in results]

        except Exception as e:
            self.logger.error(f"Failed to get symbols list: {e}")
            return []

    def find_missing_periods(
        self, symbol: str, start_timestamp: int, end_timestamp: int, interval: str = "1"
    ) -> List[Tuple[int, int]]:
        """
        Find missing periods in existing data for a symbol within the specified range.

        Args:
            symbol: Trading pair symbol
            start_timestamp: Start timestamp in milliseconds
            end_timestamp: End timestamp in milliseconds
            interval: Timeframe interval

        Returns:
            List of (start_ts, end_ts) tuples representing missing periods
        """
        try:
            missing_periods = []

            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Get existing data range for the symbol
                existing_range = self.get_existing_data_range(symbol, interval)

                if not existing_range:
                    # No existing data - entire range is missing
                    return [(start_timestamp, end_timestamp)]

                existing_start, existing_end = existing_range

                # Check for missing period before existing data
                if start_timestamp < existing_start:
                    missing_periods.append((start_timestamp, min(existing_start, end_timestamp)))

                # Check for missing period after existing data
                if end_timestamp > existing_end:
                    missing_periods.append((max(existing_end, start_timestamp), end_timestamp))

                # Check for gaps within existing data range (if requested range overlaps)
                if start_timestamp < existing_end and end_timestamp > existing_start:
                    gap_start = max(start_timestamp, existing_start)
                    gap_end = min(end_timestamp, existing_end)

                    # Find gaps by looking for missing minute intervals
                    cursor.execute(
                        f"""
                        WITH RECURSIVE time_series AS (
                            SELECT %s AS ts
                            UNION ALL
                            SELECT ts + 60000  -- Add 1 minute (60000 ms)
                            FROM time_series
                            WHERE ts < %s
                        )
                        SELECT ts FROM time_series
                        WHERE ts NOT IN (
                            SELECT EXTRACT(EPOCH FROM timestamp)::bigint * 1000
                            FROM {self.table_name}
                            WHERE symbol = %s
                            AND EXTRACT(EPOCH FROM timestamp)::bigint * 1000 >= %s
                            AND EXTRACT(EPOCH FROM timestamp)::bigint * 1000 <= %s
                        )
                        ORDER BY ts
                    """,
                        (gap_start, gap_end, symbol, gap_start, gap_end),
                    )

                    missing_timestamps = [row[0] for row in cursor.fetchall()]

                    # Group consecutive missing timestamps into periods
                    if missing_timestamps:
                        period_start = missing_timestamps[0]
                        last_ts = missing_timestamps[0]

                        for ts in missing_timestamps[1:]:
                            if ts - last_ts > 60000:  # Gap > 1 minute
                                missing_periods.append((period_start, last_ts))
                                period_start = ts
                            last_ts = ts

                        # Add the last period
                        missing_periods.append((period_start, last_ts))

            self.logger.debug(f"Found {len(missing_periods)} missing periods for {symbol}")
            return missing_periods

        except Exception as e:
            self.logger.error(f"Failed to find missing periods for {symbol}: {e}")
            return [(start_timestamp, end_timestamp)]  # Return full range on error

    def get_collection_plan(
        self,
        symbols: List[str],
        start_timestamp: int,
        end_timestamp: int,
        interval: str = "1",
    ) -> Dict[str, List[Tuple[int, int]]]:
        """
        Create a collection plan showing what periods need to be collected for each symbol.

        Args:
            symbols: List of trading pair symbols
            start_timestamp: Start timestamp in milliseconds
            end_timestamp: End timestamp in milliseconds
            interval: Timeframe interval

        Returns:
            Dictionary mapping symbol -> list of (start_ts, end_ts) periods to collect
        """
        try:
            collection_plan = {}

            for symbol in symbols:
                missing_periods = self.find_missing_periods(symbol, start_timestamp, end_timestamp, interval)
                collection_plan[symbol] = missing_periods

            # Log summary
            total_periods = sum(len(periods) for periods in collection_plan.values())
            self.logger.info(f"Collection plan created: {total_periods} total periods across {len(symbols)} symbols")

            return collection_plan

        except Exception as e:
            self.logger.error(f"Failed to create collection plan: {e}")
            # Return full range for all symbols on error
            return {symbol: [(start_timestamp, end_timestamp)] for symbol in symbols}


def create_database_managers(
    config_path: str = "data_collectors/data_collector_config.yaml",
) -> Tuple[DatabaseManager, CandleDataManager]:
    """
    Create and initialize database managers.

    Args:
        config_path: Path to configuration file

    Returns:
        Tuple of (DatabaseManager, CandleDataManager)
    """
    try:
        # Load configuration
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Create managers
        db_manager = DatabaseManager(config)
        candle_manager = CandleDataManager(db_manager, config)

        return db_manager, candle_manager

    except Exception as e:
        logging.error(f"Failed to create database managers: {e}")
        raise


def main():
    """Test database functionality."""
    try:
        print("🗄️ Testing database functionality...")

        # Create managers
        db_manager, candle_manager = create_database_managers()

        # Check table exists
        if db_manager.check_table_exists():
            print("✅ Table exists")
        else:
            print("❌ Table does not exist")

        # Get table info
        info = db_manager.get_table_info()
        print(f"📊 Table info: {info}")

        # Count candles
        total_candles = candle_manager.count_candles()
        print(f"📈 Total candles: {total_candles:,}")

        # Get symbols
        symbols = candle_manager.get_symbols_list()
        print(f"💱 Symbols: {symbols}")

        # Close connections
        db_manager.close()

        print("✅ Database test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


if __name__ == "__main__":
    import sys

    success = main()
    sys.exit(0 if success else 1)
