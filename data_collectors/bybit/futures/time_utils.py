#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Time Utilities for Historical Data Collection
==============================================

This module provides robust time parsing, validation, and conversion utilities
optimized for cryptocurrency market data collection with proper timezone handling.
"""

import logging
import datetime
from typing import Tuple, Optional, Dict, Any
import pytz
from decimal import Decimal


class TimeManager:
    """
    Manages time conversion, validation, and calculations for data collection.

    Features:
    - Robust timezone-aware datetime parsing
    - UTC timestamp conversion with validation
    - Collection period planning and validation
    - Expected candle count calculations
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize time manager with configuration.

        Args:
            config: Configuration dictionary containing collection settings
        """
        self.config = config
        self.collection_config = config.get("collection", {})
        self.logger = logging.getLogger("TimeManager")

        # Get timezone from config, default to UTC
        self.timezone_name = self.collection_config.get("timezone", "UTC")
        try:
            self.timezone = pytz.timezone(self.timezone_name)
        except pytz.UnknownTimeZoneError:
            self.logger.warning(f"Unknown timezone '{self.timezone_name}', using UTC")
            self.timezone = pytz.UTC
            self.timezone_name = "UTC"

    def parse_datetime_string(self, date_string: str, field_name: str = "datetime") -> datetime.datetime:
        """
        Parse datetime string with timezone awareness.

        Args:
            date_string: Datetime string in format "YYYY-MM-DD HH:MM:SS"
            field_name: Name of the field for error reporting

        Returns:
            Timezone-aware datetime object

        Raises:
            ValueError: If datetime string is invalid
        """
        try:
            # Parse the datetime string
            dt = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

            # Make it timezone-aware
            if self.timezone_name == "UTC":
                dt = dt.replace(tzinfo=pytz.UTC)
            else:
                dt = self.timezone.localize(dt)
                # Convert to UTC for consistency
                dt = dt.astimezone(pytz.UTC)

            self.logger.debug(f"Parsed {field_name}: {date_string} -> {dt} UTC")
            return dt

        except ValueError as e:
            raise ValueError(f"Invalid {field_name} format '{date_string}'. Expected: YYYY-MM-DD HH:MM:SS") from e

    def datetime_to_timestamp(self, dt: datetime.datetime) -> int:
        """
        Convert datetime to UTC timestamp in milliseconds.

        Args:
            dt: Datetime object (timezone-aware preferred)

        Returns:
            UTC timestamp in milliseconds
        """
        # Ensure datetime is timezone-aware
        if dt.tzinfo is None:
            self.logger.warning("Naive datetime provided, assuming UTC")
            dt = dt.replace(tzinfo=pytz.UTC)
        elif dt.tzinfo != pytz.UTC:
            dt = dt.astimezone(pytz.UTC)

        # Convert to milliseconds timestamp
        timestamp_ms = int(dt.timestamp() * 1000)
        self.logger.debug(f"Converted {dt} to timestamp {timestamp_ms}")
        return timestamp_ms

    def timestamp_to_datetime(self, timestamp_ms: int) -> datetime.datetime:
        """
        Convert UTC timestamp in milliseconds to datetime.

        Args:
            timestamp_ms: UTC timestamp in milliseconds

        Returns:
            UTC datetime object
        """
        dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
        return dt

    def get_collection_period(self) -> Tuple[int, int, datetime.datetime, datetime.datetime]:
        """
        Parse and validate collection period from configuration.

        Returns:
            Tuple of (start_timestamp_ms, end_timestamp_ms, start_datetime, end_datetime)

        Raises:
            ValueError: If collection period configuration is invalid
        """
        start_date_str = self.collection_config.get("start_date")
        end_date_str = self.collection_config.get("end_date")

        if not start_date_str or not end_date_str:
            raise ValueError("Both start_date and end_date must be specified in collection config")

        # Parse dates
        start_dt = self.parse_datetime_string(start_date_str, "start_date")
        end_dt = self.parse_datetime_string(end_date_str, "end_date")

        # Validate period
        if start_dt >= end_dt:
            raise ValueError(f"start_date ({start_date_str}) must be before end_date ({end_date_str})")

        # Convert to timestamps
        start_ts = self.datetime_to_timestamp(start_dt)
        end_ts = self.datetime_to_timestamp(end_dt)

        # Log the period
        period_days = (end_dt - start_dt).total_seconds() / 86400
        self.logger.info(f"Collection period: {start_dt} to {end_dt} UTC ({period_days:.2f} days)")

        return start_ts, end_ts, start_dt, end_dt

    def calculate_expected_candles(self, start_ts: int, end_ts: int, interval_minutes: int = 1) -> int:
        """
        Calculate expected number of candles for a time period.

        Args:
            start_ts: Start timestamp in milliseconds
            end_ts: End timestamp in milliseconds
            interval_minutes: Candle interval in minutes

        Returns:
            Expected number of candles
        """
        period_ms = end_ts - start_ts
        period_minutes = period_ms / (1000 * 60)  # Convert to minutes
        expected_candles = int(period_minutes / interval_minutes) + 1

        self.logger.debug(f"Expected candles: {expected_candles} for {period_minutes:.0f} minutes")
        return expected_candles

    def validate_large_collection(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> Dict[str, Any]:
        """
        Validate and provide warnings for large data collections.

        Args:
            start_dt: Start datetime
            end_dt: End datetime

        Returns:
            Dictionary with validation results and recommendations
        """
        period_days = (end_dt - start_dt).total_seconds() / 86400
        warning_days = self.collection_config.get("large_collection_warning_days", 30)

        validation = {
            "period_days": period_days,
            "is_large": period_days > warning_days,
            "warnings": [],
            "recommendations": [],
            "estimated_candles": 0,
            "estimated_size_mb": 0,
            "estimated_time_minutes": 0,
        }

        # Calculate estimates
        expected_candles = self.calculate_expected_candles(
            self.datetime_to_timestamp(start_dt), self.datetime_to_timestamp(end_dt)
        )
        validation["estimated_candles"] = expected_candles

        # Estimate storage (approximately 200 bytes per candle)
        validation["estimated_size_mb"] = (expected_candles * 200) / (1024 * 1024)

        # Estimate time (assuming 10000 candles/minute collection speed)
        validation["estimated_time_minutes"] = expected_candles / 10000

        # Generate warnings and recommendations
        if period_days > warning_days:
            validation["warnings"].append(f"Large collection: {period_days:.1f} days of data")
            validation["recommendations"].append("Consider splitting into smaller periods")

        if expected_candles > 100000:
            validation["warnings"].append(f"High volume: {expected_candles:,} candles expected")
            validation["recommendations"].append("Ensure sufficient disk space")

        if validation["estimated_time_minutes"] > 60:
            validation["warnings"].append(
                f"Long duration: ~{validation['estimated_time_minutes']:.0f} minutes estimated"
            )
            validation["recommendations"].append("Consider running during off-peak hours")

        return validation

    def format_period_summary(
        self,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
        symbols: list,
        validation: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a formatted summary of the collection period.

        Args:
            start_dt: Start datetime
            end_dt: End datetime
            symbols: List of trading symbols
            validation: Optional validation results

        Returns:
            Formatted summary string
        """
        if validation is None:
            validation = self.validate_large_collection(start_dt, end_dt)

        summary = []
        summary.append("=" * 70)
        summary.append("COLLECTION PERIOD SUMMARY")
        summary.append("=" * 70)
        summary.append(f"Period: {start_dt} → {end_dt} UTC")
        summary.append(f"Duration: {validation['period_days']:.2f} days")
        summary.append(f"Timezone: {self.timezone_name}")
        summary.append(f"Symbols: {', '.join(symbols)} ({len(symbols)} symbols)")
        summary.append(f"Expected candles per symbol: {validation['estimated_candles']:,}")
        summary.append(f"Total expected candles: {validation['estimated_candles'] * len(symbols):,}")
        summary.append(f"Estimated storage: {validation['estimated_size_mb'] * len(symbols):.1f} MB")
        summary.append(f"Estimated time: {validation['estimated_time_minutes']:.1f} minutes")

        if validation["warnings"]:
            summary.append("\n⚠️  WARNINGS:")
            for warning in validation["warnings"]:
                summary.append(f"   • {warning}")

        if validation["recommendations"]:
            summary.append("\n💡 RECOMMENDATIONS:")
            for rec in validation["recommendations"]:
                summary.append(f"   • {rec}")

        summary.append("=" * 70)
        return "\n".join(summary)
