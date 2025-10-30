#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Validator for Historical Data Collection
======================================================

This module provides comprehensive validation and templates for data collection configuration.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
import datetime
import pytz


class ConfigValidator:
    """
    Validates and processes data collection configuration.

    Features:
    - Comprehensive configuration validation
    - Template generation for common scenarios
    - Smart defaults and recommendations
    - Storage and time estimation
    """

    def __init__(self):
        self.logger = logging.getLogger("ConfigValidator")

    def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate complete configuration and return validation results.

        Args:
            config: Configuration dictionary

        Returns:
            Dictionary with validation results, warnings, and recommendations
        """
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "recommendations": [],
            "corrected_config": config.copy(),
            "estimated_metrics": {},
        }

        try:
            # Validate each section
            self._validate_api_config(config, validation_result)
            self._validate_database_config(config, validation_result)
            self._validate_collection_config(config, validation_result)
            self._validate_exchange_config(config, validation_result)
            self._validate_advanced_config(config, validation_result)

            # Calculate estimates
            self._calculate_estimates(config, validation_result)

            # Generate recommendations
            self._generate_recommendations(config, validation_result)

        except Exception as e:
            validation_result["errors"].append(f"Validation error: {str(e)}")
            validation_result["is_valid"] = False

        return validation_result

    def _validate_api_config(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Validate API configuration section."""
        api_config = config.get("api", {})
        bybit_config = api_config.get("bybit", {})

        # Required fields
        if not bybit_config.get("api_key"):
            result["errors"].append("API key is required (api.bybit.api_key)")

        if not bybit_config.get("api_secret"):
            result["errors"].append("API secret is required (api.bybit.api_secret)")

        # Validate testnet setting
        testnet = bybit_config.get("testnet", False)
        if testnet:
            result["warnings"].append("Using testnet environment - no real data will be collected")

    def _validate_database_config(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Validate database configuration section."""
        db_config = config.get("database", {})

        # Database type
        db_type = db_config.get("type", "postgres").lower()
        if db_type != "postgres":
            result["errors"].append("Only PostgreSQL is supported for database type")

        # Required PostgreSQL fields
        required_fields = ["host", "port", "database", "user"]
        for field in required_fields:
            if not db_config.get(field):
                result["errors"].append(f"Database {field} is required (database.{field})")

        # Validate connection pool settings
        pool_config = db_config.get("connection_pool", {})
        min_conn = pool_config.get("min_connections", 2)
        max_conn = pool_config.get("max_connections", 10)

        if min_conn < 1:
            result["warnings"].append("Minimum connections should be at least 1")
            result["corrected_config"]["database"]["connection_pool"]["min_connections"] = 1

        if max_conn < min_conn:
            result["warnings"].append("Maximum connections should be >= minimum connections")
            result["corrected_config"]["database"]["connection_pool"]["max_connections"] = max(min_conn, 10)

        # Table name validation
        table_name = db_config.get("table_name", "candles_bybit_futures_1m")
        if not table_name or not table_name.replace("_", "").replace("-", "").isalnum():
            result["warnings"].append(
                "Table name should contain only alphanumeric characters, hyphens, and underscores"
            )

    def _validate_collection_config(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Validate collection configuration section."""
        collection_config = config.get("collection", {})

        # Date validation
        start_date = collection_config.get("start_date")
        end_date = collection_config.get("end_date")

        if not start_date:
            result["errors"].append("Start date is required (collection.start_date)")
        else:
            try:
                start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                result["errors"].append("Invalid start_date format. Use: YYYY-MM-DD HH:MM:SS")

        if not end_date:
            result["errors"].append("End date is required (collection.end_date)")
        else:
            try:
                end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                if start_date:
                    try:
                        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                        if start_dt >= end_dt:
                            result["errors"].append("Start date must be before end date")

                        # Check for very large periods
                        period_days = (end_dt - start_dt).total_seconds() / 86400
                        if period_days > 365:
                            result["warnings"].append(f"Large collection period: {period_days:.1f} days")
                            result["recommendations"].append(
                                "Consider splitting large collections into smaller periods"
                            )

                    except ValueError:
                        pass  # Already handled above
            except ValueError:
                result["errors"].append("Invalid end_date format. Use: YYYY-MM-DD HH:MM:SS")

        # Timezone validation
        timezone = collection_config.get("timezone", "UTC")
        try:
            pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            result["warnings"].append(f"Unknown timezone '{timezone}', will use UTC")
            result["corrected_config"]["collection"]["timezone"] = "UTC"

        # Symbols validation
        symbols = collection_config.get("symbols", [])
        if not symbols:
            result["errors"].append("At least one symbol is required (collection.symbols)")
        else:
            for symbol in symbols:
                if not isinstance(symbol, str) or not symbol:
                    result["warnings"].append(f"Invalid symbol: {symbol}")
                elif not symbol.endswith("USDT"):
                    result["warnings"].append(f"Symbol {symbol} doesn't end with USDT - may not be valid")

        # Mode validation
        mode = collection_config.get("mode", "smart_gaps")
        valid_modes = ["full_range", "smart_gaps", "force_overwrite"]
        if mode not in valid_modes:
            result["warnings"].append(f"Invalid mode '{mode}'. Valid modes: {', '.join(valid_modes)}")
            result["corrected_config"]["collection"]["mode"] = "smart_gaps"

        # Interval validation
        interval = collection_config.get("interval", "1")
        if interval != "1":
            result["warnings"].append("Only 1-minute intervals are currently supported")
            result["corrected_config"]["collection"]["interval"] = "1"

        # Large collection settings
        large_warning_days = collection_config.get("large_collection_warning_days", 30)
        if large_warning_days < 1:
            result["warnings"].append("large_collection_warning_days should be at least 1")
            result["corrected_config"]["collection"]["large_collection_warning_days"] = 30

    def _validate_exchange_config(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Validate exchange configuration section."""
        exchange_config = config.get("exchange", {})

        # Exchange name
        exchange_name = exchange_config.get("name", "bybit")
        if exchange_name != "bybit":
            result["warnings"].append("Only Bybit exchange is currently supported")
            result["corrected_config"]["exchange"]["name"] = "bybit"

        # Category validation
        category = exchange_config.get("category", "linear")
        valid_categories = ["linear", "spot", "inverse"]
        if category not in valid_categories:
            result["errors"].append(f"Invalid category '{category}'. Valid categories: {', '.join(valid_categories)}")
            result["corrected_config"]["exchange"]["category"] = "linear"

        # Rate limit validation
        rate_limit = exchange_config.get("rate_limit", {})
        requests_per_minute = rate_limit.get("requests_per_minute", 100)
        if requests_per_minute > 120:
            result["warnings"].append("Bybit API limit is 120 requests/minute. Reducing to safe limit.")
            result["corrected_config"]["exchange"]["rate_limit"]["requests_per_minute"] = 100

    def _validate_advanced_config(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Validate advanced configuration section."""
        advanced_config = config.get("advanced", {})

        # Threading validation
        if advanced_config.get("use_threads", False):
            result["warnings"].append("Multi-threading is experimental and may cause issues")

        # Commit frequency
        commit_freq = advanced_config.get("commit_frequency", 1000)
        if commit_freq < 100:
            result["warnings"].append("Low commit frequency may impact performance")
        elif commit_freq > 10000:
            result["warnings"].append("High commit frequency may cause memory issues")
            result["corrected_config"]["advanced"]["commit_frequency"] = 5000

    def _calculate_estimates(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Calculate collection estimates."""
        try:
            collection_config = config.get("collection", {})
            start_date = collection_config.get("start_date")
            end_date = collection_config.get("end_date")
            symbols = collection_config.get("symbols", [])

            if start_date and end_date:
                start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

                period_minutes = (end_dt - start_dt).total_seconds() / 60
                candles_per_symbol = int(period_minutes) + 1
                total_candles = candles_per_symbol * len(symbols)

                # Storage estimates (approximately 200 bytes per candle)
                storage_bytes = total_candles * 200
                storage_mb = storage_bytes / (1024 * 1024)

                # Time estimates (assuming 15000 candles/minute average speed)
                estimated_minutes = total_candles / 15000

                result["estimated_metrics"] = {
                    "period_days": (end_dt - start_dt).total_seconds() / 86400,
                    "candles_per_symbol": candles_per_symbol,
                    "total_candles": total_candles,
                    "storage_mb": storage_mb,
                    "estimated_time_minutes": estimated_minutes,
                    "symbols_count": len(symbols),
                }
        except Exception as e:
            result["warnings"].append(f"Could not calculate estimates: {str(e)}")

    def _generate_recommendations(self, config: Dict[str, Any], result: Dict[str, Any]):
        """Generate recommendations based on configuration."""
        metrics = result.get("estimated_metrics", {})

        # Large collection recommendations
        if metrics.get("period_days", 0) > 90:
            result["recommendations"].append("Consider using multiple smaller collection runs for periods > 90 days")

        if metrics.get("total_candles", 0) > 1000000:
            result["recommendations"].append("Large dataset detected - ensure sufficient RAM and disk space")

        if metrics.get("estimated_time_minutes", 0) > 120:
            result["recommendations"].append("Long collection time - consider running during off-peak hours")

        # Performance recommendations
        collection_config = config.get("collection", {})
        if collection_config.get("batch_delay_seconds", 0.5) < 0.3:
            result["recommendations"].append("Consider increasing batch_delay_seconds to avoid rate limiting")

        # Monitoring recommendations
        if not config.get("monitoring", {}).get("track_collection_speed", True):
            result["recommendations"].append("Enable monitoring.track_collection_speed for performance insights")

    def generate_template(self, template_type: str) -> Dict[str, Any]:
        """
        Generate configuration templates for common scenarios.

        Args:
            template_type: Type of template ('quick_test', 'weekly_collection', 'full_history', 'production')

        Returns:
            Configuration template dictionary
        """
        templates = {
            "quick_test": self._template_quick_test(),
            "weekly_collection": self._template_weekly_collection(),
            "monthly_collection": self._template_monthly_collection(),
            "production": self._template_production(),
        }

        return templates.get(template_type, templates["quick_test"])

    def _template_quick_test(self) -> Dict[str, Any]:
        """Generate quick test template (last 2 hours)."""
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(hours=2)

        return {
            "collection": {
                "start_date": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timezone": "UTC",
                "interval": "1",
                "symbols": ["BTCUSDT"],
                "mode": "full_range",
                "smart_collection": True,
                "validate_continuity": True,
                "batch_delay_seconds": 0.5,
            },
            "advanced": {"test_mode": True, "use_bulk_insert": True, "commit_frequency": 1000},
        }

    def _template_weekly_collection(self) -> Dict[str, Any]:
        """Generate weekly collection template."""
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=7)

        return {
            "collection": {
                "start_date": start_time.strftime("%Y-%m-%d 00:00:00"),
                "end_date": end_time.strftime("%Y-%m-%d 23:59:59"),
                "timezone": "UTC",
                "interval": "1",
                "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                "mode": "full_range",
                "smart_collection": True,
                "validate_continuity": True,
                "batch_delay_seconds": 0.5,
                "progress_checkpoint_frequency": 5000,
            },
            "monitoring": {"track_collection_speed": True, "estimate_completion_time": True},
        }

    def _template_monthly_collection(self) -> Dict[str, Any]:
        """Generate monthly collection template."""
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=30)

        return {
            "collection": {
                "start_date": start_time.strftime("%Y-%m-%d 00:00:00"),
                "end_date": end_time.strftime("%Y-%m-%d 23:59:59"),
                "timezone": "UTC",
                "interval": "1",
                "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
                "mode": "full_range",
                "smart_collection": True,
                "validate_continuity": True,
                "batch_delay_seconds": 0.6,
                "progress_checkpoint_frequency": 10000,
                "large_collection_warning_days": 30,
            },
            "monitoring": {
                "track_collection_speed": True,
                "estimate_completion_time": True,
                "max_consecutive_errors": 5,
            },
            "advanced": {"use_bulk_insert": True, "commit_frequency": 2000},
        }

    def _template_production(self) -> Dict[str, Any]:
        """Generate production-ready template."""
        return {
            "collection": {
                "timezone": "UTC",
                "interval": "1",
                "mode": "full_range",
                "smart_collection": True,
                "validate_continuity": True,
                "batch_delay_seconds": 0.7,
                "progress_checkpoint_frequency": 10000,
                "large_collection_warning_days": 60,
            },
            "exchange": {
                "rate_limit": {"requests_per_minute": 90, "batch_delay": 0.7},  # Conservative
                "retry_attempts": 5,
                "retry_delay": 2,
            },
            "monitoring": {
                "track_collection_speed": True,
                "estimate_completion_time": True,
                "max_consecutive_errors": 3,
                "stop_on_critical_error": True,
            },
            "advanced": {"use_bulk_insert": True, "commit_frequency": 2000, "resume_from_checkpoint": True},
            "logging": {"level": "INFO", "progress_interval": 500},
        }
