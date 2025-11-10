"""
Mathematical Data Validation Suite for Technical Indicators

This package provides comprehensive validation scripts that verify the mathematical
correctness and completeness of technical indicator data stored in the database.

Unlike status check scripts (check_*_status.py) that only verify data presence,
these validation scripts recalculate indicators from source data and compare
results to detect:
- Mathematical calculation errors
- Missing data points
- Data corruption
- Inconsistencies between timeframes

Modules:
    check_sma_data: SMA (Simple Moving Average) mathematical validation
    check_ema_data: EMA (Exponential Moving Average) mathematical validation
"""
