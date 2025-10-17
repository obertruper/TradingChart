-- ============================================================================
-- Check ADX data in database
-- ============================================================================
-- This script checks the current state of ADX data loading
-- Shows:
-- 1. Count of records with ADX, +DI, -DI values
-- 2. First and last dates with data
-- 3. Percentage of filled records
--
-- Usage:
--   psql -h 82.25.115.144 -U trading_reader -d trading_data -f check_adx_data.sql
-- ============================================================================

\echo '=========================================='
\echo 'ADX Data Analysis for BTCUSDT - 1m timeframe'
\echo '=========================================='
\echo ''

-- Check ADX_7 statistics
\echo 'ADX_7 Statistics:'
\echo '------------------'
SELECT
    'Total records' as metric,
    COUNT(*) as value
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
UNION ALL
SELECT
    'Records with +DI' as metric,
    COUNT(*) as value
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_plus_di IS NOT NULL
UNION ALL
SELECT
    'Records with -DI' as metric,
    COUNT(*) as value
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_minus_di IS NOT NULL
UNION ALL
SELECT
    'Records with ADX' as metric,
    COUNT(*) as value
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7 IS NOT NULL;

\echo ''
\echo 'First and Last Dates with ADX_7 Data:'
\echo '--------------------------------------'
SELECT
    'First +DI date' as metric,
    MIN(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_plus_di IS NOT NULL
UNION ALL
SELECT
    'First -DI date' as metric,
    MIN(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_minus_di IS NOT NULL
UNION ALL
SELECT
    'First ADX date' as metric,
    MIN(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7 IS NOT NULL
UNION ALL
SELECT
    'Last +DI date' as metric,
    MAX(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_plus_di IS NOT NULL
UNION ALL
SELECT
    'Last -DI date' as metric,
    MAX(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7_minus_di IS NOT NULL
UNION ALL
SELECT
    'Last ADX date' as metric,
    MAX(timestamp) as date
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7 IS NOT NULL;

\echo ''
\echo 'Sample ADX_7 Data (latest 10 records with ADX):'
\echo '------------------------------------------------'
SELECT
    timestamp,
    ROUND(adx_7::numeric, 2) as adx,
    ROUND(adx_7_plus_di::numeric, 2) as plus_di,
    ROUND(adx_7_minus_di::numeric, 2) as minus_di,
    CASE
        WHEN adx_7 < 25 THEN 'Weak trend'
        WHEN adx_7 < 50 THEN 'Strong trend'
        WHEN adx_7 < 75 THEN 'Very strong'
        ELSE 'Extreme'
    END as strength,
    CASE
        WHEN adx_7_plus_di > adx_7_minus_di THEN 'Bullish'
        ELSE 'Bearish'
    END as direction
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND adx_7 IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10;

\echo ''
\echo '=========================================='
\echo 'Date Range Analysis'
\echo '=========================================='
\echo ''

-- Show date range of current loading progress
WITH date_stats AS (
    SELECT
        MIN(timestamp) as first_candle,
        MAX(timestamp) as last_candle,
        MAX(timestamp) FILTER (WHERE adx_7_plus_di IS NOT NULL) as last_plus_di,
        MAX(timestamp) FILTER (WHERE adx_7 IS NOT NULL) as last_adx
    FROM indicators_bybit_futures_1m
    WHERE symbol = 'BTCUSDT'
)
SELECT
    'First candle' as metric,
    first_candle as date,
    '' as days_from_start
FROM date_stats
UNION ALL
SELECT
    'Last candle' as metric,
    last_candle as date,
    EXTRACT(DAY FROM last_candle - first_candle)::text || ' days' as days_from_start
FROM date_stats
UNION ALL
SELECT
    'Last +DI data' as metric,
    last_plus_di as date,
    EXTRACT(DAY FROM last_plus_di - first_candle)::text || ' days' as days_from_start
FROM date_stats
UNION ALL
SELECT
    'Last ADX data' as metric,
    last_adx as date,
    EXTRACT(DAY FROM last_adx - first_candle)::text || ' days' as days_from_start
FROM date_stats;

\echo ''
\echo '=========================================='
\echo 'Explanation'
\echo '=========================================='
\echo ''
\echo '+DI/-DI appear after first smoothing (~7-14 candles)'
\echo 'ADX appears after DOUBLE smoothing (~14-28 candles)'
\echo ''
\echo 'For ADX_7:'
\echo '  - +DI/-DI should appear after ~7-14 minutes from start'
\echo '  - ADX should appear after ~14-28 minutes from start'
\echo ''
\echo 'Current loading status shows where the loader has progressed to.'
\echo '=========================================='
