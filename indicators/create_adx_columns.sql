-- ============================================================================
-- Create ADX (Average Directional Index) columns in indicators tables
-- ============================================================================
-- This script creates all 24 ADX columns (8 periods × 3 components)
-- for all timeframes: 1m, 15m, 1h
--
-- Run this script BEFORE loading ADX data to avoid column creation overhead
-- during data loading process.
--
-- Usage:
--   psql -h 82.25.115.144 -U trading_admin -d trading_data -f create_adx_columns.sql
-- ============================================================================

\echo '=========================================='
\echo 'Creating ADX columns for 1m timeframe'
\echo '=========================================='

-- 1m timeframe
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_7 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_7_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_7_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_10 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_10_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_10_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_14 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_14_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_14_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_20 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_20_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_20_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_21 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_21_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_21_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_25 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_25_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_25_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_30 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_30_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_30_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_50 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_50_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1m ADD COLUMN IF NOT EXISTS adx_50_minus_di DECIMAL(10,4);

\echo 'Created 24 ADX columns for 1m timeframe'

\echo '=========================================='
\echo 'Creating ADX columns for 15m timeframe'
\echo '=========================================='

-- 15m timeframe
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_7 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_7_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_7_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_10 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_10_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_10_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_14 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_14_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_14_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_20 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_20_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_20_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_21 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_21_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_21_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_25 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_25_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_25_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_30 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_30_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_30_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_50 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_50_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_15m ADD COLUMN IF NOT EXISTS adx_50_minus_di DECIMAL(10,4);

\echo 'Created 24 ADX columns for 15m timeframe'

\echo '=========================================='
\echo 'Creating ADX columns for 1h timeframe'
\echo '=========================================='

-- 1h timeframe
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_7 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_7_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_7_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_10 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_10_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_10_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_14 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_14_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_14_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_20 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_20_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_20_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_21 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_21_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_21_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_25 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_25_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_25_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_30 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_30_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_30_minus_di DECIMAL(10,4);

ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_50 DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_50_plus_di DECIMAL(10,4);
ALTER TABLE indicators_bybit_futures_1h ADD COLUMN IF NOT EXISTS adx_50_minus_di DECIMAL(10,4);

\echo 'Created 24 ADX columns for 1h timeframe'

\echo ''
\echo '=========================================='
\echo 'ADX columns creation completed!'
\echo '=========================================='
\echo 'Total: 72 columns created (24 per timeframe × 3 timeframes)'
\echo 'You can now run adx_loader.py without column creation overhead'
\echo '=========================================='
