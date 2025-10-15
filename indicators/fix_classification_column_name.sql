-- SQL Script to fix classification column name for consistency
-- Renames: fear_and_greed_classification_alternative
--       → fear_and_greed_index_classification_alternative
-- This makes Alternative.me columns consistent with CoinMarketCap naming
-- ============================================================================

BEGIN;

-- Step 1: Rename classification column in indicators_bybit_futures_1m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1m
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_index_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_1m.fear_and_greed_index_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 2: Rename classification column in indicators_bybit_futures_15m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_15m
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_index_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_15m.fear_and_greed_index_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 3: Rename classification column in indicators_bybit_futures_1h
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1h
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_index_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_1h.fear_and_greed_index_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Commit transaction
COMMIT;

-- Verification queries
-- ============================================================================

-- Check column names in all tables
SELECT
    table_name,
    column_name
FROM information_schema.columns
WHERE table_name LIKE 'indicators_bybit_futures_%'
  AND column_name LIKE '%fear_and_greed%'
ORDER BY table_name, column_name;

-- Expected result:
-- All tables should have:
-- - fear_and_greed_index_alternative
-- - fear_and_greed_index_classification_alternative  (NEW NAME)
-- - fear_and_greed_index_coinmarketcap
-- - fear_and_greed_index_coinmarketcap_classification

-- Check data integrity (sample from 1m)
SELECT
    timestamp,
    fear_and_greed_index_alternative,
    fear_and_greed_index_classification_alternative,
    fear_and_greed_index_coinmarketcap,
    fear_and_greed_index_coinmarketcap_classification
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND fear_and_greed_index_alternative IS NOT NULL
ORDER BY timestamp DESC
LIMIT 5;
