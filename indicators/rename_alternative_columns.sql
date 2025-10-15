-- SQL Script to rename Alternative.me Fear & Greed columns
-- Adds '_alternative' suffix to distinguish from CoinMarketCap columns
-- ============================================================================

BEGIN;

-- Step 1: Rename columns in indicators_bybit_futures_1m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1m
  RENAME COLUMN fear_and_greed_index TO fear_and_greed_index_alternative;

ALTER TABLE indicators_bybit_futures_1m
  RENAME COLUMN fear_and_greed_classification TO fear_and_greed_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_1m.fear_and_greed_index_alternative IS
  'Fear and Greed Index from Alternative.me (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_1m.fear_and_greed_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 2: Rename columns in indicators_bybit_futures_15m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_15m
  RENAME COLUMN fear_and_greed_index TO fear_and_greed_index_alternative;

ALTER TABLE indicators_bybit_futures_15m
  RENAME COLUMN fear_and_greed_classification TO fear_and_greed_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_15m.fear_and_greed_index_alternative IS
  'Fear and Greed Index from Alternative.me (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_15m.fear_and_greed_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 3: Rename columns in indicators_bybit_futures_1h
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1h
  RENAME COLUMN fear_and_greed_index TO fear_and_greed_index_alternative;

ALTER TABLE indicators_bybit_futures_1h
  RENAME COLUMN fear_and_greed_classification TO fear_and_greed_classification_alternative;

COMMENT ON COLUMN indicators_bybit_futures_1h.fear_and_greed_index_alternative IS
  'Fear and Greed Index from Alternative.me (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_1h.fear_and_greed_classification_alternative IS
  'Fear and Greed classification from Alternative.me (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 4: Update checkpoint names
-- -------------------------------------------------------
UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_alternative_1m'
WHERE checkpoint_name = 'fear_and_greed_1m';

UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_alternative_15m'
WHERE checkpoint_name = 'fear_and_greed_15m';

UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_alternative_1h'
WHERE checkpoint_name = 'fear_and_greed_1h';


-- Commit transaction
COMMIT;

-- Verification queries
-- ============================================================================
-- Run these queries after script execution to verify changes:

-- Check column names in 1m table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'indicators_bybit_futures_1m'
  AND column_name LIKE '%fear_and_greed%'
ORDER BY column_name;

-- Check column names in 15m table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'indicators_bybit_futures_15m'
  AND column_name LIKE '%fear_and_greed%'
ORDER BY column_name;

-- Check column names in 1h table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'indicators_bybit_futures_1h'
  AND column_name LIKE '%fear_and_greed%'
ORDER BY column_name;

-- Check checkpoint names
SELECT checkpoint_name, last_processed_date
FROM checkpoint_fear_and_greed
WHERE checkpoint_name LIKE '%fear_and_greed%'
ORDER BY checkpoint_name;

-- Check data integrity (sample from 1m)
SELECT
    timestamp,
    fear_and_greed_index_alternative,
    fear_and_greed_classification_alternative,
    fear_and_greed_index_coinmarketcap,
    fear_and_greed_index_coinmarketcap_classification
FROM indicators_bybit_futures_1m
WHERE symbol = 'BTCUSDT'
  AND fear_and_greed_index_alternative IS NOT NULL
ORDER BY timestamp DESC
LIMIT 5;
