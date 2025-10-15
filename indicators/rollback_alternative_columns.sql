-- ROLLBACK SQL Script for Alternative.me Fear & Greed columns
-- Reverts changes made by rename_alternative_columns.sql
-- ============================================================================
-- ⚠️ WARNING: Only use this script if you need to rollback the rename operation
-- ============================================================================

BEGIN;

-- Step 1: Rollback columns in indicators_bybit_futures_1m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1m
  RENAME COLUMN fear_and_greed_index_alternative TO fear_and_greed_index;

ALTER TABLE indicators_bybit_futures_1m
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_classification;

COMMENT ON COLUMN indicators_bybit_futures_1m.fear_and_greed_index IS
  'Fear and Greed Index (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_1m.fear_and_greed_classification IS
  'Fear and Greed classification (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 2: Rollback columns in indicators_bybit_futures_15m
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_15m
  RENAME COLUMN fear_and_greed_index_alternative TO fear_and_greed_index;

ALTER TABLE indicators_bybit_futures_15m
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_classification;

COMMENT ON COLUMN indicators_bybit_futures_15m.fear_and_greed_index IS
  'Fear and Greed Index (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_15m.fear_and_greed_classification IS
  'Fear and Greed classification (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 3: Rollback columns in indicators_bybit_futures_1h
-- -------------------------------------------------------
ALTER TABLE indicators_bybit_futures_1h
  RENAME COLUMN fear_and_greed_index_alternative TO fear_and_greed_index;

ALTER TABLE indicators_bybit_futures_1h
  RENAME COLUMN fear_and_greed_classification_alternative TO fear_and_greed_classification;

COMMENT ON COLUMN indicators_bybit_futures_1h.fear_and_greed_index IS
  'Fear and Greed Index (0-100)';

COMMENT ON COLUMN indicators_bybit_futures_1h.fear_and_greed_classification IS
  'Fear and Greed classification (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)';


-- Step 4: Rollback checkpoint names
-- -------------------------------------------------------
UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_1m'
WHERE checkpoint_name = 'fear_and_greed_alternative_1m';

UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_15m'
WHERE checkpoint_name = 'fear_and_greed_alternative_15m';

UPDATE checkpoint_fear_and_greed
SET checkpoint_name = 'fear_and_greed_1h'
WHERE checkpoint_name = 'fear_and_greed_alternative_1h';


-- Commit transaction
COMMIT;

-- Verification queries
-- ============================================================================
-- Run these queries after rollback to verify changes:

SELECT 'Columns reverted successfully' as status;

-- Check column names
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name LIKE 'indicators_bybit_futures%'
  AND column_name LIKE '%fear_and_greed%'
ORDER BY table_name, column_name;

-- Check checkpoint names
SELECT checkpoint_name, last_processed_date
FROM checkpoint_fear_and_greed
WHERE checkpoint_name LIKE '%fear_and_greed%'
ORDER BY checkpoint_name;
