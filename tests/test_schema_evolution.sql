
\set ON_ERROR_STOP on

-- Step 1: Schema change
ALTER TABLE core.transactions
ADD COLUMN IF NOT EXISTS installments SMALLINT;

-- Step 2: Choose an unused test ID.
SELECT COALESCE(MAX(transaction_id), 0) + 1000 AS test_id
FROM core.transactions
\gset

\echo SCHEMA_EVOLUTION_TEST_ID = :test_id

-- Step 3: Insert a transaction with the new column.
INSERT INTO core.transactions (
    transaction_id,
    merchant_id,
    channel,
    card_bin,
    amount,
    fee_amount,
    status,
    installments,
    created_at,
    updated_at
)
SELECT
    :test_id,
    merchant_id,
    'IPG',
    '603799',
    1200000,
    12000,
    'PENDING',
    4,
    NOW(),
    NOW()
FROM core.merchants
ORDER BY merchant_id
LIMIT 1;

-- Step 4: Update transaction status and installments.
UPDATE core.transactions
SET
    status = 'SUCCESS',
    installments = 6,
    updated_at = NOW()
WHERE transaction_id = :test_id;

-- Step 5: Confirm final source state.
SELECT
    transaction_id,
    status,
    installments
FROM core.transactions
WHERE transaction_id = :test_id;
