
\set ON_ERROR_STOP on

-- Use a test ID above existing transaction IDs.
-- Run with the change simulator stopped.
SELECT COALESCE(MAX(transaction_id), 0) + 10000 AS test_id
FROM core.transactions
\gset

\echo RECOVERY_TEST_ID = :test_id

-- Insert while consumer is stopped.
INSERT INTO core.transactions (
    transaction_id,
    merchant_id,
    channel,
    card_bin,
    amount,
    fee_amount,
    status,
    created_at,
    updated_at
)
SELECT
    :test_id,
    merchant_id,
    'IPG',
    '603799',
    250000,
    2500,
    'PENDING',
    NOW(),
    NOW()
FROM core.merchants
ORDER BY merchant_id
LIMIT 1;

-- Update while consumer is stopped.
UPDATE core.transactions
SET status = 'SUCCESS',
    updated_at = NOW()
WHERE transaction_id = :test_id;

SELECT transaction_id, status, amount
FROM core.transactions
WHERE transaction_id = :test_id;
