
\set ON_ERROR_STOP on

-- Allocate an ID above the current maximum.
SELECT COALESCE(MAX(transaction_id), 0) + 1 AS test_id
FROM core.transactions
\gset

\echo CDC_TEST_TRANSACTION_ID = :test_id

-- 1. INSERT
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
VALUES (
    :test_id,
    1,
    'IPG',
    '603799',
    500000,
    5000,
    'PENDING',
    NOW(),
    NOW()
);

-- 2. UPDATE: PENDING -> SUCCESS
UPDATE core.transactions
SET status = 'SUCCESS',
    updated_at = NOW()
WHERE transaction_id = :test_id;

-- 3. UPDATE: SUCCESS -> REVERSED
UPDATE core.transactions
SET status = 'REVERSED',
    updated_at = NOW()
WHERE transaction_id = :test_id;

-- 4. HARD DELETE
DELETE FROM core.transactions
WHERE transaction_id = :test_id;
 