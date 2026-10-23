
-- T5: Automated Data Quality Tests
-- Any violation raises an error and fails the test run.

-- Test 1: Transaction IDs must be unique.
SELECT throwIf(
    count() != uniqExact(transaction_id),
    'FAIL: duplicate transaction IDs in fact'
)
FROM mart.fact_transactions;

-- Test 2: Valid transaction statuses.
SELECT throwIf(
    countIf(status NOT IN (
        'PENDING', 'SUCCESS', 'FAILED', 'REVERSED'
    )) > 0,
    'FAIL: invalid transaction status'
)
FROM mart.fact_transactions;

-- Test 3: Valid transaction channels.
SELECT throwIf(
    countIf(channel NOT IN ('POS', 'IPG', 'QR')) > 0,
    'FAIL: invalid channel'
)
FROM mart.fact_transactions;

-- Test 4: No missing merchant information.
SELECT throwIf(
    countIf(
        merchant_id = 0
        OR risk_level = ''
        OR province = ''
    ) > 0,
    'FAIL: missing merchant attributes'
)
FROM mart.fact_transactions;

-- Test 5: No negative amounts.
-- amount and fee_amount are UInt64 in our model,
-- so negative values cannot be represented here.
-- This checks for unexpected zero amounts instead.
SELECT throwIf(
    countIf(amount = 0) > 0,
    'FAIL: zero transaction amount'
)
FROM mart.fact_transactions;

-- Test 6: All fact rows belong to a valid SCD2 interval.
SELECT throwIf(
    countIf(
        created_at < merchant_valid_from
        OR created_at >= merchant_valid_to
    ) > 0,
    'FAIL: invalid temporal merchant assignment'
)
FROM mart.fact_transactions;

-- Test 7: One current version per merchant.
SELECT throwIf(
    count() > 0,
    'FAIL: multiple current versions for a merchant'
)
FROM
(
    SELECT merchant_id
    FROM staging.merchants_scd2
    WHERE is_current = 1
    GROUP BY merchant_id
    HAVING count() > 1
);

-- Test 8: PII must not be exposed in mart tables/views.
SELECT throwIf(
    count() > 0,
    'FAIL: sensitive columns exposed in mart'
)
FROM system.columns
WHERE database = 'mart'
  AND (
      positionCaseInsensitive(name, 'national_id') > 0
      OR positionCaseInsensitive(name, 'mobile') > 0
      OR positionCaseInsensitive(name, 'card_bin') > 0
  );

-- Test 9: Current transactions must resolve to a non-deleted merchant history.
SELECT throwIf(
    count() > 0,
    'FAIL: transaction references missing merchant history'
)
FROM staging.transactions_current AS t
WHERE t.merchant_id NOT IN
(
    SELECT merchant_id
    FROM staging.merchants_versions FINAL
    WHERE is_deleted = 0
);

SELECT 'DATA QUALITY CHECKS PASSED' AS result;
