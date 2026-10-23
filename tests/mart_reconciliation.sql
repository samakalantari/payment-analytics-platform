-- Reconcile successful transaction counts across reporting marts.
-- throwIf ensures mismatches become SQL errors and a nonzero runner exit.
WITH metrics AS
(
    SELECT
        'fact' AS model,
        countIf(status = 'SUCCESS') AS successful_count
    FROM mart.fact_transactions

    UNION ALL

    SELECT
        'daily_sales',
        ifNull(sum(successful_count), 0)
    FROM mart.daily_sales

    UNION ALL

    SELECT
        'daily_channel_success',
        ifNull(sum(successful_transactions), 0)
    FROM mart.daily_channel_success

    UNION ALL

    SELECT
        'merchant_monthly_sales',
        ifNull(sum(successful_transactions), 0)
    FROM mart.merchant_monthly_sales

    UNION ALL

    SELECT
        'fees_by_mcc_province_daily',
        ifNull(sum(successful_transactions), 0)
    FROM mart.fees_by_mcc_province_daily

    UNION ALL

    SELECT
        'monthly_risk_volume',
        ifNull(sum(successful_transactions), 0)
    FROM mart.monthly_risk_volume
)
SELECT throwIf(
    countIf(successful_count !=
        (SELECT
            successful_count
        FROM metrics
        WHERE model = 'fact')) > 0,
    'FAIL: reporting mart successful transaction count mismatch'
)
FROM metrics;

SELECT 'MART RECONCILIATION PASSED' AS result;
