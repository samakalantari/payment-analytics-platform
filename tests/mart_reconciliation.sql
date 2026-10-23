
-- Compare successful transaction counts
-- across all reporting marts.

WITH metrics AS
(
    SELECT
        'fact' AS model,
        countIf(status = 'SUCCESS') AS successful_count
    FROM mart.fact_transactions

    UNION ALL

    SELECT
        'daily_sales',
        sum(successful_count)
    FROM mart.daily_sales

    UNION ALL

    SELECT
        'daily_channel_success',
        sum(successful_transactions)
    FROM mart.daily_channel_success

    UNION ALL

    SELECT
        'merchant_monthly_sales',
        sum(successful_transactions)
    FROM mart.merchant_monthly_sales

    UNION ALL

    SELECT
        'fees_by_mcc_province_daily',
        sum(successful_transactions)
    FROM mart.fees_by_mcc_province_daily

    UNION ALL

    SELECT
        'monthly_risk_volume',
        sum(successful_transactions)
    FROM mart.monthly_risk_volume
)
SELECT
    model,
    successful_count,
    successful_count -
        (SELECT successful_count FROM metrics WHERE model = 'fact')
        AS difference
FROM metrics
ORDER BY model;
