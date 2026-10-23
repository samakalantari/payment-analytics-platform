
CREATE TABLE IF NOT EXISTS mart.daily_sales
(
    transaction_date Date,
    successful_count UInt64,
    successful_amount UInt64,
    refreshed_at DateTime('UTC')
)
ENGINE = MergeTree()
ORDER BY transaction_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_daily_sales
REFRESH EVERY 10 MINUTE
TO mart.daily_sales
AS
SELECT
    toDate(created_at) AS transaction_date,
    count() AS successful_count,
    sum(amount) AS successful_amount,
    now('UTC') AS refreshed_at
FROM mart.fact_transactions
WHERE status = 'SUCCESS'
GROUP BY transaction_date;
