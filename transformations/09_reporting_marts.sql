
-- =====================================================
-- Zibal Payment Analytics - Reporting Marts
-- Source: mart.fact_transactions
-- Refresh strategy: full refresh every 10 minutes
-- =====================================================

-- -----------------------------------------------------
-- Card 02: Daily Success Rate by Channel
-- Grain: transaction_date + channel
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.daily_channel_success
(
    transaction_date Date,
    channel LowCardinality(String),
    total_transactions UInt64,
    successful_transactions UInt64,
    refreshed_at DateTime('UTC')
)
ENGINE = MergeTree()
ORDER BY (transaction_date, channel);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_daily_channel_success
REFRESH EVERY 10 MINUTE
TO mart.daily_channel_success
AS
SELECT
    toDate(created_at) AS transaction_date,
    channel,
    count() AS total_transactions,
    countIf(status = 'SUCCESS') AS successful_transactions,
    now('UTC') AS refreshed_at
FROM mart.fact_transactions
GROUP BY transaction_date, channel;


-- -----------------------------------------------------
-- Card 03: Top Merchants by Monthly Sales
-- Grain: transaction_month + merchant_id
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.merchant_monthly_sales
(
    transaction_month Date,
    merchant_id UInt64,
    merchant_name String,
    successful_transactions UInt64,
    total_amount UInt64,
    refreshed_at DateTime('UTC')
)
ENGINE = MergeTree()
ORDER BY (transaction_month, merchant_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_merchant_monthly_sales
REFRESH EVERY 10 MINUTE
TO mart.merchant_monthly_sales
AS
SELECT
    toStartOfMonth(toDate(created_at)) AS transaction_month,
    merchant_id,
    argMax(merchant_name, created_at) AS merchant_name,
    count() AS successful_transactions,
    sum(amount) AS total_amount,
    now('UTC') AS refreshed_at
FROM mart.fact_transactions
WHERE status = 'SUCCESS'
GROUP BY transaction_month, merchant_id;


-- -----------------------------------------------------
-- Card 04: Fees by MCC and Historical Province
-- Grain: transaction_date + mcc + province
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.fees_by_mcc_province_daily
(
    transaction_date Date,
    mcc LowCardinality(String),
    province LowCardinality(String),
    successful_transactions UInt64,
    total_fee_amount UInt64,
    refreshed_at DateTime('UTC')
)
ENGINE = MergeTree()
ORDER BY (transaction_date, mcc, province);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_fees_by_mcc_province_daily
REFRESH EVERY 10 MINUTE
TO mart.fees_by_mcc_province_daily
AS
SELECT
    toDate(created_at) AS transaction_date,
    mcc,
    province,
    count() AS successful_transactions,
    sum(fee_amount) AS total_fee_amount,
    now('UTC') AS refreshed_at
FROM mart.fact_transactions
WHERE status = 'SUCCESS'
GROUP BY transaction_date, mcc, province;


-- -----------------------------------------------------
-- Card 05: Monthly Volume by Historical Risk Level
-- Grain: transaction_month + risk_level
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.monthly_risk_volume
(
    transaction_month Date,
    risk_level LowCardinality(String),
    successful_transactions UInt64,
    total_amount UInt64,
    refreshed_at DateTime('UTC')
)
ENGINE = MergeTree()
ORDER BY (transaction_month, risk_level);

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_monthly_risk_volume
REFRESH EVERY 10 MINUTE
TO mart.monthly_risk_volume
AS
SELECT
    toStartOfMonth(toDate(created_at)) AS transaction_month,
    risk_level,
    count() AS successful_transactions,
    sum(amount) AS total_amount,
    now('UTC') AS refreshed_at
FROM mart.fact_transactions
WHERE status = 'SUCCESS'
GROUP BY transaction_month, risk_level;
