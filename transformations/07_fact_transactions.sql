
CREATE DATABASE IF NOT EXISTS mart;

CREATE VIEW IF NOT EXISTS mart.fact_transactions AS
SELECT
    t.transaction_id,
    t.merchant_id,

    -- Transaction attributes
    t.channel,
    t.amount,
    t.fee_amount,
    t.status,
    t.created_at,
    t.updated_at,

    -- Historical merchant attributes
    m.name AS merchant_name,
    m.mcc,
    m.province,
    m.risk_level,

    -- SCD2 version reference
    m.valid_from AS merchant_valid_from,
    m.valid_to AS merchant_valid_to

FROM
(
    SELECT *
    FROM staging.transactions_current
    WHERE created_at IS NOT NULL
) AS t

ASOF INNER JOIN
(
    SELECT
        merchant_id,
        name,
        mcc,
        province,
        risk_level,
        valid_from,
        valid_to
    FROM staging.merchants_scd2
) AS m

ON t.merchant_id = m.merchant_id
AND t.created_at >= m.valid_from

-- Verify the selected SCD2 version is still valid.
WHERE t.created_at < m.valid_to;
