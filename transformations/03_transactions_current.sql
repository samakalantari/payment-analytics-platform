
CREATE VIEW IF NOT EXISTS staging.transactions_current AS
SELECT
    transaction_id,
    argMax(merchant_id, kafka_offset) AS merchant_id,
    argMax(channel, kafka_offset) AS channel,
    argMax(amount, kafka_offset) AS amount,
    argMax(fee_amount, kafka_offset) AS fee_amount,
    argMax(status, kafka_offset) AS status,
    argMax(created_at, kafka_offset) AS created_at,
    argMax(updated_at, kafka_offset) AS updated_at
FROM staging.transactions_versions
GROUP BY transaction_id
HAVING argMax(is_deleted, kafka_offset) = 0;
