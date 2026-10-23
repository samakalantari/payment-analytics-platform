
CREATE DATABASE IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.transactions_versions
(
    transaction_id UInt64,
    merchant_id UInt64,
    channel LowCardinality(String),
    card_bin String,
    amount UInt64,
    fee_amount UInt64,
    status LowCardinality(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    operation LowCardinality(String),
    is_deleted UInt8,
    partition_id UInt16,
    kafka_offset UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (transaction_id, partition_id, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS staging.mv_transactions_versions
TO staging.transactions_versions
AS
SELECT
    JSONExtractUInt(record_json, 'transaction_id')
        AS transaction_id,
    JSONExtractUInt(record_json, 'merchant_id')
        AS merchant_id,
    JSONExtractString(record_json, 'channel')
        AS channel,
    JSONExtractString(record_json, 'card_bin')
        AS card_bin,
    JSONExtractUInt(record_json, 'amount')
        AS amount,
    JSONExtractUInt(record_json, 'fee_amount')
        AS fee_amount,
    JSONExtractString(record_json, 'status')
        AS status,
    parseDateTime64BestEffortOrNull(
        JSONExtractString(record_json, 'created_at'), 6, 'UTC'
    ) AS created_at,
    parseDateTime64BestEffortOrNull(
        JSONExtractString(record_json, 'updated_at'), 6, 'UTC'
    ) AS updated_at,
    operation,
    toUInt8(operation = 'd') AS is_deleted,
    partition_id,
    kafka_offset
FROM
(
    SELECT
        operation,
        partition_id,
        kafka_offset,
        if(
            operation = 'd',
            JSONExtractRaw(event_json, 'before'),
            JSONExtractRaw(event_json, 'after')
        ) AS record_json
    FROM raw.cdc_events
    WHERE topic = 'zibal.core.transactions'
      AND operation IN ('r', 'c', 'u', 'd')
);
