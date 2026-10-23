
CREATE TABLE IF NOT EXISTS staging.merchants_versions
(
    merchant_id UInt64,
    name String,
    mcc String,
    province String,
    risk_level String,

    -- Event time based on the source database.
    effective_at DateTime64(3, 'UTC'),

    operation LowCardinality(String),
    is_deleted UInt8,

    partition_id UInt16,
    kafka_offset UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (merchant_id, partition_id, kafka_offset);


CREATE MATERIALIZED VIEW IF NOT EXISTS staging.mv_merchants_versions
TO staging.merchants_versions
AS
SELECT
    JSONExtractUInt(record_json, 'merchant_id') AS merchant_id,
    JSONExtractString(record_json, 'name') AS name,
    JSONExtractString(record_json, 'mcc') AS mcc,
    JSONExtractString(record_json, 'province') AS province,
    JSONExtractString(record_json, 'risk_level') AS risk_level,

    if(
        operation = 'r',
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC'),
        fromUnixTimestamp64Milli(
            toInt64(JSONExtractUInt(event_json, 'source', 'ts_ms')),
            'UTC'
        )
    ) AS effective_at,

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
        event_json,
        if(
            operation = 'd',
            JSONExtractRaw(event_json, 'before'),
            JSONExtractRaw(event_json, 'after')
        ) AS record_json
    FROM raw.cdc_events
    WHERE topic = 'zibal.core.merchants'
      AND operation IN ('r', 'c', 'u', 'd')
);
