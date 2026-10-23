
INSERT INTO staging.merchants_versions
(
    merchant_id,
    name,
    mcc,
    province,
    risk_level,
    effective_at,
    operation,
    is_deleted,
    partition_id,
    kafka_offset
)
SELECT
    JSONExtractUInt(record_json, 'merchant_id'),
    JSONExtractString(record_json, 'name'),
    JSONExtractString(record_json, 'mcc'),
    JSONExtractString(record_json, 'province'),
    JSONExtractString(record_json, 'risk_level'),

    if(
        operation = 'r',
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC'),
        fromUnixTimestamp64Milli(
            toInt64(JSONExtractUInt(event_json, 'source', 'ts_ms')),
            'UTC'
        )
    ),

    operation,
    toUInt8(operation = 'd'),
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
    FROM raw.cdc_events FINAL
    WHERE topic = 'zibal.core.merchants'
      AND operation IN ('r', 'c', 'u', 'd')
) AS events
LEFT ANTI JOIN staging.merchants_versions AS existing
    USING (partition_id, kafka_offset);
