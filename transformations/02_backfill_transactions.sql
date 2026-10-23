
INSERT INTO staging.transactions_versions
SELECT
    JSONExtractUInt(record_json, 'transaction_id'),
    JSONExtractUInt(record_json, 'merchant_id'),
    JSONExtractString(record_json, 'channel'),
    JSONExtractString(record_json, 'card_bin'),
    JSONExtractUInt(record_json, 'amount'),
    JSONExtractUInt(record_json, 'fee_amount'),
    JSONExtractString(record_json, 'status'),
    parseDateTime64BestEffortOrNull(
        JSONExtractString(record_json, 'created_at'), 6, 'UTC'
    ),
    parseDateTime64BestEffortOrNull(
        JSONExtractString(record_json, 'updated_at'), 6, 'UTC'
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
        if(
            operation = 'd',
            JSONExtractRaw(event_json, 'before'),
            JSONExtractRaw(event_json, 'after')
        ) AS record_json
    FROM raw.cdc_events FINAL
    WHERE topic = 'zibal.core.transactions'
      AND operation IN ('r', 'c', 'u', 'd')
) AS events
LEFT ANTI JOIN staging.transactions_versions AS existing
    USING (partition_id, kafka_offset);
