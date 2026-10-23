CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cdc_events
(
    topic LowCardinality(String),
    partition_id UInt16,
    kafka_offset UInt64,

    operation LowCardinality(String),
    entity_id UInt64,
    source_lsn UInt64,

    event_json String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (topic, partition_id, kafka_offset);