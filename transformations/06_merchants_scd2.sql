
CREATE VIEW IF NOT EXISTS staging.merchants_scd2 AS
WITH
deduplicated AS
(
    SELECT *
    FROM staging.merchants_versions FINAL
),
changes AS
(
    SELECT
        merchant_id,
        name,
        mcc,
        province,
        risk_level,
        effective_at,
        operation,
        is_deleted,
        kafka_offset,

        lagInFrame(province, 1, '') OVER history
            AS previous_province,

        lagInFrame(risk_level, 1, '') OVER history
            AS previous_risk,

        lagInFrame(is_deleted, 1, toUInt8(0))
            OVER history AS previous_deleted,

        row_number() OVER history AS version_number

    FROM deduplicated

    WINDOW history AS
    (
        PARTITION BY merchant_id
        ORDER BY kafka_offset
        ROWS BETWEEN UNBOUNDED PRECEDING
                 AND UNBOUNDED FOLLOWING
    )
),
significant_changes AS
(
    SELECT *
    FROM changes
    WHERE version_number = 1
       OR province != previous_province
       OR risk_level != previous_risk
       OR is_deleted != previous_deleted
),
history AS
(
    SELECT
        merchant_id,
        name,
        mcc,
        province,
        risk_level,
        effective_at AS valid_from,

        leadInFrame(
            effective_at,
            1,
            toDateTime64('2299-12-31 00:00:00', 3, 'UTC')
        ) OVER
        (
            PARTITION BY merchant_id
            ORDER BY kafka_offset
            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND UNBOUNDED FOLLOWING
        ) AS valid_to,

        is_deleted,
        kafka_offset

    FROM significant_changes
)
SELECT
    merchant_id,
    name,
    mcc,
    province,
    risk_level,
    valid_from,
    valid_to,

    toUInt8(
        valid_to = toDateTime64(
            '2299-12-31 00:00:00', 3, 'UTC'
        ) AND is_deleted = 0
    ) AS is_current,

    kafka_offset

FROM history
WHERE is_deleted = 0;
