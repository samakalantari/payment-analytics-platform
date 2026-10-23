"""Create ClickHouse transformations before the consumer reads Kafka."""
from pathlib import Path


TRANSFORMATIONS = Path("/app/transformations")
BOOTSTRAP_NAME = "staging_raw_event_backfill_v1"


def statements(path):
    return [part.strip() for part in path.read_text(encoding="utf-8").split(";") if part.strip()]


def execute_file(client, filename):
    path = TRANSFORMATIONS / filename
    for statement in statements(path):
        client.command(statement)


def bootstrap(client):
    # Stage materialized views exist before this consumer subscribes, so fresh
    # snapshot events are captured directly. Existing raw events are backfilled
    # once; anti-join checks make a retry after interruption safe.
    execute_file(client, "01_transactions_staging.sql")
    execute_file(client, "04_merchants_staging.sql")

    client.command("""
        CREATE TABLE IF NOT EXISTS raw.pipeline_bootstrap
        (
            name LowCardinality(String),
            completed_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(completed_at)
        ORDER BY name
    """)
    completed = client.query(
        "SELECT count() FROM raw.pipeline_bootstrap FINAL WHERE name = {name:String}",
        parameters={"name": BOOTSTRAP_NAME},
    ).result_rows[0][0]
    if not completed:
        execute_file(client, "02_backfill_transactions.sql")
        execute_file(client, "05_backfill_merchants.sql")
        client.insert(
            "raw.pipeline_bootstrap",
            [[BOOTSTRAP_NAME]],
            column_names=["name"],
        )
        print("Completed idempotent ClickHouse staging backfill", flush=True)

    for filename in (
        "03_transactions_current.sql",
        "06_merchants_scd2.sql",
        "07_fact_transactions.sql",
        "08_daily_sales.sql",
        "09_reporting_marts.sql",
    ):
        execute_file(client, filename)
        print(f"Initialized {filename}", flush=True)
