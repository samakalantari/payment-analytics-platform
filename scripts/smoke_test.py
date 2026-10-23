#!/usr/bin/env python3
"""Bounded readiness checks for the local payment analytics stack."""
import json
import os
import sys
import time
import urllib.request

import clickhouse_connect
import psycopg
from confluent_kafka.admin import AdminClient


def wait_until(description, check, deadline):
    while time.monotonic() < deadline:
        try:
            if check():
                print(f"PASS: {description}", flush=True)
                return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"{description} did not become ready before timeout")


def main():
    timeout = int(os.getenv("SMOKE_TIMEOUT_SECONDS", "1800"))
    deadline = time.monotonic() + timeout
    try:
        with psycopg.connect(
            host="postgres-source",
            port=5432,
            dbname=os.getenv("POSTGRES_DB", "payment_db"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.environ["POSTGRES_PASSWORD"],
        ) as pg:
            merchant_count = pg.execute("SELECT count(*) FROM core.merchants").fetchone()[0]
            transaction_count = pg.execute("SELECT count(*) FROM core.transactions").fetchone()[0]
        if merchant_count < 2_000 or transaction_count < 5_000_000:
            raise RuntimeError("PostgreSQL seed counts are below the expected minimum")
        print("PASS: PostgreSQL schema and seed counts", flush=True)

        admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
        topics = admin.list_topics(timeout=15).topics
        expected_topics = {"zibal.core.merchants", "zibal.core.transactions"}
        if not expected_topics.issubset(topics):
            raise RuntimeError("Expected Debezium Kafka topics are missing")
        print("PASS: Kafka broker and CDC topics", flush=True)

        connect_url = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
        connector_url = f"{connect_url}/connectors/zibal-postgres-cdc/status"
        def connector_running():
            with urllib.request.urlopen(connector_url, timeout=10) as response:
                status = json.loads(response.read())
            return (
                status.get("connector", {}).get("state") == "RUNNING"
                and bool(status.get("tasks"))
                and all(task.get("state") == "RUNNING" for task in status["tasks"])
            )

        wait_until("Debezium connector and task", connector_running, deadline)

        def metabase_healthy():
            with urllib.request.urlopen(
                os.getenv("METABASE_URL", "http://metabase:3000") + "/api/health", timeout=10
            ) as response:
                return response.status == 200

        wait_until("Metabase HTTP health", metabase_healthy, deadline)

        ch = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="zibal",
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
        try:
            ch.query("SELECT 1")
            print("PASS: ClickHouse is healthy", flush=True)
            target_raw_events = transaction_count + merchant_count
            while time.monotonic() < deadline:
                raw_events = ch.query("""
                    SELECT coalesce(sum(rows), 0)
                    FROM system.parts
                    WHERE database = 'raw' AND table = 'cdc_events' AND active
                """).result_rows[0][0]
                if int(raw_events) < target_raw_events:
                    time.sleep(20)
                    continue

                # The raw event count is cheap metadata. Once the initial
                # snapshot volume arrived, perform exact whole-model counts
                # once rather than repeatedly scanning millions of rows.
                current_transactions = int(ch.query(
                    "SELECT count() FROM staging.transactions_current"
                ).result_rows[0][0])
                current_merchants = ch.query("""
                    SELECT count(), uniqExact(merchant_id)
                    FROM staging.merchants_scd2
                    WHERE is_current = 1
                """).result_rows[0]
                fact_count = int(ch.query(
                    "SELECT count() FROM mart.fact_transactions"
                ).result_rows[0][0])
                if not (
                    current_transactions == transaction_count
                    and int(current_merchants[0]) == merchant_count
                    and int(current_merchants[1]) == merchant_count
                    and fact_count == current_transactions
                ):
                    raise RuntimeError("ClickHouse staging, SCD2 or fact counts do not reconcile")
                print("PASS: ClickHouse snapshot, staging, SCD2 and fact counts", flush=True)

                while time.monotonic() < deadline:
                    mart_status = {
                        table: ch.query(
                            f"SELECT count(), max(refreshed_at) >= now('UTC') - INTERVAL 15 MINUTE "
                            f"FROM mart.{table}"
                        ).result_rows[0]
                        for table in (
                            "daily_sales",
                            "daily_channel_success",
                            "merchant_monthly_sales",
                            "fees_by_mcc_province_daily",
                            "monthly_risk_volume",
                        )
                    }
                    if all(
                        int(values[0]) > 0 and bool(values[1])
                        for values in mart_status.values()
                    ):
                        print("PASS: all five reporting marts refreshed with rows", flush=True)
                        return 0
                    time.sleep(20)
                raise RuntimeError("Reporting marts did not complete a recent refresh before timeout")
        finally:
            ch.close()

        raise RuntimeError("ClickHouse snapshot/models/marts did not reach expected state before timeout")
    except Exception as exc:
        print(f"FAIL: smoke check stopped ({type(exc).__name__}: {exc})", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
