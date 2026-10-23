#!/usr/bin/env python3
"""Run routine ClickHouse SQL checks and PostgreSQL/ClickHouse aggregates."""
import os
import sys
from pathlib import Path

import clickhouse_connect
import psycopg
from dotenv import load_dotenv

load_dotenv()

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
TEST_FILES = (
    "tests/data_quality.sql",
    "tests/mart_reconciliation.sql",
)
RECONCILIATION_SQL = {
    "postgres_totals": "tests/reconciliation/postgres_totals.sql",
    "postgres_daily": "tests/reconciliation/postgres_daily.sql",
    "clickhouse_totals": "tests/reconciliation/clickhouse_totals.sql",
    "clickhouse_daily": "tests/reconciliation/clickhouse_daily.sql",
}


def env(name, default):
    return os.getenv(name, default)


def connect_postgres():
    return psycopg.connect(
        host=env("POSTGRES_HOST", "127.0.0.1"),
        port=int(env("POSTGRES_PORT", "5433")),
        dbname=env("POSTGRES_DB", "payment_db"),
        user=env("POSTGRES_USER", "admin"),
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=False,
    )


def connect_clickhouse():
    return clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(env("CLICKHOUSE_PORT", "8123")),
        username=env("CLICKHOUSE_USER", "zibal"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def load_sql(path):
    return (REPOSITORY_ROOT / path).read_text(encoding="utf-8")


def sql_statements(path):
    # Test SQL has no semicolons inside literals or comments. Send each
    # statement separately so an exception reliably aborts the runner.
    return [part.strip() for part in load_sql(path).split(";") if part.strip()]


def run_clickhouse_tests(client):
    for path in TEST_FILES:
        for statement in sql_statements(path):
            client.query(statement)
        print(f"PASS: {path}")


def pg_metrics(conn):
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    conn.execute("SET LOCAL TIME ZONE 'UTC'")
    totals = conn.execute(load_sql(RECONCILIATION_SQL["postgres_totals"])).fetchone()
    days = conn.execute(load_sql(RECONCILIATION_SQL["postgres_daily"])).fetchall()
    if totals is None or len(totals) != 3 or any(len(row) != 4 for row in days):
        raise ValueError("PostgreSQL reconciliation query returned an unexpected schema")
    return tuple(map(int, totals)), [tuple((row[0], *map(int, row[1:]))) for row in days]


def ch_metrics(client):
    totals = client.query(load_sql(RECONCILIATION_SQL["clickhouse_totals"])).result_rows[0]
    days = client.query(load_sql(RECONCILIATION_SQL["clickhouse_daily"])).result_rows
    if len(totals) != 3 or any(len(row) != 4 for row in days):
        raise ValueError("ClickHouse reconciliation query returned an unexpected schema")
    return tuple(map(int, (totals[0], totals[1], totals[2] or 0))), [
        (row[0], int(row[1]), int(row[2]), int(row[3] or 0)) for row in days
    ]


def run():
    if env("RECONCILE_QUIESCENT", "0").lower() not in ("1", "true", "yes"):
        print("FAIL: pause PostgreSQL source writers and set RECONCILE_QUIESCENT=1")
        return 1

    pg = ch = None
    try:
        pg = connect_postgres()
        ch = connect_clickhouse()
        source_totals, source_days = pg_metrics(pg)

        # A flag only declares the writer state. Success additionally requires
        # matching independent totals and every per-day aggregate.
        sink_totals, sink_days = ch_metrics(ch)
        failures = []
        if sink_totals != source_totals:
            failures.append("source/sink transaction totals differ")
        if sink_days != source_days:
            failures.append("source/sink daily totals differ")
        if failures:
            for failure in failures:
                print(f"FAIL: {failure}")
            return 1

        print(
            "PASS: PostgreSQL/ClickHouse totals match "
            f"(transactions={source_totals[0]}, successful={source_totals[1]}, "
            f"successful_amount={source_totals[2]})"
        )
        run_clickhouse_tests(ch)
        return 0
    except Exception as exc:
        # Do not print SQL or row data; only expose a safe exception category.
        print(f"FAIL: test run could not complete ({type(exc).__name__})")
        return 1
    finally:
        if pg is not None:
            pg.rollback()
            pg.close()
        if ch is not None:
            ch.close()


if __name__ == "__main__":
    sys.exit(run())
