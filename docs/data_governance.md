# Data Governance, PII and Ownership

This document describes the current repository implementation. Suggested role
ownership below is a practical model, not a record of formally assigned people
or teams.

## Sensitive data flow

| Field | PostgreSQL source | Debezium/Kafka and `raw.cdc_events` | Staging | Fact and reporting marts |
|---|---|---|---|---|
| `owner_national_id` | Stored in `core.merchants` | Included in Debezium row JSON, Kafka records and raw `event_json` | Not selected into `staging.merchants_versions` | Not selected into `mart.fact_transactions` or the five reporting marts |
| `owner_mobile` | Stored in `core.merchants` | Included in Debezium row JSON, Kafka records and raw `event_json` | Not selected into `staging.merchants_versions` | Not selected into Fact or reporting marts |
| `card_bin` | Stored in `core.transactions` | Included in Debezium row JSON, Kafka records and raw `event_json` | Retained in `staging.transactions_versions`; omitted from `staging.transactions_current` | Omitted from Fact and reporting marts |

Debezium captures all columns from the included source tables. The consumer
stores the complete event value as the `String` column `raw.cdc_events.event_json`;
it does not mask, hash or redact these fields. Kafka topics therefore also
contain the source row values. The current ClickHouse transformations
selectively exclude the merchant owner fields and remove `card_bin` from the
current-state view. The Fact and five reporting marts contain no named PII
columns. The data-quality SQL checks mart column names for terms such as
`national_id`, `mobile` and `card_bin`; it does not scan JSON values. Thus the
check confirms schema exposure by column name, not absence of sensitive values
from arbitrary serialized payloads. No masking or hashing is implemented.

The source and CDC layers remain sensitive stores. Access to PostgreSQL,
Kafka topics, ClickHouse `raw` and `staging`, backups and diagnostic tooling
should be limited to operational roles that need them. Proportionate follow-up
controls include access review, secret-managed credentials, encrypted transport
and storage where supported by the deployment, and a retention/deletion policy
for raw events. These are recommendations; this repository does not configure
field-level redaction, Kafka ACLs, encryption-at-rest policy, or raw-event TTL.

## Proposed data ownership

| Responsibility | Proposed accountable role | Current implementation evidence |
|---|---|---|
| PostgreSQL source schema and source data | Payment application/data owner | `core.merchants` and `core.transactions` are initialized in PostgreSQL |
| CDC capture and transport | Data platform/streaming operator | Debezium connector, Kafka and the Python CDC consumer |
| Raw, staging and analytical models | Analytics/data engineering | ClickHouse transformations and `mart.fact_transactions` |
| Business metric meaning | Business owner for payments/finance, with analytics engineering implementation | Metric SQL is defined in reporting transformations; no named business approver is recorded |
| Data quality and cross-system reconciliation | Data engineering/platform operations, with source and metric owners resolving findings | `tests/data_quality.sql`, `tests/mart_reconciliation.sql` and `scripts/reconcile.py` |

These roles are recommendations only. No individuals or formal organizational
assignments are specified in the project.

## Retention, delivery and freshness

- Kafka broker log retention is configured to 168 hours (`KAFKA_LOG_RETENTION_HOURS`).
  This is a broker setting; the repository does not specify a separate retention
  override for the CDC topics. Kafka Connect internal topics use broker defaults.
- PostgreSQL logical replication uses the Debezium slot `zibal_cdc_slot`.
  Compose does not configure `max_slot_wal_keep_size`; if the connector stops
  consuming for an extended period, retained WAL can grow and consume disk.
- PostgreSQL, ClickHouse raw/staging/mart tables and Metabase use named persistent
  volumes. The SQL definitions shown here contain no row-level TTL or scheduled
  deletion policy. Volume persistence is not a retention schedule.
- The consumer disables automatic commits and commits Kafka offsets only after
  ClickHouse accepts a batch. A crash between insert and commit can replay the
  batch, so delivery is at least once. Raw and version tables use
  `ReplacingMergeTree` keys containing topic/partition/offset (or partition/offset)
  to collapse duplicate versions during merges/FINAL reads; this is not an
  exactly-once transport guarantee.
- Materialized views ingest newly inserted raw events into staging. The bootstrap
  process backfills previously stored raw events once and uses offset anti-joins
  to make a retry safe. Current transaction state is derived from staged
  versions.
- The five reporting marts are refreshable materialized views configured for
  `REFRESH EVERY 10 MINUTE`. They perform periodic full-query refreshes; they are
  not incrementally maintained aggregates. A dashboard can therefore lag CDC
  ingestion by up to a refresh interval plus processing/refresh time. Full
  refresh work grows with the source Fact data and consumes ClickHouse resources.
- No daily scheduler for quality or reconciliation checks is configured. The
  reconciliation command is one-shot and may be invoked by an external scheduler.
- This is a single-host Docker Compose demo. Kafka/Connect run on the internal
  Compose network without configured Kafka ACLs or TLS; this is not a production
  high-availability or production security configuration.

## Quality and reconciliation operation

Routine ClickHouse checks and the cross-database comparison are run with:

```sh
RECONCILE_QUIESCENT=1 python scripts/reconcile.py
```

This command runs `tests/data_quality.sql` and `tests/mart_reconciliation.sql`
after comparing PostgreSQL and ClickHouse transaction totals and UTC daily
totals. SQL failures or aggregate differences return a nonzero exit code. The
environment flag is an operator assertion that source writers are paused; it
does not detect or enforce quiescence. Pause the change simulator and any other
PostgreSQL writers before using it. The PostgreSQL side is read-only and uses a
repeatable-read transaction; ClickHouse is queried separately. If writers are
active, the two reads can represent different CDC positions and a mismatch is
not necessarily a data defect.

`docker compose run --rm smoke-test` is a bounded startup/readiness check. The
SQL scripts `tests/test_cdc.sql`, `tests/test_schema_evolution.sql`, and
`tests/test_recovery.sql` mutate source state or test recovery/schema behavior;
they are integration tests and are not run by routine reconciliation. See
[data dictionary and modeling rules](data_dictionary.md) for grains, metric
definitions and temporal attribution.

The checked-in CDC and schema-evolution SQL scripts issue source-side changes;
they do not assert the resulting ClickHouse row state. The recovery SQL expects
the consumer to be stopped while its test transaction is inserted and updated,
but does not itself stop/restart the consumer or remove the test row. Treat
these scripts as manual mutation steps, not as fully automated end-to-end
assertions.

The SQL files use `psql` metacommands and are intended to be invoked separately
from the repository root against a controlled local database. Load the local
`.env` values in the shell without printing them:

```sh
set -a
. ./.env
set +a
export PGPASSWORD="$POSTGRES_PASSWORD"
psql -h 127.0.0.1 -p 5433 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 -f tests/test_cdc.sql
```

Replace the final file path with `tests/test_schema_evolution.sql` or
`tests/test_recovery.sql` to run those scripts individually. Schema evolution
adds the `installments` column and leaves it in place. Recovery expects the
simulator and consumer to be stopped and leaves its inserted row in the source.
Do not run these mutation scripts as routine checks or against data you need to
preserve.

## Known limitations

- Initial Debezium snapshot records for merchants are assigned the sentinel
  `1970-01-01 00:00:00 UTC`. The source does not provide prior risk/province
  history, so values before the first observed CDC change are an initial-state
  assumption, not verified historical truth.
- Merchant SCD2 `effective_at` uses Debezium `source.ts_ms`, which has millisecond
  precision, while Kafka offset is used to order versions. Closely spaced events
  can share the same effective timestamp; late/reordered source events and
  corrections to prior history are not separately reconciled by this model.
- The initial snapshot cannot establish how merchant risk or province changed
  before capture began. Only observed changes after the snapshot can be described
  as history, subject to the timestamp and ordering behavior above.
- The automated mart PII test is a column-name check. It does not prove that all
  serialized values or future view expressions are free of sensitive content.
- PostgreSQL owner identifiers and mobile numbers are excluded from ClickHouse
  staging and marts, not masked or hashed. This does not satisfy the original
  assignment's explicit mask-or-hash requirement; no such transformation is
  implemented.
