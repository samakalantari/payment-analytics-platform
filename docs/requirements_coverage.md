# Original Assignment Requirements Coverage

This mapping uses `docs/senior-data-engineer-task-Zibal.pdf` as the acceptance
source. Status describes the repository and evidence available at review time;
it does not imply that runtime behavior passed unless explicitly stated.

## T0 — Source environment

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| PostgreSQL `core` schema, 2,000 merchants, at least 5 million transactions in 180 days, approximate status distribution | `docker/postgres/init/01_schema.sql`; `scripts/seed_data.py` (counts, 180-day timestamps, seeded status distribution) | `scripts/smoke_test.py` checks minimum 2,000 merchants / 5,000,000 transactions. The assignment owner reports a successful fresh startup; this review did not execute it. | PARTIAL | Runtime counts/distribution not independently checked in this review. Seed distribution targets roughly 80% SUCCESS, 10% FAILED, 10% REVERSED; it does not produce 10% PENDING in the initial seed. |
| Continuous change simulator: ~20 new transactions/sec, settlement, reversals, merchant risk/province change each minute, test insert/delete | `scripts/change_simulator.py`; default `change-simulator` service in `docker-compose.yml` | Compose orders it after healthy `startup-readiness`; no simulator runtime evidence collected in this review. | PARTIAL | The implementation changes one merchant every 60 cycles (about once per minute); event propagation was not independently observed. |
| Five-card Metabase dashboard directly querying `core` and record execution times | Metabase service in `docker-compose.yml`; observations in `docs/benchmark.md` | Timings are owner-reported. No dashboard/question definitions or export are tracked; no live H2 inspection was performed. | NOT VERIFIED | A clean Metabase volume is not automatically populated with the dashboard. Current Compose starts Metabase but does not provision saved questions/cards. |
| One-command local startup, no paid/cloud dependency, suitable for 16 GB host | `docker-compose.yml`; README startup instructions | `docker compose config --quiet` passed in this review. Owner reports fresh startup. | PARTIAL | No runtime/resource test on a 16 GB machine was performed in this review. README requires `.env` setup first. |

## T1 — CDC

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| Initial snapshot and continuous capture for both source tables | `docker/debezium/register.py` (`snapshot.mode=initial`, table include list); `cdc_consumer/main.py` topic subscription | `scripts/smoke_test.py` checks topics and connector/task state. Earlier conversation context reports manual snapshot/CDC checks passed; logs are not checked in and were not independently verified here. | PARTIAL | Runtime evidence is owner-reported; exact counts and offsets were not inspected in this review. |
| INSERT, UPDATE and DELETE correctly reflected in analytical current state | `cdc_consumer/main.py`; `transformations/01_transactions_staging.sql`; `transformations/03_transactions_current.sql`; `transformations/04_merchants_staging.sql`; `transformations/06_merchants_scd2.sql` | `tests/test_cdc.sql` issues source INSERT/status updates/DELETE, but has no ClickHouse assertions. | PARTIAL | Checked-in CDC SQL is a source mutation script, not a full end-to-end assertion. |
| Change order/history retained; schema addition of `installments` does not stop pipeline | Kafka topic/partition/offset stored by `cdc_consumer/main.py`; `raw.cdc_events`; `tests/test_schema_evolution.sql` | The SQL adds the column and checks PostgreSQL source state only. | PARTIAL | Raw event JSON can retain the new field, but current staging does not project `installments`; the test does not prove event ingestion or that the pipeline continues. Current ordering is offset-based and assumes one partition per topic. |

## T2 — Layering and incremental processing

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| Documented raw-to-reporting layers and correct current state | `docker/clickhouse/init/02_raw_tables.sql`; `transformations/01`–`09`; `docs/architecture.md`; `docs/data_dictionary.md` | Smoke test checks current transaction, merchant, Fact and mart counts when run. Not run by this reviewer. | PARTIAL | Views and counts are present, but exact row-state correctness is not independently verified here. |
| Incremental processing rather than complete rebuild each run | `cdc_consumer/main.py`; staging materialized views in `transformations/01_transactions_staging.sql` and `04_merchants_staging.sql`; guarded backfill in `scripts/bootstrap_clickhouse.py` | Static inspection confirms incremental insert path and one-time backfill marker/anti-join. | PARTIAL | No restart/replay runtime test was executed in this review; current-state and Fact views still compute over version history. |
| Owner national ID and mobile masked or hashed before reporting layer | Source columns in `docker/postgres/init/01_schema.sql`; raw JSON in `cdc_consumer/main.py`; transformations exclude them from merchant staging and marts | Static SQL search confirms exclusion from Fact/reporting definitions; no masking/hash expression exists. | NOT IMPLEMENTED | These fields are excluded, not masked or hashed. The literal mask/hash acceptance criterion is not implemented. |

## T3 — Analytical model

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| Documented table grains, layer rules, and model diagram | `docs/data_dictionary.md`; diagrams and decisions in `docs/architecture.md` | Reviewed against model SQL. | PASS | Documentation matches defined table/view grains at a static level. |
| Transaction state and merchant SCD2; risk/province at transaction time | `transformations/03_transactions_current.sql`; `transformations/06_merchants_scd2.sql`; `transformations/07_fact_transactions.sql`; `transformations/09_reporting_marts.sql` | `tests/data_quality.sql` checks Fact interval membership. No live query run in this review. | PARTIAL | SCD2 history before initial snapshot is unknown; event timestamp precision is milliseconds. SCD2 versions province/risk/delete, not name/MCC-only changes. |
| Card 5 reports transaction-time risk rather than current risk | Fact temporal join and `monthly_risk_volume` grouping by Fact `risk_level` | Static model inspection. Saved dashboard query and output not tracked or verified. | PARTIAL | Model path implements the intended attribution; actual Metabase card wiring is unverified. |

## T4 — Metabase optimization

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| Five optimized reports backed by ClickHouse aggregates | `transformations/08_daily_sales.sql`; `transformations/09_reporting_marts.sql` | `scripts/smoke_test.py` checks nonempty/recent refreshes when run. Not run in this review. | PARTIAL | Metabase questions are not provisioned from tracked files and were not inspected in the live app. |
| Cards 1–4 match original output or explain differences; card 5 uses transaction-time risk | Marts in `transformations/08`–`09`; timing documentation in `docs/benchmark.md` | Current timing values are owner-reported. Query definitions and result comparisons are absent from the repository. | NOT VERIFIED | Exact metric equivalence, report date filters, saved question connections and card 5 wiring cannot be confirmed. |
| Before/after card timings and optimization actions documented | `docs/benchmark.md`; `docs/baseline.md` | Ratio arithmetic was checked against owner-provided timings; no performance test run. | PARTIAL | Measurements are one-off observations; refresh cost excluded; no environment/repetition/cache methodology. |

## T5 — Data quality and governance

| Requirement | Implementation evidence | Validation evidence | Status | Limitation |
|---|---|---|---|---|
| Automated checks for keys and allowed values | `tests/data_quality.sql`; runner `scripts/reconcile.py` | Static inspection only in this review. | PARTIAL | Checks status/channel and Fact ID uniqueness; does not validate risk values or require exactly one current SCD2 row per source merchant. |
| Compare successful daily transaction count and amount between `core` and reporting layer | `tests/reconciliation/postgres_daily.sql`; `tests/reconciliation/clickhouse_daily.sql`; `scripts/reconcile.py`; `tests/mart_reconciliation.sql` | Runner compares PostgreSQL daily metrics to `mart.fact_transactions`; mart SQL compares successful counts across Fact and five marts. Not run in this review. | PARTIAL | Source-to-sink daily metrics compare against Fact, not daily reporting mart amounts. Mart reconciliation checks counts only, not amount/fee sums. |
| Document data ownership and sensitive columns per reporting table | `docs/data_governance.md`; `docs/data_dictionary.md`; reporting definitions in `transformations/08`–`09` | Static inspection confirms named sensitive columns are not selected into Fact/mart definitions. | PARTIAL | Role ownership is proposed, not formally assigned. Quality SQL checks column names, not arbitrary JSON content. |
| Run routine reconciliation only on a consistent/quiescent boundary | `scripts/reconcile.py`; `docs/data_governance.md` | Script checks `RECONCILE_QUIESCENT=1` and compares totals/daily metrics; not run in this review. | PARTIAL | Flag is an operator assertion, not a lock or source-writer detector. Daily scheduling is not configured. |

## Verification provenance

- **Executed in this review:** `docker compose config --quiet` passed; `git diff
  --check` is run as part of documentation verification. Docker runtime access was
  unavailable, so no containers, source rows, Kafka offsets or ClickHouse rows
  were inspected or changed.
- **Owner-reported, not independently executed here:** a successful fresh startup
  from empty PostgreSQL/Kafka/ClickHouse volumes and the benchmark timings in
  `docs/benchmark.md`.
- **Earlier owner-reported manual checks, not independently executed here:**
  initial snapshot ingestion; INSERT/UPDATE/DELETE; `installments` schema
  evolution; consumer restart/replay; merchant risk/province history; and basic
  quality/mart reconciliation. No command output or result artifacts for these
  checks are committed in the repository.
- **Repository test commands:** `docker compose run --rm smoke-test` runs the
  bounded readiness checks. Routine source/sink and SQL checks use
  `RECONCILE_QUIESCENT=1 python scripts/reconcile.py` after stopping writers and
  allowing CDC to catch up. `tests/test_cdc.sql`,
  `tests/test_schema_evolution.sql`, and `tests/test_recovery.sql` are mutating
  `psql` scripts; they do not constitute automated sink assertions and are not
  run by routine reconciliation.

## Assumptions and unimplemented requirements

- The assignment specifies integer Rial amounts; the source schema uses BIGINT
  and does not enforce currency or fee rules.
- The original snapshot is treated as the earliest known merchant state. It
  cannot prove risk/province values before that observation.
- The assignment's mask/hash requirement is not met by column exclusion alone.
- No dashboard provisioning, daily reconciliation scheduler, row TTL, HA, or
  production-grade Kafka security is configured in this local demo.
