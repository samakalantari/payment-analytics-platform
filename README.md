# Payment Analytics Data Platform

A local end-to-end data engineering project.

## Objectives

- Simulate a payment processing OLTP database.
- Capture database changes using CDC.
- Build an incremental analytical data pipeline.
- Preserve historical merchant attributes.
- Optimize Metabase dashboards.
- Validate data quality and reconciliation.

## Start the end-to-end demo

Start the platform with:

```sh
docker compose up -d --build
```

Compose initializes and seeds PostgreSQL, registers Debezium, starts the CDC
consumer, and runs the existing smoke-test checks in the startup-readiness
service. It waits for the initial snapshot, staging, Merchant SCD2, Fact and
all five reporting marts. The readiness service stays healthy after the test
passes, so repeating `up` with the existing containers does not rerun the
initial smoke test while the simulator is writing. If Compose recreates the
readiness container, its check runs again before the simulator starts.

The smoke test can also be run independently:

```sh
docker compose run --rm smoke-test
```

Stop or restart only the simulator with:

```sh
docker compose stop change-simulator
docker compose start change-simulator
```
