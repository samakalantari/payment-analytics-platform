# Data Dictionary and Modeling Rules

Definitions below follow the current PostgreSQL schema and ClickHouse SQL. They
describe implemented behavior, including its limitations.

## Identifiers, allowed values and grains

| Model | Grain | Identifier / key | Notes |
|---|---|---|---|
| `core.merchants` | One current source row per merchant | `merchant_id` primary key | Includes owner national ID and mobile in source |
| `core.transactions` | One current source row per transaction | `transaction_id` primary key | `merchant_id` references `core.merchants.merchant_id` |
| `raw.cdc_events` | One ingested Kafka record per topic, partition and offset (duplicates may exist before replacement merges) | `(topic, partition_id, kafka_offset)` | `event_json` stores the complete Debezium event string |
| `staging.transactions_versions` | One transaction CDC version per source event | Transaction ID plus Kafka partition/offset ordering fields | Includes `card_bin`; deletes are marked `is_deleted` |
| `staging.transactions_current` | One latest non-deleted transaction per transaction ID | `transaction_id` | Uses `argMax(..., kafka_offset)`; excludes `card_bin` |
| `staging.merchants_versions` | One merchant CDC version per source event | Merchant ID plus Kafka partition/offset ordering fields | Contains name, MCC, province and risk; excludes owner PII |
| `staging.merchants_scd2` | One retained significant merchant history interval | `merchant_id`, `valid_from` (event order is Kafka offset) | A new interval is retained for first version, province/risk change or deletion transition; deleted rows are filtered from the view |
| `mart.fact_transactions` | One current, non-deleted transaction with a matching merchant interval | `transaction_id` | Attributes include merchant values effective at transaction time |
| `mart.daily_sales` | One transaction date | `transaction_date` | Successful count and amount |
| `mart.daily_channel_success` | One transaction date and channel | `(transaction_date, channel)` | All-status transaction count and successful count |
| `mart.merchant_monthly_sales` | One transaction month and merchant ID | `(transaction_month, merchant_id)` | Successful count and amount |
| `mart.fees_by_mcc_province_daily` | One date, MCC and historically assigned province | `(transaction_date, mcc, province)` | Successful count and successful fee sum |
| `mart.monthly_risk_volume` | One transaction month and historically assigned risk level | `(transaction_month, risk_level)` | Successful count and amount |

The five reporting marts are the tables `daily_sales`,
`daily_channel_success`, `merchant_monthly_sales`,
`fees_by_mcc_province_daily`, and `monthly_risk_volume` in database `mart`.

### Valid source values

- Transaction status: `PENDING`, `SUCCESS`, `FAILED`, `REVERSED`.
- Transaction channel: `POS`, `IPG`, `QR`.
- Merchant risk level: `LOW`, `MEDIUM`, `HIGH`.
- Merchant and transaction primary/business IDs are positive integer IDs in
  seeded data; the schema declares them as `BIGINT` primary keys but does not
  add a positivity constraint.

## Metric definitions

| Metric | Implemented definition |
|---|---|
| Successful transaction count | Count of rows whose current transaction status is exactly `SUCCESS` |
| Successful amount | Sum of `amount` for rows whose current status is `SUCCESS` |
| Successful fee | Sum of `fee_amount` for rows whose current status is `SUCCESS` |
| Daily success rate | Derived as `successful_transactions / total_transactions` in `daily_channel_success`; the mart stores counts, not a rate column. Zero-denominator handling is left to the consuming query/dashboard. |
| Total transactions by channel/day | Count of all Fact rows in that date/channel, including non-success statuses |

`daily_sales`, merchant monthly sales, fees by MCC/province, and monthly risk
volume filter to `SUCCESS`. `daily_channel_success` includes all statuses in
its denominator. The reconciliation SQL sums the successful counts across
reporting marts and compares them with Fact. These definitions reflect current
SQL; they are not a policy statement about whether reversals should be netted
against prior successful amounts. In the current state model, a transaction
whose latest status is `REVERSED` is not counted as successful.

The seed generator produces `fee_amount` as integer `amount // 100`, but the
database schema does not enforce that ratio. Reports sum the stored fee value;
they do not recalculate fees.

## Transaction-time merchant attribution

`mart.fact_transactions` joins each current transaction to the merchant SCD2
interval satisfying:

```text
merchant_id matches
created_at >= valid_from
created_at < valid_to
```

This is half-open interval attribution at transaction creation time. The
Fact therefore reports the merchant name, MCC, province and risk level from
the selected historical interval, rather than joining only to the merchant's
latest values. The model uses UTC timestamps. Transaction timestamps are
parsed as `DateTime64(6, 'UTC')`; merchant CDC `source.ts_ms` is converted to
`DateTime64(3, 'UTC')`.

The initial merchant snapshot is assigned `valid_from = 1970-01-01 UTC` because
the snapshot is treated as the earliest known state. This supports attribution
for seeded transactions, but does not prove that those merchant attributes
were historically true at every earlier transaction time. No pre-snapshot
risk/province history is available in the source snapshot.

Merchant SCD2 ordering uses Kafka offset, while interval boundaries use the
millisecond Debezium source timestamp. Multiple changes can have equal
millisecond timestamps, producing zero-length intervals; timestamp precision
and event ordering are therefore relevant limits. The current significant-change
rule versions province, risk level and deletion state; changes to merchant name
or MCC alone do not create a new history interval in this SQL.

## Data quality coverage

`tests/data_quality.sql` checks Fact transaction-ID uniqueness, allowed status
and channel values, nonempty merchant attributes, zero amounts (amount columns
are unsigned, so negative values cannot be represented there), temporal interval
membership, at most one current SCD2 version, mart column names for sensitive
terms, and whether current transactions refer to non-deleted merchant history.
The current-version query checks for more than one row; it does not assert that
every merchant has exactly one current row.

`tests/mart_reconciliation.sql` checks successful transaction counts across the
Fact and five marts. It does not reconcile mart amount or fee sums. The
PostgreSQL/ClickHouse reconciliation compares transaction count, successful
count, successful amount and daily equivalents using UTC day boundaries.
Because PostgreSQL and ClickHouse are read independently, run it with all source
writers paused; the `RECONCILE_QUIESCENT=1` flag is an operator assertion, not a
writer-state lock. See [governance and PII flow](data_governance.md) for
commands, ownership and operational boundaries.
