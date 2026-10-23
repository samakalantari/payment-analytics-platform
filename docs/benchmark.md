# Dashboard Performance Observations

## Recorded timings

The following are the latest one-off timings supplied by the project owner.
They are recorded observations, not timings independently measured during this
review and not controlled multi-run medians.

| Report | PostgreSQL operational query | ClickHouse reporting mart | Calculated ratio |
|---|---:|---:|---:|
| Daily Successful Transactions | 4,000 ms | 63 ms | 63.5x |
| Daily Success Rate by Channel | 4,100 ms | 133 ms | 30.8x |
| Top 10 Merchants | 2,100 ms | 80 ms | 26.3x |
| Fees by MCC and Province | 2,500 ms | 84 ms | 29.8x |
| Monthly Transactions by Risk | 6,600 ms | 89 ms | 74.2x |

Ratios are PostgreSQL timing divided by ClickHouse timing, rounded to one
decimal place. Earlier PostgreSQL observations are superseded and are not mixed
into this table; see [the baseline note](baseline.md).

The implemented pre-aggregations and the corresponding report intent are:

| Report | ClickHouse mart | Implemented aggregation |
|---|---|---|
| Daily Successful Transactions | `mart.daily_sales` | Successful count and amount by transaction date |
| Daily Success Rate by Channel | `mart.daily_channel_success` | All-status and successful transaction counts by date and channel |
| Top 10 Merchants | `mart.merchant_monthly_sales` | Successful count and amount by month and merchant; top-N selection is not in the mart definition |
| Fees by MCC and Province | `mart.fees_by_mcc_province_daily` | Successful fee amount and count by date, MCC and province |
| Monthly Transactions by Risk | `mart.monthly_risk_volume` | Successful count and amount by month and Fact risk level |

## Provenance and comparability

The latest values above were supplied by the project owner as observed card
execution times. The repository does not contain the original PostgreSQL card
SQL, the optimized card SQL, exported Metabase questions, or a timing log with
hardware, cache state, concurrency and repetition details. The SQL definitions
that are versioned in the repository are the ClickHouse Fact and mart
transformations in `transformations/07_fact_transactions.sql`,
`transformations/08_daily_sales.sql` and
`transformations/09_reporting_marts.sql`; they define aggregate tables, not
the complete dashboard question filters and presentation queries. If the card
queries are maintained only as saved Metabase questions, they are outside the
versioned evidence inspected here. No query text has been inferred or invented.

Metric equivalence for cards 1–4 has therefore not been independently verified.
The reporting mart definitions indicate successful-only aggregation for daily
sales, merchant monthly sales, fees and risk volume; daily channel success stores
all-status and successful counts. The assignment calls for the fifth optimized
card to use risk at transaction time. `mart.fact_transactions` joins merchant
SCD2 by transaction `created_at`, and `monthly_risk_volume` groups on that Fact
risk value. This matches the intended model in code, but the saved card query
and displayed results are not present in the repository, so the live dashboard
behavior remains unverified.

The ClickHouse timings are for reports backed by pre-aggregated reporting marts.
Those marts use refreshable materialized views with a full query refresh every
10 minutes. Dashboard query timings exclude the work and resource cost of those
refreshes. They should not be interpreted as a like-for-like comparison of the
total cost of PostgreSQL queries and the ClickHouse pipeline.

## Limitations

- No controlled benchmark protocol, repeated samples, variance, machine
  specification or cache-state record is included.
- Exact baseline and saved dashboard SQL are not versioned, so equivalence for
  cards 1–4 cannot be established from this repository.
- The marts aggregate all available dates; the assignment's 90-day, 30-day and
  current-month windows may be applied in saved Metabase questions, but those
  filters cannot be verified here.
- Full refresh duration and peak memory were not included in the reported card
  latency and were not measured in this review.

These results are evidence of observed query latency only. They do not establish
controlled or repeatable performance gains.
