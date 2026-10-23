## Performance Benchmark

| Report | PostgreSQL (ms) | ClickHouse (ms) | Speedup |
|---|---:|---:|---:|
| Daily Successful Transactions | 4000 | 63 | 63.5x |
| Daily Success Rate by Channel | 4100 | 133 | 30.8x |
| Top 10 Merchants | 2100 | 80 | 26.3x |
| Fees by MCC and Province | 2500 | 84 | 29.8x |
| Monthly Transactions by Risk | 6600 | 89 | 74.2x |

The performance improvement comes from ClickHouse and
precomputed reporting marts.

Reporting marts use periodic full refreshes, so query
latency does not include the cost of refreshing aggregates.

These are observed timings, not controlled multi-run medians.