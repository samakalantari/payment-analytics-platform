# Metabase Performance Benchmark

## 1. Baseline Performance Results

### Environment

- **Database:** PostgreSQL 17 (OLTP)
- **Transactions:** 5,000,000
- **Merchants:** 2,000
- **Dashboard:** Daily Sales - OLTP Baseline
- **BI Tool:** Metabase

### Initial Execution Times

| Card | Execution Time (s) | Execution Time (ms) |
|---|---:|---:|
| 01 - Daily Sales | 2.5 | 2,500 |
| 02 - Success Rate | 2.0 | 2,000 |
| 03 - Top Merchants | 1.7 | 1,700 |
| 04 - Fees | 2.2 | 2,200 |
| 05 - Risk Analysis | 5.1 | 5,100 |

### Initial Observations

- **Slowest query:** Risk Analysis (5.1 seconds).
- **Fastest query:** Top Merchants (1.7 seconds).
- Risk Analysis takes approximately 3 times longer than Top Merchants.
- All queries currently execute directly against the OLTP database.

### Performance Analysis

The current