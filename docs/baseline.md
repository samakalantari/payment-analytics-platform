# PostgreSQL Dashboard Baseline

The current recorded PostgreSQL baseline timings are the values in
[benchmark.md](benchmark.md). Those measurements were supplied by the project
owner as one-off observations; this repository does not contain the baseline
SQL, exported Metabase questions, or a repeatable timing protocol.

Older PostgreSQL timing observations have been superseded and are intentionally
omitted so they are not mistaken for the current benchmark run. The original
dashboard is described in the assignment as querying PostgreSQL `core` directly.
The source schema and seed are implemented in
`docker/postgres/init/01_schema.sql` and `scripts/seed_data.py`.

For the current timings, query-by-query equivalence to the ClickHouse reports
cannot be verified from versioned query text. See the benchmark documentation
for provenance, calculated ratios, refresh-cost exclusions and limitations.
