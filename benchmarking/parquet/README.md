# Parquet Benchmarks

Goals:

1. Find Parquet features that Daft underperforms on
2. Compare Daft against other frameworks for reading Parquet files

## Running the benchmarks:

**Goal 1**: run only Daft reads and group by Parquet feature:

```bash
pytest benchmarking/parquet/ --benchmark-only --benchmark-group-by=group -k daft
```

**Goal 2**: run all the different frameworks' reads and group results by file path:

```bash
pytest benchmarking/parquet/ --benchmark-only --benchmark-group-by=param:path
```
