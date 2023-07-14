# Parquet Benchmarks

Goals:

1. Find Parquet features that Daft underperforms on
2. Compare Daft against other frameworks

## Setup

Create a new virtual environment and install the dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r benchmark-requirements.txt
```

Now, install the version of Daft you wish to use for benchmarking (either a released wheel, or if you want, a local build)

```bash
pip install getdaft
```

## Running the benchmarks:

### Goal 1: Find Parquet features that Daft underperforms on

```bash
pytest benchmarking/parquet/ --benchmark-only --benchmark-group-by=group -k daft
```

### Goal 2: Compare Daft against other frameworks

```bash
pytest benchmarking/parquet/ --benchmark-only --benchmark-group-by=param:path
```

### Check peak memory usage

Ensure that you have `pytest-memray` installed.

```bash
pytest benchmarking/parquet/ --benchmark-only --memray
```
