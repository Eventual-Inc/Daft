# Writing to ClickHouse

[ClickHouse](https://clickhouse.com/) is a fast, open-source columnar database for real-time analytics. Daft can write DataFrames to ClickHouse tables using [`df.write_clickhouse()`][daft.dataframe.DataFrame.write_clickhouse].

## Installing Dependencies

ClickHouse support requires the `clickhouse-connect` package:

```bash
pip install clickhouse-connect
```

## Basic Usage

```python
import daft

# Create a DataFrame
df = daft.from_pydict({
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "value": [100.5, 200.3, 150.7, 300.2],
})

# Write to ClickHouse
result = df.write_clickhouse(
    table="my_table",
    host="localhost",
    port=8123,
    user="default",
    password="",
)
result.show()
```

## Output Schema

The write operation returns a DataFrame with write statistics:

| Column | Type | Description |
|--------|------|-------------|
| `total_written_rows` | `int64` | Total number of rows written |
| `total_written_bytes` | `int64` | Total number of bytes written |

## Connection Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `table` | `str` | Yes | Name of the ClickHouse table to write to |
| `host` | `str` | Yes | ClickHouse server hostname |
| `port` | `int` | No | ClickHouse HTTP port (default: 8123) |
| `user` | `str` | No | ClickHouse username |
| `password` | `str` | No | ClickHouse password |
| `database` | `str` | No | ClickHouse database name |

## Advanced Configuration

### Client Options

Pass additional options to the ClickHouse client:

```python
result = df.write_clickhouse(
    table="my_table",
    host="localhost",
    client_kwargs={
        "secure": True,
        "verify": True,
        "connect_timeout": 30,
    },
)
```

### Write Options

Pass additional options to the write operation:

```python
result = df.write_clickhouse(
    table="my_table",
    host="localhost",
    write_kwargs={
        "settings": {
            "async_insert": 1,
            "wait_for_async_insert": 1,
        },
    },
)
```

## Use Cases

### Analytics Pipeline

```python
import daft
from daft import col

# Process data
df = daft.read_parquet("s3://bucket/events/*.parquet")
aggregated = df.groupby("event_type").agg(
    col("value").sum().alias("total_value"),
    col("user_id").count().alias("event_count"),
)

# Write aggregated results to ClickHouse for dashboards
aggregated.write_clickhouse(
    table="event_aggregates",
    host="clickhouse.example.com",
    database="analytics",
    user="writer",
    password="secret",
)
```

### Real-Time Data Ingestion

```python
import daft

# Read streaming batch
df = daft.read_json("/data/batch/*.json")

# Transform and load to ClickHouse
df.write_clickhouse(
    table="events",
    host="localhost",
    write_kwargs={
        "settings": {"async_insert": 1},
    },
)
```

## Notes

- The target table must exist in ClickHouse before writing
- Column names and types in the DataFrame should match the table schema
- ClickHouse will perform type coercion where possible
