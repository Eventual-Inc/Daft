# Writing to Google Cloud Bigtable

!!! warning "Experimental"

    This connector is experimental and the API may change in future releases.

[Google Cloud Bigtable](https://cloud.google.com/bigtable) is a fully managed, scalable NoSQL database service. Daft can write DataFrames to Bigtable tables using [`df.write_bigtable()`][daft.dataframe.DataFrame.write_bigtable].

## Installing Dependencies

Bigtable support requires the `google-cloud-bigtable` package:

```bash
pip install google-cloud-bigtable
```

## Basic Usage

```python
import daft

# Create a DataFrame
df = daft.from_pydict({
    "user_id": ["user_001", "user_002", "user_003"],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
})

# Write to Bigtable
result = df.write_bigtable(
    project_id="my-gcp-project",
    instance_id="my-bigtable-instance",
    table_id="users",
    row_key_column="user_id",
    column_family_mappings={
        "name": "profile",
        "age": "profile",
        "email": "contact",
    },
)
```

## Key Concepts

### Row Keys

Every Bigtable row requires a unique row key. Use `row_key_column` to specify which DataFrame column should be used as the row key:

```python
df.write_bigtable(
    ...,
    row_key_column="user_id",  # This column becomes the row key
)
```

### Column Families

Bigtable organizes columns into column families. Use `column_family_mappings` to specify which family each column belongs to:

```python
df.write_bigtable(
    ...,
    column_family_mappings={
        "name": "user_data",      # 'name' column goes to 'user_data' family
        "age": "user_data",       # 'age' column goes to 'user_data' family
        "email": "contact_info",  # 'email' column goes to 'contact_info' family
    },
)
```

The column families must already exist in the Bigtable table.

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | `str` | Yes | Google Cloud project ID |
| `instance_id` | `str` | Yes | Bigtable instance ID |
| `table_id` | `str` | Yes | Bigtable table ID |
| `row_key_column` | `str` | Yes | Column name to use as the row key |
| `column_family_mappings` | `dict[str, str]` | Yes | Mapping of column names to column families |
| `client_kwargs` | `dict` | No | Additional arguments for the Bigtable Client |
| `write_kwargs` | `dict` | No | Additional arguments for MutationsBatcher |
| `serialize_incompatible_types` | `bool` | No | Auto-convert incompatible types to JSON (default: True) |

## Data Type Handling

Bigtable cells only accept data that can be converted to bytes. By default, Daft automatically serializes incompatible types to JSON:

```python
df = daft.from_pydict({
    "id": ["row1"],
    "data": [{"nested": "object"}],  # Complex type
})

# Complex types are automatically serialized to JSON
df.write_bigtable(
    ...,
    serialize_incompatible_types=True,  # Default behavior
)
```

To disable automatic serialization (will raise an error for incompatible types):

```python
df.write_bigtable(
    ...,
    serialize_incompatible_types=False,
)
```

## Advanced Configuration

### Client Options

Pass additional options to the Bigtable client:

```python
result = df.write_bigtable(
    ...,
    client_kwargs={
        "admin": True,
        "channel": custom_channel,
    },
)
```

### Write Options

Configure the MutationsBatcher for write operations:

```python
result = df.write_bigtable(
    ...,
    write_kwargs={
        "flush_count": 1000,
        "max_row_bytes": 5 * 1024 * 1024,  # 5MB
    },
)
```

## Use Cases

### IoT Data Storage

```python
import daft
from daft import col

# Read sensor data
df = daft.read_parquet("s3://bucket/sensors/*.parquet")

# Prepare for Bigtable (create composite row key)
df = df.with_column(
    "row_key",
    col("device_id").concat("#").concat(col("timestamp").cast(str)),
)

# Write to Bigtable
df.write_bigtable(
    project_id="iot-project",
    instance_id="sensor-data",
    table_id="readings",
    row_key_column="row_key",
    column_family_mappings={
        "temperature": "metrics",
        "humidity": "metrics",
        "device_id": "metadata",
        "timestamp": "metadata",
    },
)
```

### User Profile Storage

```python
import daft

df = daft.from_pydict({
    "user_id": ["u001", "u002"],
    "preferences": [{"theme": "dark"}, {"theme": "light"}],
    "last_login": ["2024-01-15", "2024-01-16"],
})

df.write_bigtable(
    project_id="my-project",
    instance_id="user-store",
    table_id="profiles",
    row_key_column="user_id",
    column_family_mappings={
        "preferences": "settings",
        "last_login": "activity",
    },
)
```

## Notes

- The Bigtable table and column families must exist before writing
- Row keys should be designed carefully for efficient access patterns
- Consider Bigtable's row key design best practices for optimal performance
