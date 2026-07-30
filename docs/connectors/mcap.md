# Reading MCAP Files

[MCAP](https://mcap.dev/) is an open-source container file format for multimodal log data, commonly used in robotics and autonomous systems. Daft can read MCAP files using [`daft.read_mcap()`][daft.io.read_mcap].

## Basic Usage

=== "Local File"

    ```python
    import daft

    df = daft.read_mcap("/path/to/recording.mcap")
    df.show()
    ```

=== "Remote File (S3)"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(s3=S3Config(region_name="us-west-2"))
    df = daft.read_mcap("s3://bucket/recordings/data.mcap", io_config=io_config)
    df.show()
    ```

=== "Remote File (HTTP/HTTPS)"

    ```python
    import daft

    df = daft.read_mcap("https://example.com/recordings/session.mcap")
    df.show()
    ```

=== "Directory of Files"

    ```python
    import daft

    # Read all MCAP files in a directory
    df = daft.read_mcap("/recordings/")
    df.show()
    ```

## Output Schema

The `read_mcap` function returns a DataFrame with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `source_path` | `string` | Path of the MCAP file containing the message |
| `topic` | `string` | The topic name the message was published on |
| `log_time` | `uint64` | Timestamp when the message was logged (nanoseconds) |
| `publish_time` | `uint64` | Timestamp when the message was published (nanoseconds) |
| `sequence` | `uint32` | Sequence number of the message |
| `data` | `binary` | Raw message payload |

## Filtering Options

### Time Range

Filter messages by time range. The time unit matches the MCAP `log_time` field (typically nanoseconds):

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    start_time=1609459200000000000,  # 2021-01-01 UTC in nanoseconds
    end_time=1609545600000000000,    # 2021-01-02 UTC in nanoseconds
)
df.show()
```

!!! warning "Unsigned timestamp migration"

    As shown in the [output schema](#output-schema), MCAP timestamps now use the format's native `uint64` type instead of `int64`. `start_time`, `end_time`, and times returned by `topic_start_time_resolver` must therefore be between `0` and `2**64 - 1`. Negative time values accepted by earlier Daft versions now raise `OverflowError`.

### Topic Filtering

Read only specific topics:

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    topics=["/camera/image", "/lidar/points"],
)
df.show()
```

### Predicate Pushdown

Filters on `topic` and `log_time` are pushed into the reader, where they prune
chunks through the file's summary index, so queries like this read only the
relevant byte ranges from storage:

```python
df = daft.read_mcap("/path/to/recording.mcap").where(
    (col("topic") == "/camera/image")
    & (col("log_time") >= 1609459200000000000)
    & (col("log_time") < 1609545600000000000)
)
```

Pushed filters combine with any explicit `topics`/`start_time`/`end_time`
arguments, and the full predicate is still applied to decoded batches, so
results are identical either way.

### Ordering

Indexed MCAP files (files with a summary and chunk indexes, the common case)
are read in `log_time` order within each file, even when messages are
physically out of order across chunks. Files without an index stream in file
order, and ordering across multiple files is not guaranteed.

### Batch Size

Control memory usage by adjusting the batch size:

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    batch_size=500,  # Number of messages per batch (default: 1000)
)
```

## Advanced: Topic Start Time Resolver

!!! warning "Deprecated"
    `topic_start_time_resolver` is deprecated and will be removed in a future
    release. Use explicit `topics` and `start_time` values for MCAP scans.
    Dedicated support for custom video decoding will be added separately.

For advanced use cases, you can provide a callable that computes per-file, per-topic start times. This is useful for resuming reads from specific positions:

```python
def get_topic_start_times(file_path: str) -> dict[str, int]:
    """Return a mapping of topic names to start times."""
    # Your logic to determine start times per topic
    return {
        "/camera/image": 1609459200000000000,
        "/lidar/points": 1609459200500000000,
    }

df = daft.read_mcap(
    "/path/to/recording.mcap",
    topic_start_time_resolver=get_topic_start_times,
)
```

When a resolver is provided:
- One scan task is created per (file, topic) combination
- The task's start_time is set to `max(start_time, resolver(file)[topic])`

## Use Cases

### Robotics Data Processing

```python
import daft
from daft import col

# Read sensor data
df = daft.read_mcap("/recordings/*.mcap", topics=["/imu/data"])

# Filter and process
recent_data = df.where(col("log_time") > 1609459200000000000)
recent_data.show()
```

### Multi-Topic Analysis

```python
import daft
from daft import col

# Read multiple topics
df = daft.read_mcap(
    "/recordings/session.mcap",
    topics=["/camera/image", "/gps/fix", "/vehicle/speed"],
)

# Group by topic
topic_counts = df.groupby("topic").count()
topic_counts.show()
```
