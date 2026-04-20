# Reading MCAP Files

[MCAP](https://mcap.dev/) is an open-source container file format for multimodal log data, commonly used in robotics and autonomous systems. Daft can read MCAP files using [`daft.read_mcap()`][daft.io.read_mcap].

## Installing Dependencies

MCAP support requires the `mcap` package:

```bash
pip install mcap
```

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
| `topic` | `string` | The topic name the message was published on |
| `log_time` | `int64` | Timestamp when the message was logged (nanoseconds) |
| `publish_time` | `int64` | Timestamp when the message was published (nanoseconds) |
| `sequence` | `int32` | Sequence number of the message |
| `data` | `string` | Message data as a string |

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

### Topic Filtering

Read only specific topics:

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    topics=["/camera/image", "/lidar/points"],
)
df.show()
```

### Batch Size

Control memory usage by adjusting the batch size:

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    batch_size=500,  # Number of messages per batch (default: 1000)
)
```

## Advanced: Topic Start Time Resolver

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
