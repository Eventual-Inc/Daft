# Reading MCAP Files

[MCAP](https://mcap.dev/) is an open-source container file format for multimodal log data, commonly used in robotics and autonomous systems. Daft can inspect MCAP topic manifests with [`daft.inspect_mcap()`][daft.io.inspect_mcap], plan topic/time read windows with [`daft.plan_mcap_reads()`][daft.io.plan_mcap_reads], and read messages using [`daft.read_mcap()`][daft.io.read_mcap].

## Installing Dependencies

MCAP support requires the `mcap` package:

```bash
pip install mcap
```

## Basic Usage

=== "Inspect Topics"

    ```python
    import daft

    manifest = daft.inspect_mcap("/path/to/recording.mcap")
    manifest.show()
    ```

=== "Plan Reads"

    ```python
    import daft

    plan = daft.plan_mcap_reads(
        "/path/to/recording.mcap",
        max_messages_per_task=100_000,
    )
    plan.show()
    ```

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

## Inspecting Topic Manifests

`inspect_mcap` scans MCAP records without decoding payloads into the message table. This is useful for large robotics and autonomous-vehicle logs where you want to plan reads by topic, schema, time range, and payload size before processing sensor streams.

```python
manifest = daft.inspect_mcap(
    "s3://robotics-logs/session-42/*.mcap",
    topics=["/camera/front/image_compressed", "/imu/data"],
)

manifest.show()
```

The manifest contains one row per file/topic/schema/message-encoding combination:

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | `string` | MCAP file path |
| `topic` | `string` | Topic name |
| `schema_name` | `string` | MCAP schema name, such as a ROS2 message type |
| `schema_encoding` | `string` | Schema encoding stored in the MCAP file |
| `message_encoding` | `string` | Channel message encoding |
| `message_count` | `int64` | Number of matching messages |
| `first_log_time` | `int64` | Earliest matching message log time |
| `last_log_time` | `int64` | Latest matching message log time |
| `first_publish_time` | `int64` | Earliest matching publish time |
| `last_publish_time` | `int64` | Latest matching publish time |
| `min_message_size` | `int64` | Smallest raw payload size in bytes |
| `max_message_size` | `int64` | Largest raw payload size in bytes |
| `total_message_size` | `int64` | Total raw payload bytes |
| `avg_message_size` | `float64` | Average raw payload size in bytes |

## Planning Distributed Reads

Large robotics logs often mix high-rate IMU streams, lower-rate LiDAR sweeps, and large camera payloads in the same MCAP file. `plan_mcap_reads` turns the topic manifest into approximate topic/time windows so you can shard work, resume from a specific window, or schedule heavier topics separately.

```python
plan = daft.plan_mcap_reads(
    "s3://robotics-logs/session-42/*.mcap",
    topics=[
        "/camera/front/image_compressed",
        "/imu/data",
        "/lidar/points",
    ],
    max_messages_per_task=100_000,
)

plan.show()
```

Each row describes a scoped read:

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | `string` | MCAP file path to read |
| `topic` | `string` | Topic to read |
| `schema_name` | `string` | MCAP schema name, such as a ROS2 message type |
| `schema_encoding` | `string` | Schema encoding stored in the MCAP file |
| `message_encoding` | `string` | Channel message encoding |
| `window_index` | `int64` | Zero-based window index for this file/topic |
| `window_count` | `int64` | Number of windows planned for this file/topic |
| `start_time` | `int64` | Inclusive log-time lower bound |
| `end_time` | `int64` | Exclusive log-time upper bound |
| `estimated_message_count` | `int64` | Approximate number of messages in this window |
| `estimated_payload_bytes` | `int64` | Approximate raw payload bytes in this window |

You can feed a planned row back into `read_mcap`:

```python
planned = plan.to_pandas().iloc[0]

df = daft.read_mcap(
    planned.file_path,
    topics=[planned.topic],
    start_time=planned.start_time,
    end_time=planned.end_time,
)
```

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

### Record Metadata

For robotics and autonomous-vehicle logs, it is often useful to inspect topic schemas and payload sizes before decoding large sensor streams. Set `include_record_metadata=True` to include MCAP schema/channel metadata alongside each message:

```python
df = daft.read_mcap(
    "s3://robotics-logs/session-42/*.mcap",
    topics=["/camera/front/image_compressed", "/imu/data"],
    include_record_metadata=True,
)

df.select("topic", "schema_name", "message_encoding", "message_size").show()
```

This adds:

| Column | Type | Description |
|--------|------|-------------|
| `schema_name` | `string` | MCAP schema name, such as a ROS2 message type |
| `schema_encoding` | `string` | Schema encoding stored in the MCAP file |
| `message_encoding` | `string` | Channel message encoding |
| `message_size` | `int64` | Raw payload size in bytes |

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
