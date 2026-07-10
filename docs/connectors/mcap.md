# Reading MCAP Files

[MCAP](https://mcap.dev/) is an open-source container file format for multimodal log data, commonly used in robotics and autonomous systems. Daft can read MCAP files using [`daft.read_mcap()`][daft.io.read_mcap].

MCAP reading is built into Daft through the official Foxglove Rust library. The
optional Python `mcap` packages are only needed when authoring files or decoding
application-specific schemas in Python.

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

=== "Bounded Exact Paths"

    ```python
    import daft

    selected_paths = [
        "s3://robot-logs/episode-001.mcap",
        "s3://robot-logs/episode-019.mcap",
    ]
    df = daft.read_mcap(selected_paths, topics=["/left-arm-state"])
    df.show()
    ```

    An empty path sequence returns an empty DataFrame with the standard MCAP
    schema, which is useful after a catalog filter selects no episodes. A
    nonempty sequence fails if one of its exact path selectors cannot be
    resolved, rather than silently returning partial data.

## Output Schema

The `read_mcap` function returns a DataFrame with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `source_path` | `string` | Source MCAP path for episode/file provenance |
| `topic` | `string` | The topic name the message was published on |
| `log_time` | `uint64` | Timestamp when the message was logged (nanoseconds) |
| `publish_time` | `uint64` | Timestamp when the message was published (nanoseconds) |
| `sequence` | `uint32` | Sequence number of the message |
| `data` | `binary` | Raw message payload without string conversion |

## Foxglove compressed video

Set `decode_video=True` to decode H.264 or H.265
[`foxglove.CompressedVideo`](https://docs.foxglove.dev/docs/sdk/schemas/compressed-video)
messages into Daft images. Decoding is source-integrated rather than a row UDF:
Daft pushes the requested topic into the native MCAP reader, then keeps one
ordered PyAV codec context alive across message batches. Install the optional
video dependencies with `pip install "daft[video]"`.

```python
frames = daft.read_mcap(
    "s3://robot-logs/episode.mcap",
    topics=["/camera/top/compressed"],
    decode_video=True,
    image_height=360,
    image_width=640,
)
```

Explicit topics are required so non-video sensor payloads are pruned before
decode. The decoded schema retains `source_path`, `topic`, `log_time`,
`publish_time`, and `sequence`, and adds `frame_id`, `format`, `frame_index`,
`frame_time`, exact `timestamp_ns`, and `is_key_frame`. `data` is an RGB
`Image` rather than the encoded binary payload. Image dimensions are variable
unless both output dimensions are supplied. `frame_index` starts at zero for
each `(source_path, topic)` scan task, so a selective scan that begins at a
later keyframe does not preserve the index from a full-file decode.

Foxglove requires one Annex-B encoded image per message, no B-frames, and codec
parameter sets on every keyframe. Daft also keeps PyAV's elementary-stream
parser alive across MCAP rows because real datasets such as ABC can fragment an
access unit across several `CompressedVideo.data` chunks. For conforming input,
frame metadata is attributed to its source message. For fragmented input,
`log_time`, `publish_time`, `sequence`, `frame_id`, and `timestamp_ns` are a
deterministic attribution to the earliest buffered source chunk, not a claim
that the chunk timestamp is an encoded frame PTS. `source_path` and `topic`
provenance remain exact. Daft supports the Foxglove `h264` and `h265` format
tags (`h265` maps to FFmpeg/PyAV's `hevc` decoder).

### Selective video decoding

Topic filters are always pushed below video decode. A time window beginning on
a delta frame must first warm the codec from a preceding keyframe. On indexed
MCAPs, Daft uses reverse log-time reads to find the nearest H.264 IDR or H.265
IRAP, then reads forward from that message and suppresses warmup frames before
the requested bound. Unindexed MCAPs fall back to decoding the selected topic
from its beginning. Supply an external keyframe lookup to skip the reverse scan
when a dataset already has such a catalog:

```python
def previous_keyframe(file_path: str, topic: str, requested_start: int) -> int:
    # Look up the nearest keyframe time <= requested_start in an external index.
    return keyframe_catalog[file_path, topic].floor(requested_start)

frames = daft.read_mcap(
    "s3://robot-logs/episode.mcap",
    topics=["/camera/top/compressed"],
    start_time=start_ns,
    end_time=end_ns,
    decode_video=True,
    video_start_time_resolver=previous_keyframe,
)
```

The physical scan starts at the returned keyframe while the logical result
still begins at `start_time`. Daft applies safe limits to decoded output frames,
not to encoded messages, because codec output cardinality need not equal input
batch cardinality.

## Filtering Options

### Time Range

Filter messages by time range. The start is inclusive and the end is exclusive;
the unit matches the MCAP `log_time` field (typically nanoseconds):

```python
df = daft.read_mcap(
    "/path/to/recording.mcap",
    start_time=1609459200000000000,  # 2021-01-01 UTC in nanoseconds
    end_time=1609545600000000000,    # 2021-01-02 UTC in nanoseconds
)
df.show()
```

## Query pushdown

For indexed MCAP files, Daft reads the footer summary first and asks the native
reader only for chunks that can contain the requested topics and log-time range.
This works for filters supplied directly to `read_mcap` and for equivalent
planner predicates:

```python
df = (
    daft.read_mcap("s3://robot-logs/episode.mcap")
    .where(
        daft.col("topic").is_in(["/camera", "/imu"])
        & (daft.col("log_time") >= start_ns)
        & (daft.col("log_time") < end_ns)
    )
    .select("source_path", "topic", "log_time")
)
```

Topic equality/`is_in`, log-time bounds, projection, and safe limits are pushed
into the reader. The original predicate remains as a residual check. Omitting
`data` also avoids payload-column allocation. MCAPs without chunk indexes fall
back to a native sequential scan; that fallback yields file order, while the
indexed path yields log-time order.

Use `daft.from_glob_path` for cheap file discovery and object sizes, then pass a
bounded set of exact paths to `daft.read_mcap` for message reads. See
[`daft.McapFile`][daft.McapFile] for range-backed summary inspection without
materializing the full object.

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
