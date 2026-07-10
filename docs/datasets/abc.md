# ABC-130k with Daft

[ABC-130k](https://huggingface.co/datasets/XDOF/ABC-130k) is a large-scale
robot teleoperation dataset containing 130,703 bimanual YAM episodes and more
than 3,590 hours of demonstrations. Each episode is stored as an MCAP file with
asynchronous robot state, action, gripper, instruction, calibration, and
compressed-video streams. Some episodes also have a separate MCAP containing
free-form subtask annotations.

Daft exposes ABC-130k through [`daft.datasets.abc`](../api/datasets.md#abc-130k).
The API deliberately separates cheap episode discovery from MCAP metadata
sniffing, message reads, and video decoding so that you can bound work before
touching large episode files.

!!! warning "Beta"

    This API is new and may evolve as ABC-130k and Daft's native MCAP support
    develop.

## Prerequisites

ABC-130k is a gated Hugging Face dataset. First accept the access conditions on
the [dataset page](https://huggingface.co/datasets/XDOF/ABC-130k), then provide
a token with access to the repository. Keeping the token in the `HF_TOKEN`
environment variable avoids putting credentials in source code; an existing
`hf auth login` credential is also recognized.

```python
import os

from daft.io import HuggingFaceConfig, IOConfig

io_config = IOConfig(
    hf=HuggingFaceConfig(
        token=os.environ["HF_TOKEN"],
        use_xet=True,
    )
)
```

You can also point the helpers at a local mirror or another object-store path.
In that case, pass the corresponding `IOConfig` if credentials are required.
Video decoding requires Daft's optional video dependencies:

```bash
pip install "daft[huggingface,video]"
```

## Quickstart: discover episodes first

Use [`daft.datasets.abc.raw`](../api/datasets.md#daft.datasets.abc.raw) to build
an episode-level catalog. Restrict the split and task in `raw()` so that the
remote object listing is narrow before applying DataFrame operations.

```python
from daft.datasets import abc

episodes = abc.raw(
    split="train",
    tasks=["clip_the_socks_to_the_hanger"],
    io_config=io_config,
)

sample = episodes.sort("episode_size").limit(4)
sample.select(
    "split",
    "task_slug",
    "episode_id",
    "episode_size",
    "annotated",
).show()
```

`raw()` uses object metadata to discover paths and sizes. For the default
Hugging Face source, it uses the official client's paginated recursive-tree API
instead of issuing one listing request per episode directory. Local and other
object-store mirrors use Daft's native glob scan. The Hub tree is consumed
eagerly when `raw()` is called; subsequent DataFrame transforms are lazy. It
does **not** open the MCAP data section, decode protobuf payloads, or inspect
every episode's MCAP summary. A DataFrame `limit()` bounds the rows passed to
later helpers, but the backing store still lists the selected split/task scope.

### Episode layout

The Hugging Face release is organized as follows:

```text
data/
|-- train/
|   `-- <task_slug>/
|       `-- episode_<uuid>/
|           |-- episode.mcap
|           `-- annotation.mcap  # Optional
`-- val/
    `-- <task_slug>/
        `-- episode_<uuid>/
            `-- episode.mcap
```

Some format documentation uses `task=<task_slug>` directory names. `raw()`
accepts and normalizes both layouts when filtering by `tasks=`.

`raw()` returns one row per episode with these catalog columns:

| Column | Description |
| --- | --- |
| `split` | Dataset split parsed from the path (`train` or `val`) |
| `task_slug` | Task directory name; kept separate from the human-readable instruction |
| `episode_id` | Episode UUID, without the `episode_` directory prefix |
| `episode_dir` | Episode directory path |
| `episode_path` | Path to `episode.mcap` |
| `episode_size` | Size of `episode.mcap` in bytes |
| `annotation_path` | Path to `annotation.mcap`, or null when absent |
| `annotation_size` | Size of `annotation.mcap` in bytes, or null |
| `annotated` | Whether an annotation MCAP was discovered |
| `episode_mcap` | Lazy [`daft.McapFile`](../connectors/mcap.md) reference |
| `annotation_mcap` | Nullable lazy `daft.McapFile` reference |

Set `include_annotations=False` when the annotation catalog is not needed.

## Sniff metadata for a bounded catalog

Call [`daft.datasets.abc.metadata`](../api/datasets.md#daft.datasets.abc.metadata)
only after selecting a useful, bounded episode catalog. It range-reads MCAP
footer and summary metadata instead of downloading complete episode files on
storage backends that support range requests.

```python
sniffed = abc.metadata(sample)

sniffed.select(
    "episode_id",
    "task_name",
    "message_count",
    "message_start_time",
    "message_end_time",
    "topics",
    "video_topics",
).show()
```

`metadata()` preserves the catalog columns and adds:

| Column | Description |
| --- | --- |
| `session_id` | Session identifier reported by the episode metadata, when present |
| `operator_id` | Operator identifier reported by the episode metadata, when present |
| `task_name` | Human-readable task instruction, when present |
| `duration_seconds` | Reported or derived episode duration in seconds |
| `message_count` | Indexed MCAP message count |
| `message_start_time` | Earliest indexed message time in Unix nanoseconds |
| `message_end_time` | Latest indexed message time in Unix nanoseconds |
| `chunk_count` | Number of indexed MCAP chunks |
| `topics` | Topics declared by the MCAP channel catalog |
| `video_topics` | Topics whose schema is `foxglove.CompressedVideo` |
| `indexed` | Whether chunk indexes are available for pushdown |
| `episode_metadata_json` | JSON copy of the matched record name and metadata key/value map |

ABC releases have used more than one metadata-record shape. Daft keeps
path-derived `episode_id` and `task_slug` in the catalog and exposes the raw
metadata JSON so release-specific fields remain available.

!!! tip

    `raw()` is appropriate for scanning a large catalog of paths and sizes.
    `metadata()` performs range requests per surviving episode, so a call such
    as `abc.metadata(abc.raw(...))` over an entire split is intentionally much
    more expensive.

## Read state and action messages

[`daft.datasets.abc.messages`](../api/datasets.md#daft.datasets.abc.messages)
collects the bounded episode catalog to obtain its paths, then constructs a
native [`daft.read_mcap`](../connectors/mcap.md) scan. Collecting the catalog
does not materialize the MCAP payloads themselves.

The default topic set contains the eight arm and gripper state/action streams.
It excludes camera payloads, calibration, and instruction messages so a bare
call does not accidentally read video.

```python
one_episode = episodes.sort("episode_size").limit(1)

bounds = (
    abc.metadata(one_episode)
    .select("message_start_time")
    .collect()
    .to_pydict()
)
start_ns = bounds["message_start_time"][0]

messages = abc.messages(
    one_episode,
    topics=["/left-arm-state", "/right-arm-state"],
    start_time=start_ns,
    end_time=start_ns + 2_000_000_000,
    io_config=io_config,
)

messages.select(
    "split",
    "task_slug",
    "episode_id",
    "source_path",
    "topic",
    "log_time",
    "data",
).show()
```

`start_time` is inclusive and `end_time` is exclusive. Both use the MCAP
message clock, normally absolute Unix nanoseconds for ABC-130k. The output
includes episode identity, `source_path` provenance, topic, log and publish
times, sequence number, and the raw protobuf payload as binary `data`.

Topic and time constraints are pushed into the indexed MCAP reader. Equivalent
conjunctive filters on `topic` and `log_time` are also recognized by Daft's
query planner. Projection is pushed down too, so omit `data` when you only need
counts or timing information:

```python
counts = (
    abc.messages(
        one_episode,
        topics=["/left-arm-state", "/right-arm-state"],
        start_time=start_ns,
        end_time=start_ns + 2_000_000_000,
        io_config=io_config,
    )
    .select("episode_id", "topic", "log_time")
    .groupby("episode_id", "topic")
    .count()
)
```

The native reader uses chunk and message indexes when available and falls back
to a correct sequential scan for unindexed MCAPs.

### State and action topics

| Topic | Meaning |
| --- | --- |
| `/left-arm-state`, `/right-arm-state` | Observed six-joint arm state and end-effector pose |
| `/left-arm-action`, `/right-arm-action` | Commanded six-joint arm position and pose |
| `/left-ee-state`, `/right-ee-state` | Observed gripper aperture (`0` closed, `1` open) |
| `/left-ee-action`, `/right-ee-action` | Commanded gripper aperture |

ABC streams run on independent clocks. `messages()` does not silently resample,
join, or align state, action, gripper, annotation, and camera rows. Use
`log_time` to implement the nearest-neighbor or causal alignment appropriate
for your model; do not assume equal message counts or matching row indexes.

## Read subtask annotations

[`daft.datasets.abc.annotations`](../api/datasets.md#daft.datasets.abc.annotations)
reads `/subtask-annotation` from the optional annotation MCAPs and decodes the
stable annotation protobuf into event rows.

```python
labels = abc.annotations(
    one_episode,
    start_time=start_ns,
    end_time=start_ns + 10_000_000_000,
    io_config=io_config,
)

labels.select(
    "episode_id",
    "log_time",
    "timestamp_ns",
    "label",
).show()
```

Each event marks the start of a subtask. Its segment continues until the next
event, or until the episode ends for the final event. Labels are free-form text
and are intentionally not normalized to an enum. Episodes without an
`annotation.mcap` contribute no annotation rows.

## Decode camera frames

Use [`daft.datasets.abc.camera_frames`](../api/datasets.md#daft.datasets.abc.camera_frames)
only after bounding the episode catalog and, when possible, the time range.
The helper first applies MCAP topic/time pushdown, then decodes the surviving
Foxglove `CompressedVideo` messages with a persistent codec context.

```python
frames = abc.camera_frames(
    one_episode,
    cameras=["top", "left_wrist"],
    start_time=start_ns,
    end_time=start_ns + 2_000_000_000,
    width=224,
    height=224,
    io_config=io_config,
)

frames.select(
    "episode_id",
    "camera",
    "topic",
    "format",
    "timestamp_ns",
    "data",
).show()
```

Camera aliases map to MCAP topics as follows:

| Alias | Topic or topics |
| --- | --- |
| `top` | All top streams present: `/top-camera`, `/top-left-camera`, `/top-right-camera` |
| `top_mono` | `/top-camera` |
| `top_left` | `/top-left-camera` |
| `top_right` | `/top-right-camera` |
| `left_wrist` | `/left-wrist-camera` |
| `right_wrist` | `/right-wrist-camera` |

The `top` alias does not silently choose one stereo eye. The result keeps both
the resolved `camera` alias and exact `topic`, so stereo streams remain
distinguishable.

Do not infer a codec from the camera name or station type. ABC contains H.264
and H.265 streams, and the `format` field inside each `CompressedVideo` message
is authoritative. Daft uses it to choose the decoder and preserves it in the
output alongside source provenance, `frame_id`, `timestamp_ns`, frame metadata,
and RGB image `data`. For time ranges that begin between keyframes, the reader
warms the decoder from a preceding random-access point while suppressing frames
before the requested logical start.

Projecting only frame metadata allows Daft to avoid materializing RGB images:

```python
frame_catalog = frames.select(
    "episode_id",
    "camera",
    "topic",
    "format",
    "log_time",
    "is_key_frame",
)
```

## Recommended workflow

For large ABC-130k jobs:

1. Restrict `split` and `tasks` in `raw()` before remote discovery.
2. Use catalog columns such as `episode_size` and `annotated` to choose a
   bounded episode sample.
3. Call `metadata()` only for that sample when you need topics, bounds, or
   indexed message counts.
4. Pass explicit topics and time bounds to `messages()` or `annotations()`.
5. Decode cameras last, and project out image `data` when only frame metadata
   is required.

This keeps object listing, MCAP summary sniffing, message reads, and video
materialization as separate measurable stages.

See the [ABC-130k format specification](https://huggingface.co/datasets/XDOF/ABC-130k/blob/main/docs/YAM_DATA_FORMAT.md)
for the source schemas and the [MCAP connector guide](../connectors/mcap.md) for
lower-level reader behavior. Complete parameter documentation is in the
[Dataset API reference](../api/datasets.md#abc-130k).
