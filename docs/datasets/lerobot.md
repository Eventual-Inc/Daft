# LeRobot datasets with Daft

[LeRobot](https://huggingface.co/docs/lerobot/lerobot-dataset-v3) stores robot learning data as Parquet (`meta/`, `data/`) plus per-camera MP4 under `videos/`. Daft exposes this layout under [`daft.datasets.lerobot`](../api/datasets.md) so you can stay at **episode granularity** for filtering, then expand to **frames** only for the episodes you need.

The reader accepts **v2.0 / v2.1** (one Parquet and one MP4 per episode) and **v3.0** (many episodes packed into shared Parquet/MP4 shards). Version is taken from `meta/info.json` → `codebase_version`.

!!! warning "Beta"

    This API is new and may evolve as we add optimizations (for example deeper integration with Parquet predicate pushdown).

## Frame-level reads

Use [`daft.datasets.lerobot.read`](../api/datasets.md#daft.datasets.lerobot.read) for the common case: a lazy DataFrame with one row per frame, episode metadata broadcast onto each frame. Pass `load_video_frames=True` (or a camera key / list of keys) to also decode each row's camera image from the MP4 files.

```python
import daft
from daft.datasets import lerobot

df = lerobot.read("your-org/your-robot-dataset", load_video_frames=True)
```

`dataset_uri` can be:

- A local directory that contains `meta/`, `data/`, etc.
- An `hf://datasets/org/name` URI (Hub layout matches the on-disk tree)
- A bare `org/name` string, which is interpreted as `hf://datasets/org/name`

## Episode metadata

Use [`daft.datasets.lerobot.read_episodes`](../api/datasets.md#daft.datasets.lerobot.read_episodes) for one row per episode:

- **v3:** `meta/episodes/**/*.parquet`
- **v2:** `meta/episodes.jsonl` (any extra per-episode fields in that file are kept as columns)

Per-episode `meta/` and `stats/` columns are hidden by default; opt in with `include_meta=True` / `include_stats=True`. On v2.1, stats are joined from `meta/episodes_stats.jsonl`.

```python
import daft
from daft.datasets.lerobot import load_episode_frames, read_episodes

repo = "hf://datasets/your-org/your-robot-dataset"
ep = read_episodes(repo)
long = ep.where(daft.col("length") > 100)
frames = load_episode_frames(long, repo)
```

[`load_episode_frames`](../api/datasets.md#daft.datasets.lerobot.load_episode_frames) reads the per-frame Parquet under `data/**` and joins it to the provided episode rows on `episode_index`, producing one row per frame. Filter the episode DataFrame first so only the surviving episodes contribute frames.

## Tasks

[`read_tasks`](../api/datasets.md#daft.datasets.lerobot.read_tasks) loads task metadata, preferring `meta/tasks.parquet` and falling back to `meta/tasks.jsonl` (the v2 default).

## Write a LeRobot v3 dataset

Use [`DataFrame.write_lerobot`](../api/io.md#daft.dataframe.DataFrame.write_lerobot) to write a frame-level DataFrame as a LeRobot v3 dataset. Each row is one frame. The input must have zero-based, contiguous `episode_index` and `frame_index` columns and a string `task` column. Daft generates `timestamp` when it is absent, as well as the global `index`, `task_index`, episode metadata, task table, and feature statistics.

```python
import daft

df = daft.from_pydict(
    {
        "episode_index": [0, 0, 1],
        "frame_index": [0, 1, 0],
        "task": ["pick", "pick", "place"],
        "action": [0.1, 0.2, 0.3],
    }
)

result = df.write_lerobot("/tmp/robot-runs", fps=30)
```

Scalar Arrow columns are inferred automatically. Vector and tensor columns must use fixed-size-list Arrow types, or provide explicit LeRobot feature descriptors with the `features` argument. The initial writer supports tabular features; image and video encoding are not yet supported.

The writer runs with both Daft's native and Ray runners. For a multi-node Ray cluster, use an object-store URI or a local filesystem mounted at the same path on every worker.

## Video frames

With `load_video_frames`, [`read`](../api/datasets.md#daft.datasets.lerobot.read) decodes each frame from its MP4 by **timestamp**: Daft combines the episode's `from_timestamp` offset within the file with the frame's episode-local `timestamp`, and matches the closest decoded frame within half a frame period.

- **v3:** a shard packs many episodes back to back, so `from_timestamp` is where that episode starts inside the shard.
- **v2:** each episode has its own MP4, so `from_timestamp` is `0`.

Decoding requires PyAV and Pillow (`pip install av pillow`).

## Layout cheat sheet

| | v2.0 / v2.1 | v3.0 |
|---|---|---|
| Episode metadata | `meta/episodes.jsonl` | `meta/episodes/**/*.parquet` |
| Frame data | `data/chunk-XXX/episode_YYYYYY.parquet` | `data/chunk-XXX/file-YYY.parquet` (many episodes) |
| Video | one MP4 per episode per camera | shared MP4 shards + `from_timestamp` |
| Tasks | `meta/tasks.jsonl` | `meta/tasks.parquet` (jsonl fallback) |

## API reference

::: daft.datasets.lerobot.read
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.read_episodes
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.load_episode_frames
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.read_tasks
    options:
        filters: ["!^_"]
        heading_level: 3
