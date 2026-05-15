# LeRobot v3 datasets with Daft

[LeRobot Dataset v3.0](https://huggingface.co/docs/lerobot/lerobot-dataset-v3) stores robot learning data as chunked Parquet (`meta/episodes`, `data/`) and per-camera MP4 shards under `videos/`. Daft exposes this layout under [`daft.datasets.lerobot`](../../api/datasets.md) so you can stay at **episode granularity** for filtering, then expand to **frames** only for the episodes you need.

!!! warning "Beta"

    This API is new and may evolve as we add optimizations (for example deeper integration with Parquet predicate pushdown).

## Episode metadata

Use [`daft.datasets.lerobot.episodes`](../api/datasets.md#daft.datasets.lerobot.episodes) to scan `meta/episodes/**/*.parquet`. The dataframe includes a `lerobot_dataset_root` column used by frame expansion helpers.

`dataset_uri` can be:

- A local directory that contains `meta/`, `data/`, etc.
- An `hf://datasets/org/name` URI (Hub layout matches the on-disk v3 tree)
- A bare `org/name` string, which is interpreted as `hf://datasets/org/name`

```python
import daft
from daft.datasets.lerobot import episodes, load_episode_frames

repo = "hf://datasets/your-org/your-robot-dataset"
ep = episodes(repo)
long = ep.where(daft.col("length") > 100)
frames = load_episode_frames(long)
```

[`load_episode_frames`](../api/datasets.md#daft.datasets.lerobot.load_episode_frames) reads only the `data/chunk-*/file-*.parquet` shards referenced by the (possibly filtered) episode rows, then keeps rows whose `episode_index` is still present. It runs a small eager step to list **distinct shard paths**; the heavy Parquet scan stays lazy afterward. Pass `columns=[...]` to project frame fields with :meth:`daft.DataFrame.select` semantics.

## Dataset-level JSON and tasks

Bounded metadata files are exposed as small helpers:

- [`read_info`](../api/datasets.md#daft.datasets.lerobot.read_info) → `meta/info.json`
- [`read_stats`](../api/datasets.md#daft.datasets.lerobot.read_stats) → `meta/stats.json`
- [`read_tasks`](../api/datasets.md#daft.datasets.lerobot.read_tasks) → prefers `meta/tasks.parquet`, falls back to `meta/tasks.jsonl`

For Hub datasets, JSON is fetched over HTTPS from `resolve/main` (public files only unless your environment supplies credentials via your HTTP stack).

## Video frames

Daft already decodes video via [`read_video_frames`](../api/io.md#daft.read_video_frames) and [`daft.VideoFile`](../modalities/videos.md). Episode metadata includes per-camera chunk/file indices and timestamp offsets (`videos/{camera}/...` fields in LeRobot v3). Build the MP4 path from those columns (plus `lerobot_dataset_root`), then call `read_video_frames` with `sample_interval_seconds` or decode with `daft.functions.video_frames` for precise timestamps.

## API reference

::: daft.datasets.lerobot.episodes
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

::: daft.datasets.lerobot.read_info
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.datasets.lerobot.read_stats
    options:
        filters: ["!^_"]
        heading_level: 3
