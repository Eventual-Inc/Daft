# LeRobot v3 datasets with Daft

[LeRobot Dataset v3.0](https://huggingface.co/docs/lerobot/lerobot-dataset-v3) stores robot learning data as chunked Parquet (`meta/episodes`, `data/`) and per-camera MP4 shards under `videos/`. Daft exposes this layout under [`daft.datasets.lerobot`](../api/datasets.md) so you can stay at **episode granularity** for filtering, then expand to **frames** only for the episodes you need.

!!! warning "Beta"

    This API is new and may evolve as we add optimizations (for example deeper integration with Parquet predicate pushdown).

## Frame-level reads

Use [`daft.datasets.lerobot.read`](../api/datasets.md#daft.datasets.lerobot.read) for the common case: a lazy DataFrame with one row per frame, episode metadata broadcast onto each frame. Pass `load_video_frames=True` (or a camera key / list of keys) to also decode each row's camera image from the MP4 shards.

```python
import daft
from daft.datasets import lerobot

df = lerobot.read("your-org/your-robot-dataset", load_video_frames=True)
```

`dataset_uri` can be:

- A local directory that contains `meta/`, `data/`, etc.
- An `hf://datasets/org/name` URI (Hub layout matches the on-disk v3 tree)
- A bare `org/name` string, which is interpreted as `hf://datasets/org/name`

## Episode metadata

Use [`daft.datasets.lerobot.read_episodes`](../api/datasets.md#daft.datasets.lerobot.read_episodes) to scan `meta/episodes/**/*.parquet` (one row per episode). Per-episode `meta/` and `stats/` columns are hidden by default; opt in with `include_meta=True` / `include_stats=True`.

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

[`read_tasks`](../api/datasets.md#daft.datasets.lerobot.read_tasks) loads task metadata, preferring `meta/tasks.parquet` and falling back to legacy `meta/tasks.jsonl`.

## Video frames

With `load_video_frames`, [`read`](../api/datasets.md#daft.datasets.lerobot.read) decodes each frame from its MP4 shard by **timestamp**: a shard packs many episodes back to back, so Daft combines the episode's `from_timestamp` offset within the shard with the frame's episode-local `timestamp`, and matches the closest decoded frame within half a frame period. Decoding requires PyAV and Pillow (`pip install av pillow`).

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
