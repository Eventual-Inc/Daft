# How to use EgoDex with Daft

[EgoDex](https://github.com/apple/ml-egodex) is a large-scale egocentric dexterous manipulation dataset from Apple, with 829 hours of Apple Vision Pro video across 194 tabletop tasks. Each episode pairs an egocentric 1080p/30fps video with frame-aligned 3D pose annotations: SE(3) transforms for the camera and 68 upper-body and hand joints, per-joint tracking confidences, the camera intrinsics, and natural language task descriptions.

Daft provides a simple way to explore a downloaded EgoDex release as a lazy, episode-level DataFrame with the annotation HDF5 files attached as [`daft.Hdf5File`](../modalities/tensors.md#hdf5-files) and the egocentric videos as [`daft.VideoFile`](../modalities/videos.md) columns.

## Prerequisites: downloading EgoDex

The EgoDex dataset is released under CC-BY-NC-ND terms, which prohibit redistribution — so Daft cannot host a copy or default to a public bucket. Download the archives directly from Apple's CDN and extract them locally (or to an object store you control):

```bash
# Test split (~16 GB) — a good starting point
curl -O https://ml-site.cdn-apple.com/datasets/egodex/test.zip

# Training splits (5 parts, ~300 GB each)
curl -O https://ml-site.cdn-apple.com/datasets/egodex/part1.zip
# ... part2.zip through part5.zip

# Additional data (~200 GB)
curl -O https://ml-site.cdn-apple.com/datasets/egodex/extra.zip
```

!!! note
    See the [official EgoDex repository](https://github.com/apple/ml-egodex) for the full download list and license terms. The license also prohibits distributing derivative datasets, such as converted copies — with Daft you can query the raw release in place, so no conversion or re-hosting is required.

## Quickstart

Point [`daft.datasets.egodex.raw()`](../api/datasets.md#daft.datasets.egodex.raw) at your extracted root (the root, a single part, or a single task directory all work):

```python
import daft

df = daft.datasets.egodex.raw("/data/egodex")
df.select("task", "episode_id", "metadata", "video").show(3)
```

**Each row corresponds to one EgoDex episode.** Episodes are discovered by globbing `*.hdf5` files; `task` and `episode_id` are derived from the file path, and the episode-level annotation attributes are read into a typed `metadata` struct.

For fast iteration, filter by path-derived fields directly in `raw()` so Daft can narrow the catalog before reading HDF5 attributes:

```python
towels = daft.datasets.egodex.raw(
    "/data/egodex",
    tasks="fold_towel",
    episode_ids=[0, 1, 2],
)
```

## API scope

The first-class `daft.datasets.egodex` API is intentionally centered on stable dataset access patterns:

- `raw()` catalogs downloaded episodes and attaches lazy `Hdf5File` / `VideoFile` columns.
- `trajectory()` reads selected known HDF5 datasets into typed tensor columns.
- `camera_frames()` decodes frame-aligned egocentric video when pixels are needed.
- `JOINTS`, `TRANSFORM_JOINTS`, `TRAJECTORY_FIELDS`, and `DEFAULT_TRAJECTORY_FIELDS` expose the stable EgoDex field catalogs.

Higher-level curation logic such as hand-pose feature engineering, motion trimming, tracking-quality scoring, visual overlays, embeddings, reward models, and scenario queries is kept in examples. Those pieces are built out of normal Daft expressions and UDFs, so teams can copy, tune, and version them without locking task-specific heuristics into the dataset reader API.

## Episode layout

EgoDex stores episodes as sibling file pairs inside one directory per task:

```
<root>/
|---- part1/
|       |---- fold_towel/
|       |       |---- 0.hdf5    # Pose annotations + episode metadata attributes
|       |       |---- 0.mp4     # Egocentric 1080p 30fps video
|       |       |---- 1.hdf5
|       |       |---- 1.mp4
|       |---- stack_cups/
|               |---- ...
|---- test/
        |---- ...
```

## Data schema

`raw()` returns one row per episode:

| Column        | Type      | Description                                                        |
| ------------- | --------- | ------------------------------------------------------------------ |
| `task`        | String    | Task name, derived from the episode's parent directory             |
| `episode_id`  | Int64     | Episode id, derived from the file stem                             |
| `metadata`    | Struct    | Episode annotation attributes (see below)                          |
| `trajectory`  | File      | Lazy reference to the episode's pose annotation HDF5 file          |
| `video`       | VideoFile | Lazy reference to the egocentric MP4, or null if missing           |

The `metadata` struct carries the HDF5 root attributes, including the natural language `llm_description` / `llm_description2`, the extracted `llm_verbs` and `llm_objects`, and annotation provenance fields such as `annotated` and `annotator_version`. Attributes absent from older files surface as nulls.

## Reading pose trajectories

Every episode file contains 138 float32 datasets, cataloged in `daft.datasets.egodex.TRAJECTORY_FIELDS`:

- `camera/intrinsic`: the `(3, 3)` camera intrinsic matrix
- `transforms/<joint>`: `(N, 4, 4)` world-frame SE(3) poses per frame, for the camera and each of the 68 joints in `daft.datasets.egodex.JOINTS`
- `confidences/<joint>`: `(N,)` per-frame tracking confidence per joint

[`trajectory()`](../api/datasets.md#daft.datasets.egodex.trajectory) reads a selected set of fields as tensor columns, one row per episode. The default field set, exposed as `daft.datasets.egodex.DEFAULT_TRAJECTORY_FIELDS`, covers the camera intrinsics and pose plus both hand poses and confidences:

```python
from daft.datasets.egodex import raw, trajectory

episodes = raw("/data/egodex").where(daft.col("task") == "fold_towel").limit(4)
traj = trajectory(episodes, fields=["transforms/leftHand", "transforms/rightHand"])
traj.select("task", "episode_id", "transforms/rightHand").collect()
```

Filter and `limit` the episode DataFrame *before* calling `trajectory()` so only the episodes you need are read.

## Decoding video frames

[`camera_frames()`](../api/datasets.md#daft.datasets.egodex.camera_frames) appends a `video_frames` column of decoded frames. Videos are 1080p at 30fps and frame-aligned with the pose annotations, so use `sample_interval_seconds` and `width`/`height` to keep decode volume manageable:

```python
from daft.datasets.egodex import camera_frames, raw

episodes = raw("/data/egodex").limit(1)
frames = camera_frames(episodes, width=224, height=224, sample_interval_seconds=1.0)
frames.select("task", "episode_id", "video_frames").collect()
```

## Putting it together

Find episodes by language description, then load poses and subsampled frames for just those episodes:

```python
import daft
from daft.datasets.egodex import camera_frames, raw, trajectory

matches = raw("/data/egodex", tasks="fold_towel").where(
    daft.col("metadata")["llm_description"].contains("towel")
).limit(8)

df = camera_frames(
    trajectory(matches, fields=["transforms/leftHand", "transforms/rightHand"]),
    width=224,
    height=224,
    sample_interval_seconds=1.0,
)
df.collect()
```

## Next steps

- Follow the [EgoDex pose curation tutorial](../examples/egodex-pose-features.md) to compute hand-motion features, trim idle frames, score tracking quality, and mine grasping episodes — or run it directly via [`examples/egodex_pose_features.py`](https://github.com/Eventual-Inc/Daft/blob/main/examples/egodex_pose_features.py).
- See the [Videos modality guide](../modalities/videos.md) for decoding frames with [`video_frames`][daft.functions.video_frames] and working with [`daft.VideoFile`](../api/datatypes/file_types.md).
- See the [Files modality guide](../modalities/files.md) for reading annotation HDF5 files with [`daft.File`](../api/datatypes/file_types.md).
- Visit the [official EgoDex repository](https://github.com/apple/ml-egodex) for the dataset paper, download links, and license terms.
- See the [EgoDex Dataset API reference](../api/datasets.md#egodex) for complete parameter documentation.
