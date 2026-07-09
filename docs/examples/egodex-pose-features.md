# Curating EgoDex Episodes with Pose Features

Every [EgoDex](../datasets/egodex.md) episode carries frame-aligned SE(3) poses for the egocentric camera and 68 upper-body and hand joints, plus per-joint tracking confidences. That's enough signal to run the episode operations that dataset curation usually requires — **motion trimming**, **tracking-quality scoring**, and **grasp detection** — directly on the raw release, with no conversion pipeline and no separate tooling. Filter to the episodes you want, compute features with one Daft UDF, and write a curated table.

This tutorial mirrors the runnable script at [`examples/egodex_pose_features.py`](https://github.com/Eventual-Inc/Daft/blob/main/examples/egodex_pose_features.py).

## Setup

```bash
pip install "daft[hdf5]" numpy
```

You'll need a local EgoDex download — the CC-BY-NC-ND license prohibits redistribution, so there is no hosted copy. See the [EgoDex dataset guide](../datasets/egodex.md#prerequisites-downloading-egodex) for download instructions.

## 1. Catalog the episodes

[`daft.datasets.egodex.raw()`](../api/datasets.md#daft.datasets.egodex.raw) lazily catalogs one row per episode, with the annotation attributes in a typed `metadata` struct. You can filter on language descriptions before reading any pose data:

```python
import daft
from daft import col
from daft.datasets import egodex

episodes = egodex.raw("/data/egodex")

# Optional: narrow the catalog by task or language description first.
episodes = episodes.where(col("metadata")["llm_description"].contains("towel"))
```

## 2. Read just the pose fields you need

Each episode file holds 138 datasets; [`trajectory()`](../api/datasets.md#daft.datasets.egodex.trajectory) reads only the fields you request. For hand-centric curation we need the wrists, the thumb and index fingertips, and the hand confidences:

```python
POSE_FIELDS = (
    "transforms/leftHand",
    "transforms/rightHand",
    "transforms/leftThumbTip",
    "transforms/leftIndexFingerTip",
    "transforms/rightThumbTip",
    "transforms/rightIndexFingerTip",
    "confidences/leftHand",
    "confidences/rightHand",
)

trajectories = egodex.trajectory(episodes, fields=POSE_FIELDS)
```

Each field arrives as a tensor column — transforms are `(N, 4, 4)` pose stacks and confidences are `(N,)` tracks, where `N` is the episode's frame count at 30fps.

## 3. Compute pose features in one pass

A single [`@daft.func`](../api/udf.md) turns the pose tensors into per-frame feature tracks and per-episode summary statistics. With `unnest=True`, each struct field becomes its own column:

```python
import numpy as np
from daft import DataType

FPS = 30.0  # EgoDex videos and pose annotations are captured at 30fps.
SPEED_THRESHOLD_M_S = 0.05  # Below this wrist speed, a frame counts as idle.
PINCH_THRESHOLD_M = 0.03  # Below this thumb-index distance, a frame counts as a grasp.

_POSE_FEATURES_DTYPE = DataType.struct(
    {
        "num_frames": DataType.int64(),
        "duration_s": DataType.float32(),
        "mean_confidence": DataType.float32(),
        "left_wrist_pos": DataType.tensor(DataType.float32()),
        "right_wrist_pos": DataType.tensor(DataType.float32()),
        "right_hand_speed": DataType.tensor(DataType.float32()),
        "hands_distance": DataType.tensor(DataType.float32()),
        "left_pinch_aperture": DataType.tensor(DataType.float32()),
        "right_pinch_aperture": DataType.tensor(DataType.float32()),
        "active_start": DataType.int64(),
        "active_end": DataType.int64(),
        "active_fraction": DataType.float32(),
        "grasp_frames": DataType.int64(),
    }
)


def _translations(transforms: np.ndarray) -> np.ndarray:
    """Extract the (N, 3) translation track from an (N, 4, 4) SE(3) pose tensor."""
    return np.asarray(transforms, dtype=np.float32)[:, :3, 3]


@daft.func(return_dtype=_POSE_FEATURES_DTYPE, unnest=True)
def pose_features(
    left_hand: np.ndarray,
    right_hand: np.ndarray,
    left_thumb_tip: np.ndarray,
    left_index_tip: np.ndarray,
    right_thumb_tip: np.ndarray,
    right_index_tip: np.ndarray,
    left_confidence: np.ndarray,
    right_confidence: np.ndarray,
) -> dict[str, object]:
    left_wrist = _translations(left_hand)
    right_wrist = _translations(right_hand)
    num_frames = len(right_wrist)

    # Wrist speed in m/s from frame-to-frame displacement.
    speed = np.zeros(num_frames, dtype=np.float32)
    if num_frames > 1:
        speed[1:] = np.linalg.norm(np.diff(right_wrist, axis=0), axis=1) * FPS

    # Grasp proxy: distance between thumb tip and index fingertip.
    left_pinch = np.linalg.norm(_translations(left_thumb_tip) - _translations(left_index_tip), axis=1)
    right_pinch = np.linalg.norm(_translations(right_thumb_tip) - _translations(right_index_tip), axis=1)

    # Motion trimming: the span between the first and last non-idle frame.
    active = speed > SPEED_THRESHOLD_M_S
    if active.any():
        active_start = int(np.argmax(active))
        active_end = int(num_frames - 1 - np.argmax(active[::-1]))
    else:
        active_start = None
        active_end = None

    return {
        "num_frames": num_frames,
        "duration_s": num_frames / FPS,
        "mean_confidence": float(np.mean([left_confidence, right_confidence])),
        "left_wrist_pos": left_wrist,
        "right_wrist_pos": right_wrist,
        "right_hand_speed": speed,
        "hands_distance": np.linalg.norm(left_wrist - right_wrist, axis=1).astype(np.float32),
        "left_pinch_aperture": left_pinch.astype(np.float32),
        "right_pinch_aperture": right_pinch.astype(np.float32),
        "active_start": active_start,
        "active_end": active_end,
        "active_fraction": float(active.mean()),
        "grasp_frames": int((right_pinch < PINCH_THRESHOLD_M).sum()),
    }


features = trajectories.select(
    "task",
    "episode_id",
    col("metadata")["llm_description"].alias("description"),
    pose_features(
        col("transforms/leftHand"),
        col("transforms/rightHand"),
        col("transforms/leftThumbTip"),
        col("transforms/leftIndexFingerTip"),
        col("transforms/rightThumbTip"),
        col("transforms/rightIndexFingerTip"),
        col("confidences/leftHand"),
        col("confidences/rightHand"),
    ),
)
```

Because everything is lazy, the HDF5 files are read exactly once, when the query below executes — and only the eight requested datasets per file.

## 4. Trim, score, and filter

The feature columns are now ordinary Daft columns, so curation is plain DataFrame code. Drop poorly tracked episodes, drop episodes with no motion, and compute the trimmed duration implied by each episode's active segment:

```python
curated = features.where(
    (col("mean_confidence") > 0.5) & col("active_start").not_null()
).with_column(
    "trimmed_duration_s",
    (col("active_end") - col("active_start") + 1) / FPS,
)
```

`active_start` and `active_end` are the motion-trim bounds: slice frame-aligned data (poses or decoded video frames) to `[active_start, active_end]` to cut idle lead-in and lead-out from every episode.

## 5. Query for training scenarios

Scenario mining is a filter. For example, rank episodes by how much grasping they contain:

```python
grasping = curated.where(col("grasp_frames") > 0).sort("grasp_frames", desc=True)
grasping.select(
    "task",
    "episode_id",
    "description",
    "grasp_frames",
    "active_fraction",
    "trimmed_duration_s",
    "mean_confidence",
).show(8)
```

## 6. Write the curated table

Persist the features for training-set assembly and later queries:

```python
curated.write_parquet("egodex_pose_features.parquet")
```

The Parquet table keys on `task` and `episode_id`, so you can join it back to [`camera_frames()`](../api/datasets.md#daft.datasets.egodex.camera_frames) output whenever a downstream step needs pixels — for example, decoding only the trimmed active segment of the grasping episodes you selected above.

## Next steps

- See the [EgoDex dataset guide](../datasets/egodex.md) for the full reading API, including video decoding.
- See [UDF Patterns](udf-patterns.md) for more ways to structure user-defined functions like `pose_features`.
- See the [EgoDex API reference](../api/datasets.md#egodex) for complete parameter documentation.
