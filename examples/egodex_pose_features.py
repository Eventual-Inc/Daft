"""Curate EgoDex episodes with pose features computed in Daft.

EgoDex episodes carry frame-aligned SE(3) poses for the camera and 68 joints.
This example reads a downloaded EgoDex release in place with
``daft.datasets.egodex`` and derives episode-curation signals directly from the
pose tensors — no dataset conversion required:

- per-frame wrist positions, hand speed, and thumb-index pinch apertures
- motion trimming: the active segment of each episode, minus idle lead-in/out
- tracking-quality scores from the per-joint confidences
- grasp detection from pinch aperture, for scenario filtering

The curated per-episode feature table is written to Parquet at the end, ready
for supervised-learning dataset prep or further querying.

Requires a local EgoDex download (the CC-BY-NC-ND license prohibits
redistribution; see https://github.com/apple/ml-egodex):

    pip install "daft[hdf5]" numpy
    python examples/egodex_pose_features.py /data/egodex [features.parquet]
"""

from __future__ import annotations

import sys

import numpy as np

import daft
from daft import DataType, col
from daft.datasets import egodex

FPS = 30.0  # EgoDex videos and pose annotations are captured at 30fps.
SPEED_THRESHOLD_M_S = 0.05  # Below this wrist speed, a frame counts as idle.
PINCH_THRESHOLD_M = 0.03  # Below this thumb-index distance, a frame counts as a grasp.

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


def main(egodex_root: str, output_path: str) -> None:
    # 1. Catalog the episodes and read the pose fields we need — nothing else.
    episodes = egodex.raw(egodex_root)
    trajectories = egodex.trajectory(episodes, fields=POSE_FIELDS)

    # 2. Compute per-frame features and per-episode summaries in one pass.
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

    # 3. Curate: drop poorly tracked episodes and episodes with no motion,
    #    and add the trimmed duration implied by the active segment.
    curated = features.where((col("mean_confidence") > 0.5) & col("active_start").not_null()).with_column(
        "trimmed_duration_s",
        (col("active_end") - col("active_start") + 1) / FPS,
    )

    # 4. Query the curated table, e.g. for grasp-heavy episodes to train on.
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

    # 5. Persist the feature table for training-set assembly and later queries.
    curated.write_parquet(output_path)
    print(f"Wrote curated pose features to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: {sys.argv[0]} <egodex_root> [output.parquet]")
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "egodex_pose_features.parquet")
