# /// script
# description = "Compute EgoDex-paper keypoint error metrics over H-RDT predictions"
# requires-python = ">=3.12, <3.13"
# dependencies = [
#     "daft>=0.7.15",
#     "numpy",
# ]
# ///
"""Score the predictions written by predict_hrdt.py.

Kept separate from the prediction pipeline on purpose: predictions cost
minutes of model time, metrics cost milliseconds. Splitting them means you can
re-score (or add new metrics) without re-running the model, and this script's
environment needs no torch at all.

Reads `out/egodex_hrdt_predictions/`, writes per-frame metrics to
`out/egodex_hrdt_metrics/`, and prints overall + per-episode summaries.

    uv run compute_metrics.py
"""

import os

import numpy as np

import daft
from daft import DataType, col

PREDICTIONS_DIR = os.path.join(os.path.dirname(__file__), "out", "egodex_hrdt_predictions")
METRICS_DIR = os.path.join(os.path.dirname(__file__), "out", "egodex_hrdt_metrics")


@daft.func(return_dtype=DataType.float64())
def avg_keypoint_distance_m(predicted_action: list[float], ground_truth_action: list[float]) -> float:
    """EgoDex paper's trajectory-prediction metric (arXiv:2505.11709, Sec 4.3).

    "Euclidean distance between predicted 3D keypoint positions and their
    ground truth 3D counterparts, averaged over ... each of the 12 keypoints"
    (both wrists + all 10 fingertips), in meters. The 2x6 wrist-rotation dims
    are excluded: they are unitless rotation-matrix columns.

    Ours is the metric at a 1-step horizon with K=1: the paper averages over
    every timestep of the predicted chunk and scores the best of K sampled
    trajectories, while we keep only the chunk's first step and sample once.
    """
    predicted = np.asarray(predicted_action, dtype=np.float64)
    ground_truth = np.asarray(ground_truth_action, dtype=np.float64)
    distances = []
    for base in (0, 24):  # left hand dims 0-23, right hand dims 24-47
        keypoint_starts = [base] + [base + 9 + 3 * i for i in range(5)]  # wrist, then 5 fingertips
        for start in keypoint_starts:
            distances.append(np.linalg.norm(predicted[start : start + 3] - ground_truth[start : start + 3]))
    return float(np.mean(distances))


if __name__ == "__main__":
    metrics = (
        daft.read_parquet(f"{PREDICTIONS_DIR}/**")
        .with_column(
            "avg_keypoint_distance_m",
            avg_keypoint_distance_m(col("predicted_action"), col("ground_truth_action")),
        )
        # Keep only identifiers + the metric; the predictions stay in their own files.
        .select("episode_index", "frame_index", "timestamp", "task_index", "avg_keypoint_distance_m")
    )

    metrics.write_parquet(METRICS_DIR)
    print(f"Wrote per-frame metrics to {METRICS_DIR}\n")

    results = daft.read_parquet(f"{METRICS_DIR}/**")

    print("Per-episode:")
    results.groupby("episode_index").agg(
        col("avg_keypoint_distance_m").mean().alias("mean_m"),
        col("avg_keypoint_distance_m").max().alias("worst_frame_m"),
        col("avg_keypoint_distance_m").count().alias("frames"),
    ).sort("episode_index").show()

    print("Overall:")
    results.select(
        col("avg_keypoint_distance_m").mean().alias("dataset_avg_keypoint_distance_m"),
    ).show()
