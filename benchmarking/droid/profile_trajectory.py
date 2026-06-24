"""Profile DROID trajectory loading for speedscope."""

from __future__ import annotations

import daft
from daft.datasets.droid import raw, trajectory

NUM_EPISODES = 5
FIELDS = ["joint_position", "gripper_position", "robot_joint_positions"]


def main() -> None:
    episodes = raw().where(daft.col("success")).limit(NUM_EPISODES)
    traj = trajectory(episodes, fields=FIELDS)
    # Materialize trajectory tensors — this is the perf-critical path.
    traj.collect()


if __name__ == "__main__":
    main()
