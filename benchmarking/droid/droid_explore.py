"""Break down trajectory load time by phase."""

from __future__ import annotations

import time

import daft
from daft.datasets.droid import raw, trajectory


def timed(label: str, fn):
    t0 = time.perf_counter()
    result = fn()
    print(f"{label:45s} {time.perf_counter() - t0:7.2f}s")
    return result


def main() -> None:
    episodes = timed(
        "1. raw().where(success).limit(5) [lazy]",
        lambda: raw().where(daft.col("success")).limit(5),
    )

    traj = timed(
        "2. trajectory(..., 3 fields) [lazy plan]",
        lambda: trajectory(episodes, fields=["joint_position", "gripper_position", "robot_joint_positions"]),
    )

    timed("3. traj.collect() [materialize]", lambda: traj.collect())

    # Direct Hdf5File read breakdown (bypasses Daft UDF scheduling)
    episode_dir = (
        raw()
        .where(daft.col("success"))
        .limit(1)
        .collect()
        .to_pydict()["episode_dir"][0]
    )
    f = daft.Hdf5File(f"{episode_dir}/trajectory.h5")

    timed("4a. Hdf5File.read(field #1)", lambda: f.read("action/joint_position"))
    timed("4b. Hdf5File.read(field #2)", lambda: f.read("action/gripper_position"))
    timed("4c. Hdf5File.read(field #3)", lambda: f.read("observation/robot_state/joint_positions"))


if __name__ == "__main__":
    
    df = daft.datasets.droid.raw()

    df.show()
