"""Profile the Python-side HDF5 read path (what trajectory.collect executes per field)."""

from __future__ import annotations

import daft
from daft.datasets.droid import raw
from daft.functions.hdf5 import hdf5_read_impl


def main() -> None:
    episode_dir = (
        raw()
        .where(daft.col("success"))
        .limit(1)
        .collect()
        .to_pydict()["episode_dir"][0]
    )
    file = daft.Hdf5File(f"{episode_dir}/trajectory.h5")

    paths = [
        "action/joint_position",
        "action/gripper_position",
        "observation/robot_state/joint_positions",
    ]
    for path in paths:
        hdf5_read_impl(file, path)


if __name__ == "__main__":
    main()
