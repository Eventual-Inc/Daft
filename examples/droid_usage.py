from __future__ import annotations

import daft

# Load a sample of the raw DROID data
df = daft.datasets.droid.raw().limit(100)


# Filter the data to only include scenes of type "Home kitchen"
df = daft.datasets.droid.filter_scenes(df, "Home kitchen")


# Load the trajectory data into tensor columns
# df = daft.datasets.droid.trajectory(df, fields=["action/joint_position", "action/gripper_position"])

df.select(
    "uuid",
    "scene_id",
    "scene_classification",
    "current_task",
    "success",
).show(3)
