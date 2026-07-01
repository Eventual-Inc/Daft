import daft

# Load a sample of the raw DROID data
df = daft.datasets.droid.raw()


# Read and filter the scene classification table
scene_classifications = daft.datasets.droid.scenes().where(
    daft.col("scene_classification") == "Home kitchen"
)

# Join scene labels onto the episode data
df = df.join(scene_classifications, on="scene_id", how="inner")


# Load the trajectory data into tensor columns
# df = daft.datasets.droid.trajectory(df, fields=["action/joint_position", "action/gripper_position"])

df.select(
    "uuid",
    "scene_id",
    "scene_classification",
    "current_task",
    "success",
).explain(show_all=True)
