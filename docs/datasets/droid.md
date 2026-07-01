# How to use DROID with Daft

<img src="https://droid-dataset.github.io/droid/assets/index/droid_teaser.jpg" alt="Droid Dataset Image" style="max-width: 100%; height: auto;" />

[DROID](https://droid-dataset.github.io/) (Distributed Robot Interaction Dataset) is one of the most popular large-scale, in-the-wild robot manipulation datasets, with 76,000 demonstration trajectories and 350 hours of interaction data. It was collected across 564 scenes and 86 tasks using the Franka Panda robot platform, and includes synchronized RGB camera streams, camera calibration, and natural language task descriptions.

Daft provides a simple way to explore the raw DROID release as a lazy, episode-level DataFrame with metadata, trajectory files as [`daft.Hdf5File`](../modalities/tensors.md#hdf5-files), and camera videos attached as [`daft.VideoFile`](../modalities/videos.md) columns.

## Quickstart

The simplest way to get started is to load a small sample of data from the public source:

```python
import daft

# Load a sample of the raw DROID data
daft.datasets.droid.raw().show(3)
```

```
╭────────────────────────────────┬──────────┬────────────┬────────────────────────┬────────────┬───────────────────┬────────────────────────────────┬─────────┬────────────────────────────────┬─────────────┬──────────┬─────────────────┬──────────────────────┬──────────────┬────────────────────────────────┬──────────────────┬────────────────────────────────┬────────────────────────────────┬─────────────────┬────────────────────────────────┬────────────────────────────────┬─────────────────┬────────────────────────────────┬────────────────────────────────╮
│ uuid                           ┆ lab      ┆ date       ┆ timestamp              ┆ scene_id   ┆ trajectory_length ┆ current_task                   ┆ success ┆ episode_dir                    ┆ user        ┆ user_id  ┆ building        ┆ robot_serial         ┆ r2d2_version ┆ trajectory                     ┆ wrist_cam_serial ┆ wrist_cam_extrinsics           ┆ wrist_cam_video                ┆ ext1_cam_serial ┆ ext1_cam_extrinsics            ┆ ext1_cam_video                 ┆ ext2_cam_serial ┆ ext2_cam_extrinsics            ┆ ext2_cam_video                 │
│ ---                            ┆ ---      ┆ ---        ┆ ---                    ┆ ---        ┆ ---               ┆ ---                            ┆ ---     ┆ ---                            ┆ ---         ┆ ---      ┆ ---             ┆ ---                  ┆ ---          ┆ ---                            ┆ ---              ┆ ---                            ┆ ---                            ┆ ---             ┆ ---                            ┆ ---                            ┆ ---             ┆ ---                            ┆ ---                            │
│ String                         ┆ String   ┆ Date       ┆ String                 ┆ Int64      ┆ Int64             ┆ String                         ┆ Bool    ┆ String                         ┆ String      ┆ String   ┆ String          ┆ String               ┆ String       ┆ File[Hdf5]                     ┆ String           ┆ List[Float64]                  ┆ File[Video]                    ┆ String          ┆ List[Float64]                  ┆ File[Video]                    ┆ String          ┆ List[Float64]                  ┆ File[Video]                    │
╞════════════════════════════════╪══════════╪════════════╪════════════════════════╪════════════╪═══════════════════╪════════════════════════════════╪═════════╪════════════════════════════════╪═════════════╪══════════╪═════════════════╪══════════════════════╪══════════════╪════════════════════════════════╪══════════════════╪════════════════════════════════╪════════════════════════════════╪═════════════════╪════════════════════════════════╪════════════════════════════════╪═════════════════╪════════════════════════════════╪════════════════════════════════╡
│ GuptaLab+553d1bd5+2023-07-09-… ┆ GuptaLab ┆ 2023-07-09 ┆ 2023-07-09-19h-01m-24s ┆ 500180237  ┆ 946               ┆ Do any two tasks consecutivel… ┆ true    ┆ gs://gresearch/robotics/droid… ┆ Mohan Kumar ┆ 553d1bd5 ┆ Smith Hall 121  ┆ panda-295341-1325237 ┆ 1.3          ┆ Hdf5(path: gs://gresearch/rob… ┆ 16291792         ┆ [0.1992642342748634, -0.07232… ┆ Video(path: gs://gresearch/ro… ┆ 22246076        ┆ [-0.17238707747361265, 0.8769… ┆ Video(path: gs://gresearch/ro… ┆ 26638268        ┆ [-0.09266930443312492, -0.672… ┆ Video(path: gs://gresearch/ro… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ GuptaLab+553d1bd5+2023-07-09-… ┆ GuptaLab ┆ 2023-07-09 ┆ 2023-07-09-18h-59m-44s ┆ 500180237  ┆ 668               ┆ Move object into or out of co… ┆ true    ┆ gs://gresearch/robotics/droid… ┆ Mohan Kumar ┆ 553d1bd5 ┆ Smith Hall 121  ┆ panda-295341-1325237 ┆ 1.3          ┆ Hdf5(path: gs://gresearch/rob… ┆ 16291792         ┆ [0.38476760593776255, -0.0039… ┆ Video(path: gs://gresearch/ro… ┆ 22246076        ┆ [-0.17238707747361265, 0.8769… ┆ Video(path: gs://gresearch/ro… ┆ 26638268        ┆ [-0.09266930443312492, -0.672… ┆ Video(path: gs://gresearch/ro… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+284fa481+2023-09-01-10h-4… ┆ RAD      ┆ 2023-09-01 ┆ 2023-09-01-10h-43m-47s ┆ 4823049285 ┆ 477               ┆ Move object into or out of co… ┆ true    ┆ gs://gresearch/robotics/droid… ┆ Jack Rome   ┆ 284fa481 ┆ Bayes - RAD Lab ┆ panda-295341-1325422 ┆ 1.3          ┆ Hdf5(path: gs://gresearch/rob… ┆ 15102076         ┆ [0.2773416620870791, -0.21031… ┆ Video(path: gs://gresearch/ro… ┆ 32907025        ┆ [0.20448551053113, 0.59519958… ┆ Video(path: gs://gresearch/ro… ┆ 35215462        ┆ [0.0843702104620847, -0.58301… ┆ Video(path: gs://gresearch/ro… │
╰────────────────────────────────┴──────────┴────────────┴────────────────────────┴────────────┴───────────────────┴────────────────────────────────┴─────────┴────────────────────────────────┴─────────────┴──────────┴─────────────────┴──────────────────────┴──────────────┴────────────────────────────────┴──────────────────┴────────────────────────────────┴────────────────────────────────┴─────────────────┴────────────────────────────────┴────────────────────────────────┴─────────────────┴────────────────────────────────┴────────────────────────────────╯

(Showing first 3 rows)
```

**Each row corresponds to one DROID episode.** Metadata from each episode's JSON file is unnested into top-level columns, and lazy file references are attached for the trajectory HDF5 file and three MP4 camera recordings.

## Episode layout

Each DROID episode is stored in its own directory:

```
episode/
|---- metadata_<episode_id>.json    # Episode metadata (building, task, camera serials, etc.)
|---- trajectory.h5                 # Low-dimensional action and proprioception trajectories
|---- recordings/
          |---- MP4/
                 |---- <camera_serial>.mp4
                 |---- <camera_serial>-stereo.mp4  # Optional stereo views
          |---- SVO/
                 |---- <camera_serial>.svo         # Raw ZED SVO recordings
```

[`daft.datasets.droid.raw()`][daft.datasets.droid.raw] currently attaches lazy references to:

- `trajectory`: the episode's `trajectory.h5` file
- `wrist_cam_video`: wrist camera MP4
- `ext1_cam_video`: external camera 1 MP4 (often the left view)
- `ext2_cam_video`: external camera 2 MP4 (often the right view)

Stereo MP4 and raw SVO recordings are not yet exposed as columns.

## Data schema

`raw()` returns one row per episode with metadata fields unnested from each `metadata_*.json` file, plus the following key columns:

| Column                 | Type          | Description                              |
| ---------------------- | ------------- | ---------------------------------------- |
| `episode_dir`          | String        | Path to the episode directory            |
| `uuid`                 | String        | Unique episode identifier                |
| `lab`                  | String        | Collecting lab                           |
| `user`                 | String        | Data collector name                      |
| `user_id`              | String        | Data collector identifier                |
| `date`                 | Date          | Collection date                          |
| `timestamp`            | String        | Collection timestamp                     |
| `building`             | String        | Building or environment name             |
| `scene_id`             | Int64         | Scene identifier within the building     |
| `success`              | Boolean       | Whether the demonstration was successful |
| `current_task`         | String        | Natural language task description        |
| `trajectory_length`    | Int64         | Number of timesteps in the trajectory    |
| `robot_serial`         | String        | Robot hardware serial number             |
| `wrist_cam_serial`     | String        | Wrist camera serial number               |
| `ext1_cam_serial`      | String        | External camera 1 serial number          |
| `ext2_cam_serial`      | String        | External camera 2 serial number          |
| `wrist_cam_extrinsics` | List[Float64] | Wrist camera extrinsics                  |
| `ext1_cam_extrinsics`  | List[Float64] | External camera 1 extrinsics             |
| `ext2_cam_extrinsics`  | List[Float64] | External camera 2 extrinsics             |
| `trajectory`           | File          | Lazy reference to `trajectory.h5`        |
| `wrist_cam_video`      | VideoFile     | Lazy reference to the wrist camera MP4   |
| `ext1_cam_video`       | VideoFile     | Lazy reference to external camera 1 MP4  |
| `ext2_cam_video`       | VideoFile     | Lazy reference to external camera 2 MP4  |

The raw metadata JSON includes additional path fields such as `hdf5_path`, `wrist_mp4_path`, and `ext1_mp4_path`; `raw()` exposes the constructed file columns instead.


## Prerequisites

The raw DROID dataset is hosted on Google Cloud Storage at `gs://gresearch/robotics/droid_raw` (~8.7 TB). By default, [`daft.datasets.droid.raw()`](../api/datasets.md#daft.datasets.droid.raw) reads from this public bucket, so no credentials are required to get started.

If you prefer to work with a local copy, download episodes with [`gsutil`](https://cloud.google.com/storage/docs/gsutil_install) and pass the local path to `raw()`:

```bash
# Download the full raw dataset (~8.7 TB)
gsutil -m cp -r gs://gresearch/robotics/droid_raw /path/to/droid_raw

# Or download a smaller subset for development
gsutil -m cp -r gs://gresearch/robotics/droid_raw/<episode_path> /path/to/droid_raw/
```

!!! note 
    See the [official DROID dataset documentation](https://droid-dataset.github.io/droid/the-droid-dataset) for details on the dataset format and downloading the necessary files for your use case.

For lower-level HDF5 file usage patterns, see the [HDF5 file usage notebook](https://github.com/Eventual-Inc/Daft/blob/main/examples/hdf5_file_usage.ipynb).


## Basic usage

### Loading from the public GCS bucket

This is the default behavior. Daft globs `metadata_*.json` files under the dataset root, reads each episode's metadata, and constructs paths to the associated trajectory and video files:

```python
import daft

df = daft.datasets.droid.raw()
```

### Loading from a local or custom path

Point `path` at any directory that mirrors the raw DROID layout (see [Episode layout](#episode-layout) below):

```python
import daft

df = daft.datasets.droid.raw(path="/path/to/droid_raw")
```

Remote object stores other than GCS are also supported when passed via `path` with an appropriate [`IOConfig`][daft.io.IOConfig].

### Loading a subset of episodes

Because `raw()` returns a lazy DataFrame, you can filter, project, and sample before materializing any video or trajectory data:

```python
import daft

(
    daft.datasets.droid.raw()
    .where(daft.col("success"))
    .where(daft.col("building") == "Ross")
    .select("uuid", "current_task", "trajectory_length", "wrist_cam_video")
    .limit(10)
)
```

### Scene classification table

The DROID dataset is annotated with scene classifications from GPT-4V. You can read those classifications with the `scenes` function, then filter or join the table however you need.

`scenes` reads a Parquet mirror hosted on Hugging Face at [Eventual-Inc/droid-scene-classifications](https://huggingface.co/datasets/Eventual-Inc/droid-scene-classifications). That table is derived from the DROID authors' [supplemental scene classification release](https://github.com/droid-dataset/droid/issues/6) (CC-BY 4.0). Valid labels are listed in `daft.datasets.droid.SCENE_CLASSIFICATIONS`.

```python
import daft

# Load a sample of the raw DROID data
df = daft.datasets.droid.raw().limit(100)

# Read and filter the scene classification table
scene_classifications = daft.datasets.droid.scenes().where(
    daft.col("scene_classification") == "Home kitchen"
)

# Join scene labels onto the episode data
df = df.join(scene_classifications, on="scene_id", how="inner")

df.select(
    "uuid",
    "scene_id",
    "scene_classification",
    "current_task",
    "success",
).show(3)
```

```
╭────────────────────────────────┬────────────┬──────────────────────┬────────────────────────────────┬─────────╮
│ uuid                           ┆ scene_id   ┆ scene_classification ┆ current_task                   ┆ success │
│ ---                            ┆ ---        ┆ ---                  ┆ ---                            ┆ ---     │
│ String                         ┆ Int64      ┆ String               ┆ String                         ┆ Bool    │
╞════════════════════════════════╪════════════╪══════════════════════╪════════════════════════════════╪═════════╡
│ WEIRD+5a211037+2023-11-20-20h… ┆ 2364934467 ┆ Home kitchen         ┆ Do anything you like that tak… ┆ false   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ WEIRD+5a211037+2023-11-20-21h… ┆ 2364934467 ┆ Home kitchen         ┆ Move object into or out of co… ┆ false   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ WEIRD+30c3da59+2023-11-21-22h… ┆ 3596747378 ┆ Home kitchen         ┆ Use cloth to clean something … ┆ false   │
╰────────────────────────────────┴────────────┴──────────────────────┴────────────────────────────────┴─────────╯

(Showing first 3 rows)
```

### Load trajectory data lazily with `daft.Hdf5File` built into `daft.datasets.droid.trajectory()`

The DROID dataset helper follows this pattern: it discovers episode files lazily, then reads selected known trajectory datasets into typed tensor columns.

```python
import daft

# Load a sample of the raw DROID data
df = daft.datasets.droid.raw().limit(3)

df = daft.datasets.droid.trajectory(
    df,
    fields=["action/joint_position", "action/gripper_position"],
)
df.show(3)
```

```
╭────────────────────────────────┬────────────┬──────────────────────┬──────────────┬────────────────────────────────┬─────────┬───────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────╮
│ uuid                           ┆ scene_id   ┆ robot_serial         ┆ r2d2_version ┆ current_task                   ┆ success ┆ trajectory_length ┆ action/joint_position   ┆ action/gripper_position ┆ wrist_cam_video                ┆ wrist_cam_extrinsics           ┆ ext1_cam_video                 ┆ ext1_cam_extrinsics            ┆ ext2_cam_video                 ┆ ext2_cam_extrinsics            │
│ ---                            ┆ ---        ┆ ---                  ┆ ---          ┆ ---                            ┆ ---     ┆ ---               ┆ ---                     ┆ ---                     ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            │
│ String                         ┆ Int64      ┆ String               ┆ String       ┆ String                         ┆ Bool    ┆ Int64             ┆ Tensor[Float64]         ┆ Tensor[Float64]         ┆ File[Video]                    ┆ List[Float64]                  ┆ File[Video]                    ┆ List[Float64]                  ┆ File[Video]                    ┆ List[Float64]                  │
╞════════════════════════════════╪════════════╪══════════════════════╪══════════════╪════════════════════════════════╪═════════╪═══════════════════╪═════════════════════════╪═════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╡
│ CLVR+13759f6e+2023-06-09-11h-… ┆ 6667529842 ┆ 295341-1325882       ┆ 1.1          ┆ Move object into or out of co… ┆ false   ┆ 187               ┆ <Tensor shape=(187, 7)> ┆ <Tensor shape=(187)>    ┆ Null                           ┆ [0.2601621034743318, 0.166998… ┆ Null                           ┆ [0.06296538305747777, 0.25683… ┆ Null                           ┆ [0.036736272290592786, -0.424… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ GuptaLab+553d1bd5+2023-07-09-… ┆ 500180237  ┆ panda-295341-1325237 ┆ 1.3          ┆ Do any two tasks consecutivel… ┆ true    ┆ 946               ┆ <Tensor shape=(946, 7)> ┆ <Tensor shape=(946)>    ┆ Video(path: gs://gresearch/ro… ┆ [0.1992642342748634, -0.07232… ┆ Video(path: gs://gresearch/ro… ┆ [-0.17238707747361265, 0.8769… ┆ Video(path: gs://gresearch/ro… ┆ [-0.09266930443312492, -0.672… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ IRIS+89b42cd2+2023-12-05-21h-… ┆ 3837471943 ┆ panda-295341-1326372 ┆ 1.3          ┆ Move object into or out of co… ┆ false   ┆ 26                ┆ <Tensor shape=(26, 7)>  ┆ <Tensor shape=(26)>     ┆ Video(path: gs://gresearch/ro… ┆ [0.26443079492475274, 0.09407… ┆ Video(path: gs://gresearch/ro… ┆ [0.2025336528549136, -0.64235… ┆ Video(path: gs://gresearch/ro… ┆ [0.2342123652474526, 0.538119… │
╰────────────────────────────────┴────────────┴──────────────────────┴──────────────┴────────────────────────────────┴─────────┴───────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────╯

(Showing first 3 rows)
```

For custom HDF5 layouts, create lazy `Hdf5File` references with `daft.functions.hdf5_file()` and use a typed UDF for dataset reads. If you need recursive traversal, call `Hdf5File.visit()` inside direct Python code or a UDF so that the cost is explicit.

```python
import h5py

import daft
from daft import col, DataType, Hdf5File

# Build the UDF that will read the trajectory data and return a struct of the requested fields
@daft.func(
    return_dtype=DataType.struct({
        "action/gripper_position": DataType.tensor(DataType.float64()),
        "action/target_gripper_position": DataType.tensor(DataType.float64()),
        "observation/robot_state/gripper_position": DataType.tensor(DataType.float64()),
    }),
    use_process=False,
    unnest=True,
)
def read_droid_trajectory(file: Hdf5File):
    with file.to_tempfile() as tmp, h5py.File(tmp.name, "r") as h5:
        return {
            "action/gripper_position": h5["action/gripper_position"][()],
            "action/target_gripper_position": h5["action/target_gripper_position"][()],
            "observation/robot_state/gripper_position": h5["observation/robot_state/gripper_position"][()],
        }

if __name__ == "__main__":
    df = (
        daft.datasets.droid.raw()
        .where(col("success"))
        .where(col("trajectory").not_null())
        .select(col("current_task"), read_droid_trajectory(col("trajectory")))
    )

    df.show(3)
```

```
╭────────────────────────────────┬─────────────────────────┬────────────────────────────────┬──────────────────────────────────────────╮
│ current_task                   ┆ action/gripper_position ┆ action/target_gripper_position ┆ observation/robot_state/gripper_position │
│ ---                            ┆ ---                     ┆ ---                            ┆ ---                                      │
│ String                         ┆ Tensor[Float64]         ┆ Tensor[Float64]                ┆ Tensor[Float64]                          │
╞════════════════════════════════╪═════════════════════════╪════════════════════════════════╪══════════════════════════════════════════╡
│ Do any two tasks consecutivel… ┆ <Tensor shape=(946)>    ┆ <Tensor shape=(946)>           ┆ <Tensor shape=(946)>                     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Move object into or out of co… ┆ <Tensor shape=(668)>    ┆ <Tensor shape=(668)>           ┆ <Tensor shape=(668)>                     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Move object into or out of co… ┆ <Tensor shape=(143)>    ┆ <Tensor shape=(143)>           ┆ <Tensor shape=(143)>                     │
╰────────────────────────────────┴─────────────────────────┴────────────────────────────────┴──────────────────────────────────────────╯

(Showing first 3 rows)
```

### Reading trajectories and camera frames

Use `trajectory()` to read selected HDF5 datasets into tensor columns, then use `camera_frames()` to decode MP4 camera frames when you need image data. `camera_frames()` decodes all three cameras by default. Pass a single camera name such as `cameras="wrist"` or a list of camera names to narrow the output. Supported camera names include `"wrist"`, `"ext1"`, and `"ext2"`. 


```python
import daft

episodes = (
    daft.datasets.droid.raw()
    .where(daft.col("success"))
    .limit(3)
)

traj = daft.datasets.droid.trajectory(
    episodes,
    fields=["action/joint_position", "action/gripper_position"],
)

frames = daft.datasets.droid.camera_frames(
    traj,
    cameras=["wrist", "ext1", "ext2"],
    width=224,
    height=224,
    sample_interval_seconds=0.5,
)

frames.show(3)
```

```
╭────────────────────────────────┬────────────┬────────────────┬──────────────┬────────────────────────────────┬─────────┬───────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬──────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────╮
│ uuid                           ┆ scene_id   ┆ robot_serial   ┆ r2d2_version ┆ current_task                   ┆ success ┆ trajectory_length ┆ action/joint_position   ┆ action/gripper_position ┆ wrist_cam_video                ┆ wrist_cam_extrinsics           ┆ ext1_cam_video                 ┆ ext1_cam_extrinsics            ┆ ext2_cam_video                 ┆ ext2_cam_extrinsics            ┆ wrist_cam_frames                                         ┆ ext1_cam_frames                                          ┆ ext2_cam_frames                                         │
│ ---                            ┆ ---        ┆ ---            ┆ ---          ┆ ---                            ┆ ---     ┆ ---               ┆ ---                     ┆ ---                     ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                            ┆ ---                                                      ┆ ---                                                      ┆ ---                                                     │
│ String                         ┆ Int64      ┆ String         ┆ String       ┆ String                         ┆ Bool    ┆ Int64             ┆ Tensor[Float64]         ┆ Tensor[Float64]         ┆ File[Video]                    ┆ List[Float64]                  ┆ File[Video]                    ┆ List[Float64]                  ┆ File[Video]                    ┆ List[Float64]                  ┆ List[Struct[frame_index: Int64, frame_time: Float64,     ┆ List[Struct[frame_index: Int64, frame_time: Float64,     ┆ List[Struct[frame_index: Int64, frame_time: Float64,    │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ frame_time_base: String, frame_pts: Int64, frame_dts:    ┆ frame_time_base: String, frame_pts: Int64, frame_dts:    ┆ frame_time_base: String, frame_pts: Int64, frame_dts:   │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ Int64, frame_duration: Int64, is_key_frame: Bool, data:  ┆ Int64, frame_duration: Int64, is_key_frame: Bool, data:  ┆ Int64, frame_duration: Int64, is_key_frame: Bool, data: │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ Image[MIXED]]]                                           ┆ Image[MIXED]]]                                           ┆ Image[MIXED]]]                                          │
╞════════════════════════════════╪════════════╪════════════════╪══════════════╪════════════════════════════════╪═════════╪═══════════════════╪═════════════════════════╪═════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪══════════════════════════════════════════════════════════╪══════════════════════════════════════════════════════════╪═════════════════════════════════════════════════════════╡
│ ILIAD+j807b3f8+2023-04-19-16h… ┆ 1285013161 ┆ 295341-1325494 ┆ 1.1          ┆ Move object into or out of co… ┆ true    ┆ 280               ┆ <Tensor shape=(280, 7)> ┆ <Tensor shape=(280)>    ┆ Video(path: gs://gresearch/ro… ┆ [0.2542811629384306, 0.120089… ┆ Video(path: gs://gresearch/ro… ┆ [0.10180256468176609, 0.46798… ┆ Video(path: gs://gresearch/ro… ┆ [0.2749705852352339, -0.46906… ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                       │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ frame_time:…                                             ┆ frame_time:…                                             ┆ frame_time:…                                            │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ ILIAD+j807b3f8+2023-04-19-16h… ┆ 1285013161 ┆ 295341-1325494 ┆ 1.1          ┆ Move object into or out of co… ┆ true    ┆ 376               ┆ <Tensor shape=(376, 7)> ┆ <Tensor shape=(376)>    ┆ Video(path: gs://gresearch/ro… ┆ [0.359122917548836, -0.180021… ┆ Video(path: gs://gresearch/ro… ┆ [0.10180256468176609, 0.46798… ┆ Video(path: gs://gresearch/ro… ┆ [0.2749705852352339, -0.46906… ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                       │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ frame_time:…                                             ┆ frame_time:…                                             ┆ frame_time:…                                            │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ ILIAD+j807b3f8+2023-04-19-16h… ┆ 1285013161 ┆ 295341-1325494 ┆ 1.1          ┆ Move object into or out of co… ┆ true    ┆ 263               ┆ <Tensor shape=(263, 7)> ┆ <Tensor shape=(263)>    ┆ Video(path: gs://gresearch/ro… ┆ [0.1915279636485076, -0.09894… ┆ Video(path: gs://gresearch/ro… ┆ [0.10180256468176609, 0.46798… ┆ Video(path: gs://gresearch/ro… ┆ [0.2749705852352339, -0.46906… ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                        ┆ [{frame_index: 0,                                       │
│                                ┆            ┆                ┆              ┆                                ┆         ┆                   ┆                         ┆                         ┆                                ┆                                ┆                                ┆                                ┆                                ┆                                ┆ frame_time:…                                             ┆ frame_time:…                                             ┆ frame_time:…                                            │
╰────────────────────────────────┴────────────┴────────────────┴──────────────┴────────────────────────────────┴─────────┴───────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴──────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────╯

```

## Next steps

- Run the [HDF5 file usage notebook](https://github.com/Eventual-Inc/Daft/blob/main/examples/hdf5_file_usage.ipynb) for lower-level examples of inspecting and reading HDF5 files.
- See the [Videos modality guide](../modalities/videos.md) for decoding frames with [`video_frames`][daft.functions.video_frames] and working with [`daft.VideoFile`](../api/datatypes/file_types.md).
- See the [Files modality guide](../modalities/files.md) for reading trajectory HDF5 files with [`daft.File`](../api/datatypes/file_types.md).
- Visit the [official DROID project page](https://droid-dataset.github.io/) for hardware setup, policy learning code, and additional dataset formats.
- See the [DROID Dataset API reference](../api/datasets.md#droid) for complete parameter documentation.
