# How to use DROID with Daft

[DROID](https://droid-dataset.github.io/) (Distributed Robot Interaction Dataset) is one of the most popular large-scale, in-the-wild robot manipulation dataset with 76,000 demonstration trajectories and 350 hours of interaction data. It was collected across 564 scenes and 86 tasks using the Franka Panda robot platform, and includes synchronized RGB camera streams, camera calibration, and natural language task descriptions.

Daft provides a simple way to explore the raw DROID release as a lazy, episode-level DataFrame with metadata, trajectory files, and camera videos attached as [`daft.VideoFile`](../modalities/videos.md) columns.

## Prerequisites

The raw DROID dataset is hosted on Google Cloud Storage at `gs://gresearch/robotics/droid_raw` (~8.7 TB). By default, [`daft.datasets.droid.raw()`][daft.datasets.droid.raw] reads from this public bucket, so no credentials are required to get started.

If you prefer to work with a local copy, download episodes with [`gsutil`](https://cloud.google.com/storage/docs/gsutil_install) and pass the local path to `raw()`:

```bash
# Download the full raw dataset (~8.7 TB)
gsutil -m cp -r gs://gresearch/robotics/droid_raw /path/to/droid_raw

# Or download a smaller subset for development
gsutil -m cp -r gs://gresearch/robotics/droid_raw/<episode_path> /path/to/droid_raw/
```

See the [official DROID dataset documentation](https://droid-dataset.github.io/droid/the-droid-dataset) for details on the dataset format and downloading the necessary files for your use case.

## Quickstart

The simplest way to get started is to load a small sample of data from the public source:

```python
import daft

# Load a sample of the raw DROID data
daft.datasets.droid.raw().show()
```

Each row corresponds to one DROID episode. Metadata from each episode's JSON file is unnested into top-level columns, and lazy file references are attached for the trajectory HDF5 file and three MP4 camera recordings.

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
    .select("uuid", "current_task", "trajectory_length", "wrist_video")
    .limit(10)
)
```

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
- `wrist_video`: wrist camera MP4
- `ext1_video`: external camera 1 MP4 (often the left view)
- `ext2_video`: external camera 2 MP4 (often the right view)

Stereo MP4 and raw SVO recordings are not yet exposed as columns.

## Data schema

`raw()` returns one row per episode with metadata fields unnested from each `metadata_*.json` file, plus the following key columns:

| Column | Type | Description |
| --- | --- | --- |
| `episode_dir` | String | Path to the episode directory |
| `uuid` | String | Unique episode identifier |
| `lab` | String | Collecting lab |
| `user` | String | Data collector name |
| `user_id` | String | Data collector identifier |
| `date` | Date | Collection date |
| `timestamp` | String | Collection timestamp |
| `building` | String | Building or environment name |
| `scene_id` | Int64 | Scene identifier within the building |
| `success` | Boolean | Whether the demonstration was successful |
| `current_task` | String | Natural language task description |
| `trajectory_length` | Int64 | Number of timesteps in the trajectory |
| `robot_serial` | String | Robot hardware serial number |
| `wrist_cam_serial` | String | Wrist camera serial number |
| `ext1_cam_serial` | String | External camera 1 serial number |
| `ext2_cam_serial` | String | External camera 2 serial number |
| `wrist_cam_extrinsics` | List[Float64] | Wrist camera extrinsics |
| `ext1_cam_extrinsics` | List[Float64] | External camera 1 extrinsics |
| `ext2_cam_extrinsics` | List[Float64] | External camera 2 extrinsics |
| `trajectory` | File | Lazy reference to `trajectory.h5` |
| `wrist_video` | VideoFile | Lazy reference to the wrist camera MP4 |
| `ext1_video` | VideoFile | Lazy reference to external camera 1 MP4 |
| `ext2_video` | VideoFile | Lazy reference to external camera 2 MP4 |

Additional path columns from the metadata JSON (such as `hdf5_path`, `wrist_mp4_path`, and `ext1_mp4_path`) are also available as top-level columns.

<!-- TODO: Include interesting examples of working with the data. -->

## Next steps

- See the [Videos modality guide](../modalities/videos.md) for decoding frames with [`video_frames`][daft.functions.video_frames] and working with [`daft.VideoFile`](../api/datatypes/file_types.md).
- See the [Files modality guide](../modalities/files.md) for reading trajectory HDF5 files with [`daft.File`](../api/datatypes/file_types.md).
- Visit the [official DROID project page](https://droid-dataset.github.io/) for hardware setup, policy learning code, and additional dataset formats.
- See the [DROID Dataset API reference](../api/datasets.md#droid) for complete parameter documentation.
