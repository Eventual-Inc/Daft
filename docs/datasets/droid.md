# How to use DROID with Daft

[DROID](https://droid-dataset.github.io/) (Distributed Robot Interaction Dataset) is one of the most popular large-scale, in-the-wild robot manipulation dataset with 76,000 demonstration trajectories and 350 hours of interaction data. It was collected across 564 scenes and 86 tasks using the Franka Panda robot platform, and includes synchronized RGB camera streams, camera calibration, and natural language task descriptions.

Daft provides a simple way to explore the raw DROID release as a lazy, episode-level DataFrame with metadata, trajectory files as [`daft.Hdf5File`](../modalities/hdf5.md), and camera videos attached as `[daft.VideoFile](../modalities/videos.md)` columns.

## Prerequisites

The raw DROID dataset is hosted on Google Cloud Storage at `gs://gresearch/robotics/droid_raw` (~8.7 TB). By default, [`daft.datasets.droid.raw()`][daft.datasets.droid.raw] reads from this public bucket, so no credentials are required to get started.

If you prefer to work with a local copy, download episodes with `[gsutil](https://cloud.google.com/storage/docs/gsutil_install)` and pass the local path to `raw()`:

```bash
# Download the full raw dataset (~8.7 TB)
gsutil -m cp -r gs://gresearch/robotics/droid_raw /path/to/droid_raw

# Or download a smaller subset for development
gsutil -m cp -r gs://gresearch/robotics/droid_raw/<episode_path> /path/to/droid_raw/
```

See the [official DROID dataset documentation](https://droid-dataset.github.io/droid/the-droid-dataset) for details on the dataset format and downloading the necessary files for your use case.

For lower-level HDF5 file usage patterns, see the [HDF5 file usage notebook](https://github.com/Eventual-Inc/Daft/blob/main/examples/hdf5_file_usage.ipynb).

## Quickstart

The simplest way to get started is to load a small sample of data from the public source:

```python
import daft

# Load a sample of the raw DROID data
daft.datasets.droid.raw().show(3)
```

```
╭───────────────────┬────────┬────────────┬───────────────────┬────────────┬───────────────────┬───────────────────┬─────────┬───────────────────┬────────────┬───────────────────┬───────────────────┬─────────────────┬───────────────────┬───────────────────┬─────────────────┬───────────────────┬───────────────────╮
│ uuid              ┆ lab    ┆ date       ┆ timestamp         ┆ scene_id   ┆ trajectory_length ┆ current_task      ┆ success ┆ episode_dir       ┆      …     ┆ wrist_cam_extrins ┆ wrist_cam_video   ┆ ext1_cam_serial ┆ ext1_cam_extrinsi ┆ ext1_cam_video    ┆ ext2_cam_serial ┆ ext2_cam_extrinsi ┆ ext2_cam_video    │
│ ---               ┆ ---    ┆ ---        ┆ ---               ┆ ---        ┆ ---               ┆ ---               ┆ ---     ┆ ---               ┆            ┆ ics               ┆ ---               ┆ ---             ┆ cs                ┆ ---               ┆ ---             ┆ cs                ┆ ---               │
│ String            ┆ String ┆ Date       ┆ String            ┆ Int64      ┆ Int64             ┆ String            ┆ Bool    ┆ String            ┆ (7 hidden) ┆ ---               ┆ File[Video]       ┆ String          ┆ ---               ┆ File[Video]       ┆ String          ┆ ---               ┆ File[Video]       │
│                   ┆        ┆            ┆                   ┆            ┆                   ┆                   ┆         ┆                   ┆            ┆ List[Float64]     ┆                   ┆                 ┆ List[Float64]     ┆                   ┆                 ┆ List[Float64]     ┆                   │
╞═══════════════════╪════════╪════════════╪═══════════════════╪════════════╪═══════════════════╪═══════════════════╪═════════╪═══════════════════╪════════════╪═══════════════════╪═══════════════════╪═════════════════╪═══════════════════╪═══════════════════╪═════════════════╪═══════════════════╪═══════════════════╡
│ RAD+ac111655+2023 ┆ RAD    ┆ 2023-09-18 ┆ 2023-09-18-17h-13 ┆ 2708809181 ┆ 1224              ┆ Move object to a  ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.39223871928607 ┆ Video(path: gs:// ┆ 32907025        ┆ [0.13894431928565 ┆ Video(path: gs:// ┆ 35215462        ┆ [0.23342853397786 ┆ Video(path: gs:// │
│ -09-18-17h-1…     ┆        ┆            ┆ m-26s             ┆            ┆                   ┆ new position…     ┆         ┆ botics/droid…     ┆            ┆ 32, -0.16526…     ┆ gresearch/ro…     ┆                 ┆ 992, -0.6498…     ┆ gresearch/ro…     ┆                 ┆ 84, 0.667711…     ┆ gresearch/ro…     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+284fa481+2023 ┆ RAD    ┆ 2023-09-01 ┆ 2023-09-01-10h-43 ┆ 4823049285 ┆ 477               ┆ Move object into  ┆ true    ┆ gs://gresearch/ro ┆ …          ┆ [0.27734166208707 ┆ Video(path: gs:// ┆ 32907025        ┆ [0.20448551053113 ┆ Video(path: gs:// ┆ 35215462        ┆ [0.08437021046208 ┆ Video(path: gs:// │
│ -09-01-10h-4…     ┆        ┆            ┆ m-47s             ┆            ┆                   ┆ or out of co…     ┆         ┆ botics/droid…     ┆            ┆ 91, -0.21031…     ┆ gresearch/ro…     ┆                 ┆ , 0.59519958…     ┆ gresearch/ro…     ┆                 ┆ 47, -0.58301…     ┆ gresearch/ro…     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-09h-29 ┆ 4424513213 ┆ 190               ┆ Move object to a  ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.22450600005777 ┆ Null              ┆ 32907025        ┆ [0.13364182762051 ┆ Null              ┆ 35215462        ┆ [0.04858251876058 ┆ Null              │
│ -12-20-09h-2…     ┆        ┆            ┆ m-47s             ┆            ┆                   ┆ new position…     ┆         ┆ botics/droid…     ┆            ┆ 444, 0.12355…     ┆                   ┆                 ┆ 11, 0.648632…     ┆                   ┆                 ┆ 541, -0.5586…     ┆                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-10h-36 ┆ 8647259418 ┆ 481               ┆ Hang or unhang    ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.35110737285753 ┆ Null              ┆ 32907025        ┆ [0.17394483869227 ┆ Null              ┆ 35215462        ┆ [0.15951830031526 ┆ Null              │
│ -12-20-10h-3…     ┆        ┆            ┆ m-53s             ┆            ┆                   ┆ object (ex: to…   ┆         ┆ botics/droid…     ┆            ┆ 735, -0.0442…     ┆                   ┆                 ┆ 984, 0.55957…     ┆                   ┆                 ┆ 68, -0.44426…     ┆                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-10h-37 ┆ 8647259418 ┆ 728               ┆ Hang or unhang    ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.24040109080787 ┆ Null              ┆ 32907025        ┆ [0.17394483869227 ┆ Null              ┆ 35215462        ┆ [0.15951830031526 ┆ Null              │
│ -12-20-10h-3…     ┆        ┆            ┆ m-48s             ┆            ┆                   ┆ object (ex: to…   ┆         ┆ botics/droid…     ┆            ┆ 98, 0.003635…     ┆                   ┆                 ┆ 984, 0.55957…     ┆                   ┆                 ┆ 68, -0.44426…     ┆                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-10h-38 ┆ 8647259418 ┆ 494               ┆ Hang or unhang    ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.32623796813520 ┆ Null              ┆ 32907025        ┆ [0.17394483869227 ┆ Null              ┆ 35215462        ┆ [0.15951830031526 ┆ Null              │
│ -12-20-10h-3…     ┆        ┆            ┆ m-58s             ┆            ┆                   ┆ object (ex: to…   ┆         ┆ botics/droid…     ┆            ┆ 434, -0.1115…     ┆                   ┆                 ┆ 984, 0.55957…     ┆                   ┆                 ┆ 68, -0.44426…     ┆                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-10h-46 ┆ 8647259418 ┆ 425               ┆ Hang or unhang    ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.22458811705818 ┆ Null              ┆ 32907025        ┆ [0.17394483869227 ┆ Null              ┆ 35215462        ┆ [0.15951830031526 ┆ Null              │
│ -12-20-10h-4…     ┆        ┆            ┆ m-27s             ┆            ┆                   ┆ object (ex: to…   ┆         ┆ botics/droid…     ┆            ┆ 335, 0.08554…     ┆                   ┆                 ┆ 984, 0.55957…     ┆                   ┆                 ┆ 68, -0.44426…     ┆                   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ RAD+62828209+2023 ┆ RAD    ┆ 2023-12-20 ┆ 2023-12-20-10h-49 ┆ 8647259418 ┆ 630               ┆ Hang or unhang    ┆ false   ┆ gs://gresearch/ro ┆ …          ┆ [0.39153451598295 ┆ Null              ┆ 32907025        ┆ [0.17394483869227 ┆ Null              ┆ 35215462        ┆ [0.15951830031526 ┆ Null              │
│ -12-20-10h-4…     ┆        ┆            ┆ m-04s             ┆            ┆                   ┆ object (ex: to…   ┆         ┆ botics/droid…     ┆            ┆ 73, -0.22323…     ┆                   ┆                 ┆ 984, 0.55957…     ┆                   ┆                 ┆ 68, -0.44426…     ┆                   │
╰───────────────────┴────────┴────────────┴───────────────────┴────────────┴───────────────────┴───────────────────┴─────────┴───────────────────┴────────────┴───────────────────┴───────────────────┴─────────────────┴───────────────────┴───────────────────┴─────────────────┴───────────────────┴───────────────────╯

(Showing first 8 rows)
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
    .select("uuid", "current_task", "trajectory_length", "wrist_cam_video")
    .limit(10)
)
```

### Filtering by scene classification

The DROID dataset is annotated with scene classifications from GPT-4V. You can filter the data by scene classification using the `filter_scenes` function.

```python
import daft

# Load a sample of the raw DROID data
df = daft.datasets.droid.raw().limit(100)

# Filter the data to only include scenes of type "Home kitchen"
df = daft.datasets.droid.filter_scenes(df, "Home kitchen")

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
    fields=["action/joint_position", "action/gripper_position"]
).show(3)
```

For custom HDF5 layouts, create lazy Hdf5File references with daft.functions.hdf5_file and use a typed UDF for dataset reads. If you need recursive traversal, call Hdf5File.visit() inside direct Python code or a UDF so that the cost is explicit.

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

def just_the_first_ten_data_points(f: Hdf5File) -> np.ndarray:

    f.visit()

if __name__ == "__main__":

    df = (
        daft.datasets.droid.raw()
        .where(col("success")) # filter out failed episodes
        .select(col("current_task"), read_droid_trajectory(col("trajectory")))
        .with_column("first_ten_steps", )
    )

    df.show(3) 
```

For a runnable walkthrough covering standalone Hdf5File usage, MIME detection, hierarchy traversal, DataFrame expressions, and UDF patterns, see the examples in daft-examples repository

While the Python classes provide the interface, the actual implementation lives in Rust-based PyDaftFile, which maintains optimized backends for different storage types:

Local filesystem access

Remote object stores with buffered reading

This architecture allows us to implement storage-specific optimizations (like network buffering for S3 or HTTP) while presenting a consistent interface.

### Reading trajectories and camera frames

Use `trajectory()` to read selected HDF5 datasets into tensor columns, then use `camera_frames()` to decode MP4 camera frames when you need image data:

```python
import daft

episodes = (
    daft.datasets.droid.raw()
    .where(daft.col("success"))
    .limit(10)
)

traj = daft.datasets.droid.trajectory(
    episodes,
    fields=["joint_position", "gripper_position"],
)

frames = daft.datasets.droid.camera_frames(
    traj,
    cameras=["wrist", "ext1", "ext2"],
    width=224,
    height=224,
    sample_interval_seconds=0.5,
)
```

`camera_frames()` decodes all three cameras by default. Pass a single camera name such as `cameras="wrist"` or a list of camera names to narrow the output.

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



## Next steps

- Run the [HDF5 file usage notebook](https://github.com/Eventual-Inc/Daft/blob/main/examples/hdf5_file_usage.ipynb) for lower-level examples of inspecting and reading HDF5 files.
- See the [Videos modality guide](../modalities/videos.md) for decoding frames with [`video_frames`][daft.functions.video_frames] and working with `[daft.VideoFile](../api/datatypes/file_types.md)`.
- See the [Files modality guide](../modalities/files.md) for reading trajectory HDF5 files with `[daft.File](../api/datatypes/file_types.md)`.
- Visit the [official DROID project page](https://droid-dataset.github.io/) for hardware setup, policy learning code, and additional dataset formats.
- See the [DROID Dataset API reference](../api/datasets.md#droid) for complete parameter documentation.

