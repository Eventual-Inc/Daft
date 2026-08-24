# How to use PX OmniSharing with Daft

[PX OmniSharing DB](https://huggingface.co/datasets/paxini/Omnisharing_DB_SampleData) is PaXini's omnimodal embodied-AI dataset. Unlike most manipulation datasets it records **force and tactile sensing** alongside vision: each episode captures a human wearing an instrumented exoskeleton glove, with 15 tactile sensor pads per hand, a dozen synchronized RGB cameras, stereo RGBD cameras, hand proprioception, optional object poses, audio and a natural-language instruction.

Daft reads the DF-2 HDF5 stage directly, streaming only the datasets you ask for. Episodes are 0.4-3.2 GB each, so nothing is downloaded up front.

## Quickstart

```python
import daft
from daft.datasets import omnisharing

# One row per episode; parsed from filenames, so no file bytes are read yet.
episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData")
episodes.select("episode_key", "stage", "size").show(3)
```

Filter before reading modalities, then pull in what you need:

```python
one = episodes.limit(1)

# Task labels and the language instruction.
omnisharing.episode_metadata(one).select("instruction", "task_labels").show()

# Joint angles and 6-DoF hand poses as tensors.
omnisharing.trajectory(one, fields=["observation.lefthand.joints"]).show()

# Tactile, split into one column per sensor pad.
omnisharing.tactile(one, sides="lefthand", split_by_sensor=True).show()

# Audio waveform, depth maps, and stereo calibration.
omnisharing.audio(one, mono=True, max_seconds=2.0).select("samplerate", "n_samples").show()
omnisharing.depth_frames(one, "RGBD_0").select("RGBD_0.depth").show()
omnisharing.stereo_extrinsics(one, "RGBD_0").select("RGBD_0.left_to_color").show()
```

## Pipeline stages

The [OmniSharing Toolkit](https://github.com/px-DataCollection/px_omnisharing_dataprocess_kit) emits four stages. The filename suffix tells you which one you have, and `raw()` surfaces it as a `stage` column.

| Stage | Format | Suffix | Support |
| ----- | ------ | ------ | ------- |
| DF-1 | HDF5 | *(none)* | Raw capture; encoder and tactile streams are unparsed |
| **DF-2** | HDF5 | `_glove` | **Primary target.** Parsed, with bimanual and object poses |
| DF-2R | HDF5 | `_dh13`, `_mano`, ... | Retargeted to a dexterous hand model |
| DF-3 | LeRobot v2.1 | *(none)* | Use [`daft.datasets.lerobot`](lerobot.md) instead |

The public sample release contains DF-2 only. To read a specific stage:

```python
omnisharing.raw("paxini/Omnisharing_DB_SampleData", stage="DF-2")
```

DF-2R changes tensor widths (joints become 17 wide, tactile 3750). Daft reads widths from the file, so the same code works for both stages.

## Episode layout

Every episode is a single self-describing HDF5 file:

```
episode_{index}_{HHMMSS}_{room}_{personnel}_glove.hdf5
└── dataset                          # attrs: generated_time, data_id
    ├── meta                         # attrs: vendor + task labels
    ├── action                       # leads observation by one frame
    │   └── {left,right}hand
    │       ├── joints/data          # (n, 29) float32, attrs: joint_names
    │       └── handpose/data        # (n, 7) float32, attrs: order
    └── observation
        ├── aligned_timestamp        # (n,) int64 - the reference clock
        ├── audio                    # (samples, 1) float64 PCM, attrs: samplerate, txt
        ├── image
        │   ├── RGB_Camera{i}        # H.265/HEVC Annex-B, 1920x1200
        │   │   ├── data, timestamp
        │   │   └── intrinsics, extrinsics
        │   └── RGBD_{i}             # Matroska, 1280x720
        │       ├── {color,left,right}/data
        │       ├── timestamp, {left,right}_timestamp   # one clock per eye
        │       ├── aligned_depth    # (n, H, W) uint16, RGBD_0 only
        │       └── inner_extrinsic  # JSON: stereo-to-color transform
        ├── {left,right}hand
        │   ├── joints/data          # (n, 29)
        │   ├── handpose/data        # (n, 7)
        │   └── tactile/data         # (n, 3465), attrs: sensor_names, sensor_lengths
        └── obj{i}/data              # (n, 17) - optional
```

Because the layout varies between releases, inspect it rather than assuming:

```python
layout = omnisharing.describe(episodes.limit(1))
layout.where(daft.col("kind") == "dataset").select("h5path", "shape", "dtype").show(50)
```

## Three things that are easy to get wrong

### 1. `episode_index` is not unique

It restarts per capture group, so the same index appears under different `(room_id, personnel_id)` pairs. Join on `episode_key` instead:

```python
# Both of these exist in part_01:
#   episode_1217_212953_93_110056_glove.hdf5
#   episode_1217_213630_115_110092_glove.hdf5
episodes.select("episode_key", "episode_index").show()
```

### 2. Quaternions are `qw`-first

`handpose` is laid out `[x, y, z, qw, qx, qy, qz]`. SciPy expects `[qx, qy, qz, qw]` and will silently produce wrong rotations:

```python
import numpy as np
from scipy.spatial.transform import Rotation

pose = ...  # (n, 7) from trajectory()
xyz, quat_wxyz = pose[:, :3], pose[:, 3:7]
rotation = Rotation.from_quat(np.roll(quat_wxyz, -1, axis=1))  # -> [qx, qy, qz, qw]
```

### 3. Cameras do not share the observation clock

Hands and RGBD cameras tick with `aligned_timestamp`, but RGB cameras run on their own clock, start at a different moment, and produce more frames. Camera ids are also non-contiguous. Aligning by frame index is wrong; use `frames(align_cameras=...)`, which matches by nearest timestamp and reports the residual:

```python
omnisharing.cameras(episodes.limit(1)).select("camera", "codec", "n_timestamps").show(30)

per_frame = omnisharing.frames(
    episodes.limit(1),
    fields=["observation.lefthand.joints"],
    align_cameras=["RGB_Camera0"],
)
per_frame.select(
    "frame_index", "RGB_Camera0.frame_index", "RGB_Camera0.timestamp_delta_us"
).show()
```

In one sampled episode, observation frame 0 aligns to `RGB_Camera0` frame **10**, not frame 0: that camera began recording earlier. Treating the streams as index-aligned would offset every frame by 10. `RGBD_0` shares the observation clock and does align 1:1.

## Tactile sensing

The `(n, 3465)` tactile vector is 15 sensor pads concatenated, with widths in the `sensor_lengths` attribute. `split_by_sensor=True` slices it for you:

```python
tactile = omnisharing.tactile(episodes.limit(1), sides="lefthand", split_by_sensor=True)
tactile.select("lefthand.tactile.palm_sensor1", "lefthand.tactile.J32L").show()
```

Sensor names follow the glove's finger naming (`palm_sensor1`, `J32L`, `M6L`, ...), and widths differ per pad, e.g. `219` for the palm and `378` for a fingertip.

## Frame-level expansion

`frames()` produces one row per frame and applies the dataset's action offset: `action[i]` is the state at `i + 1`, with the final action repeated.

| Frame | 0 | 1 | ... | n-2 | n-1 |
| --- | --- | --- | --- | --- | --- |
| observation | s0 | s1 | ... | s(n-2) | s(n-1) |
| action | s1 | s2 | ... | s(n-1) | s(n-1) |

`fields` is required, because a single tactile row is 3465 floats wide and unrestricted expansion is a memory hazard:

```python
omnisharing.frames(
    episodes.limit(1),
    fields=["observation.lefthand.joints", "action.lefthand.joints"],
).show()
```

## Video

Payloads are whole encoded streams stored inside the HDF5 file. Decoding needs the `daft[video]` extra:

```python
frames = omnisharing.camera_frames(episodes.limit(1), ["RGB_Camera0"], max_frames=2)
frames.select("RGB_Camera0.frames").show()
```

`max_frames` defaults to 1 because each stream holds an entire episode. If you would rather handle the bytes yourself, or a payload uses a codec Daft cannot decode, `camera_payloads()` always returns the raw bytes plus the detected codec:

```python
omnisharing.camera_payloads(episodes.limit(1), ["RGB_Camera0"]).select("RGB_Camera0.codec").show()
```

## Audio

The published layout calls this a "compressed audio stream (includes text)". It is neither: the samples are raw `float64` PCM, and the spoken instruction sits in a `txt` attribute that `episode_metadata()` surfaces as `instruction`. No audio codec is needed.

```python
clip = omnisharing.audio(episodes.limit(1), mono=True, max_seconds=2.0)
clip.select("samplerate", "n_samples", "waveform").show()
```

`max_seconds` slices at read time, so previewing a clip never pulls the whole recording across the network. It is ignored when an episode has no `samplerate` attribute, since the sample count cannot be derived without one.

## Depth

`RGBD_0/aligned_depth` holds `uint16` depth already registered to the colour image, so a depth pixel and its colour pixel share coordinates. It appears in no published layout and is not on every RGBD camera — in a sampled episode only `RGBD_0` had it.

```python
depth = omnisharing.depth_frames(episodes.limit(1), "RGBD_0", frame_indices=[0, 10])
depth.select("RGBD_0.depth", "RGBD_0.depth_frame_indices").show()
```

!!! warning "The depth unit is undocumented"

    Unlike the tactile and joint datasets, `aligned_depth` carries no attributes at all, so nothing in the file lets a scale be derived. Values are returned exactly as stored — confirm with PaXini before treating them as millimetres.

A single frame is about 1.8 MB and a whole episode roughly 380 MB per camera, so `frame_indices` defaults to frame `0` and each requested frame is read individually.

Episode length varies across a release, so an index present in one episode may be absent from another. Rather than failing the whole call, each episode reads the frames it has — so check `depth_frame_indices` instead of assuming it matches what you asked for. Pass `strict=True` to turn a short episode into an error instead.

## Stereo calibration

Each RGBD group stores an `inner_extrinsic` JSON blob, which `cameras()` passes through as an opaque string. `stereo_extrinsics()` parses out the part most people want:

```python
calib = omnisharing.stereo_extrinsics(episodes.limit(1), "RGBD_0")
calib.select("RGBD_0.left_to_color", "RGBD_0.calib_date").show()
```

`left_to_color` is the 4x4 transform from the left eye into the colour frame. Do not confuse it with the `extrinsics` that `cameras()` reports: that places the whole camera relative to a reference rig (`RGBD_0` for depth cameras, `RGB_Camera6` for colour ones), whereas this one is internal to a single camera.

The RGBD eyes also keep their own clocks — `left_timestamp` and `right_timestamp` alongside `timestamp` — so align to a specific eye rather than assuming the colour clock:

```python
per_frame = omnisharing.frames(
    episodes.limit(1),
    fields=["observation.lefthand.joints"],
    align_cameras=[("RGBD_0", "left")],
)
per_frame.select("frame_index", "RGBD_0.left.frame_index", "RGBD_0.left.timestamp_delta_us").show()
```

## Object poses

Object tracks come from the toolkit's optional pose-estimation stage, so many episodes have none. `objects()` probes for the widest episode and null-fills the rest, keeping the schema stable:

```python
objs = omnisharing.objects(episodes.limit(4))
objs.select("episode_key", "n_objects", "obj1.name", "obj1.id").show()
```

## Installation

```bash
pip install 'daft[hdf5]'          # required: reads the HDF5 episodes
pip install 'daft[hdf5,video]'    # also decodes camera payloads
```

## License

The published OmniSharing data is licensed **CC-BY-NC-SA 4.0** (non-commercial). Review the [dataset card](https://huggingface.co/datasets/paxini/Omnisharing_DB_SampleData) and the [toolkit license](https://github.com/px-DataCollection/px_omnisharing_dataprocess_kit) before use; the toolkit's binary components are proprietary and restricted to research and education.
