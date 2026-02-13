# Working with Videos

There are two main ways to work with videos in Daft:
1. Use `daft.read_video_frames` to read frames of a video into a DataFrame.
2. Use the `daft.VideoFile` class to work with video files and metadata.

`daft.VideoFile` is a subclass of `daft.File` that provides a specialized interface for video-specific operations.

- [daft.read_video_frames](../api/io.md#daft.read_video_frames) for reading video frames into a DataFrame
- [daft.VideoFile](../api/datatypes/file_types.md) for working with video files
    - [daft.functions.video_file](../api/functions/video_file.md) for working with video files
    - [daft.functions.video_metadata](../api/functions/video_metadata.md) for working with video metadata
    - [daft.functions.video_keyframes](../api/functions/video_keyframes.md) for working with video keyframes

### Reading Video Frames with `daft.read_video_frames`

This example shows reading a video's frames into a DataFrame using the `daft.read_video_frames` function.

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_video_frames(
        path="s3://daft-public-data/videos/zoo.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=True, # select only the key frames
    )

    df.show()
    ```


#### Sampling frames by time interval

You can also downsample frames on the source side by specifying ``sample_interval_seconds``.

=== "🐍 Python"

    ```python
    import daft

    # Sample approximately one frame per second based on frame_time
    df = daft.read_video_frames(
        path="s3://daft-public-data/videos/zoo.mp4",
        image_height=480,
        image_width=640,
        sample_interval_seconds=1.0,
    )

    df.show()
    ```

##### Understanding Sampling Behavior

The ``sample_interval_seconds`` parameter enables time-based frame sampling, which is particularly useful for:

- **Video summarization**: Extract key frames at regular intervals
- **Reducing processing load**: Work with fewer frames while maintaining temporal coverage
- **Creating thumbnails**: Generate representative frames from long videos
- **Analyzing trends**: Sample frames at consistent time points for comparison

##### How Sampling Works

The sampling algorithm:

1. **Target times**: Calculates target sampling times at 0, interval, 2*interval, 3*interval, ...
2. **Frame selection**: For each target time, selects the first frame whose timestamp is >= target time
3. **Timestamp-based**: Uses the frame's presentation timestamp (PTS), which indicates when the frame should be displayed
4. **Approximate**: This is an approximate sampling strategy; actual sampling times depend on available frame timestamps

##### Impact of Video Characteristics

**Constant Frame Rate (CFR) Videos**:
- Frame timestamps are evenly spaced
- Sampling is more predictable
- Example: 30 fps video with 1-second interval → ~30 frames between samples

**Variable Frame Rate (VFR) Videos**:
- Frame timestamps may be irregular
- Sampling times may vary from target times
- Common in screen recordings, animations, and optimized videos

**Frame Timestamp Precision**:
- Different video formats use different time bases (e.g., 1/90000 for NTSC)
- Floating-point precision is handled with a small epsilon tolerance
- Frames without valid timestamps are skipped

##### Examples

**Example 1: Uniform CFR Video**
```
Frame timestamps: [0.0, 0.033, 0.067, 0.100, 0.133, 0.167, 0.200, ...]
sample_interval_seconds=0.1
Sampled frames: [0.0, 0.100, 0.200, ...]  # Exact matches
```

**Example 2: Non-uniform Timestamps**
```
Frame timestamps: [0.0, 0.95, 1.05, 2.0, 2.95, 3.05]
sample_interval_seconds=1.0
Sampled frames: [0.0, 1.05, 2.0, 3.05]  # First frame >= target time
```

**Example 3: Large Frame Interval**
```
Frame timestamps: [0.0, 2.5, 5.0]
sample_interval_seconds=1.0
Sampled frames: [0.0, 2.5, 5.0]  # Closest available frames
```

**Example 4: VFR Video**
```
Frame timestamps: [0.0, 0.033, 0.100, 0.133, 0.233, 0.267, 1.0, 1.033]
sample_interval_seconds=1.0
Sampled frames: [0.0, 1.0]  # Frames at 0.0s and 1.0s
```

##### Combining with Key Frame Filtering

You can combine time-based sampling with key frame filtering:

=== "🐍 Python"

    ```python
    import daft

    # Sample key frames at 1-second intervals
    df = daft.read_video_frames(
        path="s3://daft-public-data/videos/zoo.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=True,
        sample_interval_seconds=1.0,
    )

    df.show()
    ```

This is useful for:
- **Efficient processing**: Work with fewer, more important frames
- **Video indexing**: Create a sparse representation using key frames
- **Compression-aware sampling**: Respect the video's compression structure

###### Performance Considerations

- **Source-side filtering**: Sampling happens at the data source, reducing memory and processing overhead
- **Frame decoding**: All frames are still decoded to check timestamps, but only sampled frames are processed
- **Memory efficiency**: Only sampled frames are stored in the resulting DataFrame

###### Limitations

- **Approximate sampling**: The exact sampling times may differ from target times
- **Frame availability**: If no frame exists near a target time, the closest available frame is selected
- **Timestamp requirements**: Frames without valid timestamps are skipped when sampling is enabled
- **No interpolation**: The algorithm does not interpolate between frames; it selects existing frames

```{title="Output"}
╭────────────────────────────────┬──────────────┬────────────────────┬─────────────────┬───────────┬───────────┬────────────────┬──────────────┬───────────────────────╮
│ path                           ┆ frame_index  ┆ frame_time         ┆ frame_time_base ┆ frame_pts ┆ frame_dts ┆ frame_duration ┆ is_key_frame ┆ data                  │
│ ---                            ┆ ---          ┆ ---                ┆ ---             ┆ ---       ┆ ---       ┆ ---            ┆ ---          ┆ ---                   │
│ Utf8                           ┆ Int64        ┆ Float64            ┆ Utf8            ┆ Int64     ┆ Int64     ┆ Int64          ┆ Boolean      ┆ Image[RGB; 480 x 640] │
╞════════════════════════════════╪══════════════╪════════════════════╪═════════════════╪═══════════╪═══════════╪════════════════╪══════════════╪═══════════════════════╡
│ s3://daft-public-data/videos/… ┆ 0            ┆ 0                  ┆ 1/15360         ┆ 0         ┆ 0         ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 1            ┆ 4                  ┆ 1/15360         ┆ 61440     ┆ 61440     ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 2            ┆ 5.333333333333333  ┆ 1/15360         ┆ 81920     ┆ 81920     ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 3            ┆ 9.333333333333334  ┆ 1/15360         ┆ 143360    ┆ 143360    ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 4            ┆ 10.666666666666666 ┆ 1/15360         ┆ 163840    ┆ 163840    ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 5            ┆ 14.666666666666666 ┆ 1/15360         ┆ 225280    ┆ 225280    ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ s3://daft-public-data/videos/… ┆ 6            ┆ 16                 ┆ 1/15360         ┆ 245760    ┆ 245760    ┆ 1024           ┆ true         ┆ <FixedShapeImage>     │
╰────────────────────────────────┴──────────────┴────────────────────┴─────────────────┴───────────┴───────────┴────────────────┴──────────────┴───────────────────────╯

(Showing first 7 of 7 rows)
```

!!! note "Note"

    You can specify multiple paths and use globs like `daft.read_video_frames("/path/to/file.mp4")` and `daft.read_video_frames("/path/to/files-*.mp4")`


### Reading from YouTube

This example shows reading the key frames of a youtube video, you can also pass in a list of video urls.

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_video_frames(
        path=[
            "https://www.youtube.com/watch?v=jNQXAC9IVRw",
            "https://www.youtube.com/watch?v=N2rZxCrb7iU",
            "https://www.youtube.com/watch?v=TF6cnLnEARo",
        ],
        image_height=480,
        image_width=640,
        is_key_frame=True,
    )

    df.show()
    ```

```{title="Output"}
╭────────────────────────────────┬─────────────┬───────────────────┬─────────────────┬───────────┬───────────┬────────────────┬──────────────┬───────────────────────╮
│ path                           ┆ frame_index ┆ frame_time        ┆ frame_time_base ┆ frame_pts ┆ frame_dts ┆ frame_duration ┆ is_key_frame ┆ data                  │
│ ---                            ┆ ---         ┆ ---               ┆ ---             ┆ ---       ┆ ---       ┆ ---            ┆ ---          ┆ ---                   │
│ Utf8                           ┆ Int64       ┆ Float64           ┆ Utf8            ┆ Int64     ┆ Int64     ┆ Int64          ┆ Boolean      ┆ Image[RGB; 480 x 640] │
╞════════════════════════════════╪═════════════╪═══════════════════╪═════════════════╪═══════════╪═══════════╪════════════════╪══════════════╪═══════════════════════╡
│ https://www.youtube.com/watch… ┆ 0           ┆ 0                 ┆ 1/90000         ┆ 0         ┆ 0         ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 1           ┆ 6.8068            ┆ 1/90000         ┆ 612612    ┆ 612612    ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 2           ┆ 13.2132           ┆ 1/90000         ┆ 1189188   ┆ 1189188   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 3           ┆ 18.018            ┆ 1/90000         ┆ 1621620   ┆ 1621620   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 4           ┆ 24.8248           ┆ 1/90000         ┆ 2234232   ┆ 2234232   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 5           ┆ 30.03             ┆ 1/90000         ┆ 2702700   ┆ 2702700   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 6           ┆ 36.36966666666667 ┆ 1/90000         ┆ 3273270   ┆ 3273270   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.youtube.com/watch… ┆ 7           ┆ 43.27656666666667 ┆ 1/90000         ┆ 3894891   ┆ 3894891   ┆ 3003           ┆ true         ┆ <FixedShapeImage>     │
╰────────────────────────────────┴─────────────┴───────────────────┴─────────────────┴───────────┴───────────┴────────────────┴──────────────┴───────────────────────╯

(Showing first 8 rows)
```

### Working with daft.VideoFile
The following example demonstrates how to use `daft.VideoFile` to read a video file and extract metadata.

```python
import daft
from daft.functions import video_file, video_metadata, video_keyframes

df = (
    daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/videos/*.mp4")
    .with_column("file", video_file(daft.col("path")))
    .with_column("metadata", video_metadata(daft.col("file")))
    .with_column("keyframes", video_keyframes(daft.col("file")))
    .select("path", "file", "size", "metadata", "keyframes")
)

df.show(3)
```
