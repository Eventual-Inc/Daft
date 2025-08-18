# Working with Videos

## Examples

<!-- include more examples as more daft.io.av functions are added. -->

### Reading Video Frames

This example shows reading a video's frames into a DataFrame.

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


```{title="Output"}
╭────────────────────────────────┬──────────────┬────────────────────┬─────────────────┬───────────┬───────────┬────────────────┬──────────────┬───────────────────────╮
│ path                           ┆ frame_index ┆ frame_time         ┆ frame_time_base ┆ frame_pts ┆ frame_dts ┆ frame_duration ┆ is_key_frame ┆ data                  │
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

```python
df = daft.read_video_frames(
    path="https://www.youtube.com/watch?v=jNQXAC9IVRw",
    image_height=480,
    image_width=640,
    is_key_frame=True,
)
```
