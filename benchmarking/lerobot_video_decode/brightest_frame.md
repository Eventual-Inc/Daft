# Brightest frame: an end-to-end decode task

A small real-workload demo of the batched decode: find the brightest frame of
one episode's camera. Every frame of the episode is decoded, reduced to a
mean-luminance float by a second batch UDF, and the max is picked. The decode
and brightness UDFs run back to back in the same streaming pipeline, so frames
only ever exist 16 at a time - decoded images are never materialized.

## Run

```bash
python brightest_frame.py [dataset] [episode] [camera]
```

## Result: BitRobot/HIW-500-LeRobot, episode 2534

Dataset: 23,743 episodes / 40.8M frames, 3 AV1 cameras (av1, yuv420p,
keyframe interval 2). Episode 2534 is ~100 frames (dataset median is 1,140).
Remote reads over `hf://`, single machine, native runner; measured 2026-07-06.

| episode | camera | frames | wall | per frame |
| --- | --- | --- | --- | --- |
| 2534 | observation.images.head (1280x480) | 102 | 60.4s | 0.59s |

Brightest frame: `frame_index=74` (t=2.467s), mean brightness 124.54.

The per-frame rate on a short episode is dominated by fixed overhead
(metadata reads, shard opens); longer episodes amortize it.
