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

![brightest frame: old vs new vs ray](charts/chart_brightest_frame.png)

| reader | runner | frames | wall | per frame |
| --- | --- | --- | --- | --- |
| original (per-row) | native | 102 | 685.2s | 6.72s |
| batched (this PR) | native | 102 | 60.4s | 0.59s |
| batched (this PR) | ray (local) | 102 | 71.3s | 0.70s |

Camera: observation.images.head (1280x480). 11.3x faster than the original;
all three runs found the same brightest frame (`frame_index=74`, t=2.467s,
mean brightness 124.54).

The original-reader run used the merge-base `daft/datasets/lerobot.py` swapped
into a copy of the package on `PYTHONPATH` (the fix is Python-only), same
machine, runs not concurrent.

## Distributed runner observations (local ray, 14-core M-series, 36 GB)

- Batch sizes observed by the UDF (`batch_len` probe): min 6, max 16 - the
  `into_batches(16)` bound holds on the ray runner; no oversized batches.
- Total RSS across all 19 ray processes climbed to ~2.9 GB and plateaued
  (includes ray's own baseline: GCS, raylet, idle workers). No runaway growth.
- Ray overhead vs native was ~11s, mostly startup.

The per-frame rate on a short episode is dominated by fixed overhead
(metadata reads, shard opens); longer episodes amortize it.
