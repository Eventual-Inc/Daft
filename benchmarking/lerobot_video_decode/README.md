# LeRobot video decode: per-frame → per-shard

The `daft.datasets.lerobot` reader decoded video frames with a **per-row** UDF that
re-opened the MP4 shard for every frame. Because `av.open()` on a remote shard
fetches the container index over the network, decoding N frames re-fetched the
shard N times - cost scaled ~linearly at **~3s/frame**.

This directory holds the benchmarks that diagnosed it and the fix that makes the
decode **batched**: rows sharing a shard are grouped so the shard is opened (and
fetched) once per batch.

## Where the time went (it is not decoding)

`python repro.py --rows 1 --profile` - cProfile self-time (`tottime`) for a single
frame, which is dominated by opening the shard, not decoding it:

| function | self-time |
| --- | --- |
| `av.container.core.open` (open + fetch shard index) | ~3.3s |
| decode loop (`_decode_lerobot_video_timestamp`) | ~1.3s |
| file read (`_from_file_reference`) | ~0.9s |

`av.open()` on the remote shard is the bottleneck, and the per-row UDF paid it for
every frame.

## The fix: batched decode

`_decode_lerobot_video_timestamp` in `daft/datasets/lerobot.py` is now a
`@daft.func.batch` UDF. Within each batch it groups rows by shard path, opens each
shard once, and does a single forward decode assigning the closest frame to every
requested timestamp. Output is **byte-identical** to the old per-row decode.

### Original vs batched (rows 1→10)

`sweep.py` - the original grows linearly to ~34s; the batched version stays flat at
~4s (all 10 frames share one shard → one open).

![original vs batched](charts/chart_old_vs_new.png)

| rows | original | batched |
| --- | --- | --- |
| 1 | 4.2s | 4.4s |
| 8 | **25.0s** | **3.9s** (~6.5×) |
| 10 | 34.4s | 3.9s |

8-frame output hashes matched exactly (`sha 80bdb30c…`) between versions.

## Multiprocess

Running the decode under `use_process=True` produces byte-identical output, so the
batched decode survives process serialization. Each worker/process opens the shards
in its own batches once (file handles are not shared across processes); partition by
shard to make that one download per shard per worker.

How does the change hold up as the number of workers grows? `worker_scaling.py`
reproduces the original-vs-batched frames sweep at 1/2/4/8 worker processes (8
shards, dense consecutive frames, scalar return to isolate decode compute):

![workers](charts/chart_workers.png)

At each worker count the original grows steeply with frame count while batched stays
low. Adding workers shifts the original down but with diminishing returns (it settles
around 6s at 240 frames from 4 workers on), because it re-opens and re-decodes from
the keyframe for every frame - parallelism spreads that redundant work rather than
removing it. At 240 frames, batched on one worker (2.2s) is still faster than the
original on eight (6.0s). Local copies, so this is decode-compute; parallel network
fetch of distinct shards is an extra real-cluster win not shown here.

## Running

```bash
python repro.py --rows 8             # time a decode (add --profile for the breakdown above)
python sweep.py --label batched      # rows 1..10 sweep + chart
```
