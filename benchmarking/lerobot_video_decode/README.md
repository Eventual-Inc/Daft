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

With multiple shards the decode parallelizes across worker processes
(`.with_concurrency(N)`) - `worker_scaling.py`, 8 shards, full-span decode:

![workers](charts/chart_workers.png)

| workers | wall | speedup |
| --- | --- | --- |
| 1 | 22.9s | 1.0× |
| 2 | 15.3s | 1.5× |
| 4 | 11.0s | 2.1× |
| 8 | 7.6s | 3.0× |

This isolates decode-compute scaling: each row returns a scalar so image
serialization doesn't dominate, and the shards are local copies (no network). The
sub-linear scaling is process-spawn overhead plus one-batch-per-shard granularity.
On a real cluster, parallel network fetch of distinct shards is an additional win
not captured here.

## Running

```bash
python repro.py --rows 8             # time a decode (add --profile for the breakdown above)
python sweep.py --label batched      # rows 1..10 sweep + chart
```
