# `read_video_frames` streaming performance

Tracking doc for closing the gap between `daft.io.av.read_video_frames` and raw PyAV,
and making Daft viable as a live video dataloader (no precompute pass).

Found while building the LeRobot GPU-starvation benchmark: raw PyAV decodes a
1080p AV1 file at ~205 fps/core, `read_video_frames` delivers ~35 fps/core over the
same file - a ~6x gap that is all plumbing, not decode (libdav1d is doing the same
work in both cases). Related: [#7184](https://github.com/Eventual-Inc/Daft/pull/7184)
fixed the same flavor of problem (per-frame overhead) in `daft.datasets.lerobot`.

## The problems

All verified against current `main` (`daft/io/av/_read_video_frames.py`).

1. **No limit pushdown.** `_VideoFramesSource.get_tasks(pushdowns)` ignores
   `pushdowns` entirely, and `_list_frames` decodes the whole container. A
   `.limit(4)` or `.show()` on one file costs the same as decoding the entire file.

2. **Tiny record batches + per-frame reformat.** `_max_partition_size` is 10MB; a
   1080p RGB frame is ~6.2MB, so batches carry 1-2 frames each and per-batch
   overhead (Python <-> Rust crossings) dominates. `frame.reformat(...)` +
   `to_ndarray()` also run unconditionally per emitted frame, even when no resize
   is requested.

3. **Non-incremental streaming to torch.** `read_video_frames(...).to_torch_iter_dataset()`
   buffers a large first chunk before yielding any row (~55-77s for a single 512px
   file; >700s on a full LeRobot-reader plan, an effective hang). Not affected by
   `maintain_order=False` or a smaller `default_morsel_size` - this is output
   buffering in the execution engine, not in the video source.

## Plan: isolate and fix one by one

| # | fix | scope | status |
|---|---|---|---|
| 1 | raise/repack record batch size, skip no-op reformat | `_read_video_frames.py` only | next up |
| 2 | limit pushdown into the source task | source + pushdown plumbing | after 1 |
| 3 | incremental yield to `to_torch_iter_dataset` | execution engine | needs diagnosis |

Ordered by isolation: (1) is contained in one file and directly measurable with the
fps probe; (2) touches scan pushdown plumbing; (3) is the engine-level one and needs
its own diagnosis before a fix is scoped.

Benchmarks/probes for each fix will land in this directory as they are built
(same pattern as [`../lerobot_video_decode`](../lerobot_video_decode)).
