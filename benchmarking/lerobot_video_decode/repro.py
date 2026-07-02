"""Reproduce / time the LeRobot per-frame video decode.

Before the batched fix, each frame re-opened (and re-fetched) the remote MP4 shard,
so cost scaled ~linearly at ~3s/frame. After the fix, rows sharing a shard are
grouped and the shard is opened once per batch.

    python repro.py              # 1 frame
    python repro.py --rows 8     # 8 frames
    python repro.py --rows 8 --profile
"""

from __future__ import annotations

import argparse
import os
import time

os.environ["DAFT_PROGRESS_BAR"] = "0"

DATASET = "pepijn223/egodex-test"
IMAGE_COLUMN = "observation.image"


def decode_rows(n: int) -> None:
    from daft.datasets import lerobot

    df = lerobot.read(DATASET, load_video_frames=IMAGE_COLUMN).limit(n)
    df.select("episode_index", "frame_index", IMAGE_COLUMN).collect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Time the LeRobot video decode.")
    parser.add_argument("--rows", type=int, default=1, help="frames to decode (default: 1)")
    parser.add_argument("--profile", action="store_true", help="cProfile the run (top cumulative calls)")
    args = parser.parse_args()

    import daft

    if args.profile:
        import cProfile
        import io
        import pstats

        profiler = cProfile.Profile()
        start = time.perf_counter()
        profiler.runcall(decode_rows, args.rows)
        elapsed = time.perf_counter() - start
        report = io.StringIO()
        pstats.Stats(profiler, stream=report).sort_stats("cumulative").print_stats(30)
        print(report.getvalue())
    else:
        start = time.perf_counter()
        decode_rows(args.rows)
        elapsed = time.perf_counter() - start

    print(f"daft {daft.__version__}: decoded {args.rows} frame(s) in {elapsed:.2f}s ({elapsed / args.rows:.2f}s/frame)")


if __name__ == "__main__":
    main()
