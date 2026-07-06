"""Find the brightest frame of one episode's camera - a decode-heavy end-to-end task.

Decodes every frame of the episode, reduces each to a mean-luminance float,
and picks the frame with the highest value. Exercises the batched decode on a
real workload where frames are consumed (not collected) at full resolution.

    python brightest_frame.py [dataset=BitRobot/HIW-500-LeRobot] [episode=2534] [camera=observation.images.head]
"""

from __future__ import annotations

import os
import sys
import time

import daft
from daft import DataType, Series
from daft.datasets import lerobot


@daft.func.batch(
    return_dtype=DataType.struct({"brightness": DataType.float64(), "batch_len": DataType.int64()}),
    unnest=True,
)
def mean_brightness(images: Series) -> list[dict]:
    """Mean luminance per frame, plus the size of the batch each row arrived in.

    ``batch_len`` probes the batch sizes the UDF actually receives (the decode
    UDF consumes the same morsel stream), so runs double as evidence that
    ``into_batches(16)`` bounds batches on the runner used.
    """
    import numpy as np

    n = len(images)
    return [{"brightness": float(np.asarray(img).mean()), "batch_len": n} for img in images.to_pylist()]


def main() -> int:
    dataset = sys.argv[1] if len(sys.argv) > 1 else "BitRobot/HIW-500-LeRobot"
    episodes = [int(e) for e in (sys.argv[2] if len(sys.argv) > 2 else "2534").split(",")]
    camera = sys.argv[3] if len(sys.argv) > 3 else "observation.images.head"

    # Anonymous HF requests get throttled (429) once ~10 shards are read
    # concurrently; a free read token raises the limit substantially.
    token = os.environ.get("HF_TOKEN")
    if not token:
        token_path = os.path.expanduser("~/.cache/huggingface/token")
        if os.path.exists(token_path):
            token = open(token_path).read().strip()
    io_config = daft.io.IOConfig(hf=daft.io.HuggingFaceConfig(token=token)) if token else None

    t0 = time.perf_counter()
    df = lerobot.read(dataset, load_video_frames=[camera], io_config=io_config)
    df = df.where(daft.col("episode_index").is_in(episodes))
    df = df.select(
        "episode_index",
        "frame_index",
        "timestamp",
        mean_brightness(daft.col(camera)),  # unnests to brightness + batch_len
    )
    result = df.collect().to_pydict()
    wall = time.perf_counter() - t0

    n = len(result["frame_index"])
    batch_lens = result["batch_len"]
    print(f"decoded {n} frames of {len(episodes)} episode(s) ({camera}) in {wall:.1f}s ({wall / n:.2f}s/frame)")
    for ep in episodes:
        rows = [i for i in range(n) if result["episode_index"][i] == ep]
        best = max(rows, key=lambda i: result["brightness"][i])
        print(
            f"episode {ep}: brightest frame_index={result['frame_index'][best]} "
            f"t={result['timestamp'][best]:.3f}s brightness={result['brightness'][best]:.2f} "
            f"({len(rows)} frames)"
        )
    print(f"udf batch sizes: min={min(batch_lens)} max={max(batch_lens)} (n_batches~{n / (sum(batch_lens) / n):.0f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
