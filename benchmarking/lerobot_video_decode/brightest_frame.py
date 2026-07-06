"""Find the brightest frame of one episode's camera - a decode-heavy end-to-end task.

Decodes every frame of the episode, reduces each to a mean-luminance float,
and picks the frame with the highest value. Exercises the batched decode on a
real workload where frames are consumed (not collected) at full resolution.

    python brightest_frame.py [dataset=BitRobot/HIW-500-LeRobot] [episode=2534] [camera=observation.images.head]
"""

from __future__ import annotations

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
    episode = int(sys.argv[2]) if len(sys.argv) > 2 else 2534
    camera = sys.argv[3] if len(sys.argv) > 3 else "observation.images.head"

    t0 = time.perf_counter()
    df = lerobot.read(dataset, load_video_frames=[camera])
    df = df.where(daft.col("episode_index") == episode)
    df = df.select(
        "episode_index",
        "frame_index",
        "timestamp",
        mean_brightness(daft.col(camera)),  # unnests to brightness + batch_len
    )
    result = df.collect().to_pydict()
    wall = time.perf_counter() - t0

    n = len(result["frame_index"])
    best = max(range(n), key=lambda i: result["brightness"][i])
    batch_lens = result["batch_len"]
    print(f"decoded {n} frames of episode {episode} ({camera}) in {wall:.1f}s ({wall / n:.2f}s/frame)")
    print(
        f"brightest: frame_index={result['frame_index'][best]} "
        f"t={result['timestamp'][best]:.3f}s brightness={result['brightness'][best]:.2f}"
    )
    print(f"udf batch sizes: min={min(batch_lens)} max={max(batch_lens)} (n_batches~{n / (sum(batch_lens) / n):.0f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
