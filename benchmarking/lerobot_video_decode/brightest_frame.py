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


@daft.func.batch(return_dtype=DataType.float64())
def mean_brightness(images: Series) -> list[float]:
    import numpy as np

    return [float(np.asarray(img).mean()) for img in images.to_pylist()]


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
        mean_brightness(daft.col(camera)).alias("brightness"),
    )
    result = df.collect().to_pydict()
    wall = time.perf_counter() - t0

    n = len(result["frame_index"])
    best = max(range(n), key=lambda i: result["brightness"][i])
    print(f"decoded {n} frames of episode {episode} ({camera}) in {wall:.1f}s ({wall / n:.2f}s/frame)")
    print(
        f"brightest: frame_index={result['frame_index'][best]} "
        f"t={result['timestamp'][best]:.3f}s brightness={result['brightness'][best]:.2f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
