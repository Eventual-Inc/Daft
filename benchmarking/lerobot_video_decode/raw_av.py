"""Isolate WHERE the decode cost goes, using raw PyAV (no Daft).

Compares open-once vs open-per-frame for the remote shard URL and, if given, a
local copy. Conclusion: parsing and decoding are cheap; the cost is the network
fetch that av.open() does on the remote shard, paid per frame by the old reader.

    python raw_av.py --remote
    python raw_av.py --local /path/to/shard.mp4
"""

from __future__ import annotations

import argparse
import time

import av

REMOTE_URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
N = 8


def timeit(label: str, fn) -> None:
    start = time.perf_counter()
    result = fn()
    print(f"{label:32s} {time.perf_counter() - start:7.3f}s   {result}")


def open_once_decode_n(src: str) -> str:
    out = 0
    with av.open(src) as container:
        stream = container.streams.video[0]
        for frame in container.decode(stream):
            frame.to_ndarray(format="rgb24")
            out += 1
            if out >= N:
                break
    return f"decoded {out} (1 open)"


def open_per_frame(src: str) -> str:
    out = 0
    for _ in range(N):
        with av.open(src) as container:
            stream = container.streams.video[0]
            container.seek(0, backward=True)
            for frame in container.decode(stream):
                frame.to_ndarray(format="rgb24")
                out += 1
                break
    return f"decoded {out} ({N} opens)"


def main() -> None:
    parser = argparse.ArgumentParser(description="Isolate PyAV open vs decode cost.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--local", metavar="PATH", help="path to a local .mp4 shard")
    group.add_argument("--remote", action="store_true", help="use the remote HF shard URL")
    args = parser.parse_args()

    src = args.local if args.local else REMOTE_URL
    print(f"source: {src}\n")
    timeit(f"open once + decode {N}", lambda: open_once_decode_n(src))
    timeit(f"open per frame x{N}", lambda: open_per_frame(src))


if __name__ == "__main__":
    main()
