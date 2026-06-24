"""Compare h5py.File(tmp.name) vs h5py.File(tmp) for Hdf5File.read on DROID."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Literal

import daft
from daft.datasets.droid import _resolve_trajectory_field, raw
from daft.dependencies import h5py
from daft.file.hdf5 import Hdf5File

FIELDS = ["joint_position", "gripper_position", "robot_joint_positions"]
NUM_EPISODES = 5
OpenMode = Literal["name", "fileobj"]


@dataclass
class Result:
    name: str
    seconds: float
    arrays_read: int

    @property
    def ms_per_array(self) -> float:
        return (self.seconds * 1000) / self.arrays_read


def _episode_urls(num_episodes: int) -> list[str]:
    rows = (
        raw()
        .where(daft.col("success"))
        .select("episode_dir")
        .limit(num_episodes)
        .collect()
        .to_pydict()
    )
    return [f"{d}/trajectory.h5" for d in rows["episode_dir"]]


def _read_datasets(h5: h5py.File, datasets: dict[str, str]) -> int:
    count = 0
    for path in datasets.values():
        _ = h5[path][()]
        count += 1
    return count


def _read_with_open_mode(url: str, datasets: dict[str, str], open_mode: OpenMode) -> int:
    file = Hdf5File(url)
    with file.to_tempfile() as tmp:
        if open_mode == "name":
            with h5py.File(tmp.name, "r") as h5:
                return _read_datasets(h5, datasets)
        with h5py.File(tmp, "r") as h5:
            return _read_datasets(h5, datasets)


def _run_all(urls: list[str], datasets: dict[str, str], open_mode: OpenMode) -> int:
    total = 0
    for url in urls:
        total += _read_with_open_mode(url, datasets, open_mode)
    return total


def timed(name: str, fn: Callable[[], int]) -> Result:
    t0 = time.perf_counter()
    arrays_read = fn()
    return Result(name=name, seconds=time.perf_counter() - t0, arrays_read=arrays_read)


def main() -> None:
    datasets = dict(_resolve_trajectory_field(f) for f in FIELDS)
    urls = _episode_urls(NUM_EPISODES)
    arrays = len(urls) * len(datasets)

    print(f"Benchmark: {len(urls)} episodes × {len(datasets)} fields = {arrays} array reads from GCS")
    print("Compares h5py open via tmp.name (path) vs tmp (file object)\n")

    # Warm network + import caches.
    _run_all(urls[:1], datasets, "name")

    benchmarks = [
        ("h5py.File(tmp.name, 'r')", lambda: _run_all(urls, datasets, "name")),
        ("h5py.File(tmp, 'r')", lambda: _run_all(urls, datasets, "fileobj")),
    ]

    results: list[Result] = []
    for label, fn in benchmarks:
        result = timed(label, fn)
        results.append(result)
        print(f"{result.name:30s} {result.seconds:7.2f}s  ({result.ms_per_array:6.0f} ms/array)")

    name_result, fileobj_result = results
    print()
    print(f"tmp.name / tmp (fileobj): {name_result.seconds / fileobj_result.seconds:.3f}×")
    if name_result.seconds > fileobj_result.seconds:
        pct = (name_result.seconds / fileobj_result.seconds - 1) * 100
        print(f"tmp.name is {pct:.1f}% slower than tmp")
    else:
        pct = (fileobj_result.seconds / name_result.seconds - 1) * 100
        print(f"tmp is {pct:.1f}% slower than tmp.name")


if __name__ == "__main__":
    main()
