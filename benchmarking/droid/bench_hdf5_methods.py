"""Benchmark all Hdf5File methods and expression helpers on DROID.

Run:
    DAFT_RUNNER=native .venv/bin/python benchmarking/droid/bench_hdf5_methods.py
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

import daft
from daft.datasets.droid import _resolve_trajectory_field, raw
from daft.file.hdf5 import Hdf5File
from daft.functions.hdf5 import hdf5_keys_impl, hdf5_read_impl, hdf5_read_many_impl, hdf5_visit_impl

FIELDS = ["joint_position", "gripper_position", "robot_joint_positions"]
NUM_EPISODES = 5


@dataclass
class Result:
    name: str
    seconds: float
    calls: int

    @property
    def ms_per_call(self) -> float:
        return (self.seconds * 1000) / self.calls


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


def _datasets() -> dict[str, str]:
    return dict(_resolve_trajectory_field(f) for f in FIELDS)


def _episodes_df(urls: list[str]):
    return daft.from_pydict({"trajectory": [daft.Hdf5File(url) for url in urls]})


def timed(name: str, fn: Callable[[], None], calls: int) -> Result:
    t0 = time.perf_counter()
    fn()
    return Result(name=name, seconds=time.perf_counter() - t0, calls=calls)


def main() -> None:
    urls = _episode_urls(NUM_EPISODES)
    datasets = _datasets()
    episodes = _episodes_df(urls)
    n = len(urls)

    print(f"Benchmark: {n} DROID episodes from GCS")
    print(f"Read fields: {FIELDS}\n")

    # Warm network + import caches.
    Hdf5File(urls[0]).keys()

    standalone: list[tuple[str, Callable[[], None], int]] = [
        ("Hdf5File.keys()", lambda: [Hdf5File(url).keys() for url in urls], n),
        ("Hdf5File.metadata()", lambda: [Hdf5File(url).metadata() for url in urls], n),
        ("Hdf5File.attrs()", lambda: [Hdf5File(url).attrs() for url in urls], n),
        ("Hdf5File.visit()", lambda: [Hdf5File(url).visit() for url in urls], n),
        (
            "Hdf5File.read(single)",
            lambda: [Hdf5File(url).read(datasets["joint_position"]) for url in urls],
            n,
        ),
        (
            "Hdf5File.read(mapping)",
            lambda: [Hdf5File(url).read(datasets) for url in urls],
            n,
        ),
    ]

    expressions: list[tuple[str, Callable[[], None], int]] = [
        (
            "hdf5_keys (expr)",
            lambda: episodes.select(
                daft.functions.hdf5_keys(daft.col("trajectory"), group="/").alias("keys")
            ).collect(),
            n,
        ),
        (
            "hdf5_visit (expr)",
            lambda: episodes.select(
                daft.functions.hdf5_visit(daft.col("trajectory"), group="/").alias("objects")
            ).collect(),
            n,
        ),
        (
            "hdf5_read x3 (expr)",
            lambda: episodes.select(
                *[
                    daft.functions.hdf5_read(daft.col("trajectory"), dataset=path).alias(name)
                    for name, path in datasets.items()
                ]
            ).collect(),
            n,
        ),
        (
            "hdf5_read_many (expr)",
            lambda: episodes.select(
                daft.functions.unnest(
                    daft.functions.hdf5_read_many(daft.col("trajectory"), datasets)
                )
            ).collect(),
            n,
        ),
    ]

    impls: list[tuple[str, Callable[[], None], int]] = [
        (
            "hdf5_keys_impl",
            lambda: [hdf5_keys_impl(Hdf5File(url), group="/") for url in urls],
            n,
        ),
        (
            "hdf5_visit_impl",
            lambda: [hdf5_visit_impl(Hdf5File(url), group="/") for url in urls],
            n,
        ),
        (
            "hdf5_read_impl x3",
            lambda: [
                hdf5_read_impl(Hdf5File(url), dataset=path)
                for url in urls
                for path in datasets.values()
            ],
            n * len(datasets),
        ),
        (
            "hdf5_read_many_impl",
            lambda: [hdf5_read_many_impl(Hdf5File(url), datasets=datasets) for url in urls],
            n,
        ),
    ]

    sections = [
        ("=== Hdf5File standalone methods ===", standalone),
        ("=== Expression functions (DataFrame) ===", expressions),
        ("=== UDF impls (what expressions call) ===", impls),
    ]

    for header, benchmarks in sections:
        print(header)
        for name, fn, calls in benchmarks:
            result = timed(name, fn, calls)
            print(f"{result.name:30s} {result.seconds:7.2f}s  ({result.ms_per_call:6.0f} ms/call)")
        print()


if __name__ == "__main__":
    main()
