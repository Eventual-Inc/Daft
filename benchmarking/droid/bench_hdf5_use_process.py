"""Compare hdf5_read vs hdf5_read_many with use_process True vs False on DROID."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

import daft
from daft.datasets.droid import _resolve_trajectory_field, raw
from daft.functions.hdf5 import hdf5_read_impl, hdf5_read_many_impl
from daft.udf.udf_v2 import Func

FIELDS = ["joint_position", "gripper_position", "robot_joint_positions"]
NUM_EPISODES = 5


@dataclass
class Result:
    name: str
    seconds: float
    arrays_read: int

    @property
    def ms_per_array(self) -> float:
        return (self.seconds * 1000) / self.arrays_read


def _make_read_fn(use_process: bool) -> Func:
    return Func._from_func(
        hdf5_read_impl,
        return_dtype=daft.DataType.tensor(daft.DataType.float64()),
        unnest=False,
        use_process=use_process,
        is_batch=False,
        batch_size=None,
        max_retries=None,
        on_error=None,
        name_override=f"hdf5_read_use_process_{use_process}",
    )


def _make_read_many_fn(datasets: dict[str, str], use_process: bool) -> Func:
    return Func._from_func(
        hdf5_read_many_impl,
        return_dtype=daft.DataType.struct(
            {name: daft.DataType.tensor(daft.DataType.float64()) for name in datasets}
        ),
        unnest=False,
        use_process=use_process,
        is_batch=False,
        batch_size=None,
        max_retries=None,
        on_error=None,
        name_override=f"hdf5_read_many_use_process_{use_process}",
    )


def _episodes_df(num_episodes: int):
    return (
        raw()
        .where(daft.col("success"))
        .select("episode_dir")
        .limit(num_episodes)
        .with_column(
            "trajectory",
            daft.functions.hdf5_file(
                daft.functions.format("{}/trajectory.h5", daft.col("episode_dir"))
            ),
        )
    )


def _run_read(episodes, datasets: dict[str, str], use_process: bool) -> int:
    read_fn = _make_read_fn(use_process)
    exprs = [read_fn(daft.col("trajectory"), dataset=path).alias(name) for name, path in datasets.items()]
    result = episodes.select("trajectory", *exprs).collect().to_pydict()
    return sum(len(result[name]) for name in datasets)


def _run_read_many(episodes, datasets: dict[str, str], use_process: bool) -> int:
    read_many_fn = _make_read_many_fn(datasets, use_process)
    result = (
        episodes.select(
            "trajectory",
            daft.functions.unnest(read_many_fn(daft.col("trajectory"), datasets=datasets)),
        )
        .collect()
        .to_pydict()
    )
    return sum(len(result[name]) for name in datasets)


def timed(name: str, fn: Callable[[], int]) -> Result:
    t0 = time.perf_counter()
    arrays_read = fn()
    return Result(name=name, seconds=time.perf_counter() - t0, arrays_read=arrays_read)


def main() -> None:
    datasets = dict(_resolve_trajectory_field(f) for f in FIELDS)
    episodes = _episodes_df(NUM_EPISODES)
    arrays = NUM_EPISODES * len(datasets)

    print(f"Benchmark: {NUM_EPISODES} episodes × {len(datasets)} fields = {arrays} array reads from GCS")
    print(f"Fields: {FIELDS}\n")

    # Warm caches (network + imports).
    _run_read_many(episodes.limit(1), datasets, use_process=True)

    benchmarks = [
        ("hdf5_read_many  use_process=True", lambda: _run_read_many(episodes, datasets, True)),
        ("hdf5_read_many  use_process=False", lambda: _run_read_many(episodes, datasets, False)),
        ("hdf5_read (×3)  use_process=True", lambda: _run_read(episodes, datasets, True)),
        ("hdf5_read (×3)  use_process=False", lambda: _run_read(episodes, datasets, False)),
    ]

    results: list[Result] = []
    for name, fn in benchmarks:
        result = timed(name, fn)
        results.append(result)
        print(f"{result.name:40s} {result.seconds:7.2f}s  ({result.ms_per_array:6.0f} ms/array)")

    read_many_true = next(r for r in results if "read_many" in r.name and "True" in r.name)
    read_many_false = next(r for r in results if "read_many" in r.name and "False" in r.name)
    read_true = next(r for r in results if "read (×3)" in r.name and "True" in r.name)
    read_false = next(r for r in results if "read (×3)" in r.name and "False" in r.name)

    print()
    print(f"read_many  False/True: {read_many_false.seconds / read_many_true.seconds:.2f}×")
    print(f"read (×3)  False/True: {read_false.seconds / read_true.seconds:.2f}×")
    print(f"read_many vs read (×3), both True:  {read_true.seconds / read_many_true.seconds:.2f}×")
    print(f"read_many vs read (×3), both False: {read_false.seconds / read_many_false.seconds:.2f}×")


if __name__ == "__main__":
    main()
