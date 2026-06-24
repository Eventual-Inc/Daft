"""Compare Daft trajectory loading vs handrolled h5py.

Run:
    DAFT_RUNNER=native .venv/bin/python benchmarking/droid/compare_h5py.py

Fairness notes:
- All paths read the same HDF5 datasets from GCS.
- Handrolled baselines use Daft's PyDaftFile for download so IO transport is identical;
  we are comparing orchestration patterns, not different GCS clients.
- "naive handroll" mirrors today's Daft behavior: download + h5py open per field.
- "optimal handroll" is what idiomatic h5py looks like: download once, open once, read N fields.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import Callable

import daft
from daft.daft import PyDaftFile
from daft.datasets.droid import _resolve_trajectory_field, raw, trajectory
from daft.file.hdf5 import Hdf5File

FIELDS = ["joint_position", "gripper_position", "robot_joint_positions"]
NUM_EPISODES = 3


@dataclass
class Result:
    name: str
    seconds: float
    arrays_read: int

    @property
    def ms_per_array(self) -> float:
        return (self.seconds * 1000) / self.arrays_read


def _hdf5_paths(fields: list[str]) -> list[str]:
    return [_resolve_trajectory_field(f)[1] for f in fields]


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


def _materialize(url: str) -> tempfile._TemporaryFileWrapper[bytes]:
    file = Hdf5File(url)
    with PyDaftFile._from_file_reference(file._inner) as f:  # noqa: SLF001
        temp = tempfile.NamedTemporaryFile(prefix="daft_benchmark_")
        f.seek(0)
        size = f.size()
        if not f._supports_range_requests() or size < 1024:  # noqa: SLF001
            temp.write(f.read())
        else:
            shutil.copyfileobj(f, temp, length=size)
        temp.seek(0)
        return temp


def handroll_naive(urls: list[str], paths: list[str]) -> int:
    import h5py

    count = 0
    for url in urls:
        for path in paths:
            temp = _materialize(url)
            try:
                with h5py.File(temp.name, "r") as h5:
                    _ = h5[path][()]
                    count += 1
            finally:
                temp.close()
    return count


def handroll_optimal(urls: list[str], paths: list[str]) -> int:
    import h5py

    count = 0
    for url in urls:
        temp = _materialize(url)
        try:
            with h5py.File(temp.name, "r") as h5:
                for path in paths:
                    _ = h5[path][()]
                    count += 1
        finally:
            temp.close()
    return count


def handroll_hdf5file_naive(urls: list[str], paths: list[str]) -> int:
    count = 0
    for url in urls:
        file = Hdf5File(url)
        for path in paths:
            _ = file.read(path)
            count += 1
    return count


def handroll_hdf5file_optimal(urls: list[str], paths: list[str]) -> int:
    count = 0
    for url in urls:
        file = Hdf5File(url)
        with file.open() as h5:
            for path in paths:
                _ = h5[path][()]
                count += 1
    return count


def daft_trajectory(episodes_df, fields: list[str]) -> int:
    result = trajectory(episodes_df, fields=fields).collect().to_pydict()
    return sum(len(result[f]) for f in fields)


def daft_trajectory_end_to_end(num_episodes: int, fields: list[str]) -> int:
    episodes = raw().where(daft.col("success")).limit(num_episodes)
    return daft_trajectory(episodes, fields)


def episodes_df(num_episodes: int):
    return raw().where(daft.col("success")).limit(num_episodes)


def timed(name: str, fn: Callable[[], int]) -> Result:
    t0 = time.perf_counter()
    arrays_read = fn()
    return Result(name=name, seconds=time.perf_counter() - t0, arrays_read=arrays_read)


def main() -> None:
    print("Resolving episode URLs...", flush=True)
    paths = _hdf5_paths(FIELDS)
    urls = _episode_urls(NUM_EPISODES)
    print("Building episode dataframe...", flush=True)
    episodes = episodes_df(NUM_EPISODES)
    arrays = len(urls) * len(paths)

    print(f"Benchmark: {len(urls)} episodes × {len(paths)} fields = {arrays} array reads from GCS")
    print(f"Fields: {FIELDS}\n")
    print("(Episode catalog discovery is done once up front, outside timed sections.)\n")

    # Warm network + import caches.
    handroll_optimal(urls[:1], paths[:1])

    print("=== Trajectory IO only (given episode rows) ===")
    io_benchmarks = [
        ("Daft trajectory().collect()", lambda: daft_trajectory(episodes, FIELDS)),
        ("Handroll h5py optimal (1 download, 1 open / ep)", lambda: handroll_optimal(urls, paths)),
        ("Handroll h5py naive (download+open per field)", lambda: handroll_naive(urls, paths)),
        ("Daft Hdf5File optimal (1 open / ep)", lambda: handroll_hdf5file_optimal(urls, paths)),
        ("Daft Hdf5File naive (open per field)", lambda: handroll_hdf5file_naive(urls, paths)),
    ]

    io_results: list[Result] = []
    for name, fn in io_benchmarks:
        result = timed(name, fn)
        io_results.append(result)
        print(f"{result.name:50s} {result.seconds:7.2f}s  ({result.ms_per_array:6.0f} ms/array)")

    optimal = next(r for r in io_results if "Handroll h5py optimal" in r.name)
    daft_result = next(r for r in io_results if r.name.startswith("Daft trajectory"))
    naive = next(r for r in io_results if "Handroll h5py naive" in r.name)
    hdf5_opt = next(r for r in io_results if "Hdf5File optimal" in r.name)

    print()
    print(f"Daft trajectory / optimal handroll: {daft_result.seconds / optimal.seconds:.1f}×")
    print(f"Daft trajectory / naive handroll:   {daft_result.seconds / naive.seconds:.1f}×")
    print(f"Hdf5File optimal  / optimal handroll: {hdf5_opt.seconds / optimal.seconds:.1f}×  (transport parity check)")

    print("\n=== End-to-end (raw catalog + trajectory) ===")
    e2e = timed("Daft raw().limit(N) + trajectory().collect()", lambda: daft_trajectory_end_to_end(NUM_EPISODES, FIELDS))
    print(f"{e2e.name:50s} {e2e.seconds:7.2f}s  ({e2e.ms_per_array:6.0f} ms/array)")
    print()
    print("How to interpret:")
    print("- Optimal handroll is the IO floor: one GCS fetch + one h5py open per episode.")
    print("- Naive handroll matches current Daft: re-download the whole .h5 per field.")
    print("- Daft ~= naive today; fixing Hdf5File to cache opens should reach optimal.")


if __name__ == "__main__":
    main()
