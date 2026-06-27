"""Benchmark Daft Hdf5File access patterns.

This script compares the HDF5 operations exposed by ``daft.Hdf5File`` across
several ways of opening the same file:

- ``daft``: h5py reads through ``Hdf5File.open(buffer_size=...)``.
- ``direct``: h5py opens the local path directly. This is the local baseline.
- ``python``: h5py reads through Python's built-in buffered file object.
- ``safe-tempfile``: copy through Daft in fixed-size chunks, then open the
  temporary local path directly.
- ``daft-tempfile``: use Daft's production ``File.to_tempfile()`` path. This is
  opt-in and size-guarded because it copies the whole file.

For non-Daft strategies, the script runs the same operation body as the
corresponding Hdf5File method so the open strategy is the only intentional
change.

Examples:
    python benchmarking/hdf5/hdf5_file_benchmark.py --path /data/trajectory.h5

    python benchmarking/hdf5/hdf5_file_benchmark.py \
      --path s3://bucket/trajectory.h5 \
      --strategies daft \
      --buffer-sizes 1024,4096,65536,1048576,16777216 \
      --datasets action/proprio observation/image

    python benchmarking/hdf5/hdf5_file_benchmark.py \
      --create-sample .tmp/hdf5-bench/sample.h5 \
      --path .tmp/hdf5-bench/sample.h5 \
      --profile-dir .tmp/hdf5-bench/profiles
"""

from __future__ import annotations

import argparse
import contextlib
import cProfile
import csv
import io
import math
import os
import pathlib
import pstats
import signal
import statistics
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
from typing import Any

import h5py
import numpy as np

from daft.file.hdf5 import HDF5_DEFAULT_BUFFER_SIZE, HDF5_SCAN_BUFFER_SIZE, Hdf5File

DEFAULT_BUFFER_SIZES = (
    HDF5_SCAN_BUFFER_SIZE,
    4 * 1024,
    HDF5_DEFAULT_BUFFER_SIZE,
    1024 * 1024,
    16 * 1024 * 1024,
)
DEFAULT_METHODS = ("open_raw", "open_h5py", "keys", "attrs", "visit", "metadata", "read", "read_fields")
DEFAULT_STRATEGIES = ("daft", "direct", "python")
HDF5_METADATA_FIELDS = ("h5path", "kind", "shape", "dtype", "chunks", "compression")


class BenchmarkTimeout(TimeoutError):
    pass


@dataclass
class IOStats:
    read_calls: int = 0
    readinto_calls: int = 0
    seek_calls: int = 0
    tell_calls: int = 0
    bytes_requested: int = 0
    bytes_returned: int = 0
    max_read_request: int = 0
    read_time_s: float = 0.0
    seek_time_s: float = 0.0
    estimated_range_gets: int = 0
    estimated_range_bytes: int = 0


@dataclass(frozen=True)
class DatasetInfo:
    path: str
    shape: tuple[int, ...]
    dtype: str
    nbytes: int | None


@dataclass
class BenchmarkRow:
    method: str
    strategy: str
    buffer_size: int | None
    run: int
    status: str
    wall_ms: float
    result: str
    error: str
    profile_path: str
    read_calls: int = 0
    readinto_calls: int = 0
    seek_calls: int = 0
    tell_calls: int = 0
    bytes_requested: int = 0
    bytes_returned: int = 0
    max_read_request: int = 0
    estimated_range_gets: int = 0
    estimated_range_bytes: int = 0
    estimated_waste_ratio: float = 0.0
    read_time_ms: float = 0.0
    seek_time_ms: float = 0.0


class CountingFile:
    """File-object proxy that counts h5py-level read and seek behavior.

    For the Daft strategy, it also estimates object-store range fetches caused
    by Rust's ``BufReader``. The estimate is intentionally simple: every seek
    invalidates the buffer, and the next read fetches at least ``buffer_size``
    bytes unless h5py asked for more.
    """

    def __init__(
        self,
        inner: Any,
        *,
        file_size: int | None,
        buffer_size: int | None,
        estimate_ranges: bool,
    ) -> None:
        self._inner = inner
        self._file_size = file_size
        self._buffer_size = buffer_size
        self._estimate_ranges = estimate_ranges and buffer_size is not None
        self._range_start = -1
        self._range_end = -1
        self.stats = IOStats()

    def _position(self) -> int:
        try:
            return int(self._inner.tell())
        except (AttributeError, OSError, TypeError, ValueError):
            return 0

    def _remaining_size(self, pos: int, requested: int) -> int:
        if requested >= 0:
            return requested
        if self._file_size is None:
            return 0
        return max(self._file_size - pos, 0)

    def _estimate_fetches(self, start: int, requested: int) -> None:
        if not self._estimate_ranges or requested <= 0:
            return

        assert self._buffer_size is not None
        pos = start
        remaining = requested
        while remaining > 0:
            if self._range_start <= pos < self._range_end:
                available = min(remaining, self._range_end - pos)
                pos += available
                remaining -= available
                continue

            fetch_len = max(remaining, self._buffer_size)
            if self._file_size is not None:
                fetch_len = max(0, min(fetch_len, self._file_size - pos))
            if fetch_len <= 0:
                return

            self.stats.estimated_range_gets += 1
            self.stats.estimated_range_bytes += fetch_len
            self._range_start = pos
            self._range_end = pos + fetch_len

    def read(self, size: int = -1, /) -> bytes:
        pos = self._position()
        requested = self._remaining_size(pos, size)
        self._estimate_fetches(pos, requested)

        self.stats.read_calls += 1
        self.stats.bytes_requested += max(requested, 0)
        self.stats.max_read_request = max(self.stats.max_read_request, max(requested, 0))

        start = time.perf_counter()
        data = self._inner.read(size)
        self.stats.read_time_s += time.perf_counter() - start
        self.stats.bytes_returned += len(data)
        return data

    def readinto(self, b: bytearray | memoryview) -> int:
        self.stats.readinto_calls += 1
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        self.stats.seek_calls += 1
        if self._estimate_ranges:
            self._range_start = -1
            self._range_end = -1

        start = time.perf_counter()
        result = self._inner.seek(offset, whence)
        self.stats.seek_time_s += time.perf_counter() - start
        return int(result)

    def tell(self) -> int:
        self.stats.tell_calls += 1
        return int(self._inner.tell())

    def flush(self) -> None:
        flush = getattr(self._inner, "flush", None)
        if flush is not None:
            flush()

    def close(self) -> None:
        close = getattr(self._inner, "close", None)
        if close is not None:
            close()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return bool(getattr(self._inner, "closed", False))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class OpenStrategy:
    def __init__(
        self,
        name: str,
        path: str,
        hdf5_file: Hdf5File,
        *,
        file_size: int | None,
        buffer_size: int | None,
        tempfile_max_bytes: int,
        tempfile_copy_chunk_size: int,
    ) -> None:
        self.name = name
        self.path = path
        self.hdf5_file = hdf5_file
        self.file_size = file_size
        self.buffer_size = buffer_size
        self.tempfile_max_bytes = tempfile_max_bytes
        self.tempfile_copy_chunk_size = tempfile_copy_chunk_size

    @contextlib.contextmanager
    def raw(self) -> Iterator[tuple[Any, IOStats]]:
        if self.name != "daft":
            raise RuntimeError("open_raw only applies to the daft strategy")

        with self.hdf5_file.open(buffer_size=self.buffer_size) as raw:
            counted = CountingFile(
                raw,
                file_size=self.file_size,
                buffer_size=self.buffer_size,
                estimate_ranges=True,
            )
            yield counted, counted.stats

    @contextlib.contextmanager
    def h5(self) -> Iterator[tuple[h5py.File, IOStats]]:
        if self.name == "direct":
            with h5py.File(self.path, "r") as h5:
                yield h5, IOStats()
            return

        if self.name == "python":
            buffering = -1 if self.buffer_size is None else self.buffer_size
            with open(self.path, "rb", buffering=buffering) as raw:
                counted = CountingFile(
                    raw,
                    file_size=self.file_size,
                    buffer_size=self.buffer_size,
                    estimate_ranges=False,
                )
                with h5py.File(counted, "r") as h5:
                    yield h5, counted.stats
            return

        if self.name == "daft":
            with self.hdf5_file.open(buffer_size=self.buffer_size) as raw:
                counted = CountingFile(
                    raw,
                    file_size=self.file_size,
                    buffer_size=self.buffer_size,
                    estimate_ranges=True,
                )
                with h5py.File(counted, "r") as h5:
                    yield h5, counted.stats
            return

        if self.name == "safe-tempfile":
            with self._safe_tempfile() as temp_path, h5py.File(temp_path, "r") as h5:
                yield h5, IOStats()
            return

        if self.name == "daft-tempfile":
            self._check_tempfile_size()
            with self.hdf5_file.to_tempfile() as temp_file, h5py.File(temp_file.name, "r") as h5:
                yield h5, IOStats()
            return

        raise ValueError(f"Unknown strategy: {self.name}")

    def _check_tempfile_size(self) -> None:
        if self.file_size is None:
            raise RuntimeError("Tempfile strategies require a known file size")
        if self.file_size > self.tempfile_max_bytes:
            raise RuntimeError(
                f"Refusing to copy {format_bytes(self.file_size)} to a tempfile; "
                f"raise --tempfile-max-bytes to allow it"
            )

    @contextlib.contextmanager
    def _safe_tempfile(self) -> Iterator[str]:
        self._check_tempfile_size()
        suffix = pathlib.Path(self.path).suffix or ".h5"
        temp_fd, temp_name = tempfile.mkstemp(prefix="daft_hdf5_bench_", suffix=suffix)
        os.close(temp_fd)
        try:
            with self.hdf5_file.open(buffer_size=self.buffer_size) as raw, open(temp_name, "wb") as out:
                while True:
                    chunk = raw.read(self.tempfile_copy_chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
            yield temp_name
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_name)


def parse_csv_ints(value: str) -> tuple[int, ...]:
    result = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        result.append(parse_size(part))
    if not result:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return tuple(result)


def parse_csv_strs(value: str) -> tuple[str, ...]:
    result = tuple(part.strip() for part in value.split(",") if part.strip())
    if not result:
        raise argparse.ArgumentTypeError("expected at least one value")
    return result


def parse_size(value: str) -> int:
    text = value.strip().lower().replace("_", "")
    multiplier = 1
    for suffix, factor in (
        ("kib", 1024),
        ("kb", 1024),
        ("mib", 1024**2),
        ("mb", 1024**2),
        ("gib", 1024**3),
        ("gb", 1024**3),
        ("b", 1),
    ):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)]
            break
    try:
        return int(float(text) * multiplier)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid size: {value}") from exc


def is_local_path(path: str) -> bool:
    return "://" not in path


def format_bytes(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value < 1024:
        return f"{value} B"
    units = ("KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        size /= 1024.0
        if abs(size) < 1024.0:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} PiB"


def summarize_value(value: Any) -> str:
    if isinstance(value, np.ndarray):
        return f"ndarray shape={value.shape} dtype={value.dtype} nbytes={format_bytes(value.nbytes)}"
    if isinstance(value, dict):
        return f"dict keys={list(value)[:5]} len={len(value)}"
    if isinstance(value, list):
        return f"list len={len(value)} sample={value[:3]}"
    return repr(value)


@contextlib.contextmanager
def time_limit(seconds: float | None) -> Iterator[None]:
    if seconds is None or seconds <= 0 or not hasattr(signal, "setitimer"):
        yield
        return

    def handle_timeout(signum: int, frame: Any) -> None:
        raise BenchmarkTimeout(f"timed out after {seconds:.1f}s")

    previous_handler = signal.signal(signal.SIGALRM, handle_timeout)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def hdf5_metadata_from_group(h5: h5py.File, group: str) -> list[dict[str, Any]]:
    node = h5[group]
    if not hasattr(node, "visititems"):
        raise TypeError(f"{group} is not an HDF5 group")

    group_prefix = group.strip("/")
    objects: list[dict[str, Any]] = []

    def h5path(name: str) -> str:
        return f"{group_prefix}/{name}" if group_prefix else name

    def collect(name: str, obj: Any) -> None:
        if hasattr(obj, "shape") and hasattr(obj, "dtype"):
            objects.append(
                {
                    "h5path": h5path(name),
                    "kind": "dataset",
                    "shape": list(obj.shape),
                    "dtype": str(obj.dtype),
                    "chunks": list(obj.chunks) if obj.chunks is not None else [],
                    "compression": obj.compression or "",
                }
            )
        else:
            objects.append(
                {
                    "h5path": h5path(name),
                    "kind": "group",
                    "shape": [],
                    "dtype": "",
                    "chunks": [],
                    "compression": "",
                }
            )

    node.visititems(collect)
    return objects


def dataset_nbytes(dataset: h5py.Dataset) -> int | None:
    if dataset.dtype.hasobject:
        return None
    shape = dataset.shape
    if shape is None:
        return None
    if shape == ():
        return int(dataset.dtype.itemsize)
    elements = math.prod(int(dim) for dim in shape)
    return int(elements * dataset.dtype.itemsize)


def discover_datasets(path: str, strategy: OpenStrategy) -> list[DatasetInfo]:
    datasets: list[DatasetInfo] = []

    with strategy.h5() as (h5, _stats):
        def collect(name: str, obj: Any) -> None:
            if isinstance(obj, h5py.Dataset):
                datasets.append(
                    DatasetInfo(
                        path=name,
                        shape=tuple(int(dim) for dim in obj.shape),
                        dtype=str(obj.dtype),
                        nbytes=dataset_nbytes(obj),
                    )
                )

        h5.visititems(collect)

    return datasets


def create_sample(path: str, *, groups: int, datasets_per_group: int, rows: int, cols: int, large_mib: int) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(target, "w") as h5:
        h5.attrs["source"] = "hdf5_file_benchmark"
        h5.create_dataset("scalar", data=np.array(1, dtype=np.int64))
        h5.create_dataset("small_vector", data=np.arange(1024, dtype=np.int32))
        h5.create_dataset("matrix", data=np.arange(rows * cols, dtype=np.float32).reshape(rows, cols))

        if large_mib > 0:
            elements = large_mib * 1024 * 1024 // np.dtype("float32").itemsize
            h5.create_dataset("large_vector", data=np.arange(elements, dtype=np.float32), chunks=(min(elements, 65536),))

        for group_index in range(groups):
            group = h5.create_group(f"group_{group_index:04d}")
            group.attrs["index"] = group_index
            for dataset_index in range(datasets_per_group):
                data = np.full((16, 16), group_index + dataset_index, dtype=np.float32)
                group.create_dataset(f"dataset_{dataset_index:03d}", data=data, chunks=(8, 8))


def make_operation(
    method: str,
    *,
    group: str,
    attr_path: str,
    read_dataset: str | None,
    read_fields_datasets: tuple[str, ...],
) -> Callable[[OpenStrategy], Any]:
    def open_raw(strategy: OpenStrategy) -> Any:
        with strategy.raw() as (raw, _stats):
            raw.seek(0)
            return raw.read(8)

    def open_h5py(strategy: OpenStrategy) -> Any:
        with strategy.h5() as (h5, _stats):
            return list(h5.keys())

    def keys(strategy: OpenStrategy) -> Any:
        with strategy.h5() as (h5, _stats):
            return list(h5[group].keys())

    def attrs(strategy: OpenStrategy) -> Any:
        with strategy.h5() as (h5, _stats):
            return dict(h5[attr_path].attrs)

    def visit(strategy: OpenStrategy) -> Any:
        with strategy.h5() as (h5, _stats):
            names: list[str] = []
            h5[group].visit(names.append)
            return names

    def metadata(strategy: OpenStrategy) -> Any:
        with strategy.h5() as (h5, _stats):
            return hdf5_metadata_from_group(h5, group)

    def read(strategy: OpenStrategy) -> Any:
        if read_dataset is None:
            raise RuntimeError("No dataset selected for read; pass --datasets or raise --max-read-bytes")
        with strategy.h5() as (h5, _stats):
            return h5[read_dataset][()]

    def read_fields(strategy: OpenStrategy) -> Any:
        if not read_fields_datasets:
            raise RuntimeError("No datasets selected for read_fields; pass --datasets or raise --max-read-bytes")
        with strategy.h5() as (h5, _stats):
            return {dataset: h5[dataset][()] for dataset in read_fields_datasets}

    operations = {
        "open_raw": open_raw,
        "open_h5py": open_h5py,
        "keys": keys,
        "attrs": attrs,
        "visit": visit,
        "metadata": metadata,
        "read": read,
        "read_fields": read_fields,
    }
    return operations[method]


def collect_stats_from_error_context(strategy: OpenStrategy) -> IOStats:
    return IOStats()


def run_case(
    *,
    method: str,
    strategy: OpenStrategy,
    operation: Callable[[OpenStrategy], Any],
    run: int,
    timeout_s: float | None,
    profile_dir: pathlib.Path | None,
) -> BenchmarkRow:
    profile_path = ""
    profile: cProfile.Profile | None = None
    if profile_dir is not None:
        profile_dir.mkdir(parents=True, exist_ok=True)
        buffer_label = "none" if strategy.buffer_size is None else str(strategy.buffer_size)
        profile_path = str(profile_dir / f"{method}__{strategy.name}__buf_{buffer_label}__run_{run}.prof")
        profile = cProfile.Profile()

    stats = IOStats()
    start = time.perf_counter()
    status = "ok"
    result = ""
    error = ""
    try:
        with time_limit(timeout_s):
            if profile is not None:
                profile.enable()
            value = operation(strategy)
            if profile is not None:
                profile.disable()
            result = summarize_value(value)
    except Exception as exc:  # noqa: BLE001 - benchmark rows should capture arbitrary operation failures.
        if profile is not None:
            profile.disable()
        status = "timeout" if isinstance(exc, BenchmarkTimeout) else "error"
        error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
    finally:
        wall_ms = (time.perf_counter() - start) * 1000.0

    if profile is not None and profile_path:
        profile.dump_stats(profile_path)
        text_path = pathlib.Path(profile_path).with_suffix(".txt")
        with text_path.open("w", encoding="utf-8") as out:
            stats_stream = io.StringIO()
            pstats.Stats(profile, stream=stats_stream).strip_dirs().sort_stats("cumulative").print_stats(40)
            out.write(stats_stream.getvalue())

    # Re-run a tiny stats-only wrapper would distort results. Instead, operation
    # bodies expose stats by returning through strategy context managers, so the
    # CountingFile stats are copied through this temporary hook.
    stats = getattr(strategy, "_last_stats", IOStats())

    waste_ratio = 0.0
    if stats.bytes_returned > 0 and stats.estimated_range_bytes > 0:
        waste_ratio = stats.estimated_range_bytes / stats.bytes_returned

    return BenchmarkRow(
        method=method,
        strategy=strategy.name,
        buffer_size=strategy.buffer_size,
        run=run,
        status=status,
        wall_ms=wall_ms,
        result=result,
        error=error,
        profile_path=profile_path,
        read_calls=stats.read_calls,
        readinto_calls=stats.readinto_calls,
        seek_calls=stats.seek_calls,
        tell_calls=stats.tell_calls,
        bytes_requested=stats.bytes_requested,
        bytes_returned=stats.bytes_returned,
        max_read_request=stats.max_read_request,
        estimated_range_gets=stats.estimated_range_gets,
        estimated_range_bytes=stats.estimated_range_bytes,
        estimated_waste_ratio=waste_ratio,
        read_time_ms=stats.read_time_s * 1000.0,
        seek_time_ms=stats.seek_time_s * 1000.0,
    )


def attach_last_stats(strategy: OpenStrategy) -> None:
    original_raw = strategy.raw
    original_h5 = strategy.h5

    @contextlib.contextmanager
    def raw_with_stats() -> Iterator[tuple[Any, IOStats]]:
        with original_raw() as (handle, stats):
            strategy._last_stats = stats
            yield handle, stats

    @contextlib.contextmanager
    def h5_with_stats() -> Iterator[tuple[h5py.File, IOStats]]:
        with original_h5() as (handle, stats):
            strategy._last_stats = stats
            yield handle, stats

    strategy.raw = raw_with_stats  # type: ignore[method-assign]
    strategy.h5 = h5_with_stats  # type: ignore[method-assign]
    strategy._last_stats = IOStats()


def strategy_buffer_sizes(strategy_name: str, buffer_sizes: tuple[int, ...]) -> tuple[int | None, ...]:
    if strategy_name == "direct":
        return (None,)
    if strategy_name in {"daft", "python", "safe-tempfile", "daft-tempfile"}:
        return buffer_sizes
    raise ValueError(f"Unknown strategy: {strategy_name}")


def write_csv(path: pathlib.Path, rows: list[BenchmarkRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def print_summary(rows: list[BenchmarkRow]) -> None:
    successful = [row for row in rows if row.status == "ok" and row.run >= 0]
    if not successful:
        print("No successful benchmark rows.", flush=True)
        return

    grouped: dict[tuple[str, str, int | None], list[BenchmarkRow]] = {}
    for row in successful:
        grouped.setdefault((row.method, row.strategy, row.buffer_size), []).append(row)

    summary_rows = []
    for (method, strategy, buffer_size), group in grouped.items():
        wall = [row.wall_ms for row in group]
        read_calls = max(row.read_calls for row in group)
        seek_calls = max(row.seek_calls for row in group)
        returned = max(row.bytes_returned for row in group)
        est_bytes = max(row.estimated_range_bytes for row in group)
        est_gets = max(row.estimated_range_gets for row in group)
        waste = max(row.estimated_waste_ratio for row in group)
        summary_rows.append(
            (
                statistics.median(wall),
                method,
                strategy,
                buffer_size,
                read_calls,
                seek_calls,
                returned,
                est_gets,
                est_bytes,
                waste,
            )
        )

    summary_rows.sort()
    headers = (
        "median_ms",
        "method",
        "strategy",
        "buffer",
        "reads",
        "seeks",
        "returned",
        "est_gets",
        "est_fetch",
        "est_fetch/returned",
    )
    table = []
    for median_ms, method, strategy, buffer_size, reads, seeks, returned, est_gets, est_bytes, waste in summary_rows:
        table.append(
            (
                f"{median_ms:.2f}",
                method,
                strategy,
                "n/a" if buffer_size is None else format_bytes(buffer_size),
                str(reads),
                str(seeks),
                format_bytes(returned),
                str(est_gets),
                format_bytes(est_bytes),
                f"{waste:.1f}x" if waste else "",
            )
        )

    widths = [len(header) for header in headers]
    for row in table:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row)]

    print(flush=True)
    print("Summary, fastest rows first:", flush=True)
    print("  ".join(header.ljust(width) for header, width in zip(headers, widths)), flush=True)
    print("  ".join("-" * width for width in widths), flush=True)
    for row in table:
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)), flush=True)

    slow_or_failed = [row for row in rows if row.status != "ok"]
    if slow_or_failed:
        print(flush=True)
        print("Failures/timeouts:", flush=True)
        for row in slow_or_failed[:20]:
            print(
                f"  {row.method} strategy={row.strategy} buffer={row.buffer_size}: "
                f"{row.status}: {row.error}",
                flush=True,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path", required=True, help="HDF5 file path or URL to benchmark.")
    parser.add_argument("--create-sample", help="Create a synthetic HDF5 file at this path before benchmarking.")
    parser.add_argument("--sample-groups", type=int, default=64)
    parser.add_argument("--sample-datasets-per-group", type=int, default=4)
    parser.add_argument("--sample-rows", type=int, default=1024)
    parser.add_argument("--sample-cols", type=int, default=64)
    parser.add_argument("--sample-large-mib", type=int, default=32)
    parser.add_argument("--strategies", type=parse_csv_strs, default=DEFAULT_STRATEGIES)
    parser.add_argument("--buffer-sizes", type=parse_csv_ints, default=DEFAULT_BUFFER_SIZES)
    parser.add_argument("--methods", type=parse_csv_strs, default=DEFAULT_METHODS)
    parser.add_argument("--group", default="/", help="Group path for keys/visit/metadata.")
    parser.add_argument("--attrs-path", default="/", help="Object path for attrs.")
    parser.add_argument("--datasets", type=parse_csv_strs, help="Datasets for read/read_fields. Defaults to safe small datasets.")
    parser.add_argument("--max-read-bytes", type=parse_size, default=64 * 1024 * 1024)
    parser.add_argument("--dataset-limit", type=int, default=4)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=60.0)
    parser.add_argument("--output-csv", type=pathlib.Path)
    parser.add_argument("--profile-dir", type=pathlib.Path)
    parser.add_argument("--tempfile-max-bytes", type=parse_size, default=512 * 1024 * 1024)
    parser.add_argument("--tempfile-copy-chunk-size", type=parse_size, default=8 * 1024 * 1024)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.create_sample:
        create_sample(
            args.create_sample,
            groups=args.sample_groups,
            datasets_per_group=args.sample_datasets_per_group,
            rows=args.sample_rows,
            cols=args.sample_cols,
            large_mib=args.sample_large_mib,
        )

    path = args.path
    if args.create_sample and path != args.create_sample and not os.path.exists(path):
        print(f"Created sample at {args.create_sample}, but --path points to {path}", file=sys.stderr, flush=True)

    if not is_local_path(path):
        skipped = [strategy for strategy in args.strategies if strategy in {"direct", "python"}]
        if skipped:
            print(
                f"Skipping local-only strategies for non-local path: {', '.join(skipped)}",
                file=sys.stderr,
                flush=True,
            )
        strategies = tuple(strategy for strategy in args.strategies if strategy not in {"direct", "python"})
    else:
        strategies = args.strategies

    invalid = set(strategies) - {"daft", "direct", "python", "safe-tempfile", "daft-tempfile"}
    if invalid:
        raise ValueError(f"Unknown strategies: {sorted(invalid)}")
    invalid_methods = set(args.methods) - set(DEFAULT_METHODS)
    if invalid_methods:
        raise ValueError(f"Unknown methods: {sorted(invalid_methods)}")

    hdf5_file = Hdf5File(path)
    try:
        file_size = hdf5_file.size()
    except (OSError, RuntimeError, ValueError):
        file_size = os.path.getsize(path) if is_local_path(path) and os.path.exists(path) else None

    discovery_strategy_name = "direct" if is_local_path(path) else "daft"
    discovery_strategy = OpenStrategy(
        discovery_strategy_name,
        path,
        hdf5_file,
        file_size=file_size,
        buffer_size=HDF5_SCAN_BUFFER_SIZE,
        tempfile_max_bytes=args.tempfile_max_bytes,
        tempfile_copy_chunk_size=args.tempfile_copy_chunk_size,
    )
    attach_last_stats(discovery_strategy)
    datasets = discover_datasets(path, discovery_strategy)

    if args.datasets:
        selected = tuple(args.datasets)
    else:
        safe_datasets = [
            dataset.path
            for dataset in datasets
            if dataset.nbytes is not None and dataset.nbytes <= args.max_read_bytes
        ]
        selected = tuple(safe_datasets[: args.dataset_limit])

    if selected:
        read_fields_datasets = selected[: args.dataset_limit]
        read_dataset = selected[0]
    else:
        read_fields_datasets = ()
        read_dataset = None

    print(f"Path: {path}", flush=True)
    print(f"Size: {format_bytes(file_size)}", flush=True)
    print(f"Strategies: {', '.join(strategies)}", flush=True)
    print(f"Buffer sizes: {', '.join(format_bytes(size) for size in args.buffer_sizes)}", flush=True)
    print(f"Methods: {', '.join(args.methods)}", flush=True)
    print(f"Discovered datasets: {len(datasets)}", flush=True)
    if selected:
        print(f"Read datasets: {', '.join(selected)}", flush=True)
    else:
        print("Read datasets: none selected; read/read_fields will be skipped unless --datasets is provided", flush=True)

    rows: list[BenchmarkRow] = []
    total_runs = args.warmups + args.runs
    for method in args.methods:
        operation = make_operation(
            method,
            group=args.group,
            attr_path=args.attrs_path,
            read_dataset=read_dataset,
            read_fields_datasets=read_fields_datasets,
        )
        for strategy_name in strategies:
            if method == "open_raw" and strategy_name != "daft":
                continue
            for buffer_size in strategy_buffer_sizes(strategy_name, args.buffer_sizes):
                for run_index in range(total_runs):
                    logical_run = run_index - args.warmups
                    strategy = OpenStrategy(
                        strategy_name,
                        path,
                        hdf5_file,
                        file_size=file_size,
                        buffer_size=buffer_size,
                        tempfile_max_bytes=args.tempfile_max_bytes,
                        tempfile_copy_chunk_size=args.tempfile_copy_chunk_size,
                    )
                    attach_last_stats(strategy)
                    row = run_case(
                        method=method,
                        strategy=strategy,
                        operation=operation,
                        run=logical_run,
                        timeout_s=args.timeout_s,
                        profile_dir=args.profile_dir if logical_run >= 0 else None,
                    )
                    if logical_run >= 0:
                        rows.append(row)
                        buffer_label = "n/a" if buffer_size is None else format_bytes(buffer_size)
                        print(
                            f"{method:9s} {strategy_name:13s} buffer={buffer_label:10s} "
                            f"run={logical_run} status={row.status:7s} wall={row.wall_ms:.2f}ms "
                            f"reads={row.read_calls} seeks={row.seek_calls} "
                            f"est_fetch={format_bytes(row.estimated_range_bytes)}",
                            flush=True,
                        )

    if rows:
        print_summary(rows)
        if args.output_csv:
            write_csv(args.output_csv, rows)
            print(f"\nWrote CSV: {args.output_csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
