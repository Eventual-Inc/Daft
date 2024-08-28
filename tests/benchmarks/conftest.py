from __future__ import annotations

import os
import tempfile
from collections import defaultdict

import memray
import pytest

memray_stats = defaultdict(dict)


def pytest_terminal_summary(terminalreporter):
    if memray_stats:
        for group, group_stats in sorted(memray_stats.items()):
            terminalreporter.write_sep("-", f"Memray Stats for Group: {group}")
            for nodeid, stats in group_stats.items():
                terminalreporter.write_line(
                    f"{nodeid} \t Peak Memory: {stats['peak_memory']} MB \t Total Allocations: {stats['total_allocations']}"
                )
                terminalreporter.ensure_newline()


@pytest.fixture
def benchmark_with_memray(request, benchmark):
    def track_mem(func, group):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file_path = os.path.join(tmpdir, "memray_output.bin")
            with memray.Tracker(output_file_path):
                res = func()

            reader = memray.FileReader(output_file_path)
            stats = {
                "peak_memory": reader.metadata.peak_memory / 1024 / 1024,
                "total_allocations": reader.metadata.total_allocations,
            }
            memray_stats[group][request.node.nodeid] = stats

        return res

    def benchmark_wrapper(func, group):
        benchmark.group = group
        # If running in CI, just run the benchmark
        if os.getenv("CI"):
            return benchmark(func)
        else:
            benchmark(func)
            return track_mem(func, group)

    return benchmark_wrapper
