from __future__ import annotations

import uuid

import pytest

from daft.series import Series


@pytest.mark.benchmark(group="concat")
def test_string_concat(benchmark) -> None:
    NUM_ROWS = 100_000
    data = Series.from_pylist([str(uuid.uuid4()) for _ in range(NUM_ROWS)])
    to_concat = [data] * 100

    def bench_concat() -> Series:
        return Series.concat(to_concat)

    benchmark(bench_concat)
