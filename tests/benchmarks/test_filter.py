from __future__ import annotations

import pytest

import daft
from daft import DataFrame
from daft.table import MicroPartition

NUM_ROWS = 1_000_000


# Perform filter against a int64 column: selecting every other element
def generate_int64_keep_every_other() -> tuple[dict, daft.Expression, list]:
    return (
        {"data": list(range(NUM_ROWS)), "mask": [True if i % 2 == 0 else False for i in range(NUM_ROWS)]},
        [i for i in range(NUM_ROWS) if i % 2 == 0],
    )


# Perform filter against a int64 column: selecting every third element, where every two elements in between are False and Null
def generate_int64_keep_every_other_nulls() -> tuple[dict, daft.Expression, list]:
    return (
        {
            "data": list(range(NUM_ROWS)),
            "mask": [True if i % 3 == 0 else (False if i % 3 == 1 else None) for i in range(NUM_ROWS)],
        },
        [i for i in range(NUM_ROWS) if i % 3 == 0],
    )


# Perform filter against a int64 column: contiguous segments of True/False
def generate_int64_keep_contiguous_chunk() -> tuple[dict, daft.Expression, list]:
    return (
        {
            "data": list(range(NUM_ROWS)),
            "mask": [True for _ in range(NUM_ROWS // 2)] + [False for _ in range(NUM_ROWS // 2)],
        },
        list(range(NUM_ROWS // 2)),
    )


# Perform filter against a list[int64] column: selecting every other element
def generate_list_int64_keep_every_other() -> tuple[dict, daft.Expression, list]:
    data = [list(range(i, i + 4)) for i in range(NUM_ROWS)]
    return (
        {"data": data, "mask": [True if i % 2 == 0 else False for i in range(NUM_ROWS)]},
        [d for i, d in enumerate(data) if i % 2 == 0],
    )


# Perform filter against a list[int64] column: selecting every third element, where every two elements in between are False and Null
def generate_list_int64_keep_every_other_nulls() -> tuple[dict, daft.Expression, list]:
    data = [list(range(i, i + 4)) for i in range(NUM_ROWS)]
    return (
        {"data": data, "mask": [True if i % 3 == 0 else (False if i % 3 == 1 else None) for i in range(NUM_ROWS)]},
        [d for i, d in enumerate(data) if i % 3 == 0],
    )


# Perform filter against a list[int64] column: contiguous segments of True/False
def generate_list_int64_keep_contiguous_chunks() -> tuple[dict, daft.Expression, list]:
    data = [list(range(i, i + 4)) for i in range(NUM_ROWS)]
    return (
        {"data": data, "mask": [int(i // 100) % 2 == 0 for i in range(NUM_ROWS)]},
        [d for i, d in enumerate(data) if int(i // 100) % 2 == 0],
    )


# Perform filter against a list[int64] column: keep all values
def generate_list_int64_keep_all() -> tuple[dict, daft.Expression, list]:
    data = [list(range(i, i + 4)) for i in range(NUM_ROWS)]
    return (
        {"data": data, "mask": [True for _ in range(NUM_ROWS)]},
        data,
    )


# Perform filter against a list[int64] column: keep none of the values
def generate_list_int64_keep_none() -> tuple[dict, daft.Expression, list]:
    data = [list(range(i, i + 4)) for i in range(NUM_ROWS)]
    return (
        {"data": data, "mask": [False for _ in range(NUM_ROWS)]},
        [],
    )


@pytest.mark.benchmark(group="if_else")
@pytest.mark.parametrize(
    "test_data_generator",
    [
        pytest.param(
            generate_int64_keep_every_other,
            id="int64-every-other",
        ),
        pytest.param(
            generate_int64_keep_every_other_nulls,
            id="int64-every-other-with-nulls",
        ),
        pytest.param(
            generate_int64_keep_contiguous_chunk,
            id="int64-contiguous",
        ),
        pytest.param(
            generate_list_int64_keep_every_other,
            id="int64-list-every-other",
        ),
        pytest.param(
            generate_list_int64_keep_every_other_nulls,
            id="int64-list-every-other-with-nulls",
        ),
        pytest.param(
            generate_list_int64_keep_contiguous_chunks,
            id="int64-list-contiguous",
        ),
        pytest.param(
            generate_list_int64_keep_all,
            id="int64-list-all",
        ),
        pytest.param(
            generate_list_int64_keep_none,
            id="int64-list-none",
        ),
    ],
)
def test_filter(test_data_generator, benchmark) -> None:
    """If_else between NUM_ROWS values"""
    data, expected = test_data_generator()
    table = MicroPartition.from_pydict(data)

    def bench_filter() -> DataFrame:
        return table.filter([daft.col("mask")])

    result = benchmark(bench_filter)
    assert result.to_pydict()["data"] == expected
