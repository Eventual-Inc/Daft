from __future__ import annotations

import pytest

import daft
from daft import DataFrame, Series

NUM_ROWS = 10_000_000

# Perform take against a int64 column: take all Nones
def generate_int64_take_all_none() -> tuple[dict, daft.Expression, list]:
    return (
        {"data": list(range(NUM_ROWS))},
        Series.from_pylist([None for _ in range(NUM_ROWS)]).cast(daft.DataType.int64()),
        [None for _ in range(NUM_ROWS)],
    )


# Perform take against a int64 column: take all elements in-order, but with every other element being null
def generate_int64_take_all_inorder_nones() -> tuple[dict, daft.Expression, list]:
    return (
        {"data": list(range(NUM_ROWS))},
        Series.from_pylist([i if i % 2 == 0 else None for i in range(NUM_ROWS)]).cast(daft.DataType.int64()),
        [i if i % 2 == 0 else None for i in range(NUM_ROWS)],
    )


# Perform take against a int64 column: take all elements in reverse order
def generate_int64_take_reversed() -> tuple[dict, daft.Expression, list]:
    return (
        {"data": list(range(NUM_ROWS))},
        Series.from_pylist(list(reversed(range(NUM_ROWS)))).cast(daft.DataType.int64()),
        list(reversed(range(NUM_ROWS))),
    )


# Perform take against a list[int64] column: take all Nones
def generate_list_int64_take_all_none() -> tuple[dict, daft.Expression, list]:
    data = [[i for _ in range(4)] for i in range(NUM_ROWS)]
    return (
        {"data": data},
        Series.from_pylist([None for _ in range(NUM_ROWS)]).cast(daft.DataType.int64()),
        [None for _ in range(NUM_ROWS)],
    )


# Perform take against a list[int64] column: take all elements in-order, but with every other element being null
def generate_list_int64_take_all_inorder_nones() -> tuple[dict, daft.Expression, list]:
    data = [[i for _ in range(4)] for i in range(NUM_ROWS)]
    return (
        {"data": data},
        Series.from_pylist([i if i % 2 == 0 else None for i in range(NUM_ROWS)]).cast(daft.DataType.int64()),
        [x if i % 2 == 0 else None for i, x in enumerate(data)],
    )


# Perform take against a list[int64] column: take all elements in reverse order
def generate_list_int64_take_reversed() -> tuple[dict, daft.Expression, list]:
    data = [[i for _ in range(4)] for i in range(NUM_ROWS)]
    return (
        {"data": data},
        Series.from_pylist(list(reversed(range(NUM_ROWS)))).cast(daft.DataType.int64()),
        list(reversed(data)),
    )


@pytest.mark.benchmark(group="if_else")
@pytest.mark.parametrize(
    "test_data_generator",
    [
        pytest.param(
            generate_int64_take_all_none,
            id="int64-all-none",
        ),
        pytest.param(
            generate_int64_take_all_inorder_nones,
            id="int64-inorder-every-other-none",
        ),
        pytest.param(
            generate_int64_take_reversed,
            id="int64-all-reversed",
        ),
        pytest.param(
            generate_list_int64_take_all_none,
            id="list-int64-all-none",
        ),
        pytest.param(
            generate_list_int64_take_all_inorder_nones,
            id="list-int64-inorder-every-other-none",
        ),
        pytest.param(
            generate_list_int64_take_reversed,
            id="list-int64-all-reversed",
        ),
    ],
)
def test_take(test_data_generator, benchmark) -> None:
    """If_else between NUM_ROWS values"""
    data, idx, expected = test_data_generator()
    table = daft.table.Table.from_pydict(data)

    def bench_take() -> DataFrame:
        return table.take(idx)

    result = benchmark(bench_take)
    assert result.to_pydict()["data"] == expected
