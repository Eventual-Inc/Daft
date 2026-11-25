from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataFrame, Series
from daft.recordbatch import MicroPartition

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
    """If_else between NUM_ROWS values."""
    data, idx, expected = test_data_generator()
    table = MicroPartition.from_pydict(data)

    def bench_take() -> DataFrame:
        return table.take(idx)

    result = benchmark(bench_take)
    assert result.to_pydict()["data"] == expected


@pytest.mark.benchmark(group="fixed_size_list_take")
@pytest.mark.parametrize("dtype", ["float16", "float32"])
@pytest.mark.parametrize(
    "dimension,scenario",
    [(768, "repeat16"), (768, "random"), (4, "repeat16"), *[(width, "nullable") for width in (4, 16, 17, 32)]],
)
def test_fixed_size_list_take(benchmark, dtype, dimension, scenario) -> None:
    """Copy vectors without including input construction in the timing."""
    rows = 8092
    nullable = scenario == "nullable"
    values = (np.arange(rows * dimension, dtype=np.uint32) % 1024).astype(dtype)
    data = pa.FixedSizeListArray.from_arrays(
        pa.array(values, mask=np.arange(len(values)) % 17 == 0 if nullable else None),
        dimension,
        mask=pa.array(np.arange(rows) % 11 == 0) if nullable else None,
    )
    row_indices = (
        np.random.default_rng(42).permutation(rows).astype(np.uint64)
        if scenario == "random"
        else np.repeat(np.arange(rows, dtype=np.uint64), 16)
    )
    arrow_indices = pa.array(row_indices, mask=np.arange(len(row_indices)) % 13 == 0 if nullable else None)
    source = Series.from_arrow(data)
    indices = Series.from_arrow(arrow_indices)
    result = benchmark.pedantic(lambda: source.take(indices), rounds=7, iterations=1, warmup_rounds=2)
    assert result.to_arrow().equals(data.take(arrow_indices))
