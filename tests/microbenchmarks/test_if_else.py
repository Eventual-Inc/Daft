from __future__ import annotations

import uuid

import pytest

import daft
from daft import DataFrame
from daft.table import MicroPartition

NUM_ROWS = 1_000_000


# Perform if/else against two int64 columns, selecting exactly half of the first and half of the second
def generate_int64_params() -> tuple[dict, daft.Expression, list]:
    return (
        {"lhs": [0 for _ in range(NUM_ROWS)], "rhs": [1 for _ in range(NUM_ROWS)], "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        [0 for _ in range(NUM_ROWS // 2)] + [1 for _ in range(NUM_ROWS // 2)],
    )


# Perform if/else against two int64 columns, selecting exactly half of the first and half of the second
# This differs from `generate_int64_params` in that the columns and predicate can contain nulls
def generate_int64_with_nulls_params() -> tuple[dict, daft.Expression, list]:
    lhs = [0 if i % 2 == 0 else None for i in range(NUM_ROWS)]
    rhs = [1 if i % 2 == 0 else None for i in range(NUM_ROWS)]
    pred = [i if i % 3 == 0 else None for i in range(NUM_ROWS)]
    expected = [x if p is not None else None for x, p in zip(lhs[: NUM_ROWS // 2] + rhs[NUM_ROWS // 2 :], pred)]
    return (
        {"lhs": lhs, "rhs": rhs, "pred": pred},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        expected,
    )


# Perform if/else against two int64 columns, where the first column is broadcasted
def generate_int64_broadcast_lhs_params() -> tuple[dict, daft.Expression, list]:
    return (
        {"rhs": [1 for _ in range(NUM_ROWS)], "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.lit(0), daft.col("rhs")),
        [0 for _ in range(NUM_ROWS // 2)] + [1 for _ in range(NUM_ROWS // 2)],
    )


# Perform if/else against two int64 columns, where the first column is broadcasted and predicate has nulls
def generate_int64_broadcast_lhs_with_nulls_params() -> tuple[dict, daft.Expression, list]:
    pred = [i if i % 2 == 0 else None for i in range(NUM_ROWS)]
    expected = [
        x if p is not None else None
        for x, p in zip([0 for _ in range(NUM_ROWS // 2)] + [1 for _ in range(NUM_ROWS // 2)], pred)
    ]
    return (
        {"rhs": [1 for _ in range(NUM_ROWS)], "pred": pred},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.lit(0), daft.col("rhs")),
        expected,
    )


# Perform if/else against two string columns, selecting exactly half of the first and half of the second
def generate_string_params() -> tuple[dict, daft.Expression, list]:
    lhs = [str(uuid.uuid4()) for _ in range(NUM_ROWS)]
    rhs = [str(uuid.uuid4()) for _ in range(NUM_ROWS)]
    return (
        {"lhs": lhs, "rhs": rhs, "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        lhs[: NUM_ROWS // 2] + rhs[NUM_ROWS // 2 :],
    )


# Perform if/else against two list columns, selecting exactly half of the first and half of the second
def generate_list_params() -> tuple[dict, daft.Expression, list]:
    lhs = [[0 for _ in range(5)] for _ in range(NUM_ROWS)]
    rhs = [[1 for _ in range(5)] for _ in range(NUM_ROWS)]
    return (
        {"lhs": lhs, "rhs": rhs, "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        lhs[: NUM_ROWS // 2] + rhs[NUM_ROWS // 2 :],
    )


@pytest.mark.benchmark(group="if_else")
@pytest.mark.parametrize(
    "test_data_generator",
    [
        pytest.param(
            generate_int64_params,
            id="int64",
        ),
        pytest.param(
            generate_int64_with_nulls_params,
            id="int64-with-nulls",
        ),
        pytest.param(
            generate_int64_broadcast_lhs_params,
            id="int64-broadcast-lhs",
        ),
        pytest.param(
            generate_int64_broadcast_lhs_with_nulls_params,
            id="int64-broadcast-lhs-with-nulls",
        ),
        pytest.param(
            generate_string_params,
            id="string",
        ),
        pytest.param(
            generate_list_params,
            id="list",
        ),
    ],
)
def test_if_else(test_data_generator, benchmark) -> None:
    """If_else between NUM_ROWS values"""
    data, expr, expected = test_data_generator()
    table = MicroPartition.from_pydict(data)

    def bench_if_else() -> DataFrame:
        return table.eval_expression_list([expr.alias("result")])

    result = benchmark(bench_if_else)
    assert result.to_pydict()["result"] == expected
