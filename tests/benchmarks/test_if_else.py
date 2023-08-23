from __future__ import annotations

import uuid

import pytest

import daft
from daft import DataFrame

NUM_ROWS = 1_000_000

# Perform if/else against two int64 columns, selecting exactly half of the first and half of the second
def generate_int64_params() -> tuple[dict, daft.Expression, list]:
    return (
        {"lhs": [0 for _ in range(NUM_ROWS)], "rhs": [1 for _ in range(NUM_ROWS)], "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        [0 for _ in range(NUM_ROWS // 2)] + [1 for _ in range(NUM_ROWS // 2)],
    )


# Perform if/else against two string columns, selecting exactly half of the first and half of the second
def generate_string_params() -> tuple[dict, daft.Expression, list]:
    STRING_TEST_LHS = [str(uuid.uuid4()) for _ in range(NUM_ROWS)]
    STRING_TEST_RHS = [str(uuid.uuid4()) for _ in range(NUM_ROWS)]
    return (
        {"lhs": STRING_TEST_LHS, "rhs": STRING_TEST_RHS, "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        STRING_TEST_LHS[: NUM_ROWS // 2] + STRING_TEST_RHS[NUM_ROWS // 2 :],
    )


# Perform if/else against two list columns, selecting exactly half of the first and half of the second
def generate_list_params() -> tuple[dict, daft.Expression, list]:
    LIST_TEST_LHS = [[0 for _ in range(5)] for _ in range(NUM_ROWS)]
    LIST_TEST_RHS = [[1 for _ in range(5)] for _ in range(NUM_ROWS)]
    return (
        {"lhs": LIST_TEST_LHS, "rhs": LIST_TEST_RHS, "pred": list(range(NUM_ROWS))},
        (daft.col("pred") < NUM_ROWS // 2).if_else(daft.col("lhs"), daft.col("rhs")),
        LIST_TEST_LHS[: NUM_ROWS // 2] + LIST_TEST_RHS[NUM_ROWS // 2 :],
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
    df = daft.from_pydict(data)

    def bench_if_else() -> DataFrame:
        return df.select(expr.alias("result")).collect()

    result = benchmark(bench_if_else)
    assert result.to_pydict()["result"] == expected
