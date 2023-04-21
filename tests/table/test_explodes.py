from __future__ import annotations

import pyarrow as pa
import pytest

from daft.expressions import col
from daft.series import Series
from daft.table import Table

TEST_DATA = [
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64()))),
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64()))),
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, None], type=pa.list_(pa.int64(), list_size=2))),
]


@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode(data):
    table = Table.from_pydict({"nested": data, "sidecar": ["a", "b", "c", "d"]})
    table = table.explode([col("nested")._explode()])
    assert table.to_pydict() == {"nested": [1, 2, 3, 4, None, None], "sidecar": ["a", "a", "b", "b", "c", "d"]}


@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_multiple_cols(data):
    table = Table.from_pydict({"nested": data, "nested2": data, "sidecar": ["a", "b", "c", "d"]})
    # TODO: [RUST-INT][NESTED] Need to fix to allow multiple explodes
    with pytest.raises(ValueError):
        table = table.explode([col("nested")._explode(), col("nested2")._explode()])


@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_eval_expr(data):
    table = Table.from_pydict({"nested": data})
    table = table.eval_expression_list([col("nested")._explode()])
    assert table.to_pydict() == {"nested": [1, 2, 3, 4, None, None]}


def test_explode_bad_col_type():
    table = Table.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="explode not implemented for"):
        table = table.explode([col("a")._explode()])
