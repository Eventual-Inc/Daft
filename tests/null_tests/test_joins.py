from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataFrame
from daft.errors import ExpressionTypeError
from tests.conftest import assert_arrow_equals


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, None, 3],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = DataFrame.from_pydict(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.join(daft_df2, on="id", how="inner")

    expected_arrow_table = {
        "id": pa.array([1, 3]),
        "values_left": pa.array(["a1", "c1"]),
        "values_right": pa.array(["a2", "c2"]),
    }
    daft_df.collect()

    assert_arrow_equals(daft_df.to_pydict(), expected_arrow_table, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join_multikey(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, None, None],
            "id2": ["foo1", "foo2", None],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = DataFrame.from_pydict(
        {
            "id": [None, None, 1],
            "id2": ["foo2", None, "foo1"],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.join(daft_df2, on=["id", "id2"], how="inner")

    expected_arrow_table = {
        "id": pa.array([1]),
        "id2": pa.array(["foo1"]),
        "values_left": pa.array(["a1"]),
        "values_right": pa.array(["c2"]),
    }
    daft_df.collect()
    assert_arrow_equals(daft_df.to_pydict(), expected_arrow_table, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join_all_null(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": pa.array([None, None, None], type=pa.int64()),
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = DataFrame.from_pydict(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.join(daft_df2, on="id", how="inner")

    expected_arrow_table = {
        "id": pa.array([], type=pa.int64()),
        "values_left": pa.array([], pa.string()),
        "values_right": pa.array([], pa.string()),
    }
    daft_df.collect()
    assert_arrow_equals(daft_df.to_pydict(), expected_arrow_table, sort_key="id")


def test_inner_join_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": pa.array([None, None, None], type=pa.null()),
            "values_left": ["a1", "b1", "c1"],
        }
    )
    daft_df2 = DataFrame.from_pydict(
        {
            "id": pa.array([None, None, None], type=pa.null()),
            "values_right": ["a2", "b2", "c2"],
        }
    )

    with pytest.raises(ExpressionTypeError):
        daft_df.join(daft_df2, on="id", how="inner")
