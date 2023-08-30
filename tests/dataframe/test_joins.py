from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from tests.utils import sort_arrow_table


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_multicol_joins(n_partitions: int):
    df = daft.from_pydict(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
            "C": [True, False, True],
        }
    ).repartition(n_partitions, "A", "B")

    joined = df.join(df, on=["A", "B"]).sort("A")
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 2, 3],
        "B": ["a", "b", "c"],
        "C": [True, False, True],
        "right.C": [True, False, True],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_limit_after_join(n_partitions: int):
    data = {
        "A": [1, 2, 3],
    }
    df1 = daft.from_pydict(data).repartition(n_partitions, "A")
    df2 = daft.from_pydict(data).repartition(n_partitions, "A")

    joined = df1.join(df2, on="A").limit(1)
    joined_data = joined.to_pydict()
    assert "A" in joined_data
    assert len(joined_data["A"]) == 1


###
# Tests for nulls
###


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join(repartition_nparts):
    daft_df = daft.from_pydict(
        {
            "id": [1, None, 3],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.join(daft_df2, on="id", how="inner")

    expected = {
        "id": [1, 3],
        "values_left": ["a1", "c1"],
        "values_right": ["a2", "c2"],
    }
    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join_multikey(repartition_nparts):
    daft_df = daft.from_pydict(
        {
            "id": [1, None, None],
            "id2": ["foo1", "foo2", None],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = daft.from_pydict(
        {
            "id": [None, None, 1],
            "id2": ["foo2", None, "foo1"],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.join(daft_df2, on=["id", "id2"], how="inner")

    expected = {
        "id": [1],
        "id2": ["foo1"],
        "values_left": ["a1"],
        "values_right": ["c2"],
    }
    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join_all_null(repartition_nparts):
    daft_df = daft.from_pydict(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.with_column("id", daft_df["id"].cast(DataType.int64())).join(daft_df2, on="id", how="inner")

    expected = {
        "id": [],
        "values_left": [],
        "values_right": [],
    }
    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


def test_inner_join_null_type_column():
    daft_df = daft.from_pydict(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        }
    )
    daft_df2 = daft.from_pydict(
        {
            "id": [None, None, None],
            "values_right": ["a2", "b2", "c2"],
        }
    )

    with pytest.raises((ExpressionTypeError, ValueError)):
        daft_df.join(daft_df2, on="id", how="inner")
