from __future__ import annotations

import pytest

from daft import DataFrame
from daft.errors import ExpressionTypeError
from tests.conftest import assert_pydict_equals


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

    expected = {
        "id": [1, 3],
        "values_left": ["a1", "c1"],
        "values_right": ["a2", "c2"],
    }
    daft_df.collect()

    assert_pydict_equals(daft_df.to_pydict(), expected, sort_key="id")


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

    expected = {
        "id": [1],
        "id2": ["foo1"],
        "values_left": ["a1"],
        "values_right": ["c2"],
    }
    daft_df.collect()
    assert_pydict_equals(daft_df.to_pydict(), expected, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_inner_join_all_null(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df2 = DataFrame.from_pydict(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.with_column("id", daft_df["id"].cast(int)).join(daft_df2, on="id", how="inner")

    expected = {
        "id": [],
        "values_left": [],
        "values_right": [],
    }
    daft_df.collect()
    assert_pydict_equals(daft_df.to_pydict(), expected, sort_key="id")


def test_inner_join_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        }
    )
    daft_df2 = DataFrame.from_pydict(
        {
            "id": [None, None, None],
            "values_right": ["a2", "b2", "c2"],
        }
    )

    with pytest.raises(ExpressionTypeError):
        daft_df.join(daft_df2, on="id", how="inner")
