from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataFrame
from daft.errors import ExpressionTypeError
from tests.conftest import assert_pydict_equals


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_int_sort_with_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [2, None, 1],
            "values": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort(daft_df["id"])

    expected = {
        "id": [1, 2, None],
        "values": ["c1", "a1", "b1"],
    }
    daft_df.collect()

    assert_pydict_equals(daft_df.to_pydict(), expected, assert_ordering=True)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_str_sort_with_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, None, 2],
            "values": ["c1", None, "a1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort(daft_df["values"])

    expected = {
        "id": [2, 1, None],
        "values": ["a1", "c1", None],
    }
    daft_df.collect()
    assert_pydict_equals(daft_df.to_pydict(), expected, assert_ordering=True)


@pytest.mark.parametrize("repartition_nparts", [1, 4, 6])
def test_sort_with_nulls_multikey(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id1": [2, None, 2, None, 1],
            "id2": [2, None, 1, 1, None],
            "values": ["a1", "b1", "c1", "d1", "e1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort([daft_df["id1"], daft_df["id2"]])

    expected = {
        "id1": [1, 2, 2, None, None],
        "id2": [None, 1, 2, 1, None],
        "values": ["e1", "c1", "a1", "d1", "b1"],
    }
    daft_df.collect()
    assert_pydict_equals(daft_df.to_pydict(), expected, assert_ordering=True)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sort_with_all_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": pa.array([None, None, None], type=pa.int64()),
            "values": ["c1", None, "a1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort(daft_df["id"])
    daft_df.collect()

    resultset = daft_df.to_pydict()
    assert len(resultset["id"]) == 3
    assert len(resultset["values"]) == 3


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_sort_with_empty(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1],
            "values": ["a1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.where(daft_df["id"] != 1).sort(daft_df["id"])
    daft_df.collect()

    resultset = daft_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0


def test_sort_with_all_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": pa.array([None, None, None], pa.null()),
            "values": ["a1", "b1", "c1"],
        }
    )

    with pytest.raises(ExpressionTypeError):
        daft_df = daft_df.sort(daft_df["id"])
