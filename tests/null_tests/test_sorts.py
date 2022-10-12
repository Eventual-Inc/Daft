import pyarrow as pa
import pytest

from daft import DataFrame
from tests.conftest import assert_arrow_equals


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sort_with_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [2, None, 1],
            "values": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort(daft_df["id"])

    expected_arrow_table = {
        "id": pa.array([1, 2, None]),
        "values": pa.array(["c1", "a1", "b1"]),
    }
    daft_df.collect()

    assert_arrow_equals(daft_df._result.to_pydict(), expected_arrow_table, assert_ordering=True)


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

    expected_arrow_table = {
        "id1": pa.array([2, 2, 1, None, None]),
        "id2": pa.array([1, 2, None, 1, None]),
        "values": pa.array(["c1", "a1", "e1", "d1", "b1"]),
    }
    daft_df.collect()

    assert_arrow_equals(daft_df._result.to_pydict(), expected_arrow_table, assert_ordering=True)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sort_with_all_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [None, None, None],
            "values": ["a1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.sort(daft_df["id"])
    daft_df.collect()

    resultset = daft_df._result.to_pydict()
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

    resultset = daft_df._result.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0
