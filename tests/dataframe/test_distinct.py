from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from tests.utils import sort_arrow_table


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_distinct_with_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, None, None, None],
            "values": ["a1", "b1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    result_df = daft_df.distinct()

    expected = {
        "id": [1, None, None],
        "values": ["a1", "b1", "c1"],
    }
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )

    # Test unique alias.
    result_df = daft_df.unique()
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )

    # Test drop_duplicates alias.
    result_df = daft_df.drop_duplicates()
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_distinct_with_all_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [None, None, None, None],
            "values": ["a1", "b1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    result_df = daft_df.select(daft_df["id"].cast(DataType.int64()), daft_df["values"]).distinct()

    expected = {
        "id": [None, None, None],
        "values": ["a1", "b1", "c1"],
    }
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )

    # Test unique alias.
    result_df = daft_df.select(daft_df["id"].cast(DataType.int64()), daft_df["values"]).unique()
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )

    # Test drop_duplicates alias.
    result_df = daft_df.select(daft_df["id"].cast(DataType.int64()), daft_df["values"]).drop_duplicates()
    assert sort_arrow_table(pa.Table.from_pydict(result_df.to_pydict()), "values") == sort_arrow_table(
        pa.Table.from_pydict(expected), "values"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_distinct_with_empty(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1],
            "values": ["a1"],
        },
        repartition=repartition_nparts,
    )
    result_df = daft_df.where(daft_df["id"] != 1).distinct()
    result_df.collect()

    resultset = result_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0

    # Test unique alias.
    result_df = daft_df.where(daft_df["id"] != 1).unique()
    result_df.collect()
    resultset = result_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0

    # Test drop_duplicates alias.
    result_df = daft_df.where(daft_df["id"] != 1).drop_duplicates()
    result_df.collect()
    resultset = result_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_distinct_some_columns(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3, 4],
            "values": ["a1", "b1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )

    expected = {
        "id": [4, 2, 1],
        "values": ["c1", "b1", "a1"],
    }
    expected_table = pa.Table.from_pydict(expected)

    result_df = daft_df.distinct("values").collect()
    resultset = result_df.to_pydict()
    # Can't predict the output of id, so fix it
    resultset["id"][resultset["values"].index("b1")] = 2
    print(resultset)
    assert sort_arrow_table(pa.Table.from_pydict(resultset), "values") == expected_table

    # Test unique alias.
    result_df = daft_df.unique("values").collect()
    resultset = result_df.to_pydict()
    resultset["id"][resultset["values"].index("b1")] = 2
    assert sort_arrow_table(pa.Table.from_pydict(resultset), "values") == expected_table

    # Test drop_duplicates alias.
    result_df = daft_df.drop_duplicates("values").collect()
    resultset = result_df.to_pydict()
    resultset["id"][resultset["values"].index("b1")] = 2
    assert sort_arrow_table(pa.Table.from_pydict(resultset), "values") == expected_table


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_distinct_some_columns_derived(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3, 4],
            "values": [1, 0, 1, 0],
        },
        repartition=repartition_nparts,
    )

    result_df = daft_df.distinct("values").collect()
    assert len(result_df) == 2

    # Test unique alias.
    result_df = daft_df.unique("values").collect()
    assert len(result_df) == 2

    # Test drop_duplicates alias.
    result_df = daft_df.drop_duplicates("values").collect()
    assert len(result_df) == 2
