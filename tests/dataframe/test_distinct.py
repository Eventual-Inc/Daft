from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataFrame
from daft.datatype import DataType


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_distinct_with_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, None, None, None],
            "values": ["a1", "b1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.distinct()

    expected = {
        "id": [1, None, None],
        "values": ["a1", "b1", "c1"],
    }
    assert pa.Table.from_pydict(daft_df.to_pydict()).sort_by("values") == pa.Table.from_pydict(expected).sort_by(
        "values"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_distinct_with_all_nulls(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [None, None, None, None],
            "values": ["a1", "b1", "b1", "c1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.select(daft_df["id"].cast(DataType.int64()), daft_df["values"]).distinct()

    expected = {
        "id": [None, None, None],
        "values": ["a1", "b1", "c1"],
    }
    assert pa.Table.from_pydict(daft_df.to_pydict()).sort_by("values") == pa.Table.from_pydict(expected).sort_by(
        "values"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_distinct_with_empty(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1],
            "values": ["a1"],
        }
    ).repartition(repartition_nparts)
    daft_df = daft_df.where(daft_df["id"] != 1).distinct()
    daft_df.collect()

    resultset = daft_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0
