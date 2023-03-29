from __future__ import annotations

import pytest

import daft


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_multicol_joins(n_partitions: int):
    df = daft.DataFrame.from_pydict(
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
    df1 = daft.DataFrame.from_pydict(data).repartition(n_partitions, "A")
    df2 = daft.DataFrame.from_pydict(data).repartition(n_partitions, "A")

    joined = df1.join(df2, on="A").limit(1)
    joined_data = joined.to_pydict()
    assert "A" in joined_data
    assert len(joined_data["A"]) == 1
