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
