import pytest


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_pivot(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 2, 1, 2],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(group_by="group", pivot_col="pivot", value_col="value", agg_fn="sum")

    expected = {
        "group": ["A", "B"],
        "1": [10, 30],
        "2": [20, 40],
    }

    assert daft_df.to_pydict() == expected
