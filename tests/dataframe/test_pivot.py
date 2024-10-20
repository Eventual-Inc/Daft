import pytest


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
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

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_col_names(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 2, 1, 2],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(
        group_by="group",
        pivot_col="pivot",
        value_col="value",
        agg_fn="sum",
        names=["1", "2"],
    )

    expected = {
        "group": ["A", "B"],
        "1": [10, 30],
        "2": [20, 40],
    }

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_col_names_subset(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 2, 1, 2],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(
        group_by="group",
        pivot_col="pivot",
        value_col="value",
        agg_fn="sum",
        names=["1"],
    )

    expected = {
        "group": ["A", "B"],
        "1": [10, 30],
    }

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_col_names_superset(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 2, 1, 2],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(
        group_by="group",
        pivot_col="pivot",
        value_col="value",
        agg_fn="sum",
        names=["1", "2", "3"],
    )

    expected = {
        "group": ["A", "B"],
        "1": [10, 30],
        "2": [20, 40],
        "3": [None, None],
    }

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_nulls(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", None, "B", "B"],
            "pivot": [1, 2, None, 2],
            "value": [10, 20, 30, None],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(group_by="group", pivot_col="pivot", value_col="value", agg_fn="sum")

    expected = {
        "group": ["A", "B", None],
        "1": [10, None, None],
        "None": [None, 30, None],
        "2": [None, None, 20],
    }

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
@pytest.mark.parametrize(
    "agg_fn, expected",
    [
        ("sum", {"group": ["A", "B"], "1": [30, 70]}),
        ("mean", {"group": ["A", "B"], "1": [15, 35]}),
        ("max", {"group": ["A", "B"], "1": [20, 40]}),
        ("min", {"group": ["A", "B"], "1": [10, 30]}),
        ("count", {"group": ["A", "B"], "1": [2, 2]}),
    ],
)
def test_pivot_with_different_aggs(make_df, repartition_nparts, agg_fn, expected):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 1, 1, 1],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(group_by="group", pivot_col="pivot", value_col="value", agg_fn=agg_fn)

    assert daft_df.sort("group").to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_multiple_group_by(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group1": ["A", "A", "A", "B", "B", "B"],
            "group2": ["X", "X", "Y", "X", "Y", "Y"],
            "pivot": [1, 2, 1, 2, 1, 2],
            "value": [10, 20, 30, 40, 50, 60],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(group_by=["group1", "group2"], pivot_col="pivot", value_col="value", agg_fn="sum")

    expected = {
        "group1": ["A", "A", "B", "B"],
        "group2": ["X", "Y", "X", "Y"],
        "1": [10, 30, None, 50],
        "2": [20, None, 40, 60],
    }
    assert daft_df.sort(["group1", "group2"]).to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_pivot_with_downstream_ops(make_df, repartition_nparts):
    daft_df = make_df(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, 2, 1, 2],
            "value": [10, 20, 30, 40],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.pivot(group_by="group", pivot_col="pivot", value_col="value", agg_fn="sum")
    daft_df = daft_df.where(daft_df["1"] == 10).select("2")

    assert daft_df.to_pydict() == {"2": [20]}
