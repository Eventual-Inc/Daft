from __future__ import annotations

import daft


def test_negative_zero_equality_across_operators():
    df = daft.from_pydict({"f": [-0.0, 0.0, 1.0]})

    assert df.where(daft.col("f") == 0.0).to_pydict()["f"] == [-0.0, 0.0]
    assert df.where(daft.col("f") != 0.0).to_pydict()["f"] == [1.0]
    assert df.where(daft.col("f") < 0.0).to_pydict()["f"] == []
    assert df.where(daft.col("f") >= 0.0).to_pydict()["f"] == [-0.0, 0.0, 1.0]

    # Non-zero literals compare both zeros identically.
    assert df.where(daft.col("f") < 1.0).to_pydict()["f"] == [-0.0, 0.0]
    assert df.where(daft.col("f") == 1.0).to_pydict()["f"] == [1.0]

    assert daft.sql("SELECT f FROM df WHERE f = 0.0", df=df).to_pydict()["f"] == [-0.0, 0.0]

    assert df.distinct().count_rows() == 2
    assert df.distinct().sort("f").to_pydict()["f"] == [0.0, 1.0]

    counts = df.groupby("f").count("f").to_pydict()
    assert sorted(counts["f"]) == [1, 2]

    left = daft.from_pydict({"k": [-0.0]})
    right = daft.from_pydict({"k": [0.0], "v": [1]})
    assert left.join(right, on="k").to_pydict()["v"] == [1]


def test_negative_zero_equality_in_nested_keys():
    # Join equality goes through Arrow's row format, which keeps the zeros
    # distinct unless nested floats are canonicalized first.
    left = daft.from_pydict({"k": [[-0.0], [1.0]]})
    right = daft.from_pydict({"k": [[0.0], [2.0]], "v": [1, 2]})
    assert left.join(right, on="k").to_pydict()["v"] == [1]

    left = daft.from_pydict({"k": [{"a": -0.0}, {"a": 1.0}]})
    right = daft.from_pydict({"k": [{"a": 0.0}, {"a": 2.0}], "v": [1, 2]})
    assert left.join(right, on="k").to_pydict()["v"] == [1]

    df = daft.from_pydict({"k": [[-0.0], [0.0], [1.0]]})
    assert df.distinct().count_rows() == 2
    assert sorted(df.groupby("k").count("k").to_pydict()["k"]) == [1, 2]
