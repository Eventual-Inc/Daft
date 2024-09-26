import daft


def test_nested():
    df = daft.from_pydict(
        {
            "A": [1, 2, 3, 4],
            "B": [1.5, 2.5, 3.5, 4.5],
            "C": [True, True, False, False],
            "D": [None, None, None, None],
        }
    )

    actual = daft.sql("SELECT (A + 1) AS try_this FROM df").collect()
    expected = df.select((daft.col("A") + 1).alias("try_this")).collect()

    assert actual.to_pydict() == expected.to_pydict()

    actual = daft.sql("SELECT *, (A + 1) AS try_this FROM df").collect()
    expected = df.with_column("try_this", df["A"] + 1).collect()

    assert actual.to_pydict() == expected.to_pydict()
