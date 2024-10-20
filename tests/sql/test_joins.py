import daft
from daft import col


def test_joins_using():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    df_sql = daft.sql("select * from df1 join df2 using (idx)")
    actual = df_sql.collect().to_pydict()

    expected = df1.join(df2, on="idx").collect().to_pydict()

    assert actual == expected


def test_joins_with_alias():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    df_sql = daft.sql("select * from df1 as foo join df2 as bar on (foo.idx=bar.idx) where bar.score>0.1")

    actual = df_sql.collect().to_pydict()

    expected = df1.join(df2, on="idx").filter(col("score") > 0.1).collect().to_pydict()

    assert actual == expected


def test_joins_with_wildcard_expansion():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [3], "score": [0.1]})
    df3 = daft.from_pydict({"idx": [1], "score": [0.1], "a": [1], "b": [2], "c": [3]})

    df_sql = (
        daft.sql("""
        select df3.*
        from df1
        left join df2 on (df1.idx=df2.idx)
        left join df3 on (df1.idx=df3.idx)
        """)
        .collect()
        .to_pydict()
    )

    expected = (
        df1.join(df2, on="idx", how="left")
        .join(df3, on="idx", how="left")
        .select(
            "idx",
            col("right.score").alias("score"),
            col("a"),
            col("b"),
            col("c"),
        )
        .collect()
        .to_pydict()
    )

    assert df_sql == expected
    # make sure it works with exclusion patterns too

    df_sql = (
        daft.sql("""
        select df3.* EXCLUDE (a,b,c)
        from df1
        left join df2 on (df1.idx=df2.idx)
        left join df3 on (df1.idx=df3.idx)
        """)
        .collect()
        .to_pydict()
    )

    expected = {
        "idx": [1, 2],
        "score": [0.1, None],
    }

    assert df_sql == expected
