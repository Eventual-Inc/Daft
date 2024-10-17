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
