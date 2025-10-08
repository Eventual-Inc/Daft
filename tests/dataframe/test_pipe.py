from __future__ import annotations

from tests.utils import sort_pydict


def test_pipe(make_df):
    df = make_df({"x": [1, 2, 3]})

    def double(df, column: str):
        return df.select((df[column] * df[column]).alias("y"))

    assert df.pipe(double, "x").to_pydict() == {"y": [1, 4, 9]}


def test_pipe_with_join(make_df):
    df1 = make_df({"A": [1, 2, 3], "B": [1, 2, 3]})
    df2 = make_df({"A": [1, 2, 3], "C": [1, 2, 3]})

    def left_join(dfa, dfb, on):
        return dfa.join(dfb, on=on)

    joined_by_join = df1.join(df2, on="A")
    joined_by_pipe = df1.pipe(left_join, df2, on="A")

    assert sort_pydict(joined_by_join.to_pydict(), "A") == sort_pydict(joined_by_pipe.to_pydict(), "A")
