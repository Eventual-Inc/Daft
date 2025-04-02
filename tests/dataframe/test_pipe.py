def test_pipe_with_join(make_df):
    df1 = make_df({"A": [1, 2, 3], "B": [1, 2, 3]})
    df2 = make_df({"A": [1, 2, 3], "C": [1, 2, 3]})

    def left_join(dfa, dfb, on):
        return dfa.join(dfb, on=on)

    joined_by_join = df1.join(df2, on="A")
    joined_by_pipe = df1.pipe(left_join, df2, on="A")

    assert joined_by_join.to_pydict() == joined_by_pipe.to_pydict()
