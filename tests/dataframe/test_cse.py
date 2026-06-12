"""End-to-end execution tests for Common Subplan Elimination (CSE)."""

from __future__ import annotations

from daft import col


def test_cse_self_concat(make_df, capsys):
    df = make_df({"a": [1, 2, 3], "b": [10, 20, 30]})
    result = df.concat(df)
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {"a": [1, 2, 3, 1, 2, 3], "b": [10, 20, 30, 10, 20, 30]}


def test_cse_compound_self_concat(make_df, capsys):
    df = make_df({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    sub = df.filter(col("a") > 2).select(col("a"), col("b"))
    result = sub.concat(sub)
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {"a": [3, 4, 5, 3, 4, 5], "b": [30, 40, 50, 30, 40, 50]}


def test_cse_triple_concat(make_df, capsys):
    df = make_df({"a": [1, 2], "b": ["x", "y"]})
    result = df.concat(df).concat(df)
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {
        "a": [1, 2, 1, 2, 1, 2],
        "b": ["x", "y", "x", "y", "x", "y"],
    }


def test_cse_self_join(make_df, capsys):
    df = make_df({"id": [1, 2, 3], "val": [100, 200, 300]})
    sub = df.filter(col("val") > 50)
    result = sub.join(sub, on="id")
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {"id": [1, 2, 3], "val": [100, 200, 300], "right.val": [100, 200, 300]}


def test_cse_empty_concat(make_df, capsys):
    df = make_df({"a": [1, 2, 3], "b": [10, 20, 30]})
    empty = df.filter(col("a") > 99)
    result = empty.concat(empty)
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {"a": [], "b": []}
