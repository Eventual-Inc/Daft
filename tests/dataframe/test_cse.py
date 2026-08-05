"""End-to-end execution tests for Common Subplan Elimination (CSE).

The CSE heuristic only applies to subplans containing expensive operators
(Join, Aggregate, Sort) AND also containing an Aggregate or Sort — which
naturally reduces output size or dominates compute cost.  Bare joins
without aggregate may produce large intermediate results whose clone cost
exceeds the recompute cost, so CSE is skipped.
"""

from __future__ import annotations

import pytest

from daft import col

# --- CSE should NOT apply (subplans without expensive ops) ---


def test_cse_self_concat(make_df, capsys):
    """Repeated concat of a plain scan: no expensive ops → no CSE."""
    df = make_df({"a": [1, 2, 3], "b": [10, 20, 30]})
    result = df.concat(df)
    result.explain(show_all=True)
    assert result.to_pydict() == {"a": [1, 2, 3, 1, 2, 3], "b": [10, 20, 30, 10, 20, 30]}


def test_cse_compound_self_concat(make_df, capsys):
    """Filter + project chain, no expensive ops → no CSE."""
    df = make_df({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    sub = df.filter(col("a") > 2).select(col("a"), col("b"))
    result = sub.concat(sub)
    result.explain(show_all=True)
    assert result.to_pydict() == {"a": [3, 4, 5, 3, 4, 5], "b": [30, 40, 50, 30, 40, 50]}


def test_cse_triple_concat(make_df, capsys):
    """Triple concat of a plain scan: no expensive ops → no CSE."""
    df = make_df({"a": [1, 2], "b": ["x", "y"]})
    result = df.concat(df).concat(df)
    result.explain(show_all=True)
    assert result.to_pydict() == {
        "a": [1, 2, 1, 2, 1, 2],
        "b": ["x", "y", "x", "y", "x", "y"],
    }


def test_cse_empty_concat(make_df, capsys):
    """Filtered-empty concat: no expensive ops → no CSE."""
    df = make_df({"a": [1, 2, 3], "b": [10, 20, 30]})
    empty = df.filter(col("a") > 99)
    result = empty.concat(empty)
    result.explain(show_all=True)
    assert result.to_pydict() == {"a": [], "b": []}


# --- CSE SHOULD apply (subplans with expensive ops + Aggregate/Sort) ---


def test_cse_with_join_concat(make_df, capsys):
    """Concat of a join result: join is expensive but has no Aggregate/Sort.

    CSE should NOT fire (clone cost of join output may exceed recompute).
    """
    df1 = make_df({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df2 = make_df({"a": [1, 2, 3], "c": [10, 20, 30]})
    joined = df1.join(df2, on="a")
    result = joined.concat(joined)
    result.explain(show_all=True)
    # Join without Aggregate/Sort: CSE is skipped.
    assert "CommonSubplan" not in capsys.readouterr().out
    assert sorted(result.to_pydict()["a"]) == [1, 1, 2, 2, 3, 3]


def test_cse_with_aggregate_concat(make_df, capsys):
    """Concat of an aggregate result: agg is expensive → CSE should fire."""
    df = make_df({"a": [1, 1, 2, 2], "b": [10, 20, 30, 40]})
    agg = df.groupby("a").agg(col("b").sum().alias("total"))
    result = agg.concat(agg)
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert sorted(result.to_pydict()["a"]) == [1, 1, 2, 2]


# --- Self-join: pre-existing deadlock ---


@pytest.mark.xfail(reason="CSE + HashJoin interaction causes deadlock — see issue #2423")
def test_cse_self_join(make_df, capsys):
    """Self-join with CSE: known to deadlock, not caused by streaming refactor."""
    df = make_df({"id": [1, 2, 3], "val": [100, 200, 300]})
    sub = df.filter(col("val") > 50)
    result = sub.join(sub, on="id")
    result.explain(show_all=True)
    assert "CommonSubplan" in capsys.readouterr().out
    assert result.to_pydict() == {"id": [1, 2, 3], "val": [100, 200, 300], "right.val": [100, 200, 300]}
