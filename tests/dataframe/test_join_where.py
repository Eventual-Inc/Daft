from __future__ import annotations

import daft
from daft import col
from daft.functions import str as str_fn


def test_join_where_single_lt_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [2, 3, 4]})

    result = df1.join_where(df2, [col("a") < col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [1, 1, 1, 2, 2, 3],
        "b": [2, 3, 4, 3, 4, 4],
    }


def test_join_where_single_gt_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") > col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [2, 3, 3],
        "b": [1, 1, 2],
    }


def test_join_where_single_lte_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") <= col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [1, 1, 1, 2, 2, 3],
        "b": [1, 2, 3, 2, 3, 3],
    }


def test_join_where_single_gte_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") >= col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [1, 2, 2, 3, 3, 3],
        "b": [1, 1, 2, 1, 2, 3],
    }


def test_join_where_multiple_predicates(make_df):
    df1 = make_df({"a": [1, 2, 3, 4, 5]})
    df2 = make_df({"b": [2, 3, 4, 5, 6]})

    result = df1.join_where(df2, [col("a") < col("b"), col("a") >= daft.lit(2)])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [2, 2, 2, 2, 3, 3, 3, 4, 4, 5],
        "b": [3, 4, 5, 6, 4, 5, 6, 5, 6, 6],
    }


def test_join_where_no_matches(make_df):
    df1 = make_df({"a": [10, 20, 30]})
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") < col("b")])
    result_data = result.to_pydict()

    assert result_data == {"a": [], "b": []}


def test_join_where_all_match(make_df):
    df1 = make_df({"a": [1, 2]})
    df2 = make_df({"b": [10, 20]})

    result = df1.join_where(df2, [col("a") < col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [1, 1, 2, 2],
        "b": [10, 20, 10, 20],
    }


def test_join_where_column_deduplication(make_df):
    df1 = make_df({"x": [1, 2, 3], "y": [10, 20, 30]})
    df2 = make_df({"x": [2, 3, 4], "z": [15, 25, 35]})

    result = df1.join_where(df2, [col("y") < col("z")])
    result = result.sort(["x", "right.x"])
    result_data = result.to_pydict()

    assert result_data == {
        "x": [1, 1, 1, 2, 2, 3],
        "y": [10, 10, 10, 20, 20, 30],
        "right.x": [2, 3, 4, 3, 4, 4],
        "z": [15, 25, 35, 25, 35, 35],
    }


def test_join_where_column_deduplication_custom_suffix(make_df):
    df1 = make_df({"x": [1, 2], "y": [10, 20]})
    df2 = make_df({"x": [3, 4], "z": [15, 25]})

    result = df1.join_where(df2, [col("y") < col("z")], suffix="_right")
    result = result.sort(["x", "x_right"])
    result_data = result.to_pydict()

    assert result_data == {
        "x": [1, 1, 2],
        "y": [10, 10, 20],
        "x_right": [3, 4, 4],
        "z": [15, 25, 25],
    }


def test_join_where_empty_predicate_list_cross_join(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [4, 5]})

    result = df1.join_where(df2, [])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    # Empty predicate list → cross join (every combination)
    assert result_data == {
        "a": [1, 1, 2, 2, 3, 3],
        "b": [4, 5, 4, 5, 4, 5],
    }


def test_join_where_empty_left_table(make_df):
    df1 = make_df({"a": [1, 2, 3]}).where(col("a") < 0)
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") < col("b")])
    result_data = result.to_pydict()

    assert result_data == {"a": [], "b": []}


def test_join_where_empty_right_table(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]}).where(col("b") < 0)

    result = df1.join_where(df2, [col("a") < col("b")])
    result_data = result.to_pydict()

    assert result_data == {"a": [], "b": []}


def test_join_where_str_contains_predicate(make_df):
    # str.contains(col) is a boolean function with 2 inputs — not a comparison operator
    df1 = make_df({"haystack": ["hello world", "foo bar", "baz"]})
    df2 = make_df({"needle": ["world", "bar", "xyz"]})

    result = df1.join_where(df2, [str_fn.contains(col("haystack"), col("needle"))])
    result = result.sort(["haystack", "needle"])
    result_data = result.to_pydict()

    # "hello world" contains "world" ✓
    # "foo bar" contains "bar" ✓
    # "baz" contains none of the needles
    assert result_data == {
        "haystack": ["foo bar", "hello world"],
        "needle": ["bar", "world"],
    }


def test_join_where_not_equal_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]})

    result = df1.join_where(df2, [col("a") != col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [1, 1, 2, 2, 3, 3],
        "b": [2, 3, 1, 3, 1, 2],
    }


def test_join_where_arithmetic_predicate(make_df):
    df1 = make_df({"a": [1, 2, 3]})
    df2 = make_df({"b": [1, 2, 3]})

    # a + b > 4  →  (2,3), (3,2), (3,3)
    result = df1.join_where(df2, [col("a") + col("b") > 4])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [2, 3, 3],
        "b": [3, 2, 3],
    }


def test_join_where_function_predicate(make_df):
    df1 = make_df({"a": [-3, -1, 2]})
    df2 = make_df({"b": [1, 2, 3]})

    # abs(a) < b  →  abs(-3)=3 < b: (b=4 - not present); abs(-1)=1 < b: 2,3; abs(2)=2 < b: 3
    result = df1.join_where(df2, [col("a").abs() < col("b")])
    result = result.sort(["a", "b"])
    result_data = result.to_pydict()

    assert result_data == {
        "a": [-1, -1, 2],
        "b": [2, 3, 3],
    }


def test_join_where_mixed_eq_and_inequality(make_df):
    df1 = make_df({"k1": [1, 1, 2], "a": [1, 2, 3]})
    df2 = make_df({"k2": [1, 2, 2], "b": [2, 3, 4]})

    # Equality on k1 == k2 AND inequality on a < b — exercises the HashJoin → Filter path
    result = df1.join_where(df2, [col("k1") == col("k2"), col("a") < col("b")])
    result = result.sort(["k1", "a", "b"])
    result_data = result.to_pydict()

    # k=1: (a=1, b=2) matches; (a=2, b=2) doesn't (not strictly less)
    # k=2: (a=3, b=3) doesn't match; (a=3, b=4) matches
    assert result_data == {
        "k1": [1, 2],
        "a": [1, 3],
        "k2": [1, 2],
        "b": [2, 4],
    }
