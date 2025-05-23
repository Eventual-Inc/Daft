from __future__ import annotations

import daft
from daft import coalesce, col, lit


def assert_eq(actual, expect):
    """Asserts two dataframes are equal for tests."""
    assert actual.to_pydict() == expect.to_pydict()


def test_coalesce_basic():
    df = daft.from_pydict(
        {
            "a": [None, 1, None, 3],
            "b": [2, None, 4, 5],
            "c": [6, 7, None, 9],
        }
    )
    #
    # single arg
    expect = df.select(coalesce(col("a")).alias("result"))
    actual = daft.sql("select coalesce(a) as result from df")
    assert_eq(actual, expect)
    #
    # two args
    expect = df.select(coalesce(col("a"), col("b")).alias("result"))
    actual = daft.sql("select coalesce(a, b) as result from df")
    assert_eq(actual, expect)
    #
    # three args
    expect = df.select(coalesce(col("a"), col("b"), col("c")).alias("result"))
    actual = daft.sql("select coalesce(a, b, c) as result from df")
    assert_eq(actual, expect)


def test_coalesce_typing():
    """Test coalesce with arguments of different but compatible types."""
    df = daft.from_pydict(
        {
            "int_col": [None, 1, None, 3],
            "float_col": [2.5, None, 4.5, None],
            "str_col": ["6", "7", None, "9"],
        }
    )
    # ok, these are coercible
    # https://github.com/Eventual-Inc/Daft/issues/3752
    daft.sql("SELECT coalesce(1::bigint, 2::int) FROM df").collect()
    daft.sql("SELECT coalesce(1::int, 2::bigint) FROM df").collect()
    #
    # comparable types with minimal common supertype in first position
    actual = daft.sql("select coalesce(float_col, int_col) as result from df")
    expect = df.select(coalesce(col("float_col"), col("int_col")).alias("result"))
    assert_eq(actual, expect)
    #
    # comparable types with minimal common supertype NOT in first position
    actual = daft.sql("select coalesce(int_col, float_col) as result from df")
    expect = df.select(coalesce(col("int_col"), col("float_col")).alias("result"))
    assert_eq(actual, expect)
    #
    # ok in Daft, but not in standard SQL (str and int not comparable).
    #   I believe we should keep as-is because changes to the minimal common supertype logic
    #   would impact all other operators using get_super_type and it is currently quite forgiving.
    daft.sql("select coalesce(int_col, float_col, str_col) as result from df").collect()


def test_coalesce_argument_order():
    df = daft.from_pydict(
        {
            "a": [None, 1, None, 3, 0],
            "b": [2, None, 4, None, 1],
        }
    )
    #
    # order: a, b
    expect_ab = df.select(coalesce(col("a"), col("b")).alias("result"))
    actual_ab = daft.sql("select coalesce(a, b) as result from df")
    assert_eq(actual_ab, expect_ab)
    #
    # order: b, a
    expect_ba = df.select(coalesce(col("b"), col("a")).alias("result"))
    actual_ba = daft.sql("select coalesce(b, a) as result from df")
    assert_eq(actual_ba, expect_ba)
    #
    # values should be different due to different argument order
    assert expect_ab.to_pydict() != expect_ba.to_pydict()


def test_coalesce_literals():
    df = daft.from_pydict({"dummy": [1, 2, 3, 4]})
    #
    # all literals
    actual = daft.sql("select coalesce(null, 1, 2) as result from df")
    expect = daft.from_pydict({"result": [1, 1, 1, 1]})
    assert_eq(actual, expect)
    #
    # mix of column and literals
    actual = daft.sql("select coalesce(dummy, 99) as result from df")
    expect = df.select(coalesce(col("dummy"), lit(99)).alias("result"))
    assert_eq(actual, expect)


def test_coalesce_with_nulls():
    df = daft.from_pydict({"col1": [None, None, None, None]})
    #
    # all null arguments
    actual = daft.sql("select coalesce(null, null, null) as result from df")
    expect = daft.from_pydict({"result": [None, None, None, None]})
    assert_eq(actual, expect)
    #
    # null last
    actual = daft.sql("select coalesce(col1, null) as result from df")
    expect = df.select(coalesce(col("col1")).alias("result"))
    assert_eq(actual, expect)
    #
    # null first
    actual = daft.sql("select coalesce(null, col1) as result from df")
    expect = df.select(coalesce(col("col1")).alias("result"))
    assert_eq(actual, expect)
