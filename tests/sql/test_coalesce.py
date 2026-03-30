from __future__ import annotations

import daft
from daft import col, lit
from daft.functions import coalesce


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


def test_fallback_early_exit():
    triggered = 0

    @daft.udf(return_dtype=daft.DataType.int64())
    def fallback(column):
        nonlocal triggered
        triggered += len(column)
        return column

    # Test short-circuit behavior
    df = daft.from_pydict({"v": [1, 2, None]})
    df.select(coalesce(col("v"), fallback(col("v")))).collect()
    assert triggered == 1, f"Expected fallback to process 1 row, got {triggered}"


def test_coalesce_short_circuit_udf():
    """Test that coalesce short-circuits and doesn't evaluate unnecessary arguments with @daft.udf."""
    triggered = 0

    def fallback(column):
        nonlocal triggered
        triggered += 1
        return column

    fallback_udf = daft.udf(return_dtype=daft.DataType.int64())(fallback)

    # Test with coalesce - should only trigger fallback for null rows
    df = daft.from_pydict({"v": [1, 2, None]})
    df.select(coalesce(col("v"), fallback_udf(col("v")))).collect()
    assert triggered == 1, f"Expected fallback to be called once, got {triggered} times"


def test_coalesce_short_circuit_func():
    """Test that coalesce short-circuits and doesn't evaluate unnecessary arguments with @daft.func."""
    triggered = 0

    @daft.func(return_dtype=daft.DataType.int64())
    def fallback(column):
        nonlocal triggered
        triggered += 1
        return column

    # Test with coalesce - should only trigger fallback for null rows
    df = daft.from_pydict({"v": [1, 2, None]})
    df.select(coalesce(col("v"), fallback(col("v")))).collect()
    assert triggered == 1, f"Expected fallback to be called once, got {triggered} times"


def test_coalesce_multi_arg_short_circuit():
    """Test that coalesce short-circuits with multiple arguments."""
    triggered = 0

    def create_fallback(name):
        def fallback(column):
            nonlocal triggered
            triggered += 1
            return column

        return daft.func(return_dtype=daft.DataType.int64())(fallback)

    # Create multiple fallback functions
    fallback1 = create_fallback("1")
    fallback2 = create_fallback("2")
    fallback3 = create_fallback("3")

    # Test multi-argument short-circuit
    df = daft.from_pydict({"v1": [1, None, None, None], "v2": [None, 2, None, None], "v3": [None, None, 3, None]})
    df.select(
        coalesce(col("v1"), fallback1(col("v1")), col("v2"), fallback2(col("v2")), col("v3"), fallback3(col("v3")))
    ).collect()

    # With row-level short-circuit, each @daft.func fallback is called per-element only on null rows:
    # After v1: [1, None, None, None] - 3 null rows
    #   fallback1 processes 3 null rows -> triggered += 3, but values are still null (identity)
    # After v2 on remaining nulls: [1, 2, None, None] - 2 null rows
    #   fallback2 processes 2 null rows -> triggered += 2
    # After v3 on remaining nulls: [1, 2, 3, None] - 1 null row
    #   fallback3 processes 1 null row -> triggered += 1
    # Total: 3 + 2 + 1 = 6
    assert triggered == 6, f"Expected fallbacks to be called 6 times, got {triggered} times"


def test_coalesce_complex_expressions():
    """Test coalesce with complex expressions that have side effects."""
    triggered = 0

    @daft.func(return_dtype=daft.DataType.int64())
    def complex_udf(column):
        nonlocal triggered
        triggered += 1
        # Simulate expensive computation
        return column

    # Test with complex expressions - should only evaluate when needed
    df = daft.from_pydict({"v": [1, None, 3]})
    df.select(coalesce(col("v"), complex_udf(col("v")))).collect()
    assert triggered == 1, f"Expected complex_udf to be called once, got {triggered} times"


def test_coalesce_many_arguments():
    """Test coalesce with many arguments."""
    # Create a DataFrame with a null value in the first column
    df = daft.from_pydict({"col0": [None], "col1": [1], "col2": [2], "col3": [3], "col4": [4], "col5": [5]})

    # Many arguments test
    result = df.select(coalesce(col("col0"), col("col1"), col("col2"), col("col3"), col("col4"), col("col5"))).collect()
    assert result.to_pydict() == {"col0": [1]}, "Expected coalesce to return the first non-null value"
