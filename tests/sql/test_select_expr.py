from __future__ import annotations

import daft
from daft import DataFrame, lit


def assert_eq(actual: DataFrame, expect: DataFrame):
    assert actual.to_pydict() == expect.to_pydict()


def singleton():
    return daft.from_pydict({"": [None]})


def test_select_lit_single():
    actual = daft.sql("SELECT 1")
    expect = singleton().select(lit(1))
    assert_eq(actual, expect)


def test_select_lit_multi():
    actual = daft.sql("SELECT 1 AS one, 2 AS two")
    expect = singleton().select(lit(1).alias("one"), lit(2).alias("two"))
    assert_eq(actual, expect)


def test_select_expr_single():
    actual = daft.sql("SELECT 1 + 1")
    expect = singleton().select(lit(1) + lit(1))
    assert_eq(actual, expect)


def test_select_expr_multi():
    actual = daft.sql("SELECT 1 + 1 AS plus, 1 - 1 AS minus")
    expect = singleton().select((lit(1) + lit(1)).alias("plus"), (lit(1) - lit(1)).alias("minus"))
    assert_eq(actual, expect)


def test_select_expr_functions():
    actual = daft.sql("SELECT lower('ABC') AS l, upper('xyz') AS u")
    expect = singleton().select(lit("ABC").str.lower().alias("l"), lit("xyz").str.upper().alias("u"))
    assert_eq(actual, expect)


def test_select_struct_lit():
    actual = daft.sql("select {'a': 'hello'}").collect()
    expect = daft.from_pydict({"literal": [{"a": "hello"}]})
    assert_eq(actual, expect)
