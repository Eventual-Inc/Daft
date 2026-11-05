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
    expect = singleton().select(
        daft.functions.lower(lit("ABC")).alias("l"), daft.functions.upper(lit("xyz")).alias("u")
    )
    assert_eq(actual, expect)


def test_select_struct_lit():
    actual = daft.sql("select {'a': 'hello'}").collect()
    expect = daft.from_pydict({"literal": [{"a": "hello"}]})
    assert_eq(actual, expect)


def test_select_struct_wildcard():
    df = daft.from_pydict(
        {"person": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]}
    )

    assert len(df.collect().to_pydict()["person"]) == 3

    actual = daft.sql("SELECT person.* FROM df").collect().to_pydict()
    expected = {"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}

    assert actual == expected


def test_select_empty_scalar_subquery_is_null():
    actual = daft.sql("SELECT (SELECT 1 WHERE FALSE) AS x").collect().to_pydict()
    assert actual == {"x": [None]}


def test_select_empty_scalar_subquery_is_null_predicate():
    actual = daft.sql("SELECT (SELECT 1 WHERE FALSE) IS NULL AS x").collect().to_pydict()
    assert actual == {"x": [True]}


def test_select_empty_scalar_subquery_preserves_outer_rows():
    df = daft.from_pydict({"a": [1, 2]})  # noqa: F841
    actual = daft.sql("SELECT a, (SELECT 1 WHERE FALSE) AS x FROM df").collect().to_pydict()
    assert actual == {"a": [1, 2], "x": [None, None]}


def test_select_scalar_subquery_with_rows():
    actual = daft.sql("SELECT (SELECT 2) AS x").collect().to_pydict()
    assert actual == {"x": [2]}


def test_coalesce_empty_scalar_subquery_uses_non_empty_branch():
    actual = daft.sql("SELECT coalesce((SELECT 1 WHERE FALSE), (SELECT 2)) AS x").collect().to_pydict()
    assert actual == {"x": [2]}
