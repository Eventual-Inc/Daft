from __future__ import annotations

import daft
import pyarrow as pa
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


def test_select_struct_lit_with_expr_values():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT {'a': x, 'b': y} AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_struct_constructor():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT struct(x, y) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(f0=daft.col("x"), f1=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_struct_constructor_named_fields():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT struct(x as a, y as b) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_named_struct():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT named_struct('a', x, 'b', y) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_map_lit():
    actual = daft.sql("SELECT MAP {'a': 1, 'b': 2} AS m").collect()
    expect = daft.from_arrow(pa.table({"m": pa.array([[('a', 1), ('b', 2)]], type=pa.map_(pa.string(), pa.int64()))}))

    assert_eq(actual, expect)


def test_select_map_with_expression_values():
    df = daft.from_pydict({"x": [1, 2], "y": [10, 20]})

    actual = daft.sql("SELECT MAP {'a': x, 'b': y + 1} AS m FROM df").collect()
    expect = daft.from_arrow(
        pa.table({"m": pa.array([[('a', 1), ('b', 11)], [('a', 2), ('b', 21)]], type=pa.map_(pa.string(), pa.int64()))})
    )

    assert_eq(actual, expect)


def test_select_map_with_expression_keys_and_values():
    df = daft.from_pydict({"k": ["x", "y"], "v": [3, 4]})

    actual = daft.sql("SELECT MAP {k: v, 'const': v + 100} AS m FROM df").collect()
    expect = daft.from_arrow(
        pa.table(
            {
                "m": pa.array(
                    [[('x', 3), ('const', 103)], [('y', 4), ('const', 104)]],
                    type=pa.map_(pa.string(), pa.int64()),
                )
            }
        )
    )

    assert_eq(actual, expect)


def test_select_struct_wildcard():
    df = daft.from_pydict(
        {"person": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]}
    )

    assert len(df.collect().to_pydict()["person"]) == 3

    actual = daft.sql("SELECT person.* FROM df").collect().to_pydict()
    expected = {"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}

    assert actual == expected
