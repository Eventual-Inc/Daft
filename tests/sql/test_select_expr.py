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


def test_select_md5_function():
    actual = daft.sql("SELECT md5('abc') AS h").collect()
    expect = daft.from_pydict({"h": ["900150983cd24fb0d6963f7d28e17f72"]})
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


def test_select_md5_numeric_types():
    """Test MD5 with numeric types"""
    df = daft.from_pydict({
        "i": [1, 2, 3],
        "f": [1.5, 2.5, 3.5],
        "b": [True, False, None]
    })
    
    actual = df.select(
        daft.functions.md5(daft.col("i")).alias("i_md5"),
        daft.functions.md5(daft.col("f")).alias("f_md5"),
        daft.functions.md5(daft.col("b")).alias("b_md5")
    ).collect()
    
    result = actual.to_pydict()
    # Verify all are non-null except the null case
    assert result["i_md5"][0] is not None
    assert result["i_md5"][1] is not None
    assert result["f_md5"][0] is not None
    assert result["b_md5"][2] is None  # Null input returns null


def test_select_md5_struct():
    """Test MD5 with struct types"""
    df = daft.from_pydict({
        "x": [1, 2],
        "y": ["a", "b"]
    })
    
    actual = df.select(
        daft.functions.to_struct(x=daft.col("x"), y=daft.col("y"))
        .alias("s")
    ).select(
        daft.functions.md5(daft.col("s")).alias("s_md5")
    ).collect()
    
    result = actual.to_pydict()
    # Verify MD5 hashes are computed
    assert result["s_md5"][0] is not None
    assert result["s_md5"][1] is not None
    assert result["s_md5"][0] != result["s_md5"][1]  # Different structs


def test_select_md5_list_order_insensitive():
    """Test that MD5 of lists with same elements in different order is equal"""
    # Create two lists with same elements but different order
    df = daft.from_pydict({
        "col": [
            None,
            [1, 2, 3],
            [3, 2, 1],  # Same elements, different order
            ["a", "b", "c"],
            ["c", "b", "a"],  # Same elements, different order
        ]
    })
    
    actual = df.select(
        daft.functions.md5(daft.col("col")).alias("hash")
    ).collect()
    
    result = actual.to_pydict()
    # Null should produce null
    assert result["hash"][0] is None
    
    # Lists with same elements in different order should have same MD5
    # (because we sort elements before hashing)
    assert result["hash"][1] == result["hash"][2], \
        f"Lists [1,2,3] and [3,2,1] should have same MD5, got {result['hash'][1]} vs {result['hash'][2]}"
    
    assert result["hash"][3] == result["hash"][4], \
        f"Lists ['a','b','c'] and ['c','b','a'] should have same MD5, got {result['hash'][3]} vs {result['hash'][4]}"


def test_select_md5_map():
    """Test MD5 with map types"""
    df = daft.from_arrow(
        pa.table({
            "m": pa.array(
                [
                    [('a', 1), ('b', 2)],
                    [('b', 2), ('a', 1)]  # Same entries, different order
                ],
                type=pa.map_(pa.string(), pa.int64())
            )
        })
    )
    
    actual = df.select(
        daft.functions.md5(daft.col("m")).alias("m_md5")
    ).collect()
    
    result = actual.to_pydict()
    assert result["m_md5"][0] is not None
    assert result["m_md5"][1] is not None
    # Maps with same key-value pairs (different order) should have same MD5
    # because keys are sorted before hashing
    assert result["m_md5"][0] == result["m_md5"][1], \
        "Maps with same entries should have same MD5"


def test_select_md5_complex_nested():
    """Test MD5 with complex nested structures"""
    df = daft.from_pydict({
        "x": [1, 2],
        "y": [10, 20],
    })
    
    actual = df.select(
        daft.functions.to_struct(
            list_field=daft.lit([1, 2, 3]),
            scalar_field=daft.col("x")
        ).alias("s")
    ).select(
        daft.functions.md5(daft.col("s")).alias("s_md5")
    ).collect()
    
    result = actual.to_pydict()
    assert result["s_md5"][0] is not None
    assert result["s_md5"][1] is not None


def test_select_md5_null_handling():
    """Test MD5 with null values"""
    df = daft.from_pydict({
        "col": [None, "hello", None, 42]
    })
    
    actual = df.select(
        daft.functions.md5(daft.col("col")).alias("hash")
    ).collect()
    
    result = actual.to_pydict()
    assert result["hash"][0] is None
    assert result["hash"][1] is not None
    assert result["hash"][2] is None
    assert result["hash"][3] is not None
