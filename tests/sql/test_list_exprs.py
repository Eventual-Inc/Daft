from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import DataType, col, list_
from daft.daft import CountMode
from daft.sql.sql import SQLCatalog


def assert_eq(actual, expect):
    """Asserts two dataframes are equal for tests."""
    assert actual.collect().to_pydict() == expect.collect().to_pydict()


def test_list_constructor_empty():
    with pytest.raises(Exception, match="List constructor requires at least one item"):
        df = daft.from_pydict({"x": [1, 2, 3]})
        daft.sql("SELECT [ ] as list FROM df")
        df  # for ruff ignore unused


def test_list_constructor_different_lengths():
    with pytest.raises(Exception, match="Expected all columns to be of the same length"):
        df = daft.from_pydict({"x": [1, 2], "y": [3]})
        daft.sql("SELECT [ x, y ] FROM df")
        df  # for ruff ignore unused


def test_list_constructor_singleton():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ x ] as list FROM df")
    expect = df.select(col("x").apply(lambda x: [x], DataType.list(DataType.int64())).alias("list"))
    assert_eq(actual, expect)


def test_list_constructor_homogeneous():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ x * 1, x * 2, x * 3 ] FROM df")
    expect = df.select(col("x").apply(lambda x: [x * 1, x * 2, x * 3], DataType.list(DataType.int64())).alias("list"))
    assert_eq(actual, expect)


def test_list_constructor_heterogeneous():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
    df = daft.sql("SELECT [ x, y ] AS heterogeneous FROM df").collect()
    assert df.to_pydict() == {"heterogeneous": [[1, 1], [2, 1], [3, 0]]}


def test_list_constructor_heterogeneous_with_cast():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
    actual = daft.sql("SELECT [ CAST(x AS STRING), CAST(y AS STRING) ] FROM df")
    expect = df.select(list_(col("x").cast(DataType.string()), col("y").cast(DataType.string())))
    assert_eq(actual, expect)


def test_list_constructor_mixed_null_first():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ NULL, x ] FROM df")
    expect = df.select(col("x").apply(lambda x: [None, x], DataType.list(DataType.int64())).alias("list"))
    assert_eq(actual, expect)


def test_list_constructor_mixed_null_mid():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ x * -1, NULL, x ] FROM df")
    expect = df.select(col("x").apply(lambda x: [x * -1, None, x], DataType.list(DataType.int64())).alias("list"))
    assert_eq(actual, expect)


def test_list_constructor_mixed_null_last():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ x, NULL ] FROM df")
    expect = df.select(col("x").apply(lambda x: [x, None], DataType.list(DataType.int64())).alias("list"))
    assert_eq(actual, expect)


def test_list_constructor_all_nulls():
    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = daft.sql("SELECT [ NULL, NULL ] FROM df")
    expect = df.select(col("x").apply(lambda x: [None, None], DataType.list(DataType.null())).alias("list"))
    assert_eq(actual, expect)


def test_list_chunk():
    df = daft.from_pydict(
        {
            "col": pa.array([], type=pa.list_(pa.int64())),
            "fixed_col": pa.array([], type=pa.list_(pa.int64(), 2)),
        }
    )
    catalog = SQLCatalog({"test": df})
    expected = df.select(
        col("col").list.chunk(1).alias("col1"),
        col("col").list.chunk(2).alias("col2"),
        col("col").list.chunk(1000).alias("col3"),
        col("fixed_col").list.chunk(1).alias("fixed_col1"),
        col("fixed_col").list.chunk(2).alias("fixed_col2"),
        col("fixed_col").list.chunk(1000).alias("fixed_col3"),
    )

    actual = daft.sql(
        """
    SELECT
        list_chunk(col, 1) as col1,
        list_chunk(col, 2) as col2,
        list_chunk(col, 1000) as col3,
        list_chunk(fixed_col, 1) as fixed_col1,
        list_chunk(fixed_col, 2) as fixed_col2,
        list_chunk(fixed_col, 1000) as fixed_col3
    FROM test
    """,
        catalog=catalog,
    ).collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_list_counts():
    df = daft.from_pydict({"col": [[1, 2, 3], [1, 2], [1, None, 4], []]})
    catalog = SQLCatalog({"test": df})
    expected = df.select(
        col("col").list.count().alias("count_valid"),
        col("col").list.count(CountMode.All).alias("count_all"),
        col("col").list.count(CountMode.Null).alias("count_null"),
    ).collect()
    actual = daft.sql(
        """
    SELECT
        list_count(col) as count_valid,
        list_count(col, 'all') as count_all,
        list_count(col, 'null') as count_null
    FROM test
    """,
        catalog=catalog,
    ).collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_list_explode():
    df = daft.from_pydict({"col": [[1, 2, 3], [1, 2], [1, None, 4], []]})
    catalog = SQLCatalog({"test": df})
    expected = df.explode(col("col"))
    actual = daft.sql("SELECT unnest(col) as col FROM test", catalog=catalog).collect()
    assert actual.to_pydict() == expected.to_pydict()
    # test with alias
    actual = daft.sql("SELECT explode(col) as col FROM test", catalog=catalog).collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_list_join():
    df = daft.from_pydict({"col": [None, [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]]})
    catalog = SQLCatalog({"test": df})
    expected = df.select(col("col").list.join(","))
    actual = daft.sql("SELECT list_join(col, ',') FROM test", catalog=catalog).collect()
    assert actual.to_pydict() == expected.to_pydict()
    # make sure it works with the `array_to_string` function too
    actual = daft.sql("SELECT array_to_string(col, ',') FROM test", catalog=catalog).collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_various_list_ops():
    df = daft.from_pydict({"col": [[1, 2, 3], [1, 2], [1, None, 4], []]})
    catalog = SQLCatalog({"test": df})
    expected = df.select(
        col("col").list.min().alias("min"),
        col("col").list.max().alias("max"),
        col("col").list.mean().alias("mean"),
        col("col").list.sum().alias("sum"),
        col("col").list.sort().alias("sort"),
        col("col").list.sort(True).alias("sort_desc"),
        col("col").list.sort(False).alias("sort_asc"),
        col("col").list.slice(1, 2).alias("slice"),
    ).collect()
    actual = daft.sql(
        """
    SELECT
        list_min(col) as min,
        list_max(col) as max,
        list_mean(col) as mean,
        list_sum(col) as sum,
        list_sort(col) as sort,
        list_sort(col, desc:=true) as sort_desc,
        list_sort(col, desc:=false) as sort_asc,
        list_slice(col, 1, 2) as slice
    FROM test
    """,
        catalog=catalog,
    ).collect()
    assert actual.to_pydict() == expected.to_pydict()
