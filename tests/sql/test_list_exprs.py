import pyarrow as pa

import daft
from daft import col
from daft.daft import CountMode
from daft.sql.sql import SQLCatalog


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
        col("col").list.sort(True).alias("sort_desc_upper"),
        col("col").list.sort(False).alias("sort_asc_upper"),
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
        list_sort(col, desc) as sort_desc,
        list_sort(col, asc) as sort_asc,
        list_sort(col, DESC) as sort_desc_upper,
        list_sort(col, ASC) as sort_asc_upper,
        list_slice(col, 1, 2) as slice
    FROM test
    """,
        catalog=catalog,
    ).collect()
    assert actual.to_pydict() == expected.to_pydict()
