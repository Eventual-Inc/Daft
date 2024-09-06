import os

import numpy as np
import pytest

import daft
from daft.sql.sql import SQLCatalog
from tests.assets import TPCH_QUERIES


def load_tpch_queries():
    """Load all TPCH queries into a list of (name,sql) tuples"""
    queries = []
    for filename in os.listdir(TPCH_QUERIES):
        filepath = os.path.join(TPCH_QUERIES, filename)
        if os.path.isfile(filepath) and filepath.endswith(".sql"):
            with open(filepath) as f:
                sql = f.read()
                name = "TPC-H " + os.path.basename(filepath)
                queries.append((name, sql))
    return queries


def load_tpch_query(filename):
    """Load a single TPCH query from a file"""
    filepath = os.path.join(TPCH_QUERIES, filename)
    if os.path.isfile(filepath) and filepath.endswith(".sql"):
        with open(filepath) as f:
            sql = f.read()
            name = "TPC-H " + os.path.basename(filepath)
            return (name, sql)
    else:
        raise ValueError(f"File {filename} not found in {TPCH_QUERIES}")


# Load all TPCH queries once
all_tpch_queries = load_tpch_queries()


def test_sanity():
    catalog = SQLCatalog({"test": daft.from_pydict({"a": [1, 2, 3]})})
    df = daft.sql("SELECT * FROM test", catalog=catalog)
    assert isinstance(df, daft.DataFrame)


@pytest.mark.skip(reason="This test is a placeholder used to check that we can parse the TPC-H queries")
@pytest.mark.parametrize("name,sql", all_tpch_queries)
def test_parse_ok(name, sql):
    print(name)
    print(sql)
    print("--------------")


def test_fizzbuzz_sql():
    arr = np.arange(100)
    df = daft.from_pydict({"a": arr})
    catalog = SQLCatalog({"test": df})
    # test case expression
    expected = daft.from_pydict(
        {
            "a": arr,
            "fizzbuzz": [
                "FizzBuzz" if x % 15 == 0 else "Fizz" if x % 3 == 0 else "Buzz" if x % 5 == 0 else str(x)
                for x in range(0, 100)
            ],
        }
    ).collect()
    df = daft.sql(
        """
    SELECT
        a,
        CASE
            WHEN a % 15 = 0 THEN 'FizzBuzz'
            WHEN a % 3 = 0 THEN 'Fizz'
            WHEN a % 5 = 0 THEN 'Buzz'
            ELSE CAST(a AS TEXT)
        END AS fizzbuzz
    FROM test
    """,
        catalog=catalog,
    ).collect()
    assert df.to_pydict() == expected.to_pydict()


@pytest.mark.parametrize(
    "actual,expected",
    [
        ("lower(text)", daft.col("text").str.lower()),
        ("abs(n)", daft.col("n").abs()),
        ("n + 1", daft.col("n") + 1),
        ("ceil(1.1)", daft.lit(1.1).ceil()),
        ("contains(text, 'hello')", daft.col("text").str.contains("hello")),
        ("to_date(date_col, 'YYYY-MM-DD')", daft.col("date_col").str.to_date("YYYY-MM-DD")),
    ],
)
def test_sql_expr(actual, expected):
    actual = daft.sql_expr(actual)
    # Non plain-column-select expressions will be aliased with the representation
    expected = expected.alias(repr(expected))
    assert repr(actual) == repr(expected)


def test_sql_expr_plain_col():
    # Plain-column-select expressions are NOT aliased (in dataframe they will retain their original name)
    assert repr(daft.sql_expr("n")) == "col(n)"


def test_sql_global_agg():
    df = daft.from_pydict({"n": [1, 2, 3]})
    catalog = SQLCatalog({"test": df})
    df = daft.sql("SELECT max(n) max_n, sum(n) sum_n FROM test", catalog=catalog)
    assert df.collect().to_pydict() == {"max_n": [3], "sum_n": [6]}
    # If there is agg and non-agg, it should fail
    with pytest.raises(Exception, match="Expected aggregation"):
        daft.sql("SELECT n,max(n) max_n FROM test", catalog=catalog)


def test_sql_groupby_agg():
    df = daft.from_pydict({"n": [1, 1, 2, 2], "v": [1, 2, 3, 4]})
    catalog = SQLCatalog({"test": df})
    df = daft.sql("SELECT sum(v) FROM test GROUP BY n ORDER BY n", catalog=catalog)
    assert df.collect().to_pydict() == {"n": [1, 2], "sum(col(v))": [3, 7]}
