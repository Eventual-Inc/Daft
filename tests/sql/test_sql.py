import os

import numpy as np
import pytest

import daft
from daft.exceptions import DaftCoreException
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
    assert repr(actual) == repr(expected)


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
    actual = daft.sql("SELECT sum(v) as sum FROM test GROUP BY n ORDER BY n", catalog=catalog)
    assert actual.collect().to_pydict() == {"sum": [3, 7]}

    # test with grouping column
    actual = daft.sql("SELECT n, sum(v) as sum FROM test GROUP BY n ORDER BY n", catalog=catalog)
    assert actual.collect().to_pydict() == {"n": [1, 2], "sum": [3, 7]}

    # test with multiple columns
    actual = daft.sql("SELECT max(v) as max, sum(v) as sum FROM test GROUP BY n ORDER BY n", catalog=catalog)
    assert actual.collect().to_pydict() == {"max": [2, 4], "sum": [3, 7]}

    # test with aliased grouping key
    actual = daft.sql("SELECT n as n_alias, sum(v) as sum FROM test GROUP BY n ORDER BY n", catalog=catalog)
    assert actual.collect().to_pydict() == {"n_alias": [1, 2], "sum": [3, 7]}

    actual = daft.sql("SELECT n, sum(v) as sum FROM test GROUP BY n ORDER BY -n", catalog=catalog)
    assert actual.collect().to_pydict() == {"n": [2, 1], "sum": [7, 3]}

    actual = daft.sql("SELECT n, sum(v) as sum FROM test GROUP BY n ORDER BY sum", catalog=catalog)
    assert actual.collect().to_pydict() == {"n": [1, 2], "sum": [3, 7]}


def test_sql_count_star():
    df = daft.from_pydict(
        {
            "a": ["a", "b", None, "c"],
            "b": [4, 3, 2, None],
        }
    )
    catalog = SQLCatalog({"df": df})
    df2 = daft.sql("SELECT count(*) FROM df", catalog)
    actual = df2.collect().to_pydict()
    expected = df.count().collect().to_pydict()
    assert actual == expected
    df2 = daft.sql("SELECT count(b) FROM df", catalog)
    actual = df2.collect().to_pydict()
    expected = df.agg(daft.col("b").count()).collect().to_pydict()
    assert actual == expected


GLOBAL_DF = daft.from_pydict({"n": [1, 2, 3]})


def test_sql_function_sees_caller_tables():
    # sees the globals
    df = daft.sql("SELECT * FROM GLOBAL_DF")
    assert df.collect().to_pydict() == GLOBAL_DF.collect().to_pydict()
    # sees the locals
    df_copy = daft.sql("SELECT * FROM df")
    assert df.collect().to_pydict() == df_copy.collect().to_pydict()


def test_sql_function_locals_shadow_globals():
    GLOBAL_DF = None  # noqa: F841
    with pytest.raises(Exception, match="Table not found"):
        daft.sql("SELECT * FROM GLOBAL_DF")


def test_sql_function_globals_are_added_to_catalog():
    df = daft.from_pydict({"n": [1], "x": [2]})
    res = daft.sql("SELECT * FROM GLOBAL_DF g JOIN df d USING (n)", catalog=SQLCatalog({"df": df}))
    joined = GLOBAL_DF.join(df, on="n")
    assert res.collect().to_pydict() == joined.collect().to_pydict()


def test_sql_function_catalog_is_final():
    df = daft.from_pydict({"a": [1]})
    # sanity check to ensure validity of below test
    assert df.collect().to_pydict() != GLOBAL_DF.collect().to_pydict()
    res = daft.sql("SELECT * FROM GLOBAL_DF", catalog=SQLCatalog({"GLOBAL_DF": df}))
    assert res.collect().to_pydict() == df.collect().to_pydict()


def test_sql_function_register_globals():
    with pytest.raises(Exception, match="Table not found"):
        daft.sql("SELECT * FROM GLOBAL_DF", SQLCatalog({}), register_globals=False)


def test_sql_function_requires_catalog_or_globals():
    with pytest.raises(Exception, match="Must supply a catalog"):
        daft.sql("SELECT * FROM GLOBAL_DF", register_globals=False)


def test_sql_function_raises_when_cant_get_frame(monkeypatch):
    monkeypatch.setattr("inspect.currentframe", lambda: None)
    with pytest.raises(DaftCoreException, match="Cannot get caller environment"):
        daft.sql("SELECT * FROM df")


def test_sql_multi_statement_sql_error():
    catalog = SQLCatalog({})
    with pytest.raises(Exception, match="one SQL statement allowed"):
        daft.sql("SELECT * FROM df; SELECT * FROM df", catalog)
