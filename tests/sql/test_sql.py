from __future__ import annotations

import os

import numpy as np
import pytest

import daft
from daft import DataType, Series, col
from daft.exceptions import DaftCoreException
from tests.assets import TPCH_QUERIES


def load_tpch_queries():
    """Load all TPCH queries into a list of (name,sql) tuples."""
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
    """Load a single TPCH query from a file."""
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
    bindings = {"test": daft.from_pydict({"a": [1, 2, 3]})}
    df = daft.sql("SELECT * FROM test", **bindings)
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
    bindings = {"test": df}
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
        **bindings,
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
    bindings = {"test": df}
    df = daft.sql("SELECT max(n) max_n, sum(n) sum_n FROM test", **bindings)
    assert df.collect().to_pydict() == {"max_n": [3], "sum_n": [6]}
    # If there is agg and non-agg, it should fail
    # TODO: we can emit a better error message if our agg op can also include expressions on groupby expressions
    with pytest.raises(Exception):
        daft.sql("SELECT n,max(n) max_n FROM test", **bindings).collect()


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT sum(v) as sum FROM test GROUP BY n ORDER BY n", {"sum": [3, 7]}),
        ("SELECT n, sum(v) as sum FROM test GROUP BY n ORDER BY n", {"n": [1, 2], "sum": [3, 7]}),
        ("SELECT max(v) as max, sum(v) as sum FROM test GROUP BY n ORDER BY n", {"max": [2, 4], "sum": [3, 7]}),
        ("SELECT n as n_alias, sum(v) as sum FROM test GROUP BY n ORDER BY n", {"n_alias": [1, 2], "sum": [3, 7]}),
        ("SELECT n, sum(v) as sum FROM test GROUP BY n ORDER BY sum", {"n": [1, 2], "sum": [3, 7]}),
    ],
)
def test_sql_groupby_agg(query, expected):
    df = daft.from_pydict({"n": [1, 1, 2, 2], "v": [1, 2, 3, 4]})
    bindings = {"test": df}
    actual = daft.sql(query, **bindings)
    assert actual.collect().to_pydict() == expected


def test_sql_count_star():
    df = daft.from_pydict(
        {
            "a": ["a", "b", None, "c"],
            "b": [4, 3, 2, None],
        }
    )
    bindings = {"df": df}
    df2 = daft.sql("SELECT count(*) FROM df", **bindings)
    actual = df2.collect().to_pydict()
    expected = df.count().collect().to_pydict()
    assert actual == expected
    df2 = daft.sql("SELECT count(b) FROM df", **bindings)
    actual = df2.collect().to_pydict()
    expected = df.agg(daft.col("b").count()).collect().to_pydict()
    assert actual == expected


@pytest.fixture
def set_global_df():
    global GLOBAL_DF
    GLOBAL_DF = daft.from_pydict({"n": [1, 2, 3]})


def test_sql_function_sees_caller_tables(set_global_df):
    # sees the globals
    df = daft.sql("SELECT * FROM GLOBAL_DF")
    assert df.collect().to_pydict() == GLOBAL_DF.collect().to_pydict()
    # sees the locals
    df_copy = daft.sql("SELECT * FROM df")
    assert df.collect().to_pydict() == df_copy.collect().to_pydict()


def test_sql_function_locals_shadow_globals(set_global_df):
    GLOBAL_DF = None  # noqa: F841
    with pytest.raises(Exception, match="Table not found"):
        daft.sql("SELECT * FROM GLOBAL_DF")


def test_sql_function_globals_are_added_to_catalog(set_global_df):
    df = daft.from_pydict({"n": [1], "x": [2]})
    res = daft.sql("SELECT * FROM GLOBAL_DF g JOIN df d USING (n)", **{"df": df})
    joined = GLOBAL_DF.join(df, on="n")
    assert res.collect().to_pydict() == joined.collect().to_pydict()


def test_sql_function_catalog_is_final(set_global_df):
    df = daft.from_pydict({"a": [1]})
    # sanity check to ensure validity of below test
    assert df.collect().to_pydict() != GLOBAL_DF.collect().to_pydict()
    res = daft.sql("SELECT * FROM GLOBAL_DF", **{"GLOBAL_DF": df})
    assert res.collect().to_pydict() == df.collect().to_pydict()


def test_sql_function_register_globals(set_global_df):
    with pytest.raises(Exception, match="Table not found"):
        daft.sql("SELECT * FROM GLOBAL_DF", register_globals=False)


def test_sql_function_raises_when_cant_get_frame(monkeypatch):
    monkeypatch.setattr("inspect.currentframe", lambda: None)
    with pytest.raises(DaftCoreException, match="Cannot get caller environment"):
        daft.sql("SELECT * FROM df")


def test_sql_multi_statement_sql_error():
    with pytest.raises(Exception, match="one SQL statement allowed"):
        daft.sql("SELECT * FROM df; SELECT * FROM df")


def test_sql_tbl_alias():
    bindings = {"df": daft.from_pydict({"n": [1, 2, 3]})}
    df = daft.sql("SELECT df_alias.n FROM df AS df_alias where df_alias.n = 2", **bindings)
    assert df.collect().to_pydict() == {"n": [2]}


def test_sql_distinct():
    df = daft.from_pydict({"n": [1, 1, 2, 2]})
    df = daft.sql("SELECT DISTINCT n FROM df").collect().to_pydict()
    assert set(df["n"]) == {1, 2}


@pytest.mark.parametrize(
    "query",
    [
        "select utf8 from tbl1 order by utf8",
        "select utf8 from tbl1 order by utf8 asc",
        "select utf8 from tbl1 order by utf8 desc",
        "select utf8 as a from tbl1 order by a",
        "select utf8 as a from tbl1 order by utf8",
        "select utf8 as a from tbl1 order by utf8 asc",
        "select utf8 as a from tbl1 order by utf8 desc",
        "select utf8 from tbl1 group by utf8 order by utf8",
        "select utf8 as a from tbl1 group by utf8 order by utf8",
        "select utf8 as a from tbl1 group by a order by utf8",
        "select utf8 as a from tbl1 group by a order by a",
        "select sum(i32), utf8 as a from tbl1 group by utf8 order by a",
        "select sum(i32) as s, utf8 as a from tbl1 group by utf8 order by s",
    ],
)
def test_compiles(query):
    tbl1 = daft.from_pydict(
        {
            "utf8": ["group1", "group1", "group2", "group2"],
            "i32": [1, 2, 3, 3],
        }
    )
    bindings = {"tbl1": tbl1}
    try:
        res = daft.sql(query, **bindings)
        data = res.collect().to_pydict()
        assert data

    except Exception as e:
        print(f"Error: {e}")
        raise


def test_sql_cte():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
    actual = (
        daft.sql("""
        WITH cte1 AS (select * FROM df)
        SELECT * FROM cte1
        """)
        .collect()
        .to_pydict()
    )

    expected = df.collect().to_pydict()

    assert actual == expected


def test_sql_cte_column_aliases():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
    actual = (
        daft.sql("""
        WITH cte1 (cte_a, cte_b, cte_c) AS (select * FROM df)
        SELECT * FROM cte1
        """)
        .collect()
        .to_pydict()
    )

    expected = (
        df.select(
            col("a").alias("cte_a"),
            col("b").alias("cte_b"),
            col("c").alias("cte_c"),
        )
        .collect()
        .to_pydict()
    )

    assert actual == expected


def test_sql_multiple_bindings():
    df1 = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
    df2 = daft.from_pydict({"x": [1, 0, 3], "y": [True, None, False], "z": [1.0, 2.0, 3.0]})
    actual = (
        daft.sql("""
        WITH
            cte1 AS (select * FROM df1),
            cte2 AS (select x as a, y, z FROM df2)
        SELECT *
        FROM cte1
        JOIN cte2 USING (a)
        """)
        .collect()
        .to_pydict()
    )
    expected = df1.join(df2.select(col("x").alias("a"), "y", "z"), on="a").collect().to_pydict()

    assert actual == expected


def test_cast_image():
    channels = 3

    data = [
        np.arange(4 * channels, dtype=np.uint8).reshape((2, 2, channels)),
        np.arange(4 * channels, 13 * channels, dtype=np.uint8).reshape((3, 3, channels)),
        None,
    ]

    s = Series.from_pylist(data, dtype=DataType.python())
    df = daft.from_pydict({"img": s})
    actual = daft.sql("select cast(img as image(RGB)) from df", **{"df": df}).collect()
    assert actual.schema()["img"].dtype == DataType.image("RGB")
