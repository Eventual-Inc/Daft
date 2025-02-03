import os

import numpy as np
import pytest

import daft
from daft import DataType, Series, col
from daft.exceptions import DaftCoreException
from daft.sql.sql import SQLCatalog
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
    with pytest.raises(Exception, match="Column not found"):
        daft.sql("SELECT n,max(n) max_n FROM test", catalog=catalog)


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
    catalog = SQLCatalog({"test": df})
    actual = daft.sql(query, catalog=catalog)
    assert actual.collect().to_pydict() == expected


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
    res = daft.sql("SELECT * FROM GLOBAL_DF g JOIN df d USING (n)", catalog=SQLCatalog({"df": df}))
    joined = GLOBAL_DF.join(df, on="n")
    assert res.collect().to_pydict() == joined.collect().to_pydict()


def test_sql_function_catalog_is_final(set_global_df):
    df = daft.from_pydict({"a": [1]})
    # sanity check to ensure validity of below test
    assert df.collect().to_pydict() != GLOBAL_DF.collect().to_pydict()
    res = daft.sql("SELECT * FROM GLOBAL_DF", catalog=SQLCatalog({"GLOBAL_DF": df}))
    assert res.collect().to_pydict() == df.collect().to_pydict()


def test_sql_function_register_globals(set_global_df):
    with pytest.raises(Exception, match="Table not found"):
        daft.sql("SELECT * FROM GLOBAL_DF", SQLCatalog({}), register_globals=False)


def test_sql_function_requires_catalog_or_globals(set_global_df):
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


def test_sql_tbl_alias():
    catalog = SQLCatalog({"df": daft.from_pydict({"n": [1, 2, 3]})})
    df = daft.sql("SELECT df_alias.n FROM df AS df_alias where df_alias.n = 2", catalog)
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
    catalog = SQLCatalog({"tbl1": tbl1})
    try:
        res = daft.sql(query, catalog=catalog)
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


def test_sql_multiple_ctes():
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

    s = Series.from_pylist(data, pyobj="force")
    df = daft.from_pydict({"img": s})
    actual = daft.sql("select cast(img as image(RGB)) from df", catalog=SQLCatalog({"df": df})).collect()
    assert actual.schema()["img"].dtype == DataType.image("RGB")


def test_various_type_aliases():
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["a", "b", "c"], "c": [0, 1, 0]})

    actual = daft.sql("""
    SELECT
        cast(a as int16) as a_int16,
        cast(a as int32) as a_int32,
        cast(a as int64) as a_int64,
        cast(a as uint16) as a_uint16,
        cast(a as uint32) as a_uint32,
        cast(a as uint64) as a_uint64,
        cast(a as i8) as a_i8,
        cast(a as i16) as a_i16,
        cast(a as i32) as a_i32,
        cast(a as i64) as a_i64,
        cast(a as u8) as a_u8,
        cast(a as u16) as a_u16,
        cast(a as u32) as a_u32,
        cast(a as u64) as a_u64,
        cast(a as f32) as a_f32,
        cast(a as f64) as a_f64,
        cast(a as float32) as a_float32,
        cast(a as float64) as a_float64,
        cast(a as float) as a_float,
        cast(a as real) as a_real,
        cast(a as double) as a_double,
        cast(b as string) as b_string,
        cast(b as text) as b_text,
        cast(b as varchar) as b_varchar,
        cast(b as binary) as b_binary,
        cast(c as bool) as c_bool,
        cast(c as boolean) as c_boolean
    from df
    """)

    expected = df.select(
        col("a").cast(DataType.int16()).alias("a_int16"),
        col("a").cast(DataType.int32()).alias("a_int32"),
        col("a").cast(DataType.int64()).alias("a_int64"),
        col("a").cast(DataType.uint16()).alias("a_uint16"),
        col("a").cast(DataType.uint32()).alias("a_uint32"),
        col("a").cast(DataType.uint64()).alias("a_uint64"),
        col("a").cast(DataType.int8()).alias("a_i8"),
        col("a").cast(DataType.int16()).alias("a_i16"),
        col("a").cast(DataType.int32()).alias("a_i32"),
        col("a").cast(DataType.int64()).alias("a_i64"),
        col("a").cast(DataType.uint8()).alias("a_u8"),
        col("a").cast(DataType.uint16()).alias("a_u16"),
        col("a").cast(DataType.uint32()).alias("a_u32"),
        col("a").cast(DataType.uint64()).alias("a_u64"),
        col("a").cast(DataType.float32()).alias("a_f32"),
        col("a").cast(DataType.float64()).alias("a_f64"),
        col("a").cast(DataType.float32()).alias("a_float32"),
        col("a").cast(DataType.float64()).alias("a_float64"),
        col("a").cast(DataType.float32()).alias("a_float"),
        col("a").cast(DataType.float64()).alias("a_real"),
        col("a").cast(DataType.float64()).alias("a_double"),
        col("b").cast(DataType.string()).alias("b_string"),
        col("b").cast(DataType.string()).alias("b_text"),
        col("b").cast(DataType.string()).alias("b_varchar"),
        col("b").cast(DataType.binary()).alias("b_binary"),
        col("c").cast(DataType.bool()).alias("c_bool"),
        col("c").cast(DataType.bool()).alias("c_boolean"),
    )

    assert actual.to_pydict() == expected.to_pydict()
