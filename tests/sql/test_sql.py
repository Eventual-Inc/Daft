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
