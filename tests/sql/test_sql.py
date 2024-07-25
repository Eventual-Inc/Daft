import os

import pytest

import daft
from tests.assets import TPCH_QUERIES


# Load all TPCH queries into a list of (name,sql) tuples
def load_tpch_queries():
    queries = []
    for filename in os.listdir(TPCH_QUERIES):
        filepath = os.path.join(TPCH_QUERIES, filename)
        if os.path.isfile(filepath) and filepath.endswith(".sql"):
            with open(filepath) as f:
                sql = f.read()
                name = "TPC-H " + os.path.basename(filepath)
                queries.append((name, sql))
    return queries


# Load all TPCH queries once
tpch_queries = load_tpch_queries()


# Sanity check
def test_sanity():
    # Assert no throw
    df = daft.sql("SELECT * FROM my_table")
    print(df)


# Test that all TPCH queries can be parsed
@pytest.mark.skip(reason="This test is a placeholder used to check current work")
@pytest.mark.parametrize("name,sql", tpch_queries)
def test_parse_ok(name, sql):
    print(name)
    print(sql)
    print("--------------")
