import os

import pytest

import daft
from daft.sql.sql_catalog import SQLCatalog
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

"""Load all TPCH queries once"""
all_tpch_queries = load_tpch_queries()

# @pytest.mark.skip(reason="Skip the sanity check")
def test_sanity():
    # Assert no throw
    catalog = SQLCatalog({
        "test": daft.from_pydict({"a": [1, 2, 3]})
    })
    print(catalog)
    # df = daft.sql("SELECT * FROM my_table")
    # print(df)

@pytest.mark.skip(reason="This test is a placeholder used to check that we can parse the TPC-H queries")
@pytest.mark.parametrize("name,sql", all_tpch_queries)
def test_parse_ok(name, sql):
    print(name)
    print(sql)
    print("--------------")
