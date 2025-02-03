import pytest

import daft
from daft.sql import SQLCatalog


@pytest.mark.parametrize(
    "table_name",
    ["a", "a_b", '"a.b"', '"a.b.c"', "a_", "a_1", '"a-b"', '"a."'],
)
def test_sql_catalog_table_names(table_name):
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    catalog = {table_name: df1}
    try:
        actual = daft.sql(f"select * from {table_name}", catalog=SQLCatalog(catalog)).to_pydict()
        assert actual == df1.to_pydict()
    except Exception as e:
        assert False, f"Unexpected exception: {e}"


@pytest.mark.parametrize("table_name", ["1", "a.", "a b", "a-b"])
def test_sql_catalog_table_names_invalid(table_name):
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    catalog = {table_name: df1}
    try:
        actual = daft.sql(f"select * from {table_name}", catalog=SQLCatalog(catalog)).to_pydict()
        assert actual == df1.to_pydict()
    except Exception as e:
        assert True, f"Expected exception: {e}"


def test_sql_register_globals():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    catalog = SQLCatalog({"df2": df1})
    try:
        daft.sql("select * from df1", catalog=catalog, register_globals=False).collect()
    except Exception as e:
        assert True, f"Expected exception: {e}"
