from __future__ import annotations

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="iceberg only supported if pyarrow >= 8.0.0")


from pyiceberg.catalog.sql import SqlCatalog

import daft


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    catalog.create_namespace("default")
    return catalog


def test_read_after_write_append(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()


def test_read_after_write_overwrite(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]

    # write again (in append)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]

    read_back = daft.read_iceberg(table)
    assert pa.concat_tables([as_arrow, as_arrow]) == read_back.to_arrow()

    # write again (in overwrite)
    result = df.write_iceberg(table, mode="overwrite")
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD", "DELETE", "DELETE"]
    assert as_dict["rows"] == [5, 5, 5]

    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()


def test_read_and_overwrite(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]

    df = daft.read_iceberg(table).with_column("x", daft.col("x") + 1)
    result = df.write_iceberg(table, mode="overwrite")
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD", "DELETE"]
    assert as_dict["rows"] == [5, 5]

    read_back = daft.read_iceberg(table)
    assert daft.from_pydict({"x": [2, 3, 4, 5, 6]}).to_arrow() == read_back.to_arrow()


def test_missing_columns_write(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)

    df = daft.from_pydict({"y": [1, 2, 3, 4, 5]})
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]
    read_back = daft.read_iceberg(table)
    assert read_back.to_pydict() == {"x": [None] * 5}


def test_too_many_columns_write(local_catalog):
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)

    df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [5]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()


@pytest.mark.skip
def test_read_after_write_nested_fields(local_catalog):
    # We need to cast Large Types such as LargeList and LargeString to the i32 variants
    df = daft.from_pydict({"x": [["a", "b"], ["c", "d", "e"]]})
    as_arrow = df.to_arrow()
    table = local_catalog.create_table("default.test", as_arrow.schema)
    result = df.write_iceberg(table)
    as_dict = result.to_pydict()
    assert as_dict["operation"] == ["ADD"]
    assert as_dict["rows"] == [2]
    read_back = daft.read_iceberg(table)
    assert as_arrow == read_back.to_arrow()
