from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import daft
from daft import Catalog
from daft.logical.schema import DataType as dt
from daft.logical.schema import Field, Schema


@pytest.fixture(scope="session")
def catalog(local_iceberg_catalog):
    return Catalog.from_iceberg(local_iceberg_catalog)


def schema(fields: dict[str, dt]) -> Schema:
    return Schema._from_fields([Field.create(k, v) for k, v in fields.items()])


def assert_eq(df1, df2):
    assert df1.to_pydict() == df2.to_pydict()


###
# read tests
###


###
# write tests
###


@pytest.mark.integration()
def test_append(catalog):
    pass


@pytest.mark.integration()
def test_ovewrite(catalog):
    pass


def test_write_table(catalog: Catalog):
    c = catalog
    n = "test_write_table"
    t1 = f"{n}.tbl1"
    t2 = f"{n}.tbl2"
    c.create_namespace(n)
    c.create_table(t1, schema({"v": dt.int64()}))
    c.create_table(t2, schema({"v": dt.int64()}))
    #
    part1 = daft.from_pydict({"v": [1, 2, 3]})
    part2 = daft.from_pydict({"v": [4, 5, 6]})
    parts = daft.from_pydict({"v": [4, 5, 6, 1, 2, 3]})
    #
    # append parts to t1 using catalog.write_table helper
    c.write_table(t1, part1)
    assert_eq(c.read_table(t1), part1)
    c.write_table(t1, part2)
    assert_eq(c.read_table(t1), parts)
    #
    # append parts to t2 using table methods
    tbl2 = c.get_table(t2)
    tbl2.append(part1)
    assert_eq(tbl2.read(), part1)
    tbl2.append(part2)
    assert_eq(tbl2.read(), parts)
    #
    # assert overwrite
    #
    part3 = daft.from_pydict({"v": [0]})
    #
    # overwrite t1 using catalog.write with mode
    c.write_table(t1, part3, mode="overwrite")
    assert_eq(c.read_table(t1), part3)
    #
    # overwrite t2 using table methods
    tbl2.overwrite(part3)
    assert_eq(tbl2.read(), part3)
