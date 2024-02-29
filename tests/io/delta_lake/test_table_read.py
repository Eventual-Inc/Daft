from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="deltalake only supported if pyarrow >= 8.0.0")


def test_deltalake_read_basic(tmp_path, base_table):
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, base_table)
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    assert_pyarrow_tables_equal(df.to_arrow(), base_table)


def test_deltalake_read_full(local_deltalake_table):
    path, parts = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    assert_pyarrow_tables_equal(df.to_arrow(), pa.concat_tables(parts))


def test_deltalake_read_show(local_deltalake_table):
    path, _ = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df.show()
