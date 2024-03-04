from __future__ import annotations

import pytest

from daft.delta_lake.delta_lake_scan import _io_config_to_storage_options

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
    expected_schema = Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow(), base_table)


def test_deltalake_read_full(deltalake_table):
    path, io_config, parts = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow().sort_by("part_idx"), pa.concat_tables(parts))


def test_deltalake_read_show(deltalake_table):
    path, io_config, _ = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    df.show()
