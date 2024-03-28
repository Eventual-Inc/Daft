from __future__ import annotations

import pyarrow as pa
import pytest

import daft

PYARROW_LE_10_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (10, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_10_0_0, reason="hudi only supported if pyarrow >= 10.0.0")


def test_hudi_read_table(unzip_table_0_x_cow_partitioned):
    path = unzip_table_0_x_cow_partitioned
    df = daft.read_hudi(path)
    assert df.schema().column_names() == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert df.select("rider").sort("rider").to_pydict() == {
        "rider": ["rider-A", "rider-C", "rider-D", "rider-F", "rider-J"]
    }
