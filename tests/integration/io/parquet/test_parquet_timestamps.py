from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyarrow import parquet as pq

from daft import TimeUnit
from daft.table import Table


@pytest.mark.integration()
def test_ts_roundtrip(tmp_path):
    ts_str = pa.array(["1970-01-01T00:00:59.123456789", "2000-02-29T23:23:23.999999999"], pa.string())
    ts = pc.strptime(pc.utf8_slice_codeunits(ts_str, 0, 19), format="%Y-%m-%dT%H:%M:%S", unit="ns")
    index = pa.array(list(range(len(ts))))
    source_table = pa.table({"index": index, "ts": ts})
    target = f"{tmp_path}/test.parquet"
    pq.write_table(source_table, target, version="1.0", use_deprecated_int96_timestamps=True)

    for unit in ["ns", "ms"]:
        pa_read_back = pq.read_table(target, coerce_int96_timestamp_unit=unit)
        daft_read_back = Table.read_parquet(target, coerce_int96_timestamp_unit=TimeUnit.from_str(unit)).to_arrow()
        assert pa_read_back == daft_read_back, f"failed for unit {unit}"


@pytest.mark.integration()
def test_ts_roundtrip_out_of_range_for_ns(tmp_path):
    ts_str = pa.array(
        ["1200-01-01T00:00:59.123456789", "2000-02-29T23:23:23.999999999", "2400-02-29T23:23:23.999999999"], pa.string()
    )
    ts = pc.strptime(pc.utf8_slice_codeunits(ts_str, 0, 19), format="%Y-%m-%dT%H:%M:%S", unit="ms")
    index = pa.array(list(range(len(ts))))
    source_table = pa.table({"index": index, "ts": ts})
    target = f"{tmp_path}/test.parquet"
    pq.write_table(source_table, target, version="1.0", use_deprecated_int96_timestamps=True)

    pa_read_back = pq.read_table(target, coerce_int96_timestamp_unit="ms")
    daft_read_back = Table.read_parquet(target, coerce_int96_timestamp_unit=TimeUnit.from_str("ms")).to_arrow()

    assert pa_read_back == daft_read_back
