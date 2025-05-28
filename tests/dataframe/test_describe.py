from __future__ import annotations

import daft
import daft.recordbatch
from daft import DataFrame, Schema
from daft import DataType as dt
from daft.logical.schema import Field


def test_describe():
    fields = [
        Field.create("c_int8", dt.int8()),
        Field.create("c_int16", dt.int16()),
        Field.create("c_int32", dt.int32()),
        Field.create("c_int64", dt.int64()),
        Field.create("c_uint8", dt.uint8()),
        Field.create("c_uint16", dt.uint16()),
        Field.create("c_uint32", dt.uint32()),
        Field.create("c_uint64", dt.uint64()),
        Field.create("c_float32", dt.float32()),
        Field.create("c_float64", dt.float64()),
        Field.create("c_string", dt.string()),
        Field.create("c_bool", dt.bool()),
        Field.create("c_binary", dt.binary()),
        Field.create("c_date", dt.date()),
        Field.create("c_time", dt.time(daft.TimeUnit.ns())),
        Field.create("c_timestamp", dt.timestamp(daft.TimeUnit.ns())),
        Field.create("c_duration", dt.duration(daft.TimeUnit.ns())),
        Field.create("c_interval", dt.interval()),
        Field.create("c_list", dt.list(dt.int32())),
        Field.create("c_fixed_size_list", dt.fixed_size_list(dt.int32(), 2)),
        Field.create("c_map", dt.map(dt.string(), dt.int32())),
        Field.create("c_struct", dt.struct({"a": dt.int32()})),
        Field.create("c_extension", dt.extension("test", dt.int32())),
        Field.create("c_embedding", dt.embedding(dt.float32(), 4)),
        Field.create("c_image", dt.image()),
        Field.create("c_tensor", dt.tensor(dt.float32())),
        Field.create("c_sparse_tensor", dt.sparse_tensor(dt.float32())),
    ]
    # create an empty table with known schema
    mp = daft.recordbatch.MicroPartition.empty(Schema._from_fields(fields))
    df = DataFrame._from_micropartitions(mp)
    # describe..
    df = df.describe().collect()
    # assert
    assert df.count_rows() == len(fields), "Output from describe() is missing some columns"
    expect_columns = set(f.name for f in fields)
    actual_columns = df.to_pydict()["column_name"]
    for c in actual_columns:
        assert c in expect_columns, f"Column {c} from describe() not found in original schema"
