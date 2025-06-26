from __future__ import annotations

from daft import DataType

daft_int_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
]
daft_floating_types = [DataType.float32(), DataType.float64()]
daft_numeric_types = daft_int_types + daft_floating_types
daft_string_types = [DataType.string()]
daft_nonnull_types = daft_numeric_types + daft_string_types + [DataType.bool(), DataType.binary(), DataType.date()]
daft_null_types = [DataType.null()]

daft_comparable_types = daft_numeric_types + daft_string_types + daft_null_types + [DataType.bool(), DataType.date()]
