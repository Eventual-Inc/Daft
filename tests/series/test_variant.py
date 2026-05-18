from __future__ import annotations

import pyarrow as pa

from daft.datatype import DataType
from daft.series import Series


def _make_variant_series():
    """Build a Daft Variant Series from raw binary metadata+value arrays."""
    metadata_bin = [b"\x01\x00\x00", b"\x01\x00\x00", None]
    value_bin = [b"\x00", b"\x02\x01", None]

    metadata_arr = pa.array(metadata_bin, type=pa.large_binary())
    value_arr = pa.array(value_bin, type=pa.large_binary())

    struct_arr = pa.StructArray.from_arrays(
        [metadata_arr, value_arr],
        names=["metadata", "value"],
    )

    return Series.from_arrow(struct_arr, name="v", dtype=DataType.variant())


def test_variant_dtype_properties():
    dt = DataType.variant()
    assert dt.is_variant()
    assert dt.is_logical()
    assert not dt.is_struct()
    assert not dt.is_numeric()
    assert str(dt) == "Variant"


def test_variant_from_arrow():
    s = _make_variant_series()
    assert s.datatype() == DataType.variant()
    assert len(s) == 3


def test_variant_cast_to_self():
    s = _make_variant_series()
    casted = s.cast(DataType.variant())
    assert casted.datatype() == DataType.variant()
    assert len(casted) == 3


def test_variant_repr():
    s = _make_variant_series()
    r = repr(s)
    assert "Variant" in r
