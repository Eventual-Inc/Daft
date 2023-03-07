from __future__ import annotations

import numpy as np
import xxhash

from daft.datatype import DataType
from daft.series import Series


def test_hash_int_array_with_reference():
    arr = Series.from_numpy(np.random.randint(0, 127, 100)).cast(DataType.uint64())

    hashed = arr.hash()
    nbytes = 8
    for v, hv in zip(arr.to_pylist(), hashed.to_pylist()):
        vbytes = v.to_bytes(nbytes, "little")
        ref_value = xxhash.xxh3_64_intdigest(vbytes)
        assert hv == ref_value


def test_hash_bool_array_with_reference():
    arr = Series.from_pylist([False, True, True])
    expected = [xxhash.xxh3_64_intdigest(b"0"), xxhash.xxh3_64_intdigest(b"1"), xxhash.xxh3_64_intdigest(b"1")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected


def test_hash_str_array_with_reference():
    arr = Series.from_pylist(["hi", "bye", None])
    expected = [xxhash.xxh3_64_intdigest(b"hi"), xxhash.xxh3_64_intdigest(b"bye"), xxhash.xxh3_64_intdigest(b"")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected


def test_hash_null_array_with_reference():
    arr = Series.from_pylist([None, None, None])
    expected = [xxhash.xxh3_64_intdigest(b""), xxhash.xxh3_64_intdigest(b""), xxhash.xxh3_64_intdigest(b"")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected
