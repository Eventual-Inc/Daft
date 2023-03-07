from __future__ import annotations

import numpy as np
import pytest
import xxhash

from daft.datatype import DataType
from daft.series import Series


@pytest.mark.parametrize(
    "nbytes, dtype",
    [
        (1, DataType.uint8()),
        (1, DataType.int8()),
        (2, DataType.uint16()),
        (2, DataType.int16()),
        (4, DataType.uint32()),
        (4, DataType.int32()),
        (8, DataType.uint64()),
        (8, DataType.int64()),
    ],
)
def test_hash_int_array_with_reference(nbytes, dtype):
    arr = Series.from_numpy(np.random.randint(0, 127, 100)).cast(dtype)

    hashed = arr.hash()
    for v, hv in zip(arr.to_pylist(), hashed.to_pylist()):
        vbytes = v.to_bytes(nbytes, "little")
        ref_value = xxhash.xxh3_64_intdigest(vbytes)
        assert hv == ref_value

    hashed_again = arr.hash(hashed)

    for v, hv, hav in zip(arr.to_pylist(), hashed.to_pylist(), hashed_again.to_pylist()):
        vbytes = v.to_bytes(nbytes, "little")
        ref_value = xxhash.xxh3_64_intdigest(vbytes, seed=hv)
        assert hav == ref_value


def test_hash_bool_array_with_reference():
    arr = Series.from_pylist([False, True, True])
    expected = [xxhash.xxh3_64_intdigest(b"0"), xxhash.xxh3_64_intdigest(b"1"), xxhash.xxh3_64_intdigest(b"1")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected

    hashed_again = arr.hash(hashed)
    expected = [
        xxhash.xxh3_64_intdigest(b"0", expected[0]),
        xxhash.xxh3_64_intdigest(b"1", expected[1]),
        xxhash.xxh3_64_intdigest(b"1", expected[2]),
    ]
    assert hashed_again.to_pylist() == expected


def test_hash_str_array_with_reference():
    arr = Series.from_pylist(["hi", "bye", None])
    expected = [xxhash.xxh3_64_intdigest(b"hi"), xxhash.xxh3_64_intdigest(b"bye"), xxhash.xxh3_64_intdigest(b"")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected

    hashed_again = arr.hash(hashed)
    expected = [
        xxhash.xxh3_64_intdigest(b"hi", expected[0]),
        xxhash.xxh3_64_intdigest(b"bye", expected[1]),
        xxhash.xxh3_64_intdigest(b"", expected[2]),
    ]
    assert hashed_again.to_pylist() == expected


def test_hash_null_array_with_reference():
    arr = Series.from_pylist([None, None, None])
    expected = [xxhash.xxh3_64_intdigest(b""), xxhash.xxh3_64_intdigest(b""), xxhash.xxh3_64_intdigest(b"")]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected

    hashed_again = arr.hash(hashed)
    expected = [
        xxhash.xxh3_64_intdigest(b"", expected[0]),
        xxhash.xxh3_64_intdigest(b"", expected[1]),
        xxhash.xxh3_64_intdigest(b"", expected[2]),
    ]
    assert hashed_again.to_pylist() == expected


def test_hash_int_array_with_bad_seed():
    arr = Series.from_numpy(np.random.randint(0, 127, 100)).cast(DataType.uint64())

    bad_seed = Series.from_numpy(np.random.randint(0, 127, 100)).cast(DataType.float64())

    with pytest.raises(ValueError, match="We can only use UInt64 as a seed"):
        arr.hash(bad_seed)


def test_hash_int_array_with_bad_length():
    arr = Series.from_numpy(np.random.randint(0, 127, 100)).cast(DataType.uint64())

    bad_seed = Series.from_numpy(np.random.randint(0, 127, 99)).cast(DataType.uint64())

    with pytest.raises(ValueError, match="seed length does not match array length"):
        arr.hash(bad_seed)
