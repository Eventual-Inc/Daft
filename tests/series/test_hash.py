from __future__ import annotations

import decimal
from datetime import date, datetime

import numpy as np
import pytest
import pytz
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


def test_hash_binary_array_with_reference():
    arr = Series.from_pylist([b"hi", b"bye", None])
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


@pytest.mark.parametrize(
    "dtype",
    [
        DataType.uint8(),
        DataType.uint16(),
        DataType.uint32(),
        DataType.uint64(),
        DataType.int8(),
        DataType.int16(),
        DataType.int32(),
        DataType.int64(),
    ],
)
def test_murmur3_32_hash_int(dtype):
    arr = Series.from_pylist([34, None]).cast(dtype)
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [2017239379, None]


@pytest.mark.parametrize(
    "dtype",
    [
        DataType.int8(),
        DataType.int16(),
        DataType.int32(),
        DataType.int64(),
    ],
)
def test_murmur3_32_hash_signed_int(dtype):
    arr = Series.from_pylist([-1, 34, None]).cast(dtype)
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [1651860712, 2017239379, None]


def test_murmur3_32_hash_string():
    arr = Series.from_pylist(["iceberg", None])
    assert arr.datatype() == DataType.string()
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [1210000089, None]


def test_murmur3_32_hash_string():
    arr = Series.from_pylist([b"\x00\x01\x02\x03", None])
    assert arr.datatype() == DataType.binary()
    hashes = arr.murmur3_32()
    java_answer = -188683207
    assert hashes.to_pylist() == [java_answer, None]


def test_murmur3_32_hash_date():
    arr = Series.from_pylist([date(2017, 11, 16), None])
    assert arr.datatype() == DataType.date()
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-653330422, None]


def test_murmur3_32_hash_timestamp():
    arr = Series.from_pylist([datetime(2017, 11, 16, 22, 31, 8), None])
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-2047944441, None]


def test_murmur3_32_hash_timestamp_with_tz():
    dt = datetime(2017, 11, 16, 14, 31, 8)
    pst = pytz.timezone("US/Pacific")
    dt = pst.localize(dt)
    arr = Series.from_pylist([dt, None])
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-2047944441, None]


def test_murmur3_32_hash_timestamp_with_tz_nanoseconds():
    dt = datetime(2017, 11, 16, 14, 31, 8)
    pst = pytz.timezone("US/Pacific")
    dt = pst.localize(dt)
    arr = Series.from_pylist([dt, None])
    arr = arr.cast(DataType.timestamp("ns", "UTC"))
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-2047944441, None]


def test_murmur3_32_hash_decimal_unscaled():
    arr = Series.from_pylist([decimal.Decimal(1420), None])
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-500754589, None]


def test_murmur3_32_hash_decimal_scaled():
    arr = Series.from_pylist([decimal.Decimal("14.20"), None])
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-500754589, None]


def test_murmur3_32_hash_decimal_full_scaled():
    arr = Series.from_pylist([decimal.Decimal(".00001420"), None])
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-500754589, None]
