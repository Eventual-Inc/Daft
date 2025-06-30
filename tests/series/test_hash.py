from __future__ import annotations

import decimal
from datetime import date, datetime, time

import numpy as np
import pytest
import pytz
import xxhash

from daft.datatype import DataType
from daft.series import Series
from tests.test_datatypes import daft_numeric_types


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

    for v, hashed_value, hashed_again_value in zip(arr.to_pylist(), hashed.to_pylist(), hashed_again.to_pylist()):
        vbytes = v.to_bytes(nbytes, "little")
        ref_value = xxhash.xxh3_64_intdigest(vbytes, seed=hashed_value)
        assert hashed_again_value == ref_value


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


def test_hash_fixed_size_binary_array_with_reference():
    import pyarrow as pa

    arr = Series.from_arrow(pa.array([b"foo", b"bar", None], type=pa.binary(3)))
    expected = [
        xxhash.xxh3_64_intdigest(b"foo"),
        xxhash.xxh3_64_intdigest(b"bar"),
        xxhash.xxh3_64_intdigest(b"\x00\x00\x00"),
    ]
    hashed = arr.hash()
    assert hashed.to_pylist() == expected

    hashed_again = arr.hash(hashed)
    expected = [
        xxhash.xxh3_64_intdigest(b"foo", expected[0]),
        xxhash.xxh3_64_intdigest(b"bar", expected[1]),
        xxhash.xxh3_64_intdigest(b"\x00\x00\x00", expected[2]),
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

    with pytest.raises(ValueError, match="seed must be a numeric type"):
        arr.hash(bad_seed)


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_list_array_no_seed(dtype):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 2, 3], [], [], [2, 1]]).cast(DataType.list(dtype))

    hashed = arr.hash().to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]

    different_inds = [0, 1, 3, 4, 6]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [1, 2, 42])
def test_hash_list_array_seeded(dtype, seed):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 2, 3], [], [], [2, 1]]).cast(DataType.list(dtype))
    seeds = Series.from_pylist([seed] * 7).cast(DataType.uint64())
    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]

    different_inds = [0, 1, 3, 4, 6]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_list_array_no_seed_with_invalid(dtype):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 2, 3], [], [], None, [2, 1], None]).cast(DataType.list(dtype))

    hashed = arr.hash().to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]
    assert hashed[6] is None
    assert hashed[8] is None

    different_inds = [0, 1, 3, 4, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [1, 2, 42])
def test_hash_list_array_seeded_with_invalid(dtype, seed):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 2, 3], [], [], None, [2, 1], None]).cast(DataType.list(dtype))
    seeds = Series.from_pylist([seed] * 9).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]
    assert hashed[6] is None
    assert hashed[8] is None

    different_inds = [0, 1, 3, 4, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_list_array_different_seeds(dtype):
    arr = Series.from_pylist([[1, 2], [1, 2], [1, 2], [1, 2]]).cast(DataType.list(dtype))
    seeds = Series.from_pylist([1, 2, 3, 4]).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()

    different_inds = [0, 1, 2, 3]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_list_array_nested_lists(dtype):
    arr = Series.from_pylist(
        [
            [[1, 2], [3, 4]],
            [[1, 2], [3, 5]],
            [[1, 2], [3, 4]],
            [[3, 4], [1, 2]],
            [[1], [2]],
            [[], []],
            [[]],
            [],
            [[1, 2, 3]],
            [[1], [2], [3]],
        ]
    ).cast(DataType.list(DataType.list(dtype)))

    hashed = arr.hash().to_pylist()
    assert hashed[0] == hashed[2]

    different_inds = [0, 1, 3, 4, 5, 6, 7, 8, 9]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_list_array_consistency(dtype):
    data = [[1, 2], [1, 3], [1, 2], [1, 2, 3], [], [], None, [2, 1], None]
    arr1 = Series.from_pylist(data).cast(DataType.list(dtype))
    arr2 = Series.from_pylist(data).cast(DataType.list(dtype))

    hashed1 = arr1.hash().to_pylist()
    hashed2 = arr2.hash().to_pylist()
    assert hashed1 == hashed2


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_fixed_size_list_array_no_seed(dtype):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 4], [5, 5], [5, 5], [2, 1]]).cast(
        DataType.fixed_size_list(dtype, 2)
    )

    hashed = arr.hash().to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]

    different_inds = [0, 1, 3, 4, 6]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [1, 2, 42])
def test_hash_fixed_size_list_array_seeded(dtype, seed):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 4], [5, 5], [5, 5], [2, 1]]).cast(
        DataType.fixed_size_list(dtype, 2)
    )
    seeds = Series.from_pylist([seed] * len(arr)).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]

    different_inds = [0, 1, 3, 4, 6]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_fixed_size_list_array_no_seed_with_invalid(dtype):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 4], [5, 5], [5, 5], None, [2, 1], None]).cast(
        DataType.fixed_size_list(dtype, 2)
    )

    hashed = arr.hash().to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]
    assert hashed[6] is None
    assert hashed[8] is None

    different_inds = [0, 1, 3, 4, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [1, 2, 42])
def test_hash_fixed_size_list_array_seeded_with_invalid(dtype, seed):
    arr = Series.from_pylist([[1, 2], [1, 3], [1, 2], [1, 4], [5, 5], [5, 5], None, [2, 1], None]).cast(
        DataType.fixed_size_list(dtype, 2)
    )
    seeds = Series.from_pylist([seed] * 9).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[2]
    assert hashed[4] == hashed[5]
    assert hashed[6] is None
    assert hashed[8] is None

    different_inds = [0, 1, 3, 4, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_fixed_size_list_array_different_seeds(dtype):
    arr = Series.from_pylist([[1, 2], [1, 2], [1, 2], [1, 2]]).cast(DataType.fixed_size_list(dtype, 2))
    seeds = Series.from_pylist([1, 2, 3, 4]).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()

    different_inds = [0, 1, 2, 3]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_hash_fixed_size_list_array_consistency(dtype):
    data = [[1, 2], [1, 3], [1, 2], [1, 4], [5, 5], [5, 5], None, [2, 1], None]
    arr1 = Series.from_pylist(data).cast(DataType.fixed_size_list(dtype, 2))
    arr2 = Series.from_pylist(data).cast(DataType.fixed_size_list(dtype, 2))

    hashed1 = arr1.hash().to_pylist()
    hashed2 = arr2.hash().to_pylist()
    assert hashed1 == hashed2


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [None, 123])
def test_hash_struct(dtype, seed):
    data = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 1, "b": 4},
        {"a": 2, "b": 1},
        None,
        {"b": 2, "a": 1},
        {"b": 1, "a": 2},
        None,
    ]
    arr = Series.from_pylist(data).cast(DataType.struct({"a": dtype, "b": dtype}))

    seeds = None if seed is None else Series.from_pylist([seed] * len(data)).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[5]
    assert hashed[3] == hashed[6]
    assert hashed[4] is None and hashed[-1] is None

    different_inds = [0, 1, 2, 3]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [None, 123])
def test_hash_struct_nested(dtype, seed):
    data = [
        {"a": 1, "b": {"c": 1, "d": 2}},
        {"a": 3, "b": {"c": 3, "d": 4}},
        {"a": 1, "b": {"c": 3, "d": 4}},
        {"a": 2, "b": {"c": 1, "d": 2}},
        None,
        {"b": {"c": 1, "d": 2}, "a": 1},
        {"b": {"c": 1, "d": 2}, "a": 2},
        {"a": 1, "b": {"c": 2, "d": 1}},
        None,
    ]
    arr = Series.from_pylist(data).cast(DataType.struct({"a": dtype, "b": DataType.struct({"c": dtype, "d": dtype})}))

    seeds = None if seed is None else Series.from_pylist([seed] * len(data)).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[5]
    assert hashed[3] == hashed[6]
    assert hashed[4] is None and hashed[-1] is None

    different_inds = [0, 1, 2, 3, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [None, 123])
def test_hash_struct_sublist(dtype, seed):
    data = [
        {"a": 1, "b": [2, 3, 4]},
        {"a": 3, "b": [5, 6, 7]},
        {"a": 1, "b": [5, 6, 7]},
        {"a": 2, "b": [2, 3, 4]},
        None,
        {"b": [2, 3, 4], "a": 1},
        {"b": [2, 3, 4], "a": 2},
        {"a": 1, "b": [2, 4, 3]},
        None,
    ]
    arr = Series.from_pylist(data).cast(DataType.struct({"a": dtype, "b": DataType.list(dtype)}))

    seeds = None if seed is None else Series.from_pylist([seed] * len(data)).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[0] == hashed[5]
    assert hashed[3] == hashed[6]
    assert hashed[4] is None and hashed[-1] is None

    different_inds = [0, 1, 2, 3, 7]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


@pytest.mark.parametrize("dtype", daft_numeric_types)
@pytest.mark.parametrize("seed", [None, 123])
def test_hash_struct_with_nones(dtype, seed):
    data = [
        {"a": 1, "b": 2},
        {"a": None, "b": 2},
        {"a": 0, "b": 2},
        {"a": None, "b": None},
        {"a": 0, "b": 0},
        None,
        {"a": None, "b": 2},
        None,
    ]
    arr = Series.from_pylist(data).cast(DataType.struct({"a": dtype, "b": dtype}))

    seeds = None if seed is None else Series.from_pylist([seed] * len(data)).cast(DataType.uint64())

    hashed = arr.hash(seeds).to_pylist()
    assert hashed[1] == hashed[6]
    assert hashed[5] is None and hashed[-1] is None

    different_inds = [0, 1, 2, 3, 4]
    for i in range(len(different_inds)):
        for j in range(i):
            assert hashed[different_inds[i]] != hashed[different_inds[j]]


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


def test_murmur3_32_hash_bytes():
    arr = Series.from_pylist([b"\x00\x01\x02\x03", None])
    assert arr.datatype() == DataType.binary()
    hashes = arr.murmur3_32()
    java_answer = -188683207
    assert hashes.to_pylist() == [java_answer, None]


def test_murmur3_32_hash_fixed_sized_bytes():
    import pyarrow as pa

    arr = Series.from_arrow(pa.array([b"\x00\x01\x02\x03", None], type=pa.binary(4)))
    assert arr.datatype() == DataType.fixed_size_binary(4)
    hashes = arr.murmur3_32()
    java_answer = -188683207
    assert hashes.to_pylist() == [java_answer, None]


def test_murmur3_32_hash_date():
    arr = Series.from_pylist([date(2017, 11, 16), None])
    assert arr.datatype() == DataType.date()
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-653330422, None]


def test_murmur3_32_hash_time():
    arr = Series.from_pylist([time(22, 31, 8, 0), None])
    assert arr.datatype() == DataType.time("us")
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-662762989, None]


def test_murmur3_32_hash_time_nanoseconds():
    arr = Series.from_pylist([time(22, 31, 8, 0), None])
    arr = arr.cast(DataType.time("ns"))
    assert arr.datatype() == DataType.time("ns")
    hashes = arr.murmur3_32()
    assert hashes.to_pylist() == [-662762989, None]


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


# Tests for hash algorithm functionality
def test_hash_with_different_algorithms():
    """Test hash function with different algorithms."""
    arr = Series.from_pylist(["hello", "world", "test"])

    # Test default (xxhash)
    hashed_default = arr.hash()
    assert len(hashed_default) == 3
    assert all(h is not None for h in hashed_default)

    # Test explicit xxhash
    hashed_xxhash = arr.hash(hash_function="xxhash")
    assert len(hashed_xxhash) == 3
    assert all(h is not None for h in hashed_xxhash)

    # Test murmurhash3
    hashed_murmur = arr.hash(hash_function="murmurhash3")
    assert len(hashed_murmur) == 3
    assert all(h is not None for h in hashed_murmur)

    # Test sha1
    hashed_sha1 = arr.hash(hash_function="sha1")
    assert len(hashed_sha1) == 3
    assert all(h is not None for h in hashed_sha1)

    # Verify different algorithms produce different results
    assert hashed_default.to_pylist() == hashed_xxhash.to_pylist()  # Same algorithm
    assert hashed_default.to_pylist() != hashed_murmur.to_pylist()  # Different algorithms
    assert hashed_default.to_pylist() != hashed_sha1.to_pylist()  # Different algorithms
    assert hashed_murmur.to_pylist() != hashed_sha1.to_pylist()  # Different algorithms


def test_hash_with_seed_and_algorithms():
    """Test hash function with seed and different algorithms."""
    arr = Series.from_pylist(["hello", "world", "test"])
    seed = Series.from_pylist([42, 42, 42]).cast(DataType.uint64())

    # Test with seed and different algorithms
    hashed_xxhash_seeded = arr.hash(seed, hash_function="xxhash")
    hashed_murmur_seeded = arr.hash(seed, hash_function="murmurhash3")
    hashed_sha1_seeded = arr.hash(seed, hash_function="sha1")

    assert len(hashed_xxhash_seeded) == 3
    assert len(hashed_murmur_seeded) == 3
    assert len(hashed_sha1_seeded) == 3

    # Verify seeded hashes are different from unseeded
    hashed_xxhash_unseeded = arr.hash(hash_function="xxhash")
    assert hashed_xxhash_seeded.to_pylist() != hashed_xxhash_unseeded.to_pylist()


def test_hash_backward_compatibility():
    """Test that existing hash() calls work without specifying algorithm."""
    arr = Series.from_pylist(["hello", "world", "test"])

    # Test old-style call (no hash_function parameter)
    hashed_old = arr.hash()

    # Test new-style call with default algorithm
    hashed_new = arr.hash(hash_function="xxhash")

    # Both should produce the same result
    assert hashed_old.to_pylist() == hashed_new.to_pylist()


def test_hash_invalid_algorithm():
    """Test that invalid hash algorithms raise an error."""
    arr = Series.from_pylist(["hello", "world", "test"])

    with pytest.raises(ValueError, match="Invalid hash function"):
        arr.hash(hash_function="invalid_algorithm")


def test_hash_different_data_types_with_algorithms():
    """Test hash with different data types and algorithms."""
    # Test with integers
    int_arr = Series.from_pylist([1, 2, 3, 4])
    int_hashed_xxhash = int_arr.hash(hash_function="xxhash")
    int_hashed_murmur = int_arr.hash(hash_function="murmurhash3")
    int_hashed_sha1 = int_arr.hash(hash_function="sha1")

    assert len(int_hashed_xxhash) == 4
    assert len(int_hashed_murmur) == 4
    assert len(int_hashed_sha1) == 4

    # Test with floats
    float_arr = Series.from_pylist([1.1, 2.2, 3.3, 4.4])
    float_hashed_xxhash = float_arr.hash(hash_function="xxhash")
    float_hashed_murmur = float_arr.hash(hash_function="murmurhash3")
    float_hashed_sha1 = float_arr.hash(hash_function="sha1")

    assert len(float_hashed_xxhash) == 4
    assert len(float_hashed_murmur) == 4
    assert len(float_hashed_sha1) == 4

    # Test with booleans
    bool_arr = Series.from_pylist([True, False, True])
    bool_hashed_xxhash = bool_arr.hash(hash_function="xxhash")
    bool_hashed_murmur = bool_arr.hash(hash_function="murmurhash3")
    bool_hashed_sha1 = bool_arr.hash(hash_function="sha1")

    assert len(bool_hashed_xxhash) == 3
    assert len(bool_hashed_murmur) == 3
    assert len(bool_hashed_sha1) == 3


def test_hash_with_nulls_and_algorithms():
    """Test hash with null values and different algorithms."""
    arr = Series.from_pylist(["hello", None, "world", None, "test"])

    # Test with different algorithms
    hashed_xxhash = arr.hash(hash_function="xxhash")
    hashed_murmur = arr.hash(hash_function="murmurhash3")
    hashed_sha1 = arr.hash(hash_function="sha1")

    assert len(hashed_xxhash) == 5
    assert len(hashed_murmur) == 5
    assert len(hashed_sha1) == 5

    # Null values should produce hash values (not None) for all algorithms
    assert hashed_xxhash.to_pylist()[1] is not None  # null at index 1
    assert hashed_xxhash.to_pylist()[3] is not None  # null at index 3
    assert hashed_murmur.to_pylist()[1] is not None
    assert hashed_murmur.to_pylist()[3] is not None
    assert hashed_sha1.to_pylist()[1] is not None
    assert hashed_sha1.to_pylist()[3] is not None


def test_hash_algorithm_consistency():
    """Test that the same input with the same algorithm produces consistent results."""
    arr = Series.from_pylist(["hello", "world", "test"])

    # Test multiple calls with same algorithm
    hashed1 = arr.hash(hash_function="xxhash")
    hashed2 = arr.hash(hash_function="xxhash")
    hashed3 = arr.hash(hash_function="murmurhash3")
    hashed4 = arr.hash(hash_function="murmurhash3")

    # Same algorithm should produce same results
    assert hashed1.to_pylist() == hashed2.to_pylist()
    assert hashed3.to_pylist() == hashed4.to_pylist()

    # Different algorithms should produce different results
    assert hashed1.to_pylist() != hashed3.to_pylist()
