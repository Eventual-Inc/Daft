from __future__ import annotations

import daft
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import hamming_distance
from daft.series import Series


def test_bitwise_hamming_identical_uint64():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([42, 100, 0]).cast(DataType.uint64()),
            "b": Series.from_pylist([42, 100, 0]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result == [0, 0, 0]


def test_bitwise_hamming_known_values_uint64():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([0b1010, 0b1111, 1]).cast(DataType.uint64()),
            "b": Series.from_pylist([0b0101, 0b0000, 0]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result == [4, 4, 1]


def test_bitwise_hamming_max_distance_uint64():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([0]).cast(DataType.uint64()),
            "b": Series.from_pylist([0xFFFFFFFFFFFFFFFF]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result == [64]


def test_bitwise_hamming_returns_uint32():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([1]).cast(DataType.uint64()),
            "b": Series.from_pylist([2]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).collect()
    assert result.schema()["a"].dtype == DataType.uint32()


def test_bitwise_hamming_null_propagation():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([1, None, 3]).cast(DataType.uint64()),
            "b": Series.from_pylist([1, 2, None]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result[0] == 0
    assert result[1] is None
    assert result[2] is None


def test_bitwise_hamming_integer_types():
    df = daft.from_pydict(
        {
            "a": [10, 20, 30],
            "b": [10, 21, 31],
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result[0] == 0
    assert result[1] == bin(20 ^ 21).count("1")
    assert result[2] == bin(30 ^ 31).count("1")


def test_bitwise_hamming_fixed_size_binary_identical():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([b"\x00\x00\x00\x00"]).cast(DataType.fixed_size_binary(4)),
            "b": Series.from_pylist([b"\x00\x00\x00\x00"]).cast(DataType.fixed_size_binary(4)),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result == [0]


def test_bitwise_hamming_fixed_size_binary_known():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([b"\xff\x00"]).cast(DataType.fixed_size_binary(2)),
            "b": Series.from_pylist([b"\x00\xff"]).cast(DataType.fixed_size_binary(2)),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result == [16]


def test_bitwise_hamming_fixed_size_binary_null():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([b"\xff", None]).cast(DataType.fixed_size_binary(1)),
            "b": Series.from_pylist([b"\x00", b"\x00"]).cast(DataType.fixed_size_binary(1)),
        }
    )
    result = df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]
    assert result[0] == 8
    assert result[1] is None


def test_bitwise_hamming_end_to_end_with_simhash():
    df = daft.from_pydict(
        {
            "text": [
                "the quick brown fox jumps over the lazy dog",
                "the quick brown fox jumps over the lazy cat",
                "1234567890 abcdefghijklmnopqrstuvwxyz",
            ]
        }
    )
    hashed = df.select(col("text").simhash().alias("hash")).collect()
    hashes = hashed.to_pydict()["hash"]

    pair_df = daft.from_pydict(
        {
            "a": Series.from_pylist([hashes[0], hashes[0]]).cast(DataType.uint64()),
            "b": Series.from_pylist([hashes[1], hashes[2]]).cast(DataType.uint64()),
        }
    )
    distances = pair_df.select(hamming_distance(col("a"), col("b"))).to_pydict()["a"]

    assert distances[0] < distances[1]


def test_bitwise_hamming_broadcast_scalar():
    df = daft.from_pydict(
        {
            "a": Series.from_pylist([0b1010, 0b1100, 0b1111]).cast(DataType.uint64()),
        }
    )
    result = df.select(hamming_distance(col("a"), lit(0).cast(DataType.uint64()))).to_pydict()["a"]
    assert result == [2, 2, 4]
