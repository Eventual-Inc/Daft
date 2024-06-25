import itertools
from operator import and_, or_, xor

import pytest

from daft.datatype import DataType
from daft.series import Series

BITWISE_OPERATORS = [and_, or_, xor]
INT_TYPES = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
]
UINT_TYPES = [
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
]


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
@pytest.mark.parametrize(
    # Test all combinations of signed and unsigned integers EXCEPT for UINT64 because UINT64 and INT types are not compatible with our supertype rules
    "dtype",
    itertools.product(INT_TYPES + UINT_TYPES[:-1], repeat=2),
)
def test_bitwise_with_int_and_unsigned_int_types(op, dtype):
    left_dtype, right_dtype = dtype
    s1 = Series.from_pylist([0b1100, 0b1010, 0b1001]).cast(left_dtype)
    s2 = Series.from_pylist([0b1010, 0b1100, 0b1001]).cast(right_dtype)

    result = op(s1, s2)
    expected = [op(i, j) for i, j in zip(s1.to_pylist(), s2.to_pylist())]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
@pytest.mark.parametrize("dtype", itertools.product(UINT_TYPES, repeat=2))
def test_bitwise_with_different_unsigned_int_types(op, dtype):
    left_dtype, right_dtype = dtype
    s1 = Series.from_pylist([0b1100, 0b1010, 0b1001]).cast(left_dtype)
    s2 = Series.from_pylist([0b1010, 0b1100, 0b1001]).cast(right_dtype)

    result = op(s1, s2)
    expected = [op(i, j) for i, j in zip(s1.to_pylist(), s2.to_pylist())]
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([0b1100, 0b1010, 0b1001], [0b1010, 0b1100, 0b1001], id="same_length"),
        pytest.param([0b1100, 0b1010, 0b1001], [0b1010], id="broadcast_right"),
        pytest.param([0b1100], [0b1010, 0b1100, 0b1001], id="broadcast_left"),
    ],
)
@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_broadcasting(left, right, op):
    s1 = Series.from_pylist(left)
    s2 = Series.from_pylist(right)

    if len(left) == 1:
        left = left * len(right)
    if len(right) == 1:
        right = right * len(left)

    result = op(s1, s2)
    expected = [op(i, j) for i, j in zip(left, right)]
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([0b1100, None, 0b1001], [0b1010, 0b1100, None], id="same_length"),
        pytest.param([0b1100, 0b1010, 0b1001], [None], id="broadcast_right"),
        pytest.param([None], [0b1010, 0b1100, 0b1001], id="broadcast_left"),
    ],
)
@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_broadcasting_with_nulls(left, right, op):
    s1 = Series.from_pylist(left).cast(DataType.int8())
    s2 = Series.from_pylist(right).cast(DataType.int8())

    if len(left) == 1:
        left = left * len(right)
    if len(right) == 1:
        right = right * len(left)

    result = op(s1, s2)
    expected = [op(i, j) if i is not None and j is not None else None for i, j in zip(left, right)]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_errors_with_non_integer_inputs(op):
    s1 = Series.from_pylist([0b1100, 0b1010, 0b1001])
    s2 = Series.from_pylist(["a", "b", "c"])

    with pytest.raises(ValueError, match="Cannot perform logic on types:"):
        op(s1, s2)


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_errors_with_incompatible_lengths(op):
    s1 = Series.from_pylist([0b1100, 0b1010, 0b1001])
    s2 = Series.from_pylist([0b1010, 0b1100])

    with pytest.raises(ValueError, match="trying to operate on different length arrays:"):
        op(s1, s2)
