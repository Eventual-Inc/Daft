from __future__ import annotations

import random
import string

import numpy as np
import pyarrow as pa
import pytest
import xxhash

from daft.internal.kernels.hashing import hash_chunked_array

int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
float_types = [pa.float32(), pa.float64()]
date_types = [pa.date32(), pa.date64()]
number_types = int_types + float_types + date_types


@pytest.mark.parametrize("shift", range(1, 4))
@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", number_types, ids=[repr(it) for it in number_types])
def test_hash_chunked_number_array_shift(shift, num_chunks, dtype):

    arr = pa.chunked_array([[None, 1, 2, 3, 4, None] for _ in range(num_chunks)], type=dtype)
    hash_all = hash_chunked_array(arr)
    overall_shift = num_chunks * shift
    shifted = hash_chunked_array(arr[overall_shift:])
    assert hash_all[overall_shift:] == shifted
    hashed_shifted_again = hash_chunked_array(arr[overall_shift:], seed=hash_all[overall_shift:])
    assert hash_all[overall_shift:] != hashed_shifted_again


@pytest.mark.parametrize("shift", range(0, 4))
@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", int_types, ids=[repr(it) for it in int_types])
def test_hash_chunked_int_array_with_reference(shift, num_chunks, dtype):
    arr = pa.chunked_array([np.random.randint(0, 127, 100) for _ in range(num_chunks)], type=dtype)[
        shift * num_chunks :
    ]
    hash_all = hash_chunked_array(arr)
    nbytes = dtype.bit_width // 8
    for v, hv in zip(arr, hash_all):
        scalar = v.as_py()
        hash_scalar = hv.as_py()
        vbytes = scalar.to_bytes(nbytes, "little")
        ref_value = xxhash.xxh3_64_intdigest(vbytes)
        assert hash_scalar == ref_value


@pytest.mark.parametrize("shift", range(1, 4))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_hash_chunked_string_array_shift(
    shift,
    num_chunks,
):
    arr = pa.chunked_array(
        [[None, "", "a", "ab", "abc", "abcd", "", None] for _ in range(num_chunks)], type=pa.string()
    )
    hash_all = hash_chunked_array(arr)
    overall_shift = num_chunks * shift
    assert hash_all[overall_shift:] == hash_chunked_array(arr[overall_shift:])
    hashed_shifted_again = hash_chunked_array(arr[overall_shift:], seed=hash_all[overall_shift:])
    assert hash_all[overall_shift:] != hashed_shifted_again


@pytest.mark.parametrize("shift", range(0, 4))
@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_hash_chunked_string_array_with_reference(shift, str_len, num_chunks):
    def gen_random_str(k: int):
        return "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=k))

    arr = pa.chunked_array(
        [
            ([None for _ in range(2)] + [gen_random_str(str_len) for _ in range(10)] + [None for _ in range(4)])
            for _ in range(num_chunks)
        ]
    )[shift * num_chunks :]
    hash_all = hash_chunked_array(arr)
    assert len(hash_all) == len(arr)
    for i, (v, hv) in enumerate(zip(arr, hash_all)):
        scalar = v.as_py()
        hash_scalar = hv.as_py()
        assert hash_scalar is not None
        if scalar is None:
            assert hash_scalar == xxhash.xxh3_64_intdigest(b"")
        else:
            ref_value = xxhash.xxh3_64_intdigest(scalar.encode())
            assert ref_value == hash_scalar


@pytest.mark.parametrize("shift", range(0, 4))
@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_hash_chunked_large_string_array_with_reference(shift, str_len, num_chunks):
    def gen_random_str(k: int):
        return "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=k))

    arr = pa.chunked_array(
        [
            ([None for _ in range(2)] + [gen_random_str(str_len) for _ in range(10)] + [None for _ in range(4)])
            for _ in range(num_chunks)
        ],
        type=pa.large_string(),
    )[shift * num_chunks :]
    hash_all = hash_chunked_array(arr)
    assert len(hash_all) == len(arr)
    for i, (v, hv) in enumerate(zip(arr, hash_all)):
        scalar = v.as_py()
        hash_scalar = hv.as_py()
        assert hash_scalar is not None
        if scalar is None:
            assert hash_scalar == xxhash.xxh3_64_intdigest(b"")
        else:
            ref_value = xxhash.xxh3_64_intdigest(scalar.encode())
            assert ref_value == hash_scalar
