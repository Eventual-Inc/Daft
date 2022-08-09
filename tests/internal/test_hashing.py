import pyarrow as pa
import pytest

from daft.internal.hashing import hash_chunked_array

int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]


@pytest.mark.parametrize("shift", range(1, 4))
@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", int_types, ids=[repr(it) for it in int_types])
def test_hash_chunked_int_array_shift(shift, num_chunks, dtype):
    arr = pa.chunked_array([[None, 1, 2, 3, 4, None] for _ in range(num_chunks)], type=dtype)
    hash_all = hash_chunked_array(arr)
    overall_shift = num_chunks * shift
    shifted = hash_chunked_array(arr[overall_shift:])
    assert hash_all[overall_shift:] == shifted


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
