import pyarrow as pa
import pytest

from daft.internal.hashing import hash_chunked_array

int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]


@pytest.mark.parametrize("dtype", int_types, ids=[repr(it) for it in int_types])
def test_hash_chunked_int_array(dtype):
    arr = pa.chunked_array([[None, 1, 2, 3, 4, None]], type=dtype)
    hash_all = hash_chunked_array(arr)
    assert hash_all[0] == pa.scalar(None, type=pa.uint64())
    assert hash_all[-1] == pa.scalar(None, type=pa.uint64())

    shifted = hash_chunked_array(arr[1:])

    assert hash_all[1:] == shifted
    assert shifted[0] != pa.scalar(None, type=pa.uint64())
    assert shifted[-1] == pa.scalar(None, type=pa.uint64())


def test_hash_chunked_string_array():
    arr = pa.chunked_array([[None, "", "a", "ab", "abc", "abcd", "", None]], type=pa.string())
    hash_all = hash_chunked_array(arr)
    assert hash_all[1:] == hash_chunked_array(arr[1:])
