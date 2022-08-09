import pyarrow as pa

from daft.internal.hashing import hash_chunked_array


def test_hash_chunked_int_array():
    arr = pa.chunked_array([[None, 1, 2, 3, 4] + [None] * 4, [5, 6, 7, 8]], type=pa.uint8())
    arr = pa.chunked_array([arr.chunk(0)[1:]])
    result = hash_chunked_array(arr)
    print(result)


def test_hash_chunked_string_array():
    arr = pa.chunked_array([[None, "", "a", "ab", "abc", "abcd", "", None]], type=pa.string())
    print(hash_chunked_array(arr))
    arr = pa.chunked_array([arr.chunk(0)[1:]])
    print(hash_chunked_array(arr))
