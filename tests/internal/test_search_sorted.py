from __future__ import annotations

import random
import string

import numpy as np
import pyarrow as pa
import pytest

from daft.internal.kernels.search_sorted import search_sorted

int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
float_types = [pa.float32(), pa.float64()]
number_types = int_types + float_types


@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", number_types, ids=[repr(it) for it in number_types])
def test_number_array(num_chunks, dtype) -> None:
    pa_keys = pa.chunked_array([np.array([4, 2, 1, 3], dtype=dtype.to_pandas_dtype()) for _ in range(num_chunks)])
    np_keys = pa_keys.to_numpy()
    np_sorted_arr = np.arange(100, dtype=dtype.to_pandas_dtype())
    result = np.searchsorted(np_sorted_arr, np_keys)
    pa_sorted_arr = pa.chunked_array([np_sorted_arr], type=dtype)
    pa_result = search_sorted(pa_sorted_arr, pa_keys)

    assert np.all(result == pa_result.to_numpy())

    # Key Slice
    pa_result_slice = search_sorted(pa_sorted_arr, pa_keys[1:])
    assert np.all(result[1:] == pa_result_slice.to_numpy())


def test_number_array_with_nulls() -> None:
    keys = np.random.randint(0, 100, 1000)
    data = np.arange(100)
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([pa.chunked_array([keys] + [[None] * 10] + [keys]).combine_chunks()])
    pa_result = search_sorted(pa_data, pa_keys)
    np_result = pa_result.to_numpy()

    assert np.all(result == np_result[:1000])
    assert np.all(result == np_result[1000 + 10 :])
    assert np.all(np_result[1000 : 1000 + 10] == np_result[1000])

    # Key Slice
    pa_result_slice = search_sorted(pa_data, pa_keys[1:])
    np_result_slice = pa_result_slice.to_numpy()

    assert np.all(result[1:] == np_result_slice[: 1000 - 1])
    assert np.all(result == np_result_slice[1000 + 10 - 1 :])
    assert np.all(np_result_slice[1000 - 1 : 1000 + 10 - 1] == np_result_slice[1000 - 1])

    # Data Slice
    pa_result_slice = search_sorted(pa_data[1:], pa_keys)
    np_result_slice = pa_result_slice.to_numpy()
    new_result = np.maximum((result - 1), 0)
    assert np.all(new_result == np_result_slice[:1000])
    assert np.all(new_result == np_result_slice[1000 + 10 :])
    assert np.all(np_result_slice[1000 : 1000 + 10] == np_result_slice[1000])


def gen_random_str(k: int):
    return "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=k))


@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_string_array(str_len, num_chunks) -> None:

    pa_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)],
    )
    keys = pa_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_result = search_sorted(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())

    # Key Slice
    pa_result_slice = search_sorted(pa_data, pa_keys[1:])
    assert np.all(result[1:] == pa_result_slice.to_numpy())

    # Data Slice
    pa_result_slice = search_sorted(pa_data[1:], pa_keys)
    new_result = np.maximum((result - 1), 0)
    assert np.all(new_result == pa_result_slice.to_numpy())


def test_string_array_with_nulls() -> None:
    py_keys = [str(i) for i in range(100)]
    random.shuffle(py_keys)
    keys = np.array(py_keys)
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([py_keys + [None] * 10 + py_keys])
    pa_result = search_sorted(pa_data, pa_keys)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result[:100])
    assert np.all(result == np_result[100 + 10 :])
    assert np.all(np_result[100 : 100 + 10] == np_result[100])

    # Key Slice
    pa_result = search_sorted(pa_data, pa_keys[1:])
    np_result = pa_result.to_numpy()
    assert np.all(result[1:] == np_result[: 100 - 1])
    assert np.all(result == np_result[100 + 10 - 1 :])
    assert np.all(np_result[100 - 1 : 100 + 10 - 1] == np_result[100 - 1])

    # Data Slice
    pa_result_slice = search_sorted(pa_data[1:], pa_keys)
    new_result = np.maximum((result - 1), 0)
    np_result_slice = pa_result_slice.to_numpy()
    assert np.all(new_result == np_result_slice[:100])
    assert np.all(new_result == np_result_slice[100 + 10 :])
    assert np.all(np_result_slice[100 : 100 + 10] == np_result_slice[100])


@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_large_string_array(str_len, num_chunks) -> None:
    pa_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)], type=pa.large_string()
    )

    keys = pa_keys.cast(pa.string()).to_numpy()
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data]).cast(pa.large_string())
    pa_result = search_sorted(pa_data, pa_keys)
    pa_result = search_sorted(pa_data, pa_keys)
    np_result = pa_result.to_numpy()

    assert np.all(result == np_result)


def test_large_string_array_with_nulls() -> None:
    py_keys = [str(i) for i in range(100)]
    random.shuffle(py_keys)
    keys = np.array(py_keys)
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data]).cast(pa.large_string())
    pa_keys = pa.chunked_array([py_keys + [None] * 10 + py_keys], type=pa.large_string())
    pa_result = search_sorted(pa_data, pa_keys)
    np_result = pa_result.to_numpy()

    assert np.all(result == np_result[:100])
    assert np.all(result == np_result[100 + 10 :])
    assert np.all(np_result[100 : 100 + 10] == np_result[100])


@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", number_types, ids=[repr(it) for it in number_types])
def test_single_column_number_table(num_chunks, dtype) -> None:
    pa_keys = pa.chunked_array([np.array([4, 2, 1, 3], dtype=dtype.to_pandas_dtype()) for _ in range(num_chunks)])
    np_keys = pa_keys.to_numpy()
    np_sorted_arr = np.arange(100, dtype=dtype.to_pandas_dtype())
    result = np.searchsorted(np_sorted_arr, np_keys)
    pa_sorted_arr = pa.chunked_array([np_sorted_arr], type=dtype)
    pa_sorted_table = pa.table([pa_sorted_arr], ["a"])
    pa_table_keys = pa.table([pa_keys], ["a"])
    pa_result = search_sorted(pa_sorted_table, pa_table_keys)
    assert np.all(result == pa_result.to_numpy())

    # Key Slice
    pa_result_slice = search_sorted(pa_sorted_table, pa_table_keys[1:])
    assert np.all(result[1:] == pa_result_slice.to_numpy())

    # Data Slice
    pa_result_slice = search_sorted(pa_sorted_table[1:], pa_table_keys)
    new_result = np.maximum((result - 1), 0)

    assert np.all(new_result == pa_result_slice.to_numpy())


@pytest.mark.parametrize("num_chunks", range(1, 4))
@pytest.mark.parametrize("dtype", number_types, ids=[repr(it) for it in number_types])
def test_multi_column_number_table(num_chunks, dtype) -> None:
    pa_keys = pa.chunked_array([np.array([4, 2, 1, 3], dtype=dtype.to_pandas_dtype()) for _ in range(num_chunks)])
    np_keys = pa_keys.to_numpy()
    np_sorted_arr = np.arange(100, dtype=dtype.to_pandas_dtype())

    result = np.searchsorted(np_sorted_arr, np_keys)

    pa_sorted_arr = pa.chunked_array([np_sorted_arr], type=dtype)

    sorted_table = pa.table([pa_sorted_arr, pa_sorted_arr, pa_sorted_arr], names=["a", "b", "c"])
    key_table = pa.table([pa_keys, pa_keys, pa_keys], names=["a", "b", "c"])
    pa_result = search_sorted(sorted_table, key_table)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)

    # Key Slice
    pa_result_slice = search_sorted(sorted_table, key_table[1:])
    np_result_slice = pa_result_slice.to_numpy()

    assert np.all(result[1:] == np_result_slice)

    # Data Slice
    pa_result_slice = search_sorted(sorted_table[1:], key_table)
    np_result_slice = pa_result_slice.to_numpy()

    new_result = np.maximum((result - 1), 0)

    assert np.all(new_result == np_result_slice)


@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_multi_column_mixed_number_table(num_chunks) -> None:
    pa_keys = pa.chunked_array([np.array([4, 2, 1, 3], dtype=np.uint32()) for _ in range(num_chunks)])
    np_keys = pa_keys.to_numpy()
    np_sorted_arr = np.arange(100, dtype=np.uint32())

    result = np.searchsorted(np_sorted_arr, np_keys)

    pa_sorted_arr = pa.chunked_array([np_sorted_arr], type=pa.uint32())

    sorted_table = pa.table(
        [pa_sorted_arr, pa_sorted_arr.cast(pa.float32()), pa_sorted_arr.cast(pa.uint64())], names=["a", "b", "c"]
    )
    key_table = pa.table([pa_keys, pa_keys.cast(pa.float32()), pa_keys.cast(pa.uint64())], names=["a", "b", "c"])
    pa_result = search_sorted(sorted_table, key_table)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)

    # Key Slice
    pa_result_slice = search_sorted(sorted_table, key_table[1:])
    np_result_slice = pa_result_slice.to_numpy()

    assert np.all(result[1:] == np_result_slice)

    # Data Slice
    pa_result_slice = search_sorted(sorted_table[1:], key_table)
    np_result_slice = pa_result_slice.to_numpy()

    new_result = np.maximum((result - 1), 0)

    assert np.all(new_result == np_result_slice)


@pytest.mark.parametrize("str_len", [0, 1, 5])
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_number_string_table(str_len, num_chunks) -> None:
    pa_int_keys = pa.chunked_array([np.zeros(10) for _ in range(num_chunks)])
    pa_str_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)],
    )
    keys = pa_str_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    sorted_table = pa.table([np.zeros(10), pa_data], names=["a", "b"])
    key_table = pa.table([pa_int_keys, pa_str_keys], names=["a", "b"])

    pa_result = search_sorted(sorted_table, key_table)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)

    # Key Slice
    pa_result_slice = search_sorted(sorted_table, key_table[1:])
    np_result_slice = pa_result_slice.to_numpy()

    assert np.all(result[1:] == np_result_slice)

    # Data Slice
    pa_result_slice = search_sorted(sorted_table[1:], key_table)
    np_result_slice = pa_result_slice.to_numpy()

    new_result = np.maximum((result - 1), 0)

    assert np.all(new_result == np_result_slice)


@pytest.mark.parametrize("str_len", [1, 5])
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_number_string_table_desc(str_len, num_chunks) -> None:
    pa_int_keys = pa.chunked_array([np.zeros(100) for _ in range(num_chunks)])
    pa_str_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(100)] for _ in range(num_chunks)],
    )
    keys = pa_str_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(100)])
    data.sort()
    result = len(data) - np.searchsorted(data, keys)

    data = data[::-1]

    pa_data = pa.chunked_array([data])
    sorted_table = pa.table([np.zeros(100), pa_data], names=["a", "b"])
    key_table = pa.table([pa_int_keys, pa_str_keys], names=["a", "b"])

    pa_result = search_sorted(sorted_table, key_table, input_reversed=[False, True])
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)


@pytest.mark.parametrize("str_len", [0, 1, 5])
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_string_number_table(str_len, num_chunks) -> None:
    pa_int_keys = pa.chunked_array([np.zeros(10) for _ in range(num_chunks)])
    pa_str_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)],
    )
    keys = pa_str_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    sorted_table = pa.table([pa_data, np.zeros(10)], names=["a", "b"])
    key_table = pa.table([pa_str_keys, pa_int_keys], names=["a", "b"])

    pa_result = search_sorted(sorted_table, key_table)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)


@pytest.mark.parametrize("str_len", range(0, 10))
@pytest.mark.parametrize("num_chunks", range(1, 4))
def test_string_table(str_len, num_chunks) -> None:

    pa_keys = pa.chunked_array(
        [[gen_random_str(str_len) for _ in range(10)] for _ in range(num_chunks)],
    )
    keys = pa_keys.to_numpy()

    data = np.array([gen_random_str(str_len + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])

    sorted_table = pa.table([pa_data, pa_data, pa_data], names=["a", "b", "c"])
    key_table = pa.table([pa_keys, pa_keys, pa_keys], names=["a", "b", "c"])

    pa_result = search_sorted(sorted_table, key_table)
    np_result = pa_result.to_numpy()
    assert np.all(result == np_result)


def test_string_table_with_nulls() -> None:
    py_keys = [str(i) for i in range(100)]
    random.shuffle(py_keys)
    keys = np.array(py_keys)
    data = np.array([gen_random_str(10 + 1) for i in range(10)])
    data.sort()
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([py_keys + [None] * 10 + py_keys])
    pa_data_table = pa.table([pa_data, pa_data], names=["a", "b"])
    pa_key_table = pa.table([pa_keys, pa_keys], names=["a", "b"])

    pa_result = search_sorted(pa_data_table, pa_key_table)
    np_result = pa_result.to_numpy()

    assert np.all(result == np_result[:100])
    assert np.all(result == np_result[100 + 10 :])
    assert np.all(np_result[100 : 100 + 10] == np_result[100])

    # Key Slice
    pa_result_slice = search_sorted(pa_data_table, pa_key_table[1:])
    np_result_slice = pa_result_slice.to_numpy()
    assert np.all(result[1:] == np_result_slice[: 100 - 1])
    assert np.all(result == np_result_slice[100 + 10 - 1 :])
    assert np.all(np_result_slice[100 - 1 : 100 + 10 - 1] == np_result_slice[100 - 1])

    # Data Slice
    pa_result_slice = search_sorted(pa_data_table[1:], pa_key_table)
    np_result_slice = pa_result_slice.to_numpy()

    new_result = np.maximum((result - 1), 0)

    assert np.all(new_result == np_result_slice[:100])
    assert np.all(new_result == np_result_slice[100 + 10 :])
    assert np.all(np_result_slice[100 : 100 + 10] == np_result_slice[100])
