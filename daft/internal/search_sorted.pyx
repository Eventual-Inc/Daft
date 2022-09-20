# distutils: language=c++

from libc cimport math, stdint
from libc.string cimport memset
from pyarrow.lib cimport (
    AllocateBuffer,
    CArray,
    CArrayData,
    CBuffer,
    CChunkedArray,
    CDataType,
    CResult,
    CScalarFunction,
    GetPrimitiveType,
    GetResultValue,
    MakeArray,
    Type,
    make_shared,
    pyarrow_unwrap_array,
    pyarrow_unwrap_chunked_array,
    pyarrow_wrap_chunked_array,
    shared_ptr,
    to_shared,
    unique_ptr,
    vector,
)

import cython


cdef extern from "arrow_search_sorted.h" nogil:
    cdef void primative_binsearch_nonnull[T](const stdint.uint8_t *arr, const stdint.uint8_t *key, stdint.uint8_t *ret, const size_t arr_len,
            size_t key_len, const size_t arr_str, const size_t key_str, const size_t ret_str);


cdef shared_ptr[CBuffer] _shift_valid_bits(
    const shared_ptr[CBuffer] valid_bits,
    const stdint.int64_t num_values,
    const stdint.int64_t offset):
    if valid_bits.get() == NULL:
        return valid_bits
    cdef const stdint.uint8_t* valid_bits_data_ptr = valid_bits.get().data()
    cdef stdint.int64_t result_valid_bits_buffer_size = <stdint.uint64_t> math.ceil(num_values / 8.)
    cdef shared_ptr[CBuffer] result_valid_bits_buffer = to_shared(GetResultValue(AllocateBuffer(result_valid_bits_buffer_size, NULL)))
    cdef stdint.uint8_t* result_valid_bits_data_ptr = <stdint.uint8_t*> result_valid_bits_buffer.get().mutable_data()
    memset(result_valid_bits_data_ptr, 0, result_valid_bits_buffer_size )
    cdef stdint.int64_t byte_offset = offset // 8
    cdef stdint.uint8_t bit_offset = offset % 8
    cdef stdint.int64_t i, idx
    cdef stdint.uint8_t mask
    for i in range(num_values):
        idx = i + offset
        is_valid = (valid_bits_data_ptr[idx // 8] & (1 << (idx & 0b111))) != 0
        result_valid_bits_data_ptr[i // 8] |= (is_valid << (i & 0b111))

    return result_valid_bits_buffer



cdef shared_ptr[CArray] _search_sorted_primative_array(shared_ptr[CArray] arr, shared_ptr[CArray] keys):
    type_id = arr.get().type_id()

    if not ((Type._Type_UINT8 <= type_id) and (type_id <= Type._Type_INT64)):
        raise TypeError('expected integer array')

    cdef shared_ptr[CArrayData] array_data = arr.get().data()
    cdef stdint.int64_t length = array_data.get().length
    cdef stdint.int64_t offset = array_data.get().offset
    cdef stdint.int64_t null_count = array_data.get().null_count
    assert null_count == 0
    cdef vector[shared_ptr[CBuffer]] buffers = array_data.get().buffers

    cdef shared_ptr[CBuffer] valid_bits = buffers.at(0)
    cdef const stdint.uint8_t* data_ptr = buffers.at(1).get().data()


    cdef shared_ptr[CArrayData] key_data = keys.get().data()


    cdef stdint.int64_t key_length = key_data.get().length
    cdef stdint.int64_t key_offset = key_data.get().offset
    cdef stdint.int64_t key_null_count = key_data.get().null_count
    assert key_null_count == 0
    cdef vector[shared_ptr[CBuffer]] key_buffers = key_data.get().buffers

    cdef shared_ptr[CBuffer] key_valid_bits = key_buffers.at(0)

    cdef const stdint.uint8_t* key_data_ptr = key_buffers.at(1).get().data()


    cdef stdint.uint64_t result_buffer_size = key_length * cython.sizeof(stdint.uint64_t)
    cdef shared_ptr[CBuffer] result_buffer = to_shared(GetResultValue(AllocateBuffer(result_buffer_size, NULL)))
    cdef stdint.uint8_t* buffer_data_ptr = <stdint.uint8_t*> result_buffer.get().mutable_data()

    cdef stdint.int64_t byte_width =  (buffers.at(1).get().size() // (length + offset)) if length > 0 else 0

    primative_binsearch_nonnull[stdint.int64_t](data_ptr + byte_width*offset, key_data_ptr + byte_width*key_offset, buffer_data_ptr, length, key_length, byte_width, byte_width, cython.sizeof(stdint.uint64_t))

    cdef vector[shared_ptr[CBuffer]] result_buffer_vector
    result_buffer_vector.push_back(_shift_valid_bits(key_valid_bits, key_length, key_offset))
    result_buffer_vector.push_back(result_buffer)

    cdef shared_ptr[CArrayData] result_array_data = CArrayData.Make(
        GetPrimitiveType(Type._Type_UINT64),
        key_length,
        result_buffer_vector,
        key_null_count,
        0,
    )
    return MakeArray(result_array_data)

def search_sorted_chunked_array(data_arr,keys):
    cdef shared_ptr[CChunkedArray] carr = pyarrow_unwrap_chunked_array(data_arr)
    if carr.get() == NULL:
        raise TypeError("not a chunked array")
    num_chunks: cython.int = carr.get().num_chunks()
    assert num_chunks == 1
    cdef shared_ptr[CDataType] ctype = carr.get().type()
    cdef Type type_id = ctype.get().id()
    is_integer: cython.bool = False
    is_string: cython.bool = False

    if (Type._Type_UINT8 <= type_id) and (type_id <= Type._Type_INT64):
        is_integer = True
        # elif type_id == Type._Type_STRING:
        #     is_string = True
    else:
        raise TypeError('excepted integer or string got neither')

    cdef shared_ptr[CChunkedArray] key_arr = pyarrow_unwrap_chunked_array(keys)
    if key_arr.get() == NULL:
        raise TypeError("not a chunked array")
    assert key_arr.get().num_chunks() == 1
    cdef shared_ptr[CDataType] key_ctype = key_arr.get().type()
    cdef Type key_type_id = key_ctype.get().id()
    assert key_type_id == type_id

    cdef shared_ptr[CArray] arr

    cdef shared_ptr[CArray] key_single_arr

    cdef vector[shared_ptr[CArray]] search_results
    for i in range(num_chunks):
        arr = carr.get().chunk(i)
        key_single_arr = key_arr.get().chunk(i)

        if is_integer:
            search_results.push_back(_search_sorted_primative_array(arr, key_single_arr))
            # elif is_string:
            #     hash_results.push_back(_hash_string_array(arr))


    cdef shared_ptr[CChunkedArray] result = make_shared[CChunkedArray](search_results, GetPrimitiveType(Type._Type_UINT64))
    return pyarrow_wrap_chunked_array(result)
