# distutils: language=c++
# distutils: sources = daft/internal/xxhash.cc

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


cdef extern from "xxhash.h":
    stdint.uint64_t XXH3_64bits(const void* input, size_t length);



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


cdef shared_ptr[CArray] _hash_integer_array(shared_ptr[CArray] arr):
    type_id = arr.get().type_id()

    if not ((Type._Type_UINT8 <= type_id) and (type_id <= Type._Type_INT64)):
        raise TypeError('expected integer array')

    cdef shared_ptr[CArrayData] array_data = arr.get().data()
    cdef stdint.int64_t length = array_data.get().length
    cdef stdint.int64_t offset = array_data.get().offset
    cdef stdint.int64_t null_count = array_data.get().null_count
    cdef vector[shared_ptr[CBuffer]] buffers = array_data.get().buffers

    cdef shared_ptr[CBuffer] valid_bits = buffers.at(0)
    cdef shared_ptr[CBuffer] data = buffers.at(1)
    cdef const stdint.uint8_t* data_ptr = data.get().data()



    cdef stdint.uint64_t result_buffer_size = length * cython.sizeof(stdint.uint64_t)
    cdef shared_ptr[CBuffer] result_buffer = to_shared(GetResultValue(AllocateBuffer(result_buffer_size, NULL)))
    cdef stdint.uint64_t* buffer_data_ptr = <stdint.uint64_t*> result_buffer.get().mutable_data()


    cdef stdint.int64_t byte_width =  (data.get().size() // (length + offset)) if length > 0 else 0

    cdef stdint.int64_t i, idx
    cdef stdint.uint64_t h_value = 0
    for i in range(length):
        # TODO Skip nulls
        idx = (i + offset)
        h_value = XXH3_64bits(data_ptr + idx*byte_width, byte_width)
        buffer_data_ptr[i] = h_value

    cdef vector[shared_ptr[CBuffer]] result_buffer_vector
    result_buffer_vector.push_back(_shift_valid_bits(valid_bits, length, offset))
    result_buffer_vector.push_back(result_buffer)

    cdef shared_ptr[CArrayData] result_array_data = CArrayData.Make(
        GetPrimitiveType(Type._Type_UINT64),
        length,
        result_buffer_vector,
        null_count,
        0,
    )
    return MakeArray(result_array_data)

cdef shared_ptr[CArray] _hash_string_array(shared_ptr[CArray] arr):
    "https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout"
    type_id = arr.get().type_id()

    if not (type_id == Type._Type_STRING):
        raise TypeError('expected string array')

    cdef shared_ptr[CArrayData] array_data = arr.get().data()
    cdef stdint.int64_t length = array_data.get().length
    cdef stdint.int64_t offset = array_data.get().offset
    cdef stdint.int64_t null_count = array_data.get().null_count

    cdef vector[shared_ptr[CBuffer]] buffers = array_data.get().buffers

    cdef shared_ptr[CBuffer] valid_bits = buffers.at(0)
    cdef shared_ptr[CBuffer] offsets = buffers.at(1)
    cdef shared_ptr[CBuffer] data = buffers.at(2)

    cdef const stdint.uint8_t* data_ptr = data.get().data()
    cdef const stdint.uint32_t* offsets_ptr = <const stdint.uint32_t*> offsets.get().data()


    cdef stdint.uint64_t result_buffer_size = length * cython.sizeof(stdint.uint64_t)
    cdef shared_ptr[CBuffer] result_buffer = to_shared(GetResultValue(AllocateBuffer(result_buffer_size, NULL)))
    cdef stdint.uint64_t* buffer_data_ptr = <stdint.uint64_t*> result_buffer.get().mutable_data()

    cdef stdint.int64_t i, idx, word_size
    cdef stdint.uint64_t h_value

    for i in range(length):
        idx = i + offset
        # TODO Skip nulls
        word_size = offsets_ptr[idx+1] - offsets_ptr[idx]
        h_value = XXH3_64bits(data_ptr + offsets_ptr[idx], word_size)
        buffer_data_ptr[i] = h_value

    cdef vector[shared_ptr[CBuffer]] result_buffer_vector
    result_buffer_vector.push_back(_shift_valid_bits(valid_bits, length, offset))
    result_buffer_vector.push_back(result_buffer)

    cdef shared_ptr[CArrayData] result_array_data = CArrayData.Make(
        GetPrimitiveType(Type._Type_UINT64),
        length,
        result_buffer_vector,
        null_count,
        0,
    )
    return MakeArray(result_array_data)


def hash_chunked_array(obj):
    cdef shared_ptr[CChunkedArray] carr = pyarrow_unwrap_chunked_array(obj)
    if carr.get() == NULL:
        raise TypeError("not a chunked array")
    num_chunks: cython.int = carr.get().num_chunks()
    cdef shared_ptr[CDataType] ctype = carr.get().type()
    cdef Type type_id = ctype.get().id()
    is_integer: cython.bool = False
    is_string: cython.bool = False

    if (Type._Type_UINT8 <= type_id) and (type_id <= Type._Type_INT64):
        is_integer = True
    elif type_id == Type._Type_STRING:
        is_string = True
    else:
        raise TypeError('excepted integer or string got neither')

    cdef shared_ptr[CArray] arr
    cdef vector[shared_ptr[CArray]] hash_results
    for i in range(num_chunks):
        arr = carr.get().chunk(i)
        if is_integer:
            hash_results.push_back(_hash_integer_array(arr))
        elif is_string:
            hash_results.push_back(_hash_string_array(arr))


    cdef shared_ptr[CChunkedArray] result = make_shared[CChunkedArray](hash_results, GetPrimitiveType(Type._Type_UINT64))
    return pyarrow_wrap_chunked_array(result)
