from __future__ import annotations

import sys

import pyarrow as pa


def ensure_array(arr: pa.Array) -> pa.Array:
    """Applies all fixes to an Arrow array"""
    arr = _FixEmptyStructArrays.ensure_array(arr)
    arr = _FixSliceOffsets.ensure_array(arr)
    return arr


def ensure_chunked_array(arr: pa.ChunkedArray) -> pa.ChunkedArray:
    """Applies all fixes to an Arrow chunked array"""
    arr = _FixEmptyStructArrays.ensure_chunked_array(arr)
    arr = _FixSliceOffsets.ensure_chunked_array(arr)
    return arr


def ensure_table(tbl: pa.Table) -> pa.Table:
    """Applies all fixes to an Arrow table"""
    tbl = _FixEmptyStructArrays.ensure_table(tbl)
    tbl = _FixSliceOffsets.ensure_table(tbl)
    return tbl


class _FixEmptyStructArrays:
    """Converts StructArrays that are empty (have no fields) to StructArrays with a single field
    named "" and with a NullType

    This is done because arrow2::ffi cannot handle empty StructArrays and we need to handle this on the
    Python layer before going through ffi into Rust.
    """

    EMPTY_STRUCT_TYPE = pa.struct([])
    SINGLE_FIELD_STRUCT_TYPE = pa.struct({"": pa.null()})
    SINGLE_FIELD_STRUCT_VALUE = {"": None}

    def ensure_table(table: pa.Table) -> pa.Table:
        empty_struct_fields = [
            (i, f) for (i, f) in enumerate(table.schema) if f.type == _FixEmptyStructArrays.EMPTY_STRUCT_TYPE
        ]
        if not empty_struct_fields:
            return table
        for i, field in empty_struct_fields:
            table = table.set_column(i, field.name, _FixEmptyStructArrays.ensure_chunked_array(table[field.name]))
        return table

    def ensure_chunked_array(arr: pa.ChunkedArray) -> pa.ChunkedArray:
        if arr.type != _FixEmptyStructArrays.EMPTY_STRUCT_TYPE:
            return arr
        return pa.chunked_array([_FixEmptyStructArrays.ensure_array(chunk) for chunk in arr.chunks])

    def ensure_array(arr: pa.Array) -> pa.Array:
        if arr.type != _FixEmptyStructArrays.EMPTY_STRUCT_TYPE:
            return arr
        return pa.array(
            [_FixEmptyStructArrays.SINGLE_FIELD_STRUCT_VALUE if valid else None for valid in arr.is_valid()],
            type=_FixEmptyStructArrays.SINGLE_FIELD_STRUCT_TYPE,
        )


class _FixSliceOffsets:
    # TODO(Clark): For pyarrow < 12.0.0, struct array slice offsets are dropped
    # when converting to record batches. We work around this below by flattening
    # the field arrays for all struct arrays, which propagates said offsets to
    # the child (field) arrays. Note that this flattening is zero-copy unless
    # the column contains a validity bitmap (i.e. the column has one or more nulls).
    #
    # We should enforce a lower Arrow version bound once the upstream fix is
    # released.
    #
    # See:
    #  - https://github.com/apache/arrow/issues/34639
    #  - https://github.com/apache/arrow/pull/34691

    @staticmethod
    def ensure_table(arrow_table: pa.Table) -> pa.Table:
        """
        Ensures that table-level slice offsets are properly propagated to child arrays
        to prevent them from being dropped upon record batch conversion and FFI transfer.
        """
        arrow_schema = arrow_table.schema
        for idx, name in enumerate(arrow_schema.names):
            field = arrow_schema.field(name)
            column = arrow_table.column(name)
            if _FixSliceOffsets._chunked_array_needs_slice_offset_propagation(column):
                new_column = _FixSliceOffsets._propagate_chunked_array_slice_offsets(column)
                arrow_table = arrow_table.set_column(idx, field, new_column)
        return arrow_table

    @staticmethod
    def ensure_chunked_array(chunked_array: pa.ChunkedArray) -> pa.ChunkedArray:
        """
        Ensures that chunked-array-level slice offsets are properly propagated to child arrays
        to prevent them from being dropped upon record batch conversion and FFI transfer.
        """
        if _FixSliceOffsets._chunked_array_needs_slice_offset_propagation(chunked_array):
            return _FixSliceOffsets._propagate_chunked_array_slice_offsets(chunked_array)
        else:
            return chunked_array

    @staticmethod
    def ensure_array(array: pa.Array) -> pa.Array:
        """
        Ensures that array-level slice offsets are properly propagated to child arrays
        to prevent them from being dropped upon record batch conversion and FFI transfer.
        """
        if _FixSliceOffsets._array_needs_slice_offset_propagation(array):
            return _FixSliceOffsets._propagate_array_slice_offsets(array)
        else:
            return array

    @staticmethod
    def _chunked_array_needs_slice_offset_propagation(chunked_array: pa.ChunkedArray) -> bool:
        """
        Whether an Arrow ChunkedArray needs slice offset propagation.

        This is currently only true for struct arrays and fixed-size list arrays that contain
        slice offsets/truncations.
        """
        if not pa.types.is_struct(chunked_array.type) and not pa.types.is_fixed_size_list(chunked_array.type):
            return False
        return any(_FixSliceOffsets._array_needs_slice_offset_propagation(chunk) for chunk in chunked_array.chunks)

    @staticmethod
    def _array_needs_slice_offset_propagation(array: pa.Array) -> bool:
        """
        Whether an Arrow array needs slice offset propagation.

        This is currently only true for struct arrays and fixed-size list arrays that contain
        slice offsets/truncations.
        """
        if pa.types.is_struct(array.type):
            return _FixSliceOffsets._struct_array_needs_slice_offset_propagation(array)
        elif pa.types.is_fixed_size_list(array.type):
            return _FixSliceOffsets._fixed_size_list_array_needs_slice_offset_propagation(array)
        else:
            return False

    @staticmethod
    def _struct_array_needs_slice_offset_propagation(array: pa.StructArray) -> bool:
        """
        Whether the provided struct array needs slice offset propagation.
        """
        assert isinstance(array, pa.StructArray)
        # TODO(Clark): Only propagate slice offsets if a slice exists; checking whether the
        # array length has been truncated is currently difficult since StructArray.field()
        # propagates the slice to the field arrays, and there's no other convenient way to
        # access the child field arrays other than reconstructing them from the raw buffers,
        # which is complex.
        # if array.type.num_fields == 0:
        #     return False
        # return array.offset > 0 or len(array) < len(array.field(0))
        return True

    @staticmethod
    def _fixed_size_list_array_needs_slice_offset_propagation(array: pa.FixedSizeListArray) -> bool:
        """
        Whether the provided fixed-size list array needs slice offset propagation.
        """
        assert isinstance(array, pa.FixedSizeListArray)
        return array.offset > 0 or len(array) < array.type.list_size * len(array.values)

    @staticmethod
    def _propagate_chunked_array_slice_offsets(chunked_array: pa.ChunkedArray) -> pa.ChunkedArray:
        """
        Propagate slice offsets for the provided chunked array to the child arrays of each chunk.
        """
        new_chunks = []
        # Flatten each chunk to propagate slice offsets to child arrays.
        for chunk in chunked_array.chunks:
            new_chunk = _FixSliceOffsets._propagate_array_slice_offsets(chunk)
            new_chunks.append(new_chunk)
        return pa.chunked_array(new_chunks)

    @staticmethod
    def _propagate_array_slice_offsets(array: pa.Array) -> pa.Array:
        """
        Propagate slice offsets for the provided array to its child arrays.
        """
        assert _FixSliceOffsets._array_needs_slice_offset_propagation(array)
        dtype = array.type
        if pa.types.is_struct(dtype):
            fields = [dtype[i] for i in range(dtype.num_fields)]
            # StructArray.flatten() will propagate slice offsets to the underlying field arrays
            # while also propagating the StructArray-level bitmap.
            new_field_arrays = array.flatten()
            return pa.StructArray.from_arrays(new_field_arrays, fields=fields, mask=array.is_null())
        elif pa.types.is_fixed_size_list(dtype):
            # TODO(Clark): FixedSizeListArray.flatten() doesn't properly handle bitmap propagation
            # to the child array, instead returning a concatenation of non-null array fragments.
            # We therefore manually slice the full values array and take care of the bitmap
            # propagation ourselves.
            # new_array = array.flatten()
            new_array = array.values.slice(array.offset * dtype.list_size, len(array) * dtype.list_size)
            bitmap_buf = array.buffers()[0]
            if bitmap_buf is not None:
                bitmap_buf = _FixSliceOffsets._slice_bitmap_buffer(bitmap_buf, array.offset, len(array))
            out = pa.FixedSizeListArray.from_buffers(dtype, len(array), [bitmap_buf], offset=0, children=[new_array])
            return out
        else:
            raise ValueError(f"Array type doesn't need array slice offset propagation: {dtype}")

    @staticmethod
    def _slice_bitmap_buffer(buf: pa.Buffer, offset: int, length: int) -> pa.Buffer:
        """
        Slice the provided bitpacked boolean bitmap buffer at the given offset and length.

        This function takes care of the byte and bit offset bookkeeping required due to the buffer
        being bitpacked.
        """
        # Offset to the leftmost byte of the buffer slice.
        byte_offset = offset // 8
        # Bit offset into the leftmost byte of the buffer slice.
        bit_offset = offset % 8
        # The byte-padded number of bits for the buffer slice.
        # This rounds up the buffer slice to the nearest multiple of 8.
        num_bytes_for_bits = (bit_offset + length + 7) & (-8)
        # The number of bytes in the byte-padded buffer slice.
        byte_length = num_bytes_for_bits // 8
        buf = buf.slice(byte_offset, byte_length)
        if bit_offset != 0:
            # If we have a bit offset into the leftmost byte of the buffer slice,
            # we need to bitshift the buffer to eliminate the bit offset.
            bytes_ = buf.to_pybytes()
            bytes_as_int = int.from_bytes(bytes_, sys.byteorder)
            bytes_as_int >>= bit_offset
            bytes_ = bytes_as_int.to_bytes(byte_length, sys.byteorder)
            buf = pa.py_buffer(bytes_)
        return buf
