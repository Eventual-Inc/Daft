from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from daft.daft import PyTable as _PyTable
from daft.datatype import DataType
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False

_PANDAS_AVAILABLE = True
try:
    import pandas as pd
except ImportError:
    _PANDAS_AVAILABLE = False

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import pyarrow as pa


class Table:
    _table: _PyTable

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Table via __init__ ")

    def schema(self) -> Schema:
        return Schema._from_pyschema(self._table.schema())

    def column_names(self) -> list[str]:
        return self._table.column_names()

    def get_column(self, name: str) -> Series:
        return Series._from_pyseries(self._table.get_column(name))

    def size_bytes(self) -> int:
        return self._table.size_bytes()

    def __len__(self) -> int:
        return len(self._table)

    def __repr__(self) -> str:
        return repr(self._table)

    ###
    # Creation methods
    ###

    @staticmethod
    def empty(schema: Schema | None = None) -> Table:
        pyt = _PyTable.empty(None) if schema is None else _PyTable.empty(schema._schema)
        return Table._from_pytable(pyt)

    @staticmethod
    def _from_pytable(pyt: _PyTable) -> Table:
        assert isinstance(pyt, _PyTable)
        tab = Table.__new__(Table)
        tab._table = pyt
        return tab

    @staticmethod
    def from_arrow(arrow_table: pa.Table) -> Table:
        assert isinstance(arrow_table, pa.Table)
        schema = Schema._from_field_name_and_types(
            [(f.name, DataType.from_arrow_type(f.type)) for f in arrow_table.schema]
        )
        arrow_table = _ensure_table_slice_offsets_are_propagated(arrow_table)
        pyt = _PyTable.from_arrow_record_batches(arrow_table.to_batches(), schema._schema)
        return Table._from_pytable(pyt)

    @staticmethod
    def from_pydict(data: dict) -> Table:
        series_dict = dict()
        for k, v in data.items():
            if isinstance(v, list):
                series = Series.from_pylist(v)
            elif _NUMPY_AVAILABLE and isinstance(v, np.ndarray):
                series = Series.from_numpy(v)
            elif isinstance(v, Series):
                series = v
            elif isinstance(v, pa.Array):
                v = _ensure_array_slice_offsets_are_propagated(v)
                series = Series.from_arrow(v)
            elif isinstance(v, pa.ChunkedArray):
                v = _ensure_chunked_array_slice_offsets_are_propagated(v)
                series = Series.from_arrow(v)
            else:
                raise ValueError(f"Creating a Series from data of type {type(v)} not implemented")
            series_dict[k] = series._series
        return Table._from_pytable(_PyTable.from_pylist_series(series_dict))

    @classmethod
    def concat(cls, to_merge: list[Table]) -> Table:
        tables = []
        for t in to_merge:
            if not isinstance(t, Table):
                raise TypeError(f"Expected a Table for concat, got {type(t)}")
            tables.append(t._table)
        return Table._from_pytable(_PyTable.concat(tables))

    ###
    # Exporting methods
    ###

    def to_arrow(self) -> pa.Table:
        python_fields = {field.name: field for field in self.schema() if field.dtype == DataType.python()}
        if python_fields:
            table = {}
            for colname in self.column_names():
                column_series = self.get_column(colname)
                if colname in python_fields:
                    # TODO(Clark): Get the column as a top-level ndarray to ensure it remains contiguous.
                    column = column_series.to_pylist()
                    # TODO(Clark): Infer the tensor extension type even when the column is empty.
                    # This will probably require encoding more information in the Daft type that we use to
                    # represent tensors.
                    if len(column) > 0 and isinstance(column[0], np.ndarray):
                        from ray.data.extensions import ArrowTensorArray

                        column = ArrowTensorArray.from_numpy(column)
                else:
                    column = column_series.to_arrow()
                table[colname] = column

            return pa.Table.from_pydict(table)
        else:
            return pa.Table.from_batches([self._table.to_arrow_record_batch()])

    def to_pydict(self) -> dict[str, list]:
        return {colname: self.get_column(colname).to_pylist() for colname in self.column_names()}

    def to_pylist(self) -> list[dict[str, Any]]:
        # TODO(Clark): Avoid a double-materialization of the table once the Rust-side table supports
        # by-row selection or iteration.
        table = self.to_pydict()
        column_names = self.column_names()
        return [{colname: table[colname][i] for colname in column_names} for i in range(len(self))]

    def to_pandas(self, schema: Schema | None = None) -> pd.DataFrame:
        if not _PANDAS_AVAILABLE:
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")
        return self.to_arrow().to_pandas()

    ###
    # Compute methods (Table -> Table)
    ###

    def eval_expression_list(self, exprs: ExpressionsProjection) -> Table:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return Table._from_pytable(self._table.eval_expression_list(pyexprs))

    def head(self, num: int) -> Table:
        return Table._from_pytable(self._table.head(num))

    def take(self, indices: Series) -> Table:
        assert isinstance(indices, Series)
        return Table._from_pytable(self._table.take(indices._series))

    def filter(self, exprs: ExpressionsProjection) -> Table:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return Table._from_pytable(self._table.filter(pyexprs))

    def sort(self, sort_keys: ExpressionsProjection, descending: bool | list[bool] | None = None) -> Table:
        assert all(isinstance(e, Expression) for e in sort_keys)
        pyexprs = [e._expr for e in sort_keys]
        if descending is None:
            descending = [False for _ in pyexprs]
        elif isinstance(descending, bool):
            descending = [descending for _ in pyexprs]
        elif isinstance(descending, list):
            if len(descending) != len(sort_keys):
                raise ValueError(
                    f"Expected length of `descending` to be the same length as `sort_keys` since a list was passed in,"
                    f"got {len(descending)} instead of {len(sort_keys)}"
                )
        else:
            raise TypeError(f"Expected a bool, list[bool] or None for `descending` but got {type(descending)}")
        return Table._from_pytable(self._table.sort(pyexprs, descending))

    def sample(self, num: int) -> Table:
        return Table._from_pytable(self._table.sample(num))

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> Table:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return Table._from_pytable(self._table.agg(to_agg_pyexprs, group_by_pyexprs))

    def quantiles(self, num: int) -> Table:
        return Table._from_pytable(self._table.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> Table:
        raise NotImplementedError("TODO: [RUST-INT][NESTED] Implement for Table")

    def join(
        self,
        right: Table,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        output_projection: ExpressionsProjection | None = None,
        how: str = "inner",
    ) -> Table:
        if how != "inner":
            raise NotImplementedError("TODO: [RUST] Implement Other Join types")
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, Table):
            raise TypeError(f"Expected a Table for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return Table._from_pytable(self._table.join(right._table, left_on=left_exprs, right_on=right_exprs))

    def partition_by_hash(self, exprs: ExpressionsProjection, num_partitions: int) -> list[Table]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        pyexprs = [e._expr for e in exprs]
        return [Table._from_pytable(t) for t in self._table.partition_by_hash(pyexprs, num_partitions)]

    def partition_by_range(
        self, partition_keys: ExpressionsProjection, boundaries: Table, descending: list[bool]
    ) -> list[Table]:
        if not isinstance(boundaries, Table):
            raise TypeError(f"Expected a Table for `boundaries` in partition_by_range but got {type(boundaries)}")

        exprs = [e._expr for e in partition_keys]
        return [Table._from_pytable(t) for t in self._table.partition_by_range(exprs, boundaries._table, descending)]

    def partition_by_random(self, num_partitions: int, seed: int) -> list[Table]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        if not isinstance(seed, int):
            raise TypeError(f"Expected a seed to be int, got {type(seed)}")

        return [Table._from_pytable(t) for t in self._table.partition_by_random(num_partitions, seed)]

    ###
    # Compute methods (Table -> Series)
    ###

    def argsort(self, sort_keys: ExpressionsProjection, descending: bool | list[bool] | None = None) -> Series:
        assert all(isinstance(e, Expression) for e in sort_keys)
        pyexprs = [e._expr for e in sort_keys]
        if descending is None:
            descending = [False for _ in pyexprs]
        elif isinstance(descending, bool):
            descending = [descending for _ in pyexprs]
        elif isinstance(descending, list):
            if len(descending) != len(sort_keys):
                raise ValueError(
                    f"Expected length of `descending` to be the same length as `sort_keys` since a list was passed in,"
                    f"got {len(descending)} instead of {len(sort_keys)}"
                )
        else:
            raise TypeError(f"Expected a bool, list[bool] or None for `descending` but got {type(descending)}")
        return Series._from_pyseries(self._table.argsort(pyexprs, descending))

    def __reduce__(self) -> tuple:
        names = self.column_names()
        return Table.from_pydict, ({name: self.get_column(name) for name in names},)


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


def _ensure_table_slice_offsets_are_propagated(arrow_table: pa.Table) -> pa.Table:
    """
    Ensures that table-level slice offsets are properly propagated to child arrays
    to prevent them from being dropped upon record batch conversion and FFI transfer.
    """
    arrow_schema = arrow_table.schema
    for idx, name in enumerate(arrow_schema.names):
        field = arrow_schema.field(name)
        column = arrow_table.column(name)
        if _chunked_array_needs_slice_offset_propagation(column):
            new_column = _propagate_chunked_array_slice_offsets(column)
            arrow_table = arrow_table.set_column(idx, field, new_column)
    return arrow_table


def _ensure_chunked_array_slice_offsets_are_propagated(chunked_array: pa.ChunkedArray) -> pa.ChunkedArray:
    """
    Ensures that chunked-array-level slice offsets are properly propagated to child arrays
    to prevent them from being dropped upon record batch conversion and FFI transfer.
    """
    if _chunked_array_needs_slice_offset_propagation(chunked_array):
        return _propagate_chunked_array_slice_offsets(chunked_array)
    else:
        return chunked_array


def _ensure_array_slice_offsets_are_propagated(array: pa.Array) -> pa.Array:
    """
    Ensures that array-level slice offsets are properly propagated to child arrays
    to prevent them from being dropped upon record batch conversion and FFI transfer.
    """
    if _array_needs_slice_offset_propagation(array):
        return _propagate_array_slice_offsets(array)
    else:
        return array


def _chunked_array_needs_slice_offset_propagation(chunked_array: pa.ChunkedArray) -> bool:
    """
    Whether an Arrow ChunkedArray needs slice offset propagation.

    This is currently only true for struct arrays and fixed-size list arrays that contain
    slice offsets/truncations.
    """
    if not pa.types.is_struct(chunked_array.type) and not pa.types.is_fixed_size_list(chunked_array.type):
        return False
    return any(_array_needs_slice_offset_propagation(chunk) for chunk in chunked_array.chunks)


def _array_needs_slice_offset_propagation(array: pa.Array) -> bool:
    """
    Whether an Arrow array needs slice offset propagation.

    This is currently only true for struct arrays and fixed-size list arrays that contain
    slice offsets/truncations.
    """
    if pa.types.is_struct(array.type):
        return _struct_array_needs_slice_offset_propagation(array)
    elif pa.types.is_fixed_size_list(array.type):
        return _fixed_size_list_array_needs_slice_offset_propagation(array)
    else:
        return False


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


def _fixed_size_list_array_needs_slice_offset_propagation(array: pa.FixedSizeListArray) -> bool:
    """
    Whether the provided fixed-size list array needs slice offset propagation.
    """
    assert isinstance(array, pa.FixedSizeListArray)
    return array.offset > 0 or len(array) < array.type.list_size * len(array.values)


def _propagate_chunked_array_slice_offsets(chunked_array: pa.ChunkedArray) -> pa.ChunkedArray:
    """
    Propagate slice offsets for the provided chunked array to the child arrays of each chunk.
    """
    new_chunks = []
    # Flatten each chunk to propagate slice offsets to child arrays.
    for chunk in chunked_array.chunks:
        new_chunk = _propagate_array_slice_offsets(chunk)
        new_chunks.append(new_chunk)
    return pa.chunked_array(new_chunks)


def _propagate_array_slice_offsets(array: pa.Array) -> pa.Array:
    """
    Propagate slice offsets for the provided array to its child arrays.
    """
    assert _array_needs_slice_offset_propagation(array)
    dtype = array.type
    if pa.types.is_struct(dtype):
        fields = [dtype.field(i) for i in range(dtype.num_fields)]
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
            bitmap_buf = _slice_bitmap_buffer(bitmap_buf, array.offset, len(array))
        out = pa.FixedSizeListArray.from_buffers(dtype, len(array), [bitmap_buf], offset=0, children=[new_array])
        return out
    else:
        raise ValueError(f"Array type doesn't need array slice offset propagation: {dtype}")


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
