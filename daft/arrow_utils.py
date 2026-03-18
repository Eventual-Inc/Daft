from __future__ import annotations

from daft.dependencies import pa


def ensure_array(arr: pa.Array) -> pa.Array:
    """Applies all fixes to an Arrow array."""
    arr = _FixEmptyStructArrays.ensure_array(arr)
    return arr


def ensure_chunked_array(arr: pa.ChunkedArray) -> pa.ChunkedArray:
    """Applies all fixes to an Arrow chunked array."""
    arr = _FixEmptyStructArrays.ensure_chunked_array(arr)
    return arr


def ensure_table(tbl: pa.Table) -> pa.Table:
    """Applies all fixes to an Arrow table."""
    tbl = _FixEmptyStructArrays.ensure_table(tbl)
    return tbl


class _FixEmptyStructArrays:
    """Converts StructArrays that are empty (have no fields) to StructArrays with a single field named "" and with a NullType.

    This is done because Daft internally cannot handle empty StructArrays (zero fields).
    """

    @staticmethod
    def get_empty_struct_type() -> pa.StructType:
        return pa.struct([])

    @staticmethod
    def get_single_field_struct_type() -> pa.StructType:
        return pa.struct({"": pa.null()})

    @staticmethod
    def get_single_field_struct_value() -> dict[str, None]:
        return {"": None}

    def ensure_table(table: pa.Table) -> pa.Table:
        empty_struct_fields = [
            (i, f) for (i, f) in enumerate(table.schema) if f.type == _FixEmptyStructArrays.get_empty_struct_type()
        ]
        if not empty_struct_fields:
            return table
        for i, field in empty_struct_fields:
            table = table.set_column(i, field.name, _FixEmptyStructArrays.ensure_chunked_array(table[field.name]))
        return table

    def ensure_chunked_array(arr: pa.ChunkedArray) -> pa.ChunkedArray:
        if arr.type != _FixEmptyStructArrays.get_empty_struct_type():
            return arr
        return pa.chunked_array([_FixEmptyStructArrays.ensure_array(chunk) for chunk in arr.chunks])

    def ensure_array(arr: pa.Array) -> pa.Array:
        """Recursively converts empty struct arrays to single-field struct arrays."""
        if arr.type == _FixEmptyStructArrays.get_empty_struct_type():
            return pa.array(
                [
                    _FixEmptyStructArrays.get_single_field_struct_value() if valid.as_py() else None
                    for valid in arr.is_valid()
                ],
                type=_FixEmptyStructArrays.get_single_field_struct_type(),
            )

        elif isinstance(arr, pa.StructArray):
            new_arrays = [ensure_array(arr.field(field.name)) for field in arr.type]
            names = [field.name for field in arr.type]
            null_mask = arr.is_null()

            return pa.StructArray.from_arrays(new_arrays, names=names, mask=null_mask)

        else:
            return arr


def remove_empty_struct_placeholders(arr: pa.Array) -> pa.Array:
    """Recursively removes the empty struct placeholders placed by _FixEmptyStructArrays.ensure_array."""
    if arr.type == _FixEmptyStructArrays.get_single_field_struct_type():
        return pa.array(
            [{} if valid.as_py() else None for valid in arr.is_valid()],
            type=_FixEmptyStructArrays.get_empty_struct_type(),
        )

    elif isinstance(arr, pa.StructArray):
        new_arrays = [remove_empty_struct_placeholders(arr.field(field.name)) for field in arr.type]
        names = [field.name for field in arr.type]
        null_mask = arr.is_null()

        return pa.StructArray.from_arrays(new_arrays, names=names, mask=null_mask)

    else:
        return arr
