from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.expressions import col

if TYPE_CHECKING:
    from daft.dependencies import np
    from daft.expressions import Expression
    from daft.series import Series


class KeySpec:
    def __init__(self, key_columns: list[str]):
        if not key_columns or any((not isinstance(c, str)) or c == "" for c in key_columns):
            raise ValueError(
                "[skip_existing] key_column must be a non-empty column name or list of non-empty column names"
            )
        self.key_columns = key_columns
        self.is_composite = len(key_columns) > 1
        self.composite_key_fields = tuple(key_columns) if self.is_composite else ()

    def to_expr(self) -> Expression:
        if self.is_composite:
            from daft.functions.struct import to_struct

            return to_struct(**{k: col(k) for k in self.key_columns})
        return col(self.key_columns[0])

    def to_numpy(self, input: Series) -> "np.ndarray":  # noqa: UP037
        if self.is_composite:
            keys = input.to_pylist()
            return _composite_keys_to_numpy(keys, self.composite_key_fields)
        return input.to_arrow().to_numpy(zero_copy_only=False)


def _composite_keys_to_numpy(keys: list[Any], composite_key_fields: tuple[str, ...]) -> "np.ndarray":  # noqa: UP037
    from daft.dependencies import np

    keys_as_tuples: list[Any] = []
    for item in keys:
        if not isinstance(item, dict):
            raise TypeError(f"Expected composite key rows to be dicts, found: {type(item)}")
        keys_as_tuples.append(tuple(item.get(k) for k in composite_key_fields))
    keys_np = np.empty(len(keys_as_tuples), dtype=object)
    keys_np[:] = keys_as_tuples
    return keys_np
