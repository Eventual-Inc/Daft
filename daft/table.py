from __future__ import annotations

import pyarrow as pa

from daft.daft import PyTable as _PyTable
from daft.expressions2 import Expression, ExpressionsProjection
from daft.logical.schema2 import Schema
from daft.series import Series

_NUMPY_AVAILABLE = True
try:
    import numpy as np
except ImportError:
    _NUMPY_AVAILABLE = False


class Table:
    _table: _PyTable

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Table via __init__ ")

    @staticmethod
    def _from_pytable(pyt: _PyTable) -> Table:
        tab = Table.__new__(Table)
        tab._table = pyt
        return tab

    @staticmethod
    def from_arrow(arrow_table: pa.Table) -> Table:
        assert isinstance(arrow_table, pa.Table)
        pyt = _PyTable.from_arrow_record_batches(arrow_table.to_batches())
        return Table._from_pytable(pyt)

    @staticmethod
    def empty() -> Table:
        pyt = _PyTable.empty()
        return Table._from_pytable(pyt)

    @staticmethod
    def from_pydict(data: dict) -> Table:
        series_dict = dict()
        for k, v in data.items():
            if isinstance(v, list):
                series = Series.from_pylist(v)
            elif isinstance(v, np.ndarray):
                series = Series.from_numpy(v)
            elif isinstance(v, Series):
                series = v
            else:
                series = Series.from_arrow(pa.array(v))
            series_dict[k] = series._series

        return Table._from_pytable(_PyTable.from_pylist_series(series_dict))

    def schema(self) -> Schema:
        return Schema._from_pyschema(self._table.schema())

    def to_arrow(self) -> pa.Table:
        return pa.Table.from_batches([self._table.to_arrow_record_batch()])

    def to_pydict(self) -> dict[str, list]:
        return self.to_arrow().to_pydict()

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
            raise ValueError(f"Expected a bool, list[bool] or None for `descending` but got {type(descending)}")
        return Table._from_pytable(self._table.sort(pyexprs, descending))

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
            raise ValueError(f"Expected a bool, list[bool] or None for `descending` but got {type(descending)}")
        return Series._from_pyseries(self._table.argsort(pyexprs, descending))

    def column_names(self) -> list[str]:
        return self._table.column_names()

    def get_column(self, name: str) -> Series:
        return Series._from_pyseries(self._table.get_column(name))

    def __len__(self) -> int:
        return len(self._table)

    def __repr__(self) -> str:
        return repr(self._table)
