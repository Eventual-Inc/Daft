from __future__ import annotations

import pyarrow as pa

from daft.daft import PyTable as _PyTable
from daft.expressions2 import Expression
from daft.series import Series


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
    def from_pydict(data: dict) -> Table:
        pya_table = pa.Table.from_pydict(data)
        return Table.from_arrow(pya_table)

    def to_arrow(self) -> pa.Table:
        return pa.Table.from_batches([self._table.to_arrow_record_batch()])

    def to_pydict(self) -> dict[str, list]:
        return self.to_arrow().to_pydict()

    def eval_expression_list(self, exprs: list[Expression]) -> Table:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return Table._from_pytable(self._table.eval_expression_list(pyexprs))

    def head(self, num: int) -> Table:
        return Table._from_pytable(self._table.head(num))

    def take(self, indices: Series) -> Table:
        assert isinstance(indices, Series)
        return Table._from_pytable(self._table.take(indices._series))

    def column_names(self) -> list[str]:
        return self._table.column_names()

    def __len__(self) -> int:
        return len(self._table)

    def __repr__(self) -> str:
        return repr(self._table)
