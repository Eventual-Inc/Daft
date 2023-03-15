from __future__ import annotations

from typing import TYPE_CHECKING

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
    def empty() -> Table:
        pyt = _PyTable.empty()
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
        pyt = _PyTable.from_arrow_record_batches(arrow_table.to_batches())
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
            elif isinstance(v, pa.Array) or isinstance(v, pa.ChunkedArray):
                series = Series.from_arrow(v)
            else:
                series = Series.from_arrow(pa.array(v))
            series_dict[k] = series._series
        return Table._from_pytable(_PyTable.from_pylist_series(series_dict))

    @classmethod
    def concat(cls, to_merge: list[Table]) -> Table:
        raise NotImplementedError("TODO: [RUST-INT][TPCH] Implement for Table")

    ###
    # Exporting methods
    ###

    def to_arrow(self) -> pa.Table:
        # TODO: [RUST-INT][PY] throw error for Python object types
        return pa.Table.from_batches([self._table.to_arrow_record_batch()])

    def to_pydict(self) -> dict[str, list]:
        # TODO: [RUST-INT][PY] support for Python object types
        return self.to_arrow().to_pydict()

    def to_pandas(self, schema: Schema | None = None) -> pd.DataFrame:
        if not _PANDAS_AVAILABLE:
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")

        # TODO: [RUST-INT][TPCH] implement and test better - this is used in test validations
        pd_df = pd.DataFrame(self.to_pydict())
        if schema is not None:
            pd_df = pd_df[schema.column_names()]
        return pd_df

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

    def agg(self, to_agg: list[tuple[Expression, str]], group_by: ExpressionsProjection | None = None) -> Table:
        raise NotImplementedError("TODO: [RUST-INT][TPCH] Implement for Table")

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

        if len(left_on) > 1:
            raise NotImplementedError("TODO: [RUST-INT][TPCH] Multicolumn joins not implemented")

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
