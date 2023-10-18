from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from daft.daft import IOConfig, JoinType
from daft.daft import PyMicroPartition as _PyMicroPartition
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series
from daft.table import Table

if TYPE_CHECKING:
    import pandas as pd


_PANDAS_AVAILABLE = True
try:
    import pandas as pd
except ImportError:
    _PANDAS_AVAILABLE = False


logger = logging.getLogger(__name__)


class Micropartition:
    _micropartition: _PyMicroPartition

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Micropartition via __init__ ")

    def schema(self) -> Schema:
        return Schema._from_pyschema(self._micropartition.schema())

    def column_names(self) -> list[str]:
        return self._micropartition.column_names()

    def get_column(self, name: str) -> Series:
        return Series._from_pyseries(self._micropartition.get_column(name))

    def size_bytes(self) -> int:
        return self._micropartition.size_bytes()

    def __len__(self) -> int:
        return len(self._micropartition)

    def __repr__(self) -> str:
        return repr(self._micropartition)

    def _repr_html_(self) -> str:
        return self._micropartition._repr_html_()

    ###
    # Creation methods
    ###

    @staticmethod
    def empty(schema: Schema | None = None) -> Micropartition:
        pyt = _PyMicroPartition.empty(None) if schema is None else _PyMicroPartition.empty(schema._schema)
        return Micropartition._from_pymicropartition(pyt)

    @staticmethod
    def _from_pymicropartition(pym: _PyMicroPartition) -> Micropartition:
        assert isinstance(pym, _PyMicroPartition)
        tab = Micropartition.__new__(Micropartition)
        tab._micropartition = pym
        return tab

    @staticmethod
    def _from_tables(tables: list[Table]) -> Micropartition:
        return _PyMicroPartition.from_tables([t._table for t in tables])

    @staticmethod
    def from_arrow(arrow_table: pa.Table) -> Micropartition:
        table = Table.from_arrow(arrow_table)
        return Micropartition._from_tables([table])

    @staticmethod
    def from_arrow_record_batches(rbs: list[pa.RecordBatch], arrow_schema: pa.Schema) -> Micropartition:
        schema = Schema._from_field_name_and_types([(f.name, DataType.from_arrow_type(f.type)) for f in arrow_schema])
        pyt = _PyMicroPartition.from_arrow_record_batches(rbs, schema._schema)
        return Micropartition._from_pymicropartition(pyt)

    @staticmethod
    def from_pandas(pd_df: pd.DataFrame) -> Micropartition:
        table = Table.from_pandas(pd_df)
        return Micropartition._from_tables([table])

    @staticmethod
    def from_pydict(data: dict) -> Micropartition:
        table = Table.from_pydict(data)
        return Micropartition._from_tables([table])

    @classmethod
    def concat(cls, to_merge: list[Micropartition]) -> Micropartition:
        micropartitions = []
        for t in to_merge:
            if not isinstance(t, Micropartition):
                raise TypeError(f"Expected a Micropartition for concat, got {type(t)}")
            micropartitions.append(t._micropartition)
        return Micropartition._from_pymicropartition(_PyMicroPartition.concat(micropartitions))

    def slice(self, start: int, end: int) -> Micropartition:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")
        return Micropartition._from_pymicropartition(self._micropartition.slice(start, end))

    ###
    # Exporting methods
    ###

    def to_tables(self) -> list[Table]:
        return [Table._from_pytable(t) for t in self._micropartition.to_tables()]

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False, convert_large_arrays: bool = False) -> pa.Table:
        return pa.Table.concat(
            [
                t.to_arrow(
                    cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype,
                    convert_large_arrays=convert_large_arrays,
                )
                for t in self.to_tables()
            ]
        )

    def to_pydict(self) -> dict[str, list]:
        dicts = [t.to_pydict() for t in self.to_tables()]
        return {field.name(): [val for d in dicts for val in d[field.name()]] for field in self.schema()}

    def to_pylist(self) -> list[dict[str, Any]]:
        return [val for t in self.to_tables() for val in t.to_pylist()]

    def to_pandas(self, schema: Schema | None = None, cast_tensors_to_ray_tensor_dtype: bool = False) -> pd.DataFrame:
        if not _PANDAS_AVAILABLE:
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")
        return pd.DataFrame.concat(
            [
                t.to_pandas(schema=schema, cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype)
                for t in self.to_tables()
            ]
        )

    ###
    # Compute methods (Micropartition -> Micropartition)
    ###

    def cast_to_schema(self, schema: Schema) -> Micropartition:
        """Casts a Micropartition into the provided schema"""
        return Micropartition._from_pymicropartition(self._micropartition.cast_to_schema(schema._schema))

    def eval_expression_list(self, exprs: ExpressionsProjection) -> Micropartition:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return Micropartition._from_pymicropartition(self._micropartition.eval_expression_list(pyexprs))

    def head(self, num: int) -> Micropartition:
        return Micropartition._from_pymicropartition(self._micropartition.head(num))

    def take(self, indices: Series) -> Micropartition:
        assert isinstance(indices, Series)
        return Micropartition._from_pymicropartition(self._micropartition.take(indices._series))

    def filter(self, exprs: ExpressionsProjection) -> Micropartition:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return Micropartition._from_pymicropartition(self._micropartition.filter(pyexprs))

    def sort(self, sort_keys: ExpressionsProjection, descending: bool | list[bool] | None = None) -> Micropartition:
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
        return Micropartition._from_pymicropartition(self._micropartition.sort(pyexprs, descending))

    def sample(self, num: int) -> Micropartition:
        return Micropartition._from_pymicropartition(self._micropartition.sample(num))

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> Micropartition:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return Micropartition._from_pymicropartition(self._micropartition.agg(to_agg_pyexprs, group_by_pyexprs))

    def quantiles(self, num: int) -> Micropartition:
        return Micropartition._from_pymicropartition(self._micropartition.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> Micropartition:
        """NOTE: Expressions here must be Explode expressions (Expression._explode())"""
        to_explode_pyexprs = [e._expr for e in columns]
        return Micropartition._from_pymicropartition(self._micropartition.explode(to_explode_pyexprs))

    def join(
        self,
        right: Micropartition,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> Micropartition:
        if how != JoinType.Inner:
            raise NotImplementedError("TODO: [RUST] Implement Other Join types")
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, Micropartition):
            raise TypeError(f"Expected a Micropartition for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return Micropartition._from_pymicropartition(
            self._micropartition.join(right._micropartition, left_on=left_exprs, right_on=right_exprs)
        )

    def partition_by_hash(self, exprs: ExpressionsProjection, num_partitions: int) -> list[Micropartition]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        pyexprs = [e._expr for e in exprs]
        return [
            Micropartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_hash(pyexprs, num_partitions)
        ]

    def partition_by_range(
        self, partition_keys: ExpressionsProjection, boundaries: Micropartition, descending: list[bool]
    ) -> list[Micropartition]:
        if not isinstance(boundaries, Micropartition):
            raise TypeError(
                f"Expected a Micropartition for `boundaries` in partition_by_range but got {type(boundaries)}"
            )

        exprs = [e._expr for e in partition_keys]
        return [
            Micropartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_range(exprs, boundaries._micropartition, descending)
        ]

    def partition_by_random(self, num_partitions: int, seed: int) -> list[Micropartition]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        if not isinstance(seed, int):
            raise TypeError(f"Expected a seed to be int, got {type(seed)}")

        return [
            Micropartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_random(num_partitions, seed)
        ]

    ###
    # Compute methods (Micropartition -> Series)
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
        return Series._from_pyseries(self._micropartition.argsort(pyexprs, descending))

    def __reduce__(self) -> tuple:
        names = self.column_names()
        return Micropartition.from_pydict, ({name: self.get_column(name) for name in names},)

    @classmethod
    def read_parquet(
        cls,
        path: str,
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        row_groups: list[int] | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> Micropartition:
        return Micropartition._from_pymicropartition(
            _PyMicroPartition.read_parquet(
                path,
                columns,
                start_offset,
                num_rows,
                row_groups,
                io_config,
                multithreaded_io,
                coerce_int96_timestamp_unit,
            )
        )
