from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from daft.daft import (
    CsvConvertOptions,
    CsvParseOptions,
    CsvReadOptions,
    IOConfig,
    JoinType,
    JsonConvertOptions,
    JsonParseOptions,
    JsonReadOptions,
)
from daft.daft import PyMicroPartition as _PyMicroPartition
from daft.daft import PyTable as _PyTable
from daft.daft import ScanTask as _ScanTask
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series
from daft.table.table import Table

if TYPE_CHECKING:
    import pandas as pd


_PANDAS_AVAILABLE = True
try:
    import pandas as pd
except ImportError:
    _PANDAS_AVAILABLE = False


logger = logging.getLogger(__name__)


class MicroPartition:
    _micropartition: _PyMicroPartition

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a MicroPartition via __init__ ")

    def schema(self) -> Schema:
        return Schema._from_pyschema(self._micropartition.schema())

    def column_names(self) -> list[str]:
        return self._micropartition.column_names()

    def get_column(self, name: str) -> Series:
        return Series._from_pyseries(self._micropartition.get_column(name))

    def size_bytes(self) -> int | None:
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
    def empty(schema: Schema | None = None) -> MicroPartition:
        pyt = _PyMicroPartition.empty(None) if schema is None else _PyMicroPartition.empty(schema._schema)
        return MicroPartition._from_pymicropartition(pyt)

    @staticmethod
    def _from_scan_task(scan_task: _ScanTask) -> MicroPartition:
        assert isinstance(scan_task, _ScanTask)
        return MicroPartition._from_pymicropartition(_PyMicroPartition.from_scan_task(scan_task))

    @staticmethod
    def _from_pytable(pyt: _PyTable) -> MicroPartition:
        assert isinstance(pyt, _PyTable)
        return MicroPartition._from_pymicropartition(_PyMicroPartition.from_tables([pyt]))

    @staticmethod
    def _from_pymicropartition(pym: _PyMicroPartition) -> MicroPartition:
        assert isinstance(pym, _PyMicroPartition)
        tab = MicroPartition.__new__(MicroPartition)
        tab._micropartition = pym
        return tab

    @staticmethod
    def _from_tables(tables: list[Table]) -> MicroPartition:
        return MicroPartition._from_pymicropartition(_PyMicroPartition.from_tables([t._table for t in tables]))

    @staticmethod
    def from_arrow(arrow_table: pa.Table) -> MicroPartition:
        table = Table.from_arrow(arrow_table)
        return MicroPartition._from_tables([table])

    @staticmethod
    def from_arrow_record_batches(rbs: list[pa.RecordBatch], arrow_schema: pa.Schema) -> MicroPartition:
        schema = Schema._from_field_name_and_types([(f.name, DataType.from_arrow_type(f.type)) for f in arrow_schema])
        pyt = _PyMicroPartition.from_arrow_record_batches(rbs, schema._schema)
        return MicroPartition._from_pymicropartition(pyt)

    @staticmethod
    def from_pandas(pd_df: pd.DataFrame) -> MicroPartition:
        table = Table.from_pandas(pd_df)
        return MicroPartition._from_tables([table])

    @staticmethod
    def from_pydict(data: dict) -> MicroPartition:
        table = Table.from_pydict(data)
        return MicroPartition._from_tables([table])

    @classmethod
    def concat(cls, to_merge: list[MicroPartition]) -> MicroPartition:
        micropartitions = []
        for t in to_merge:
            if not isinstance(t, MicroPartition):
                raise TypeError(f"Expected a MicroPartition for concat, got {type(t)}")
            micropartitions.append(t._micropartition)
        return MicroPartition._from_pymicropartition(_PyMicroPartition.concat(micropartitions))

    def slice(self, start: int, end: int) -> MicroPartition:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")
        return MicroPartition._from_pymicropartition(self._micropartition.slice(start, end))

    ###
    # Exporting methods
    ###

    def to_table(self) -> Table:
        return Table._from_pytable(self._micropartition.to_table())

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False, convert_large_arrays: bool = False) -> pa.Table:
        return self.to_table().to_arrow(
            cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype, convert_large_arrays=convert_large_arrays
        )

    def to_pydict(self) -> dict[str, list]:
        return self.to_table().to_pydict()

    def to_pylist(self) -> list[dict[str, Any]]:
        return self.to_table().to_pylist()

    def to_pandas(
        self,
        schema: Schema | None = None,
        cast_tensors_to_ray_tensor_dtype: bool = False,
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.DataFrame:
        return self.to_table().to_pandas(
            schema=schema,
            cast_tensors_to_ray_tensor_dtype=cast_tensors_to_ray_tensor_dtype,
            coerce_temporal_nanoseconds=coerce_temporal_nanoseconds,
        )

    ###
    # Compute methods (MicroPartition -> MicroPartition)
    ###

    def cast_to_schema(self, schema: Schema) -> MicroPartition:
        """Casts a MicroPartition into the provided schema"""
        return MicroPartition._from_pymicropartition(self._micropartition.cast_to_schema(schema._schema))

    def eval_expression_list(self, exprs: ExpressionsProjection) -> MicroPartition:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return MicroPartition._from_pymicropartition(self._micropartition.eval_expression_list(pyexprs))

    def head(self, num: int) -> MicroPartition:
        return MicroPartition._from_pymicropartition(self._micropartition.head(num))

    def take(self, indices: Series) -> MicroPartition:
        assert isinstance(indices, Series)
        return MicroPartition._from_pymicropartition(self._micropartition.take(indices._series))

    def filter(self, exprs: ExpressionsProjection) -> MicroPartition:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return MicroPartition._from_pymicropartition(self._micropartition.filter(pyexprs))

    def sort(self, sort_keys: ExpressionsProjection, descending: bool | list[bool] | None = None) -> MicroPartition:
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
        return MicroPartition._from_pymicropartition(self._micropartition.sort(pyexprs, descending))

    def sample(
        self,
        fraction: float | None = None,
        size: int | None = None,
        with_replacement: bool = False,
        seed: int | None = None,
    ) -> MicroPartition:
        if fraction is not None and size is not None:
            raise ValueError("Must specify either `fraction` or `size`, but not both")
        elif fraction is not None:
            return MicroPartition._from_pymicropartition(
                self._micropartition.sample_by_fraction(float(fraction), with_replacement, seed)
            )
        elif size is not None:
            return MicroPartition._from_pymicropartition(
                self._micropartition.sample_by_size(size, with_replacement, seed)
            )
        else:
            raise ValueError("Must specify either `fraction` or `size`")

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> MicroPartition:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return MicroPartition._from_pymicropartition(self._micropartition.agg(to_agg_pyexprs, group_by_pyexprs))

    def pivot(
        self, group_by: ExpressionsProjection, pivot_column: Expression, values_column: Expression, names: list[str]
    ) -> MicroPartition:
        group_by_pyexprs = [e._expr for e in group_by]
        pivot_column_pyexpr = pivot_column._expr
        values_column_pyexpr = values_column._expr
        return MicroPartition._from_pymicropartition(
            self._micropartition.pivot(group_by_pyexprs, pivot_column_pyexpr, values_column_pyexpr, names)
        )

    def quantiles(self, num: int) -> MicroPartition:
        return MicroPartition._from_pymicropartition(self._micropartition.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> MicroPartition:
        """NOTE: Expressions here must be Explode expressions (Expression._explode())"""
        to_explode_pyexprs = [e._expr for e in columns]
        return MicroPartition._from_pymicropartition(self._micropartition.explode(to_explode_pyexprs))

    def unpivot(
        self, ids: ExpressionsProjection, values: ExpressionsProjection, variable_name: str, value_name: str
    ) -> MicroPartition:
        ids_pyexprs = [e._expr for e in ids]
        values_pyexprs = [e._expr for e in values]
        return MicroPartition._from_pymicropartition(
            self._micropartition.unpivot(ids_pyexprs, values_pyexprs, variable_name, value_name)
        )

    def hash_join(
        self,
        right: MicroPartition,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> MicroPartition:
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, MicroPartition):
            raise TypeError(f"Expected a MicroPartition for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return MicroPartition._from_pymicropartition(
            self._micropartition.hash_join(right._micropartition, left_on=left_exprs, right_on=right_exprs, how=how)
        )

    def sort_merge_join(
        self,
        right: MicroPartition,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
        is_sorted: bool = False,
    ) -> MicroPartition:
        if how != JoinType.Inner:
            raise NotImplementedError("TODO: [RUST] Implement Other Join types")
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, MicroPartition):
            raise TypeError(f"Expected a MicroPartition for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return MicroPartition._from_pymicropartition(
            self._micropartition.sort_merge_join(
                right._micropartition, left_on=left_exprs, right_on=right_exprs, is_sorted=is_sorted
            )
        )

    def partition_by_hash(self, exprs: ExpressionsProjection, num_partitions: int) -> list[MicroPartition]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        pyexprs = [e._expr for e in exprs]
        return [
            MicroPartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_hash(pyexprs, num_partitions)
        ]

    def partition_by_range(
        self, partition_keys: ExpressionsProjection, boundaries: Table, descending: list[bool]
    ) -> list[MicroPartition]:
        if not isinstance(boundaries, Table):
            raise TypeError(f"Expected a Table for `boundaries` in partition_by_range but got {type(boundaries)}")

        exprs = [e._expr for e in partition_keys]
        return [
            MicroPartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_range(exprs, boundaries._table, descending)
        ]

    def partition_by_random(self, num_partitions: int, seed: int) -> list[MicroPartition]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        if not isinstance(seed, int):
            raise TypeError(f"Expected a seed to be int, got {type(seed)}")

        return [
            MicroPartition._from_pymicropartition(t)
            for t in self._micropartition.partition_by_random(num_partitions, seed)
        ]

    def partition_by_value(self, partition_keys: ExpressionsProjection) -> tuple[list[MicroPartition], MicroPartition]:
        exprs = [e._expr for e in partition_keys]
        pytables, values = self._micropartition.partition_by_value(exprs)
        return [MicroPartition._from_pymicropartition(t) for t in pytables], MicroPartition._from_pymicropartition(
            values
        )

    def add_monotonically_increasing_id(self, partition_num: int, column_name: str) -> MicroPartition:
        return MicroPartition._from_pymicropartition(
            self._micropartition.add_monotonically_increasing_id(partition_num, column_name)
        )

    ###
    # Compute methods (MicroPartition -> Series)
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
        return MicroPartition.from_pydict, ({name: self.get_column(name) for name in names},)

    @classmethod
    def read_parquet_statistics(
        cls,
        paths: Series | list[str],
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> Table:
        return Table.read_parquet_statistics(paths=paths, io_config=io_config, multithreaded_io=multithreaded_io)

    @classmethod
    def read_parquet(
        cls,
        path: str,
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        row_groups: list[int] | None = None,
        predicate: Expression | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> MicroPartition:
        return MicroPartition._from_pymicropartition(
            _PyMicroPartition.read_parquet(
                path,
                columns,
                start_offset,
                num_rows,
                row_groups,
                predicate._expr if predicate is not None else None,
                io_config,
                multithreaded_io,
                coerce_int96_timestamp_unit._timeunit,
            )
        )

    @classmethod
    def read_parquet_bulk(
        cls,
        paths: list[str],
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        row_groups_per_path: list[list[int] | None] | None = None,
        predicate: Expression | None = None,
        io_config: IOConfig | None = None,
        num_parallel_tasks: int | None = 128,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> MicroPartition:
        return MicroPartition._from_pymicropartition(
            _PyMicroPartition.read_parquet_bulk(
                paths,
                columns,
                start_offset,
                num_rows,
                row_groups_per_path,
                predicate._expr if predicate is not None else None,
                io_config,
                num_parallel_tasks,
                multithreaded_io,
                coerce_int96_timestamp_unit._timeunit,
            )
        )

    @classmethod
    def read_csv(
        cls,
        path: str,
        convert_options: CsvConvertOptions,
        parse_options: CsvParseOptions,
        read_options: CsvReadOptions,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> MicroPartition:
        return MicroPartition._from_pymicropartition(
            _PyMicroPartition.read_csv(
                uri=path,
                convert_options=convert_options,
                parse_options=parse_options,
                read_options=read_options,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
            )
        )

    @classmethod
    def read_json(
        cls,
        path: str,
        convert_options: JsonConvertOptions | None = None,
        parse_options: JsonParseOptions | None = None,
        read_options: JsonReadOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> MicroPartition:
        return MicroPartition._from_pymicropartition(
            _PyMicroPartition.read_json_native(
                uri=path,
                convert_options=convert_options,
                parse_options=parse_options,
                read_options=read_options,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
            )
        )
