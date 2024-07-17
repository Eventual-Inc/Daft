from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from daft.arrow_utils import ensure_table
from daft.daft import (
    CsvConvertOptions,
    CsvParseOptions,
    CsvReadOptions,
    JoinType,
    JsonConvertOptions,
    JsonParseOptions,
    JsonReadOptions,
)
from daft.daft import PyTable as _PyTable
from daft.daft import ScanTask as _ScanTask
from daft.daft import read_csv as _read_csv
from daft.daft import read_json as _read_json
from daft.daft import read_parquet as _read_parquet
from daft.daft import read_parquet_bulk as _read_parquet_bulk
from daft.daft import read_parquet_into_pyarrow as _read_parquet_into_pyarrow
from daft.daft import read_parquet_into_pyarrow_bulk as _read_parquet_into_pyarrow_bulk
from daft.daft import read_parquet_statistics as _read_parquet_statistics
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series, item_to_series

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

    from daft.io import IOConfig


logger = logging.getLogger(__name__)


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

    def _repr_html_(self) -> str:
        return self._table._repr_html_()

    ###
    # Creation methods
    ###

    @staticmethod
    def empty(schema: Schema | None = None) -> Table:
        pyt = _PyTable.empty(None) if schema is None else _PyTable.empty(schema._schema)
        return Table._from_pytable(pyt)

    @staticmethod
    def _from_scan_task(_: _ScanTask) -> Table:
        raise NotImplementedError("_from_scan_task is not implemented for legacy Python Table.")

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
        non_native_fields = [
            field.name
            for field in schema
            if field.dtype == DataType.python()
            or field.dtype._is_tensor_type()
            or field.dtype._is_fixed_shape_tensor_type()
        ]
        if non_native_fields:
            # If there are any contained Arrow types that are not natively supported, go through Table.from_pydict()
            # path.
            logger.debug("Unsupported Arrow types detected for columns: %s", non_native_fields)
            return Table.from_pydict(dict(zip(arrow_table.column_names, arrow_table.columns)))
        else:
            # Otherwise, go through record batch happy path.
            arrow_table = ensure_table(arrow_table)
            pyt = _PyTable.from_arrow_record_batches(arrow_table.to_batches(), schema._schema)
            return Table._from_pytable(pyt)

    @staticmethod
    def from_arrow_record_batches(rbs: list[pa.RecordBatch], arrow_schema: pa.Schema) -> Table:
        schema = Schema._from_field_name_and_types([(f.name, DataType.from_arrow_type(f.type)) for f in arrow_schema])
        pyt = _PyTable.from_arrow_record_batches(rbs, schema._schema)
        return Table._from_pytable(pyt)

    @staticmethod
    def from_pandas(pd_df: pd.DataFrame) -> Table:
        if not _PANDAS_AVAILABLE:
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")
        assert isinstance(pd_df, pd.DataFrame)
        try:
            arrow_table = pa.Table.from_pandas(pd_df)
        except pa.ArrowInvalid:
            pass
        else:
            return Table.from_arrow(arrow_table)
        # Fall back to pydict path.
        df_as_dict = pd_df.to_dict(orient="series")
        return Table.from_pydict(df_as_dict)

    @staticmethod
    def from_pydict(data: dict) -> Table:
        series_dict = dict()
        for k, v in data.items():
            series = item_to_series(k, v)
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

    def slice(self, start: int, end: int) -> Table:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")
        return Table._from_pytable(self._table.slice(start, end))

    ###
    # Exporting methods
    ###

    def to_table(self) -> Table:
        """For compatibility with MicroPartition"""
        return self

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False, convert_large_arrays: bool = False) -> pa.Table:
        python_fields = set()
        tensor_fields = set()
        for field in self.schema():
            if field.dtype._is_python_type():
                python_fields.add(field.name)
            elif field.dtype._is_tensor_type() or field.dtype._is_fixed_shape_tensor_type():
                tensor_fields.add(field.name)
        if python_fields or tensor_fields:
            table = {}
            for colname in self.column_names():
                column_series = self.get_column(colname)
                if colname in python_fields:
                    column = column_series.to_pylist()
                else:
                    column = column_series.to_arrow(cast_tensors_to_ray_tensor_dtype)
                table[colname] = column

            tab = pa.Table.from_pydict(table)
        else:
            tab = pa.Table.from_batches([self._table.to_arrow_record_batch()])

        if not convert_large_arrays:
            return tab

        new_columns = []
        for col in tab.columns:
            new_columns.append(_trim_pyarrow_large_arrays(col))

        return pa.Table.from_arrays(new_columns, names=tab.column_names)

    def to_pydict(self) -> dict[str, list]:
        return {colname: self.get_column(colname).to_pylist() for colname in self.column_names()}

    def to_pylist(self) -> list[dict[str, Any]]:
        # TODO(Clark): Avoid a double-materialization of the table once the Rust-side table supports
        # by-row selection or iteration.
        table = self.to_pydict()
        column_names = self.column_names()
        return [{colname: table[colname][i] for colname in column_names} for i in range(len(self))]

    def to_pandas(
        self,
        schema: Schema | None = None,
        cast_tensors_to_ray_tensor_dtype: bool = False,
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.DataFrame:
        from packaging.version import parse

        if not _PANDAS_AVAILABLE:
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")
        python_fields = set()
        tensor_fields = set()
        for field in self.schema():
            if field.dtype._is_python_type():
                python_fields.add(field.name)
            elif field.dtype._is_tensor_type() or field.dtype._is_fixed_shape_tensor_type():
                tensor_fields.add(field.name)
        if python_fields or tensor_fields:
            # Use Python list representation for Python typed columns.
            table = {}
            for colname in self.column_names():
                column_series = self.get_column(colname)
                if colname in python_fields or (colname in tensor_fields and not cast_tensors_to_ray_tensor_dtype):
                    column = column_series.to_pylist()
                else:
                    # Arrow-native field, so provide column as Arrow array.
                    column_arrow = column_series.to_arrow(cast_tensors_to_ray_tensor_dtype)
                    if parse(pa.__version__) < parse("13.0.0"):
                        column = column_arrow.to_pandas()
                    else:
                        column = column_arrow.to_pandas(coerce_temporal_nanoseconds=coerce_temporal_nanoseconds)
                table[colname] = column

            return pd.DataFrame.from_dict(table)
        else:
            arrow_table = self.to_arrow(cast_tensors_to_ray_tensor_dtype)
            if parse(pa.__version__) < parse("13.0.0"):
                return arrow_table.to_pandas()
            else:
                return arrow_table.to_pandas(coerce_temporal_nanoseconds=coerce_temporal_nanoseconds)

    ###
    # Compute methods (Table -> Table)
    ###

    def cast_to_schema(self, schema: Schema) -> Table:
        """Casts a Table into the provided schema"""
        return Table._from_pytable(self._table.cast_to_schema(schema._schema))

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

    def sample(
        self,
        fraction: float | None = None,
        size: int | None = None,
        with_replacement: bool = False,
        seed: int | None = None,
    ) -> Table:
        if fraction is not None and size is not None:
            raise ValueError("Must specify either `fraction` or `size`, but not both")
        elif fraction is not None:
            return Table._from_pytable(self._table.sample_by_fraction(fraction, with_replacement, seed))
        elif size is not None:
            return Table._from_pytable(self._table.sample_by_size(size, with_replacement, seed))
        else:
            raise ValueError("Must specify either `fraction` or `size`")

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> Table:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return Table._from_pytable(self._table.agg(to_agg_pyexprs, group_by_pyexprs))

    def pivot(
        self, group_by: ExpressionsProjection, pivot_column: Expression, values_column: Expression, names: list[str]
    ) -> Table:
        group_by_pyexprs = [e._expr for e in group_by]
        return Table._from_pytable(self._table.pivot(group_by_pyexprs, pivot_column._expr, values_column._expr, names))

    def quantiles(self, num: int) -> Table:
        return Table._from_pytable(self._table.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> Table:
        """NOTE: Expressions here must be Explode expressions (Expression._explode())"""
        to_explode_pyexprs = [e._expr for e in columns]
        return Table._from_pytable(self._table.explode(to_explode_pyexprs))

    def hash_join(
        self,
        right: Table,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> Table:
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, Table):
            raise TypeError(f"Expected a Table for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return Table._from_pytable(
            self._table.hash_join(right._table, left_on=left_exprs, right_on=right_exprs, how=how)
        )

    def sort_merge_join(
        self,
        right: Table,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
        is_sorted: bool = False,
    ) -> Table:
        if how != JoinType.Inner:
            raise NotImplementedError("TODO: [RUST] Implement Other Join types")
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, Table):
            raise TypeError(f"Expected a Table for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return Table._from_pytable(
            self._table.sort_merge_join(right._table, left_on=left_exprs, right_on=right_exprs, is_sorted=is_sorted)
        )

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

    def partition_by_value(self, partition_keys: ExpressionsProjection) -> tuple[list[Table], Table]:
        exprs = [e._expr for e in partition_keys]
        pytables, values = self._table.partition_by_value(exprs)

        return [Table._from_pytable(t) for t in pytables], Table._from_pytable(values)

    def add_monotonically_increasing_id(self, partition_num: int, column_name: str) -> Table:
        return Table._from_pytable(self._table.add_monotonically_increasing_id(partition_num, column_name))

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
    ) -> Table:
        return Table._from_pytable(
            _read_parquet(
                uri=path,
                columns=columns,
                start_offset=start_offset,
                num_rows=num_rows,
                row_groups=row_groups,
                predicate=predicate._expr if predicate is not None else None,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
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
    ) -> list[Table]:
        pytables = _read_parquet_bulk(
            uris=paths,
            columns=columns,
            start_offset=start_offset,
            num_rows=num_rows,
            row_groups=row_groups_per_path,
            predicate=predicate._expr if predicate is not None else None,
            io_config=io_config,
            num_parallel_tasks=num_parallel_tasks,
            multithreaded_io=multithreaded_io,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
        )
        return [Table._from_pytable(t) for t in pytables]

    @classmethod
    def read_parquet_statistics(
        cls,
        paths: Series | list[str],
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> Table:
        if not isinstance(paths, Series):
            paths = Series.from_pylist(paths, name="uris").cast(DataType.string())
        assert paths.name() == "uris", f"Expected input series to have name 'uris', but found: {paths.name()}"
        return Table._from_pytable(
            _read_parquet_statistics(
                uris=paths._series,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
            )
        )

    @classmethod
    def read_csv(
        cls,
        path: str,
        convert_options: CsvConvertOptions | None = None,
        parse_options: CsvParseOptions | None = None,
        read_options: CsvReadOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> Table:
        return Table._from_pytable(
            _read_csv(
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
        max_chunks_in_flight: int | None = None,
    ) -> Table:
        return Table._from_pytable(
            _read_json(
                uri=path,
                convert_options=convert_options,
                parse_options=parse_options,
                read_options=read_options,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
                max_chunks_in_flight=max_chunks_in_flight,
            )
        )


def _trim_pyarrow_large_arrays(arr: pa.ChunkedArray) -> pa.ChunkedArray:
    if pa.types.is_large_binary(arr.type) or pa.types.is_large_string(arr.type):
        if pa.types.is_large_binary(arr.type):
            target_type = pa.binary()
        else:
            target_type = pa.string()

        all_chunks = []
        for chunk in arr.chunks:
            if len(chunk) == 0:
                continue
            offsets = np.frombuffer(chunk.buffers()[1], dtype=np.int64)
            if offsets[-1] < 2**31:
                all_chunks.append(chunk.cast(target_type))
            else:
                raise ValueError(
                    f"Can not convert {arr.type} into {target_type} due to the offset array being too large: {offsets[-1]}. Maximum: {2**31}"
                )

        return pa.chunked_array(all_chunks, type=target_type)
    else:
        return arr


def read_parquet_into_pyarrow(
    path: str,
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[int] | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    file_timeout_ms: int | None = 900_000,  # 15 minutes
) -> pa.Table:
    fields, metadata, columns, num_rows_read = _read_parquet_into_pyarrow(
        uri=path,
        columns=columns,
        start_offset=start_offset,
        num_rows=num_rows,
        row_groups=row_groups,
        io_config=io_config,
        multithreaded_io=multithreaded_io,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
        file_timeout_ms=file_timeout_ms,
    )
    schema = pa.schema(fields, metadata=metadata)
    columns = [pa.chunked_array(c, type=f.type) for f, c in zip(schema, columns)]  # type: ignore

    if columns:
        return pa.table(columns, schema=schema)
    else:
        # If data contains no columns, we return an empty table with the appropriate size using `Table.drop`
        return pa.table({"dummy_column": pa.array([None] * num_rows_read)}).drop(["dummy_column"])


def read_parquet_into_pyarrow_bulk(
    paths: list[str],
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups_per_path: list[list[int] | None] | None = None,
    io_config: IOConfig | None = None,
    num_parallel_tasks: int | None = 128,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
) -> list[pa.Table]:
    bulk_result = _read_parquet_into_pyarrow_bulk(
        uris=paths,
        columns=columns,
        start_offset=start_offset,
        num_rows=num_rows,
        row_groups=row_groups_per_path,
        io_config=io_config,
        num_parallel_tasks=num_parallel_tasks,
        multithreaded_io=multithreaded_io,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
    )

    tables = []
    for fields, metadata, columns, num_rows_read in bulk_result:
        if columns:
            table = pa.table(
                [pa.chunked_array(c, type=f.type) for f, c in zip(fields, columns)],
                schema=pa.schema(fields, metadata=metadata),
            )
        else:
            # If data contains no columns, we return an empty table with the appropriate size using `Table.drop`
            table = pa.table({"dummy_col": [None] * num_rows_read}).drop(["dummy_col"])
        tables.append(table)
    return tables
