from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Literal, cast

from daft.arrow_utils import ensure_table
from daft.daft import (
    CsvConvertOptions,
    CsvParseOptions,
    CsvReadOptions,
    JoinType,
    JsonConvertOptions,
    JsonParseOptions,
    JsonReadOptions,
    PySeries,
)
from daft.daft import PyRecordBatch as _PyRecordBatch
from daft.daft import ScanTask as _ScanTask
from daft.daft import read_csv as _read_csv
from daft.daft import read_json as _read_json
from daft.daft import read_parquet as _read_parquet
from daft.daft import read_parquet_bulk as _read_parquet_bulk
from daft.daft import read_parquet_into_pyarrow as _read_parquet_into_pyarrow
from daft.daft import read_parquet_into_pyarrow_bulk as _read_parquet_into_pyarrow_bulk
from daft.daft import read_parquet_statistics as _read_parquet_statistics
from daft.datatype import DataType, TimeUnit
from daft.dependencies import pa, pd
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series, item_to_series

if TYPE_CHECKING:
    from collections.abc import Mapping

    from daft.io import IOConfig

logger = logging.getLogger(__name__)


class RecordBatch:
    _recordbatch: _PyRecordBatch

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a RecordBatch via __init__ ")

    def schema(self) -> Schema:
        return Schema._from_pyschema(self._recordbatch.schema())

    def get_column(self, idx: int) -> Series:
        return Series._from_pyseries(self._recordbatch.get_column(idx))

    def columns(self) -> list[Series]:
        return [Series._from_pyseries(s) for s in self._recordbatch.columns()]

    def size_bytes(self) -> int:
        return self._recordbatch.size_bytes()

    def __len__(self) -> int:
        return len(self._recordbatch)

    def __repr__(self) -> str:
        return repr(self._recordbatch)

    def _repr_html_(self) -> str:
        return self._recordbatch._repr_html_()

    ###
    # Creation methods
    ###

    @staticmethod
    def empty(schema: Schema | None = None) -> RecordBatch:
        pyt = _PyRecordBatch.empty(None) if schema is None else _PyRecordBatch.empty(schema._schema)
        return RecordBatch._from_pyrecordbatch(pyt)

    @staticmethod
    def _from_scan_task(_: _ScanTask) -> RecordBatch:
        raise NotImplementedError("_from_scan_task is not implemented for legacy Python RecordBatch.")

    @staticmethod
    def _from_pyrecordbatch(pyt: _PyRecordBatch) -> RecordBatch:
        assert isinstance(pyt, _PyRecordBatch)
        tab = RecordBatch.__new__(RecordBatch)
        tab._recordbatch = pyt
        return tab

    @staticmethod
    def _from_series(series: list[Series]) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(_PyRecordBatch.from_pyseries_list([s._series for s in series]))

    @staticmethod
    def from_arrow_table(arrow_table: pa.Table) -> RecordBatch:
        assert isinstance(arrow_table, pa.Table)
        schema = Schema._from_field_name_and_types(
            [(f.name, DataType.from_arrow_type(f.type)) for f in arrow_table.schema]
        )
        non_native_fields = [
            field.name
            for field in schema
            if field.dtype == DataType.python()
            or field.dtype.is_tensor()
            or field.dtype.is_fixed_shape_tensor()
            or field.dtype.is_sparse_tensor()
            or field.dtype.is_fixed_shape_sparse_tensor()
        ]
        if non_native_fields:
            # If there are any contained Arrow types that are not natively supported, go through Table.from_pydict()
            # path.
            logger.debug("Unsupported Arrow types detected for columns: %s", non_native_fields)
            return RecordBatch.from_pydict(dict(zip(arrow_table.column_names, arrow_table.columns)))
        else:
            # Otherwise, go through record batch happy path.
            arrow_table = ensure_table(arrow_table)
            pyt = _PyRecordBatch.from_arrow_record_batches(arrow_table.to_batches(), schema._schema)
            return RecordBatch._from_pyrecordbatch(pyt)

    @staticmethod
    def from_arrow_record_batches(rbs: list[pa.RecordBatch], arrow_schema: pa.Schema) -> RecordBatch:
        schema = Schema._from_field_name_and_types([(f.name, DataType.from_arrow_type(f.type)) for f in arrow_schema])
        pyt = _PyRecordBatch.from_arrow_record_batches(rbs, schema._schema)
        return RecordBatch._from_pyrecordbatch(pyt)

    @staticmethod
    def from_pandas(pd_df: pd.DataFrame) -> RecordBatch:
        if not pd.module_available():  # type: ignore[attr-defined]
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")
        assert isinstance(pd_df, pd.DataFrame)
        try:
            arrow_table = pa.Table.from_pandas(pd_df)
        except pa.ArrowInvalid:
            pass
        else:
            return RecordBatch.from_arrow_table(arrow_table)
        # Fall back to pydict path.
        df_as_dict = pd_df.to_dict(orient="series")
        if any(not isinstance(k, str) for k in df_as_dict.keys()):
            raise ValueError("pandas.DataFrame column names must be of type `str` to convert to a daft.RecordBatch.")
        df_as_dict_str = cast("dict[str, Any]", df_as_dict)
        return RecordBatch.from_pydict(df_as_dict_str)

    @staticmethod
    def from_pydict(data: Mapping[str, Any]) -> RecordBatch:
        series_dict: dict[str, PySeries] = dict()
        for k, v in data.items():
            series = item_to_series(k, v)
            series_dict[k] = series._series
        return RecordBatch._from_pyrecordbatch(_PyRecordBatch.from_pylist_series(series_dict))

    @classmethod
    def concat(cls, to_merge: list[RecordBatch]) -> RecordBatch:
        tables = []
        for t in to_merge:
            if not isinstance(t, RecordBatch):
                raise TypeError(f"Expected a Table for concat, got {type(t)}")
            tables.append(t._recordbatch)
        return RecordBatch._from_pyrecordbatch(_PyRecordBatch.concat(tables))

    def slice(self, start: int, end: int) -> RecordBatch:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")
        return RecordBatch._from_pyrecordbatch(self._recordbatch.slice(start, end))

    ###
    # Exporting methods
    ###

    def to_table(self) -> RecordBatch:
        """For compatibility with MicroPartition."""
        return self

    def to_arrow_record_batch(self) -> pa.RecordBatch:
        tab = pa.RecordBatch.from_pydict({column.name(): column.to_arrow() for column in self.columns()})
        return tab

    def to_arrow_table(self) -> pa.Table:
        tab = pa.Table.from_pydict({column.name(): column.to_arrow() for column in self.columns()})
        return tab

    def to_pydict(self) -> dict[str, list[Any]]:
        return {column.name(): column.to_pylist() for column in self.columns()}

    def to_pylist(self) -> list[dict[str, Any]]:
        # TODO(Clark): Avoid a double-materialization of the table once the Rust-side table supports
        # by-row selection or iteration.
        table = self.to_pydict()
        column_names = table.keys()
        return [{colname: table[colname][i] for colname in column_names} for i in range(len(self))]

    def to_pandas(
        self,
        schema: Schema | None = None,
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.DataFrame:
        from packaging.version import parse

        if not pd.module_available():  # type: ignore[attr-defined]
            raise ImportError("Unable to import Pandas - please ensure that it is installed.")

        python_fields = set()
        tensor_fields = set()
        for field in self.schema():
            if field.dtype.is_python():
                python_fields.add(field.name)
            elif field.dtype.is_tensor() or field.dtype.is_fixed_shape_tensor():
                tensor_fields.add(field.name)

        if python_fields or tensor_fields:
            table = {}
            for column_series in self.columns():
                colname = column_series.name()
                # Use Python list representation for Python typed columns or tensor columns (return as numpy)
                if colname in python_fields or colname in tensor_fields:
                    column = column_series.to_pylist()
                else:
                    # Arrow-native field, so provide column as Arrow array.
                    column_arrow = column_series.to_arrow()
                    if parse(pa.__version__) < parse("13.0.0"):
                        column = column_arrow.to_pandas()
                    else:
                        column = column_arrow.to_pandas(coerce_temporal_nanoseconds=coerce_temporal_nanoseconds)
                table[colname] = column

            return pd.DataFrame.from_dict(table)
        else:
            arrow_table = self.to_arrow_record_batch()
            if parse(pa.__version__) < parse("13.0.0"):
                return arrow_table.to_pandas()
            else:
                return arrow_table.to_pandas(coerce_temporal_nanoseconds=coerce_temporal_nanoseconds)

    ###
    # Compute methods (Table -> Table)
    ###

    def eval_expression_list(self, exprs: ExpressionsProjection) -> RecordBatch:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return RecordBatch._from_pyrecordbatch(self._recordbatch.eval_expression_list(pyexprs))

    def head(self, num: int) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(self._recordbatch.head(num))

    def take(self, indices: Series) -> RecordBatch:
        assert isinstance(indices, Series)
        return RecordBatch._from_pyrecordbatch(self._recordbatch.take(indices._series))

    def filter(self, exprs: ExpressionsProjection) -> RecordBatch:
        assert all(isinstance(e, Expression) for e in exprs)
        pyexprs = [e._expr for e in exprs]
        return RecordBatch._from_pyrecordbatch(self._recordbatch.filter(pyexprs))

    def sort(
        self,
        sort_keys: ExpressionsProjection,
        descending: bool | list[bool] | None = None,
        nulls_first: bool | list[bool] | None = None,
    ) -> RecordBatch:
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
        if nulls_first is None:
            nulls_first = descending
        elif isinstance(nulls_first, bool):
            nulls_first = [nulls_first for _ in pyexprs]
        elif isinstance(nulls_first, list):
            if len(nulls_first) != len(sort_keys):
                raise ValueError(
                    f"Expected length of `nulls_first` to be the same length as `sort_keys` since a list was passed in,"
                    f"got {len(nulls_first)} instead of {len(sort_keys)}"
                )
            else:
                nulls_first = [bool(x) for x in nulls_first]
        return RecordBatch._from_pyrecordbatch(self._recordbatch.sort(pyexprs, descending, nulls_first))

    def sample(
        self,
        fraction: float | None = None,
        size: int | None = None,
        with_replacement: bool = False,
        seed: int | None = None,
    ) -> RecordBatch:
        if fraction is not None and size is not None:
            raise ValueError("Must specify either `fraction` or `size`, but not both")
        elif fraction is not None:
            return RecordBatch._from_pyrecordbatch(
                self._recordbatch.sample_by_fraction(fraction, with_replacement, seed)
            )
        elif size is not None:
            return RecordBatch._from_pyrecordbatch(self._recordbatch.sample_by_size(size, with_replacement, seed))
        else:
            raise ValueError("Must specify either `fraction` or `size`")

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> RecordBatch:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return RecordBatch._from_pyrecordbatch(self._recordbatch.agg(to_agg_pyexprs, group_by_pyexprs))

    def pivot(
        self,
        group_by: ExpressionsProjection,
        pivot_column: Expression,
        values_column: Expression,
        names: list[str],
    ) -> RecordBatch:
        group_by_pyexprs = [e._expr for e in group_by]
        return RecordBatch._from_pyrecordbatch(
            self._recordbatch.pivot(group_by_pyexprs, pivot_column._expr, values_column._expr, names)
        )

    def quantiles(self, num: int) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(self._recordbatch.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> RecordBatch:
        """NOTE: Expressions here must be Explode expressions."""
        to_explode_pyexprs = [e._expr for e in columns]
        return RecordBatch._from_pyrecordbatch(self._recordbatch.explode(to_explode_pyexprs))

    def hash_join(
        self,
        right: RecordBatch,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> RecordBatch:
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, RecordBatch):
            raise TypeError(f"Expected a Table for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return RecordBatch._from_pyrecordbatch(
            self._recordbatch.hash_join(right._recordbatch, left_on=left_exprs, right_on=right_exprs, how=how)
        )

    def sort_merge_join(
        self,
        right: RecordBatch,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
        is_sorted: bool = False,
    ) -> RecordBatch:
        if how != JoinType.Inner:
            raise NotImplementedError("TODO: [RUST] Implement Other Join types")
        if len(left_on) != len(right_on):
            raise ValueError(
                f"Mismatch of number of join keys, left_on: {len(left_on)}, right_on: {len(right_on)}\nleft_on {left_on}\nright_on {right_on}"
            )

        if not isinstance(right, RecordBatch):
            raise TypeError(f"Expected a Table for `right` in join but got {type(right)}")

        left_exprs = [e._expr for e in left_on]
        right_exprs = [e._expr for e in right_on]

        return RecordBatch._from_pyrecordbatch(
            self._recordbatch.sort_merge_join(
                right._recordbatch,
                left_on=left_exprs,
                right_on=right_exprs,
                is_sorted=is_sorted,
            )
        )

    def partition_by_hash(self, exprs: ExpressionsProjection, num_partitions: int) -> list[RecordBatch]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        pyexprs = [e._expr for e in exprs]
        return [
            RecordBatch._from_pyrecordbatch(t) for t in self._recordbatch.partition_by_hash(pyexprs, num_partitions)
        ]

    def partition_by_range(
        self,
        partition_keys: ExpressionsProjection,
        boundaries: RecordBatch,
        descending: list[bool],
    ) -> list[RecordBatch]:
        if not isinstance(boundaries, RecordBatch):
            raise TypeError(f"Expected a RecordBatch for `boundaries` in partition_by_range but got {type(boundaries)}")

        exprs = [e._expr for e in partition_keys]
        return [
            RecordBatch._from_pyrecordbatch(t)
            for t in self._recordbatch.partition_by_range(exprs, boundaries._recordbatch, descending)
        ]

    def partition_by_random(self, num_partitions: int, seed: int) -> list[RecordBatch]:
        if not isinstance(num_partitions, int):
            raise TypeError(f"Expected a num_partitions to be int, got {type(num_partitions)}")

        if not isinstance(seed, int):
            raise TypeError(f"Expected a seed to be int, got {type(seed)}")

        return [RecordBatch._from_pyrecordbatch(t) for t in self._recordbatch.partition_by_random(num_partitions, seed)]

    def partition_by_value(self, partition_keys: ExpressionsProjection) -> tuple[list[RecordBatch], RecordBatch]:
        exprs = [e._expr for e in partition_keys]
        PyRecordBatchs, values = self._recordbatch.partition_by_value(exprs)

        return [RecordBatch._from_pyrecordbatch(t) for t in PyRecordBatchs], RecordBatch._from_pyrecordbatch(values)

    def add_monotonically_increasing_id(self, partition_num: int, column_name: str) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(
            self._recordbatch.add_monotonically_increasing_id(partition_num, column_name)
        )

    ###
    # Compute methods (Table -> Series)
    ###

    def argsort(
        self,
        sort_keys: ExpressionsProjection,
        descending: bool | list[bool] | None = None,
        nulls_first: bool | list[bool] | None = None,
    ) -> Series:
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
        if nulls_first is None:
            nulls_first = descending
        elif isinstance(nulls_first, bool):
            nulls_first = [nulls_first for _ in pyexprs]
        elif isinstance(nulls_first, list):
            if len(nulls_first) != len(sort_keys):
                raise ValueError(
                    f"Expected length of `nulls_first` to be the same length as `sort_keys` since a list was passed in,"
                    f"got {len(nulls_first)} instead of {len(sort_keys)}"
                )
            else:
                nulls_first = [bool(x) for x in nulls_first]
        else:
            raise TypeError(f"Expected a bool, list[bool] or None for `nulls_first` but got {type(nulls_first)}")
        return Series._from_pyseries(self._recordbatch.argsort(pyexprs, descending, nulls_first))

    def __reduce__(
        self,
    ) -> tuple[Callable[[list[Series]], RecordBatch], tuple[list[Series]]]:
        return RecordBatch._from_series, (self.columns(),)

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
    ) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(
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
    ) -> list[RecordBatch]:
        PyRecordBatchs = _read_parquet_bulk(
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
        return [RecordBatch._from_pyrecordbatch(t) for t in PyRecordBatchs]

    @classmethod
    def read_parquet_statistics(
        cls,
        paths: Series | list[str],
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> RecordBatch:
        if not isinstance(paths, Series):
            paths = Series.from_pylist(paths, name="uris", dtype=DataType.string())
        assert paths.name() == "uris", f"Expected input series to have name 'uris', but found: {paths.name()}"
        return RecordBatch._from_pyrecordbatch(
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
    ) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(
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
    ) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(
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


def read_parquet_into_pyarrow(
    path: str,
    columns: list[str] | None = None,
    start_offset: int | None = None,
    num_rows: int | None = None,
    row_groups: list[int] | None = None,
    io_config: IOConfig | None = None,
    multithreaded_io: bool | None = None,
    coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    string_encoding: Literal["utf-8", "raw"] = "utf-8",
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
        string_encoding=string_encoding,
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
