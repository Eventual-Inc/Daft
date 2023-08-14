from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from loguru import logger

from daft.arrow_utils import ensure_table
from daft.daft import JoinType
from daft.daft import PyTable as _PyTable
from daft.daft import read_parquet as _read_parquet
from daft.daft import read_parquet_bulk as _read_parquet_bulk
from daft.daft import read_parquet_statistics as _read_parquet_statistics
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.series import Series

if sys.version_info < (3, 8):
    pass
else:
    pass

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
            logger.debug(f"Unsupported Arrow types detected for columns: {non_native_fields}")
            return Table.from_pydict(dict(zip(arrow_table.column_names, arrow_table.columns)))
        else:
            # Otherwise, go through record batch happy path.
            arrow_table = ensure_table(arrow_table)
            pyt = _PyTable.from_arrow_record_batches(arrow_table.to_batches(), schema._schema)
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
            if isinstance(v, list):
                series = Series.from_pylist(v, name=k)
            elif _NUMPY_AVAILABLE and isinstance(v, np.ndarray):
                series = Series.from_numpy(v, name=k)
            elif isinstance(v, Series):
                series = v
            elif isinstance(v, (pa.Array, pa.ChunkedArray)):
                series = Series.from_arrow(v, name=k)
            elif _PANDAS_AVAILABLE and isinstance(v, pd.Series):
                series = Series.from_pandas(v, name=k)
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

    def slice(self, start: int, end: int) -> Table:
        if not isinstance(start, int):
            raise TypeError(f"expected int for start but got {type(start)}")
        if not isinstance(end, int):
            raise TypeError(f"expected int for end but got {type(end)}")
        return Table._from_pytable(self._table.slice(start, end))

    ###
    # Exporting methods
    ###

    def to_arrow(self, cast_tensors_to_ray_tensor_dtype: bool = False) -> pa.Table:
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

    def to_pandas(self, schema: Schema | None = None, cast_tensors_to_ray_tensor_dtype: bool = False) -> pd.DataFrame:
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
                    column = column_series.to_arrow(cast_tensors_to_ray_tensor_dtype).to_pandas()
                table[colname] = column

            return pd.DataFrame.from_dict(table)
        else:
            return self.to_arrow(cast_tensors_to_ray_tensor_dtype).to_pandas()

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

    def sample(self, num: int) -> Table:
        return Table._from_pytable(self._table.sample(num))

    def agg(self, to_agg: list[Expression], group_by: ExpressionsProjection | None = None) -> Table:
        to_agg_pyexprs = [e._expr for e in to_agg]
        group_by_pyexprs = [e._expr for e in group_by] if group_by is not None else []
        return Table._from_pytable(self._table.agg(to_agg_pyexprs, group_by_pyexprs))

    def quantiles(self, num: int) -> Table:
        return Table._from_pytable(self._table.quantiles(num))

    def explode(self, columns: ExpressionsProjection) -> Table:
        """NOTE: Expressions here must be Explode expressions (Expression._explode())"""
        to_explode_pyexprs = [e._expr for e in columns]
        return Table._from_pytable(self._table.explode(to_explode_pyexprs))

    def join(
        self,
        right: Table,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        output_projection: ExpressionsProjection | None = None,
        how: JoinType = JoinType.Inner,
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

    @classmethod
    def read_parquet(
        cls,
        path: str,
        columns: list[str] | None = None,
        start_offset: int | None = None,
        num_rows: int | None = None,
        io_config: IOConfig | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> Table:
        return Table._from_pytable(
            _read_parquet(
                uri=path,
                columns=columns,
                start_offset=start_offset,
                num_rows=num_rows,
                io_config=io_config,
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
        io_config: IOConfig | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> list[Table]:
        pytables = _read_parquet_bulk(
            uris=paths,
            columns=columns,
            start_offset=start_offset,
            num_rows=num_rows,
            io_config=io_config,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
        )
        return [Table._from_pytable(t) for t in pytables]

    @classmethod
    def read_parquet_statistics(
        cls,
        paths: Series | list[str],
        io_config: IOConfig | None = None,
    ) -> Table:
        if not isinstance(paths, Series):
            paths = Series.from_pylist(paths, name="uris")
        assert paths.name() == "uris", f"Expected input series to have name 'uris', but found: {paths.name()}"
        return Table._from_pytable(_read_parquet_statistics(uris=paths._series, io_config=io_config))
