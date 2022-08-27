from __future__ import annotations

import io
import os
import uuid
from dataclasses import dataclass
from functools import partial
from typing import IO, Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

import pandas
import pyarrow as pa
import pyarrow.parquet as papq
from fsspec import AbstractFileSystem
from loguru import logger
from pyarrow import csv, json
from tabulate import tabulate

from daft.datasources import (
    CSVSourceInfo,
    InMemorySourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
)
from daft.execution.operators import ExpressionType
from daft.expressions import ColumnExpression, Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionSet
from daft.runners.pyrunner import PyRunner
from daft.runners.ray_runner import RayRunner
from daft.runners.runner import Runner
from daft.serving.endpoint import HTTPEndpoint

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]

_RUNNER: Runner
if os.environ.get("DAFT_RUNNER", "").lower() == "ray":
    logger.info("Using RayRunner")
    _RUNNER = RayRunner()
else:
    logger.info("Using PyRunner")
    _RUNNER = PyRunner()


@dataclass(frozen=True)
class DataFrameSchemaField:
    name: str
    daft_type: ExpressionType


def _sample_with_pyarrow(
    loader_func: Callable[[IO], pa.Table],
    filepath: str,
    fs: AbstractFileSystem,
    max_bytes: int = 5 * 1024**2,
) -> ExpressionList:
    sampled_bytes = io.BytesIO()
    with fs.open(filepath, compression="infer") as f:
        lines = f.readlines(max_bytes)
        for line in lines:
            sampled_bytes.write(line)
    sampled_bytes.seek(0)
    sampled_tbl = loader_func(sampled_bytes)
    fields = [(field.name, field.type) for field in sampled_tbl.schema]
    schema = ExpressionList(
        [ColumnExpression(name, expr_type=ExpressionType.from_arrow_type(type_)) for name, type_ in fields]
    )
    assert schema is not None, f"Unable to read file {filepath} to determine schema"
    return schema


class DataFrameSchema:
    def __init__(self, fields: List[DataFrameSchemaField]):
        self._fields = {f.name: f for f in fields}

    def __getitem__(self, key: str) -> DataFrameSchemaField:
        return self._fields[key]

    def __len__(self) -> int:
        return len(self._fields)

    def column_names(self) -> List[str]:
        return list(self._fields.keys())

    @classmethod
    def from_expression_list(cls, exprs: ExpressionList) -> DataFrameSchema:
        fields = []
        for e in exprs:
            if e.resolved_type() is None:
                raise ValueError(f"Unable to parse schema from expression without type: {e}")
            if e.name() is None:
                raise ValueError(f"Unable to parse schema from expression without name: {e}")
            fields.append(DataFrameSchemaField(e.name(), e.resolved_type()))
        return cls(fields)

    def _repr_html_(self) -> str:
        rows = ["<tr><th>Name</th><th>Type</th></tr>"]
        rows += [
            f"<tr><td>{self._fields[field_name].name}</td><td>{self._fields[field_name].daft_type}</td></tr>"
            for field_name in self._fields
        ]
        nl = "\n"
        return f"""
            <table>
                {nl.join(rows)}
            </table>
        """

    def __repr__(self) -> str:
        fields = list(self._fields.values())
        return tabulate([[field.name for field in fields], [field.daft_type for field in fields]])


class DataFrame:
    def __init__(self, plan: logical_plan.LogicalPlan) -> None:
        self._plan = plan
        self._result: Optional[PartitionSet] = None

    def plan(self) -> logical_plan.LogicalPlan:
        return self._plan

    def schema(self) -> DataFrameSchema:
        return DataFrameSchema.from_expression_list(self._plan.schema())

    def column_names(self) -> List[str]:
        return [expr.name() for expr in self._plan.schema()]

    def __repr__(self) -> str:
        return f"DataFrame({self._plan.partition_spec()})\n{self.schema()}"

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: List[Dict[str, Any]]) -> DataFrame:
        if not data:
            raise ValueError("Unable to create DataFrame from empty list")
        schema = ExpressionList(
            [
                ColumnExpression(header, expr_type=ExpressionType.from_py_type(type(data[0][header])))
                for header in data[0]
            ]
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=InMemorySourceInfo(data={header: [row[header] for row in data] for header in data[0]}),
        )
        return cls(plan)

    @classmethod
    def from_pydict(cls, data: Dict[str, Any]) -> DataFrame:
        schema = ExpressionList(
            [ColumnExpression(header, expr_type=ExpressionType.from_py_type(type(data[header][0]))) for header in data]
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=InMemorySourceInfo(data=data),
        )
        return cls(plan)

    @classmethod
    def from_json(
        cls,
        path: str,
    ) -> DataFrame:
        """Creates a DataFrame from line-delimited JSON file(s)

        Args:
            path (str): Path to CSV or to a folder containing CSV files

        returns:
            DataFrame: parsed DataFrame
        """
        fs = get_filesystem_from_path(path)
        filepaths = [path] if fs.isfile(path) else fs.ls(path)

        if len(filepaths) == 0:
            raise ValueError(f"No JSON files found at {path}")

        schema = _sample_with_pyarrow(json.read_json, filepaths[0], fs)

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=JSONSourceInfo(filepaths=filepaths),
        )
        return cls(plan)

    @classmethod
    def from_csv(
        cls,
        path: str,
        has_headers: bool = True,
        column_names: Optional[List[str]] = None,
        delimiter: str = ",",
    ) -> DataFrame:
        """Creates a DataFrame from CSV file(s)

        Args:
            path (str): Path to CSV or to a folder containing CSV files
            has_headers (bool): Whether the CSV has a header or not, defaults to True
            column_names (Optional[List[str]]): Custom column names to assign to the DataFrame, defaults to None
            delimiter (Str): Delimiter used in the CSV, defaults to ","

        returns:
            DataFrame: parsed DataFrame
        """
        fs = get_filesystem_from_path(path)
        filepaths = [path] if fs.isfile(path) else fs.ls(path)

        if len(filepaths) == 0:
            raise ValueError(f"No CSV files found at {path}")

        schema = _sample_with_pyarrow(
            partial(
                csv.read_csv,
                parse_options=csv.ParseOptions(
                    delimiter=delimiter,
                ),
                read_options=csv.ReadOptions(
                    # Column names will be read from the first CSV row if column_names is None/empty and has_headers
                    autogenerate_column_names=(not has_headers) and (column_names is None),
                    column_names=column_names,
                    # If user specifies that CSV has headers, and also provides column names, we skip the header row
                    skip_rows_after_names=1 if has_headers and column_names is not None else 0,
                ),
            ),
            filepaths[0],
            fs,
        )

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=CSVSourceInfo(
                filepaths=filepaths,
                delimiter=delimiter,
                has_headers=has_headers,
            ),
        )
        return cls(plan)

    @classmethod
    def from_parquet(cls, path: str) -> DataFrame:
        """Creates a DataFrame from Parquet file(s)

        Args:
            path (str): Path to Parquet file or to a folder containing Parquet files

        returns:
            DataFrame: parsed DataFrame
        """
        fs = get_filesystem_from_path(path)
        filepaths = [path] if fs.isfile(path) else fs.ls(path)

        if len(filepaths) == 0:
            raise ValueError(f"No Parquet files found at {path}")

        # Read first Parquet file to ascertain schema
        schema = ExpressionList(
            [
                ColumnExpression(field.name, expr_type=ExpressionType.from_arrow_type(field.type))
                for field in papq.ParquetFile(fs.open(filepaths[0])).metadata.schema.to_arrow_schema()
            ]
        )

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=ParquetSourceInfo(
                filepaths=filepaths,
            ),
        )
        return cls(plan)

    @classmethod
    def from_endpoint(cls, endpoint: HTTPEndpoint) -> DataFrame:
        plan = logical_plan.HTTPRequest(schema=endpoint._request_schema)
        return cls(plan)

    ###
    # DataFrame write operations
    ###

    def write_endpoint(self, endpoint: HTTPEndpoint) -> None:
        endpoint._set_plan(self.plan())

    ###
    # DataFrame operations
    ###

    def __column_input_to_expression(self, columns: Tuple[ColumnInputType, ...]) -> ExpressionList:
        expressions = [col(c) if isinstance(c, str) else c for c in columns]
        return ExpressionList(expressions)

    def select(self, *columns: ColumnInputType) -> DataFrame:
        assert len(columns) > 0
        projection = logical_plan.Projection(self._plan, self.__column_input_to_expression(columns))
        return DataFrame(projection)

    def distinct(self) -> DataFrame:
        all_exprs = self._plan.schema()
        gb = self.groupby(*[col(e.name()) for e in all_exprs])
        first_e_name = [e.name() for e in all_exprs][0]
        dummy_col_name = str(uuid.uuid4())
        return gb.agg([(col(first_e_name).alias(dummy_col_name), "min")]).exclude(dummy_col_name)

    def exclude(self, *names: str) -> DataFrame:
        names_to_skip = set(names)
        el = ExpressionList([e for e in self._plan.schema() if e.name() not in names_to_skip])
        return DataFrame(logical_plan.Projection(self._plan, el))

    def where(self, expr: Expression) -> DataFrame:
        plan = logical_plan.Filter(self._plan, ExpressionList([expr]))
        return DataFrame(plan)

    def with_column(self, column_name: str, expr: Expression) -> DataFrame:
        prev_schema_as_cols = self._plan.schema().to_column_expressions()
        projection = logical_plan.Projection(
            self._plan, prev_schema_as_cols.union(ExpressionList([expr.alias(column_name)]))
        )
        return DataFrame(projection)

    def sort(self, column: ColumnInputType, desc: bool = False) -> DataFrame:
        sort = logical_plan.Sort(self._plan, self.__column_input_to_expression((column,)), desc=desc)
        return DataFrame(sort)

    def limit(self, num: int) -> DataFrame:
        local_limit = logical_plan.LocalLimit(self._plan, num=num)
        global_limit = logical_plan.GlobalLimit(local_limit, num=num)
        return DataFrame(global_limit)

    def repartition(self, num: int, *partition_by: ColumnInputType) -> DataFrame:
        if len(partition_by) == 0:
            scheme = logical_plan.PartitionScheme.RANDOM
            exprs: ExpressionList = ExpressionList([])
        else:
            assert len(partition_by) == 1
            scheme = logical_plan.PartitionScheme.HASH
            exprs = self.__column_input_to_expression(partition_by)

        repartition_op = logical_plan.Repartition(self._plan, num_partitions=num, partition_by=exprs, scheme=scheme)
        return DataFrame(repartition_op)

    def join(
        self,
        other: DataFrame,
        on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        left_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        right_on: Optional[Union[List[ColumnInputType], ColumnInputType]] = None,
        how: str = "inner",
    ) -> DataFrame:
        if on is None:
            if left_on is None or right_on is None:
                raise ValueError("If `on` is None then both `left_on` and `right_on` must not be None")
        else:
            if left_on is not None or right_on is not None:
                raise ValueError("If `on` is not None then both `left_on` and `right_on` must be None")
            left_on = on
            right_on = on
        assert how == "inner", "only inner joins are currently supported"

        left_exprs = self.__column_input_to_expression(tuple(left_on) if isinstance(left_on, list) else (left_on,))
        right_exprs = self.__column_input_to_expression(tuple(right_on) if isinstance(right_on, list) else (right_on,))
        join_op = logical_plan.Join(
            self._plan, other._plan, left_on=left_exprs, right_on=right_exprs, how=logical_plan.JoinType.INNER
        )
        return DataFrame(join_op)

    def _agg(self, to_agg: List[Tuple[ColumnInputType, str]], group_by: Optional[ExpressionList] = None) -> DataFrame:
        exprs_to_agg = self.__column_input_to_expression(tuple(e for e, _ in to_agg))
        ops = [op for _, op in to_agg]

        function_lookup = {
            "sum": Expression._sum,
            "count": Expression._count,
            "min": Expression._min,
            "max": Expression._max,
            "count": Expression._count,
        }
        intermediate_ops = {
            "sum": ("sum",),
            "count": ("count",),
            "mean": ("sum", "count"),
            "min": ("min",),
            "max": ("max",),
        }

        reduction_ops = {"sum": ("sum",), "count": ("sum",), "mean": ("sum", "sum"), "min": ("min",), "max": ("max",)}

        finalizer_ops_funcs = {"mean": lambda x, y: (x + 0.0) / (y + 0.0)}

        first_phase_ops: List[Tuple[Expression, str]] = []
        second_phase_ops: List[Tuple[Expression, str]] = []
        finalizer_phase_ops: List[Expression] = []
        need_final_projection = False
        for e, op in zip(exprs_to_agg, ops):
            assert op in intermediate_ops
            ops_to_add = intermediate_ops[op]

            e_intermediate_name = []
            for agg_op in ops_to_add:
                name = f"{e.name()}_{agg_op}"
                f = function_lookup[agg_op]
                new_e = f(e).alias(name)
                first_phase_ops.append((new_e, agg_op))
                e_intermediate_name.append(new_e.name())

            assert op in reduction_ops
            ops_to_add = reduction_ops[op]
            added_exprs = []
            for agg_op, result_name in zip(ops_to_add, e_intermediate_name):
                assert result_name is not None
                col_e = col(result_name)
                f = function_lookup[agg_op]
                added: Expression = f(col_e)
                if op in finalizer_ops_funcs:
                    name = f"{result_name}_{agg_op}"
                    added = added.alias(name)
                else:
                    added = added.alias(e.name())
                second_phase_ops.append((added, agg_op))
                added_exprs.append(added)

            if op in finalizer_ops_funcs:
                f = finalizer_ops_funcs[op]
                operand_args = []
                for ae in added_exprs:
                    col_name = ae.name()
                    assert col_name is not None
                    operand_args.append(col(col_name))
                final_name = e.name()
                assert final_name is not None
                new_e = f(*operand_args).alias(final_name)
                finalizer_phase_ops.append(new_e)
                need_final_projection = True
            else:
                for ae in added_exprs:
                    col_name = ae.name()
                    assert col_name is not None
                    finalizer_phase_ops.append(col(col_name))

        first_phase_lagg_op = logical_plan.LocalAggregate(self._plan, agg=first_phase_ops, group_by=group_by)
        repart_op: logical_plan.LogicalPlan
        if group_by is None:
            repart_op = logical_plan.Coalesce(first_phase_lagg_op, 1)
        else:
            repart_op = logical_plan.Repartition(
                first_phase_lagg_op,
                num_partitions=self._plan.num_partitions(),
                partition_by=group_by,
                scheme=logical_plan.PartitionScheme.HASH,
            )

        gagg_op = logical_plan.LocalAggregate(repart_op, agg=second_phase_ops, group_by=group_by)

        final_schema = ExpressionList(finalizer_phase_ops)

        if group_by is not None:
            final_schema = group_by.union(final_schema)

        final_op: logical_plan.LogicalPlan
        if need_final_projection:
            final_op = logical_plan.Projection(gagg_op, final_schema)
        else:
            final_op = gagg_op

        return DataFrame(final_op)

    def sum(self, *cols: ColumnInputType) -> DataFrame:
        return self._agg([(c, "sum") for c in cols])

    def mean(self, *cols: ColumnInputType) -> DataFrame:
        return self._agg([(c, "mean") for c in cols])

    def groupby(self, *group_by: ColumnInputType) -> GroupedDataFrame:
        return GroupedDataFrame(self, self.__column_input_to_expression(group_by))

    def collect(self) -> DataFrame:
        if self._result is None:
            self._result = _RUNNER.run(self._plan)
        return self

    def to_pandas(self) -> pandas.DataFrame:
        self.collect()
        assert self._result is not None
        pd_df = self._result.to_pandas(schema=self._plan.schema())
        del self._result
        self._result = None
        return pd_df


@dataclass
class GroupedDataFrame:
    df: DataFrame
    group_by: ExpressionList

    def sum(self, *cols: ColumnInputType) -> DataFrame:
        return self.df._agg([(c, "sum") for c in cols], group_by=self.group_by)

    def mean(self, *cols: ColumnInputType) -> DataFrame:
        return self.df._agg([(c, "mean") for c in cols], group_by=self.group_by)

    def agg(self, to_agg: List[Tuple[ColumnInputType, str]]) -> DataFrame:
        return self.df._agg(to_agg, group_by=self.group_by)
