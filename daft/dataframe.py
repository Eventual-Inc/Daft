from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import pandas
import pyarrow.parquet as papq

from daft.datasources import CSVSourceInfo, InMemorySourceInfo, ParquetSourceInfo
from daft.execution.operators import ExpressionType
from daft.expressions import ColumnExpression, Expression, col
from daft.filesystem import get_filesystem_from_path
from daft.logical import logical_plan
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionSet
from daft.runners.pyrunner import PyRunner
from daft.serving.endpoint import HTTPEndpoint

UDFReturnType = TypeVar("UDFReturnType", covariant=True)

ColumnInputType = Union[Expression, str]

_RUNNER = PyRunner()


class DataFrame:
    def __init__(self, plan: logical_plan.LogicalPlan) -> None:
        self._plan = plan
        self._result: Optional[PartitionSet] = None

    def plan(self) -> logical_plan.LogicalPlan:
        return self._plan

    def schema(self) -> ExpressionList:
        return self._plan.schema()

    def column_names(self) -> List[str]:
        col_names = []
        for expr in self._plan.schema():
            name = expr.name()
            if name is None:
                raise RuntimeError(f"Schema has expression with name None: {expr}")
            col_names.append(name)
        return col_names

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: List[Dict[str, Any]]) -> DataFrame:
        if not data:
            raise ValueError("Unable to create DataFrame from empty list")
        schema = ExpressionList(
            [ColumnExpression(header, assign_id=True, expr_type=ExpressionType.UNKNOWN) for header in data[0]]
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
            [ColumnExpression(header, assign_id=True, expr_type=ExpressionType.UNKNOWN) for header in data]
        )
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=InMemorySourceInfo(data=data),
        )
        return cls(plan)

    @classmethod
    def from_csv(
        cls, path: str, has_headers: bool = True, column_names: Optional[List[str]] = None, delimiter: str = ","
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

        # Read first row to ascertain schema
        schema = None
        if column_names:
            schema = ExpressionList(
                [ColumnExpression(header, assign_id=True, expr_type=ExpressionType.UNKNOWN) for header in column_names]
            )
        else:
            with fs.open(filepaths[0], "r") as f:
                reader = csv.reader(f, delimiter=delimiter)
                for row in reader:
                    schema = (
                        ExpressionList(
                            [
                                ColumnExpression(header, assign_id=True, expr_type=ExpressionType.UNKNOWN)
                                for header in row
                            ]
                        )
                        if has_headers
                        else ExpressionList(
                            [
                                ColumnExpression(f"col_{i}", assign_id=True, expr_type=ExpressionType.UNKNOWN)
                                for i in range(len(row))
                            ]
                        )
                    )
                    break
        assert schema is not None, "Unable to read CSV file to determine schema"

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
                ColumnExpression(field.name, assign_id=True, expr_type=ExpressionType.UNKNOWN)
                for field in papq.ParquetFile(fs.open(filepaths[0])).metadata.schema
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

    def where(self, expr: Expression) -> DataFrame:
        plan = logical_plan.Filter(self._plan, ExpressionList([expr]))
        return DataFrame(plan)

    def with_column(self, column_name: str, expr: Expression) -> DataFrame:
        prev_schema_as_cols = self.schema().to_column_expressions()
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

    def _agg(self, to_agg: List[Tuple[ColumnInputType, str]], group_by: Optional[ExpressionList] = None) -> DataFrame:
        exprs_to_agg = self.__column_input_to_expression(tuple(e for e, _ in to_agg))
        ops = [op for _, op in to_agg]

        lagg_op = logical_plan.LocalAggregate(
            self._plan, agg=[(e, op) for e, op in zip(exprs_to_agg, ops)], group_by=group_by
        )
        repart_op: logical_plan.LogicalPlan
        if group_by is None:
            repart_op = logical_plan.Coalesce(lagg_op, 1)
        else:
            repart_op = logical_plan.Repartition(
                self._plan,
                num_partitions=self._plan.num_partitions(),
                partition_by=group_by,
                scheme=logical_plan.PartitionScheme.HASH,
            )

        gagg_op = logical_plan.LocalAggregate(repart_op, agg=lagg_op._agg, group_by=group_by)
        return DataFrame(gagg_op)

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
        arrow_table = self._result.to_arrow_table()
        return arrow_table.to_pandas()


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
