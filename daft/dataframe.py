from __future__ import annotations

import csv
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import pandas

from daft.expressions import Expression, col
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
        return [expr.name() for expr in self._plan.schema()]

    ###
    # Creation methods
    ###

    @classmethod
    def from_pylist(cls, data: List[Dict[str, Any]]) -> DataFrame:
        if not data:
            raise ValueError("Unable to create DataFrame from empty list")
        schema = ExpressionList([col(header) for header in data[0]])
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=logical_plan.Scan.SourceInfo(scan_type=logical_plan.Scan.ScanType.IN_MEMORY, source=data),
        )
        return cls(plan)

    @classmethod
    def from_pydict(cls, data: Dict[str, Any]) -> DataFrame:
        schema = ExpressionList([col(header) for header in data])
        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=logical_plan.Scan.SourceInfo(scan_type=logical_plan.Scan.ScanType.IN_MEMORY, source=data),
        )
        return cls(plan)

    @classmethod
    def from_csv(cls, path: str, headers: bool = True, delimiter: str = ",") -> DataFrame:
        fs = get_filesystem_from_path(path)

        # Read first row to ascertain schema
        schema = None
        with fs.open(path, "r") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                schema = (
                    ExpressionList([col(header) for header in row])
                    if headers
                    else ExpressionList([col(f"col_{i}") for i in range(len(row))])
                )
                break
        assert schema is not None, "Unable to read CSV file to determine schema"

        plan = logical_plan.Scan(
            schema=schema,
            predicate=None,
            columns=None,
            source_info=logical_plan.Scan.SourceInfo(scan_type=logical_plan.Scan.ScanType.CSV, source=path),
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

    def repartition(self, num: int, partition_by: Optional[ColumnInputType] = None) -> DataFrame:
        if partition_by is None:
            scheme = logical_plan.PartitionScheme.RANDOM
            exprs: ExpressionList = ExpressionList([])
        else:
            raise NotImplementedError()
            scheme = logical_plan.PartitionScheme.HASH
            exprs = self.__column_input_to_expression((partition_by,))

        repartition_op = logical_plan.Repartition(self._plan, num_partitions=num, partition_by=exprs, scheme=scheme)
        return DataFrame(repartition_op)

    def collect(self) -> DataFrame:
        if self._result is None:
            self._result = _RUNNER.run(self._plan)
        return self

    def to_pandas(self) -> pandas.DataFrame:
        self.collect()
        assert self._result is not None
        arrow_table = self._result.to_arrow_table()
        return arrow_table.to_pandas()
