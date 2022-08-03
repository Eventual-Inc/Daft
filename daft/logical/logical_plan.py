from __future__ import annotations

import itertools
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, List, Optional

from daft.expressions import ColumnExpression
from daft.internal.treenode import TreeNode
from daft.logical.schema import ExpressionList


class OpLevel(IntEnum):
    ROW = 1
    PARTITION = 2
    GLOBAL = 3


class LogicalPlan(TreeNode["LogicalPlan"]):
    id_iter = itertools.count()

    def __init__(self, schema: ExpressionList, num_partitions: int, op_level: OpLevel) -> None:
        super().__init__()
        self._schema = schema
        self._op_level = op_level
        self._num_partitions = num_partitions
        self._id = next(LogicalPlan.id_iter)

    def schema(self) -> ExpressionList:
        return self._schema

    @abstractmethod
    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    @abstractmethod
    def _local_eq(self, other: Any) -> bool:
        raise NotImplementedError()

    def is_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LogicalPlan)
            and self._local_eq(other)
            and self.schema() == other.schema()
            and self.num_partitions() == other.num_partitions()
            and all(
                [self_child.is_eq(other_child) for self_child, other_child in zip(self._children(), other._children())]
            )
        )

    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError(
            "The == operation is not implemented. "
            "Use .is_eq() to check if expressions are 'equal' (ignores differences in IDs but checks for the same expression structure)"
        )

    def num_partitions(self) -> int:
        return self._num_partitions

    def id(self) -> int:
        return self._id

    def op_level(self) -> OpLevel:
        return self._op_level


class UnaryNode(LogicalPlan):
    @abstractmethod
    def copy_with_new_input(self, new_input: UnaryNode) -> UnaryNode:
        raise NotImplementedError()


class Scan(LogicalPlan):
    class ScanType(Enum):
        CSV = "CSV"
        PARQUET = "PARQUET"
        IN_MEMORY = "IN_MEMORY"

    @dataclass(frozen=True)
    class SourceInfo:
        scan_type: Scan.ScanType
        source: Optional[Any] = None

    def __init__(
        self,
        *,
        schema: ExpressionList,
        source_info: Scan.SourceInfo,
        predicate: Optional[ExpressionList] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        schema = schema.resolve()
        super().__init__(schema, num_partitions=1, op_level=OpLevel.PARTITION)

        if predicate is not None:
            self._predicate = predicate.resolve(schema)
        else:
            self._predicate = ExpressionList([])

        if columns is not None:
            new_schema = ExpressionList([ColumnExpression(c) for c in columns])
            self._output_schema = new_schema.resolve(schema)
        else:
            self._output_schema = schema

        self._columns = self._schema
        self._source_info = source_info

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"Scan\n\toutput={self.schema()}\n\tpredicate={self._predicate}\n\tcolumns={self._columns}\n\t{self._source_info}"

    def required_columns(self) -> ExpressionList:
        return self._predicate.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Scan)
            and self.schema() == other.schema()
            and self._predicate == other._predicate
            and self._columns == other._columns
            and self._source_info == other._source_info
        )


class Filter(UnaryNode):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: ExpressionList) -> None:
        super().__init__(
            input.schema().to_column_expressions(), num_partitions=input.num_partitions(), op_level=OpLevel.PARTITION
        )
        self._register_child(input)
        self._predicate = predicate.resolve(input.schema())

    def __repr__(self) -> str:
        return f"Filter\n\toutput={self.schema()}\n\tpredicate={self._predicate}"

    def required_columns(self) -> ExpressionList:
        return self._predicate.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, Filter) and self.schema() == other.schema() and self._predicate == other._predicate

    def copy_with_new_input(self, new_input: LogicalPlan) -> Filter:
        raise NotImplementedError()


class Projection(UnaryNode):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, projection: ExpressionList) -> None:
        projection = projection.resolve(input_schema=input.schema())
        super().__init__(projection, num_partitions=input.num_partitions(), op_level=OpLevel.ROW)

        self._register_child(input)
        self._projection = projection

    def __repr__(self) -> str:
        return f"Projection\n\toutput={self.schema()}"

    def required_columns(self) -> ExpressionList:
        return self._projection.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Projection) and self.schema() == other.schema() and self._projection == other._projection
        )

    def copy_with_new_input(self, new_input: LogicalPlan) -> Projection:
        raise NotImplementedError()


class Sort(UnaryNode):
    def __init__(self, input: LogicalPlan, sort_by: ExpressionList, desc: bool = False) -> None:
        super().__init__(
            input.schema().to_column_expressions(), num_partitions=input.num_partitions(), op_level=OpLevel.GLOBAL
        )
        self._register_child(input)
        assert len(sort_by.exprs) == 1, "we can only sort with 1 expression"
        self._sort_by = sort_by.resolve(input_schema=input.schema())
        self._desc = desc

    def __repr__(self) -> str:
        return f"Sort\n\toutput={self.schema()}\n\tsort_by={self._sort_by}\n\tdesc={self._desc}"

    def copy_with_new_input(self, new_input: LogicalPlan) -> Sort:
        return Sort(new_input, sort_by=self._sort_by, desc=self._desc)

    def required_columns(self) -> ExpressionList:
        return self._sort_by.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Sort)
            and self.schema() == other.schema()
            and self._sort_by == self._sort_by
            and self._desc == self._desc
        )


class LocalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), num_partitions=input.num_partitions(), op_level=OpLevel.PARTITION)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return f"LocalLimit\n\toutput={self.schema()}\n\tN={self._num}"

    def copy_with_new_input(self, new_input: LogicalPlan) -> LocalLimit:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        return ExpressionList([])

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, LocalLimit) and self.schema() == other.schema() and self._num == self._num


class GlobalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), num_partitions=input.num_partitions(), op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return f"GlobalLimit\n\toutput={self.schema()}\n\tN={self._num}"

    def copy_with_new_input(self, new_input: LogicalPlan) -> GlobalLimit:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        return ExpressionList([])

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, GlobalLimit) and self.schema() == other.schema() and self._num == self._num


class PartitionScheme(Enum):
    RANGE = "RANGE"
    HASH = "HASH"
    ROUND_ROBIN = "ROUND_ROBIN"


class Repartition(UnaryNode):
    def __init__(
        self, input: LogicalPlan, partition_by: ExpressionList, num_partitions: int, scheme: PartitionScheme
    ) -> None:
        super().__init__(input.schema().to_column_expressions(), num_partitions=num_partitions, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._partition_by = partition_by
        self._scheme = scheme
        if scheme == PartitionScheme.ROUND_ROBIN and len(partition_by.names) > 0:
            raise ValueError("Can not pass in round robin partitioning and partition_by args")

    def __repr__(self) -> str:
        return (
            f"Repartition\n\toutput={self.schema()}\n\tpartition_by={self._partition_by}"
            f"\n\tnum_partitions={self.num_partitions()}\n\tscheme={self._scheme}"
        )

    def copy_with_new_input(self, new_input: LogicalPlan) -> Repartition:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        return ExpressionList([])

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Repartition)
            and self.schema() == other.schema()
            and self._partition_by == other._partition_by
            and self._scheme == other._scheme
        )


class HTTPRequest(LogicalPlan):
    def __init__(
        self,
        schema: ExpressionList,
    ) -> None:
        self._output_schema = schema.resolve()
        super().__init__(schema, num_partitions=1, op_level=OpLevel.ROW)

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"HTTPRequest\n\toutput={self.schema()}"

    def required_columns(self) -> ExpressionList:
        return ExpressionList([])

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPRequest) and self.schema() == other.schema()


class HTTPResponse(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
    ) -> None:
        self._schema = input.schema()
        super().__init__(self._schema, num_partitions=input.num_partitions(), op_level=OpLevel.ROW)

    def schema(self) -> ExpressionList:
        return self._schema

    def __repr__(self) -> str:
        return f"HTTPResponse\n\toutput={self.schema()}"

    def required_columns(self) -> ExpressionList:
        return ExpressionList([])

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPResponse) and self.schema() == other.schema()

    def copy_with_new_input(self, new_input: LogicalPlan) -> HTTPResponse:
        raise NotImplementedError()
