from __future__ import annotations

import itertools
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, List, Optional, Tuple

from daft.datasources import SourceInfo
from daft.execution.operators import ExpressionType
from daft.expressions import ColumnExpression, Expression
from daft.internal.treenode import TreeNode
from daft.logical.schema import ExpressionList
from daft.resource_request import ResourceRequest


class OpLevel(IntEnum):
    ROW = 1
    PARTITION = 2
    GLOBAL = 3


class LogicalPlan(TreeNode["LogicalPlan"]):
    id_iter = itertools.count()

    def __init__(self, schema: ExpressionList, partition_spec: PartitionSpec, op_level: OpLevel) -> None:
        super().__init__()
        self._schema = schema
        self._op_level = op_level
        self._partition_spec = partition_spec
        self._id = next(LogicalPlan.id_iter)

    def schema(self) -> ExpressionList:
        return self._schema

    @abstractmethod
    def resource_request(self) -> ResourceRequest:
        """Resources required to execute this LogicalPlan"""
        raise NotImplementedError()

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
        return self._partition_spec.num_partitions

    def partition_spec(self) -> PartitionSpec:
        return self._partition_spec

    def id(self) -> int:
        return self._id

    def op_level(self) -> OpLevel:
        return self._op_level

    def is_disjoint(self, other: LogicalPlan) -> bool:
        self_node_ids = set(map(LogicalPlan.id, self.post_order()))
        other_node_ids = set(map(LogicalPlan.id, other.post_order()))
        return self_node_ids.isdisjoint(other_node_ids)

    @abstractmethod
    def rebuild(self) -> LogicalPlan:
        raise NotImplementedError()


class UnaryNode(LogicalPlan):
    @abstractmethod
    def copy_with_new_input(self, new_input: UnaryNode) -> UnaryNode:
        raise NotImplementedError()


class BinaryNode(LogicalPlan):
    ...


class Scan(LogicalPlan):
    def __init__(
        self,
        *,
        schema: ExpressionList,
        source_info: SourceInfo,
        predicate: Optional[ExpressionList] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        schema = schema.resolve()
        pspec = PartitionSpec(scheme=PartitionScheme.UNKNOWN, num_partitions=source_info.get_num_partitions())
        super().__init__(schema, partition_spec=pspec, op_level=OpLevel.PARTITION)

        if predicate is not None:
            self._predicate = predicate.resolve(schema)
        else:
            self._predicate = ExpressionList([])

        if columns is not None:
            new_schema = ExpressionList([ColumnExpression(c) for c in columns])
            self._output_schema = new_schema.resolve(schema)
        else:
            self._output_schema = schema
        self._column_names = columns
        self._columns = self._schema
        self._source_info = source_info

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"Scan\n\toutput={self.schema()}\n\tpredicate={self._predicate}\n\tcolumns={self._columns}\n\t{self._source_info}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

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

    def rebuild(self) -> LogicalPlan:
        return Scan(
            schema=self.schema().unresolve(),
            source_info=self._source_info,
            predicate=self._predicate.unresolve() if self._predicate is not None else None,
            columns=self._column_names,
        )


class Filter(UnaryNode):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: ExpressionList) -> None:
        super().__init__(
            input.schema().to_column_expressions(), partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION
        )
        self._register_child(input)
        self._predicate = predicate.resolve(input.schema())

        resolved_type = self._predicate.exprs[0].resolved_type()
        if not ExpressionType.is_logical(resolved_type):
            raise ValueError(
                f"Expected expression {self._predicate.exprs[0]} to resolve to type LOGICAL, but received: {resolved_type}"
            )

    def __repr__(self) -> str:
        return f"Filter\n\toutput={self.schema()}\n\tpredicate={self._predicate}"

    def resource_request(self) -> ResourceRequest:
        return self._predicate.resource_request()

    def required_columns(self) -> ExpressionList:
        return self._predicate.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, Filter) and self.schema() == other.schema() and self._predicate == other._predicate

    def copy_with_new_input(self, new_input: LogicalPlan) -> Filter:
        raise NotImplementedError()

    def rebuild(self) -> LogicalPlan:
        return Filter(input=self._children()[0].rebuild(), predicate=self._predicate.unresolve())


class Projection(UnaryNode):
    """Which columns to keep"""

    def __init__(self, input: LogicalPlan, projection: ExpressionList) -> None:
        projection = projection.resolve(input_schema=input.schema())
        super().__init__(projection, partition_spec=input.partition_spec(), op_level=OpLevel.ROW)
        self._register_child(input)
        self._projection = projection

    def __repr__(self) -> str:
        return f"Projection\n\toutput={self.schema()}"

    def resource_request(self) -> ResourceRequest:
        return self._projection.resource_request()

    def required_columns(self) -> ExpressionList:
        return self._projection.required_columns()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Projection) and self.schema() == other.schema() and self._projection == other._projection
        )

    def copy_with_new_input(self, new_input: LogicalPlan) -> Projection:
        return Projection(new_input, self._projection)

    def rebuild(self) -> LogicalPlan:
        return Projection(input=self._children()[0].rebuild(), projection=self._projection.unresolve())


class Sort(UnaryNode):
    def __init__(self, input: LogicalPlan, sort_by: ExpressionList, desc: bool = False) -> None:
        pspec = PartitionSpec(scheme=PartitionScheme.RANGE, num_partitions=input.num_partitions(), by=sort_by)
        super().__init__(input.schema().to_column_expressions(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        assert len(sort_by.exprs) == 1, "we can only sort with 1 expression"
        self._sort_by = sort_by.resolve(input_schema=input.schema())
        self._desc = desc

    def __repr__(self) -> str:
        return f"Sort\n\toutput={self.schema()}\n\tsort_by={self._sort_by}\n\tdesc={self._desc}"

    def resource_request(self) -> ResourceRequest:
        return self._sort_by.resource_request()

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

    def rebuild(self) -> LogicalPlan:
        return Sort(input=self._children()[0].rebuild(), sort_by=self._sort_by.unresolve(), desc=self._desc)


class LocalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return f"LocalLimit\n\toutput={self.schema()}\n\tN={self._num}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def copy_with_new_input(self, new_input: LogicalPlan) -> LocalLimit:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, LocalLimit) and self.schema() == other.schema() and self._num == self._num

    def rebuild(self) -> LogicalPlan:
        return LocalLimit(input=self._children()[0].rebuild(), num=self._num)


class GlobalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), partition_spec=input.partition_spec(), op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return f"GlobalLimit\n\toutput={self.schema()}\n\tN={self._num}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def copy_with_new_input(self, new_input: LogicalPlan) -> GlobalLimit:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, GlobalLimit) and self.schema() == other.schema() and self._num == self._num

    def rebuild(self) -> LogicalPlan:
        return GlobalLimit(input=self._children()[0].rebuild(), num=self._num)


class PartitionScheme(Enum):
    UNKNOWN = "UNKNOWN"
    RANGE = "RANGE"
    HASH = "HASH"
    RANDOM = "RANDOM"


@dataclass(frozen=True)
class PartitionSpec:
    scheme: PartitionScheme
    num_partitions: int
    by: Optional[ExpressionList] = None


class Repartition(UnaryNode):
    def __init__(
        self, input: LogicalPlan, partition_by: ExpressionList, num_partitions: int, scheme: PartitionScheme
    ) -> None:
        pspec = PartitionSpec(
            scheme=scheme, num_partitions=num_partitions, by=partition_by if len(partition_by) > 0 else None
        )
        super().__init__(input.schema().to_column_expressions(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._partition_by = partition_by.resolve(self.schema())
        self._scheme = scheme
        if scheme == PartitionScheme.RANDOM and len(partition_by.names) > 0:
            raise ValueError("Can not pass in random partitioning and partition_by args")

    def __repr__(self) -> str:
        return (
            f"Repartition\n\toutput={self.schema()}\n\tpartition_by={self._partition_by}"
            f"\n\tnum_partitions={self.num_partitions()}\n\tscheme={self._scheme}"
        )

    def resource_request(self) -> ResourceRequest:
        return self._partition_by.resource_request()

    def copy_with_new_input(self, new_input: LogicalPlan) -> Repartition:
        return Repartition(
            input=new_input,
            partition_by=self._partition_by,
            num_partitions=self.num_partitions(),
            scheme=self._scheme,
        )

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Repartition)
            and self.schema() == other.schema()
            and self._partition_by == other._partition_by
            and self._scheme == other._scheme
        )

    def rebuild(self) -> LogicalPlan:
        return Repartition(
            input=self._children()[0].rebuild(),
            partition_by=self._partition_by.unresolve(),
            num_partitions=self.num_partitions(),
            scheme=self._scheme,
        )


class Coalesce(UnaryNode):
    def __init__(self, input: LogicalPlan, num_partitions: int) -> None:
        pspec = PartitionSpec(
            scheme=PartitionScheme.UNKNOWN,
            num_partitions=num_partitions,
        )
        super().__init__(input.schema().to_column_expressions(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        if num_partitions > input.num_partitions():
            raise ValueError(
                f"Coalesce can only reduce the number of partitions: {num_partitions} vs {input.num_partitions()}"
            )

    def __repr__(self) -> str:
        return f"Coalesce\n\toutput={self.schema()}" f"\n\tnum_partitions={self.num_partitions()}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def copy_with_new_input(self, new_input: LogicalPlan) -> Coalesce:
        return Coalesce(
            input=new_input,
            num_partitions=self.num_partitions(),
        )

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Coalesce)
            and self.schema() == other.schema()
            and self.num_partitions() == other.num_partitions()
        )

    def rebuild(self) -> LogicalPlan:
        return Coalesce(
            input=self._children()[0].rebuild(),
            num_partitions=self.num_partitions(),
        )


class LocalAggregate(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
        agg: List[Tuple[Expression, str]],
        group_by: Optional[ExpressionList] = None,
    ) -> None:

        cols_to_agg = ExpressionList([e for e, _ in agg]).resolve(input.schema())
        schema = cols_to_agg.to_column_expressions()
        self._group_by = group_by

        if group_by is not None:
            self._group_by = group_by.resolve(input.schema())
            schema = self._group_by.union(schema)

        super().__init__(schema, partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)
        self._agg = [(e, op) for e, (_, op) in zip(cols_to_agg, agg)]

    def __repr__(self) -> str:
        return f"LocalAggregate\n\toutput={self.schema()}\n\tgroup_by={self._group_by}"

    def resource_request(self) -> ResourceRequest:
        req = ResourceRequest.default()
        if self._group_by is not None:
            req = self._group_by.resource_request()
        req = ResourceRequest.max_resources([expr.resource_request() for expr, _ in self._agg] + [req])
        return req

    def copy_with_new_input(self, new_input: LogicalPlan) -> LocalAggregate:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LocalAggregate)
            and self.schema() == other.schema()
            and self._agg == other._agg
            and self._group_by == other._group_by
        )

    def rebuild(self) -> LogicalPlan:
        return LocalAggregate(
            input=self._children()[0].rebuild(),
            agg=[(e._unresolve(), op) for e, op in self._agg],
            group_by=self._group_by.unresolve() if self._group_by is not None else None,
        )


class HTTPRequest(LogicalPlan):
    def __init__(
        self,
        schema: ExpressionList,
    ) -> None:
        self._output_schema = schema.resolve()
        pspec = PartitionSpec(scheme=PartitionScheme.UNKNOWN, num_partitions=1)
        super().__init__(schema, partition_spec=pspec, op_level=OpLevel.ROW)

    def schema(self) -> ExpressionList:
        return self._output_schema

    def __repr__(self) -> str:
        return f"HTTPRequest\n\toutput={self.schema()}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPRequest) and self.schema() == other.schema()

    def rebuild(self) -> LogicalPlan:
        return HTTPRequest(schema=self.schema().unresolve())


class HTTPResponse(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
    ) -> None:
        self._schema = input.schema()
        super().__init__(self._schema, partition_spec=input.partition_spec(), op_level=OpLevel.ROW)

    def schema(self) -> ExpressionList:
        return self._schema

    def __repr__(self) -> str:
        return f"HTTPResponse\n\toutput={self.schema()}"

    def resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPResponse) and self.schema() == other.schema()

    def copy_with_new_input(self, new_input: LogicalPlan) -> HTTPResponse:
        raise NotImplementedError()

    def rebuild(self) -> LogicalPlan:
        return HTTPResponse(
            input=self._children()[0].rebuild(),
        )


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"


class Join(BinaryNode):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_on: ExpressionList,
        right_on: ExpressionList,
        how: JoinType = JoinType.INNER,
    ) -> None:
        assert len(left_on) == len(right_on), "left_on and right_on must match size"

        if not left.is_disjoint(right):
            right = right.rebuild()
            assert left.is_disjoint(right)
        num_partitions: int
        self._left_on = left_on.resolve(left.schema())
        self._right_on = right_on.resolve(right.schema())
        self._how = how
        schema: ExpressionList
        if how == JoinType.LEFT:
            num_partitions = left.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.RIGHT:
            num_partitions = right.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.INNER:
            num_partitions = min(left.num_partitions(), right.num_partitions())
            right_id_set = self._right_on.to_id_set()
            filtered_right = [e for e in right.schema() if e.get_id() not in right_id_set]
            schema = left.schema().union(ExpressionList(filtered_right), strict=False, rename_dup="right.")

        left = Repartition(left, partition_by=self._left_on, num_partitions=num_partitions, scheme=PartitionScheme.HASH)
        right = Repartition(
            right, partition_by=self._right_on, num_partitions=num_partitions, scheme=PartitionScheme.HASH
        )

        super().__init__(
            schema.to_column_expressions(), partition_spec=left.partition_spec(), op_level=OpLevel.PARTITION
        )
        self._register_child(left)
        self._register_child(right)

    def __repr__(self) -> str:
        return (
            f"Join\n\toutput={self.schema()}"
            f"\n\tnum_partitions={self.num_partitions()}"
            f"\n\tleft_on={self._left_on}"
            f"\n\tright_on={self._right_on}"
        )

    def resource_request(self) -> ResourceRequest:
        # Note that this join creates two Repartition LogicalPlans using the left_on and right_on ExpressionLists
        # The Repartition LogicalPlans will have the (potentially) expensive ResourceRequests, but the Join itself
        # after repartitioning is done should be relatively cheap.
        return ResourceRequest.default()

    def copy_with_new_input(self, new_input: LogicalPlan) -> Coalesce:
        raise NotImplementedError()

    def required_columns(self) -> ExpressionList:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Join)
            and self.schema() == other.schema()
            and self._left_on == other._left_on
            and self._right_on == other._right_on
            and self.num_partitions() == other.num_partitions()
        )

    def rebuild(self) -> LogicalPlan:
        return Join(
            left=self._children()[0].rebuild(),
            right=self._children()[1].rebuild(),
            left_on=self._left_on.unresolve(),
            right_on=self._right_on.unresolve(),
            how=self._how,
        )
