from __future__ import annotations

import itertools
import pathlib
from abc import abstractmethod
from enum import IntEnum
from pprint import pformat
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from daft.context import get_context
from daft.daft import (
    FileFormat,
    FileFormatConfig,
    FileInfos,
    JoinType,
    PartitionScheme,
    PartitionSpec,
    ResourceRequest,
    StorageConfig,
)
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.expressions import Expression, ExpressionsProjection, col
from daft.expressions.testing import expr_structurally_equal
from daft.internal.treenode import TreeNode
from daft.logical.aggregation_plan_builder import AggregationPlanBuilder
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.map_partition_ops import ExplodeOp, MapPartitionOp
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry
from daft.runners.pyrunner import LocalPartitionSet
from daft.table import Table

if TYPE_CHECKING:
    from daft.planner.py_planner import PyPhysicalPlanScheduler


class OpLevel(IntEnum):
    ROW = 1
    PARTITION = 2
    GLOBAL = 3


class PyLogicalPlanBuilder(LogicalPlanBuilder):
    def __init__(self, plan: LogicalPlan):
        self._plan = plan

    def __repr__(self) -> str:
        return self._plan.pretty_print()

    def to_physical_plan_scheduler(self) -> PyPhysicalPlanScheduler:
        from daft.planner.py_planner import PyPhysicalPlanScheduler

        return PyPhysicalPlanScheduler(self._plan)

    def schema(self) -> Schema:
        return self._plan.schema()

    def partition_spec(self) -> PartitionSpec:
        return self._plan.partition_spec()

    def pretty_print(self, simple: bool = False) -> str:
        return self._plan.pretty_print(simple)

    def optimize(self) -> PyLogicalPlanBuilder:
        from daft.internal.rule_runner import (
            FixedPointPolicy,
            Once,
            RuleBatch,
            RuleRunner,
        )
        from daft.logical.optimizer import (
            DropProjections,
            DropRepartition,
            FoldProjections,
            PruneColumns,
            PushDownClausesIntoScan,
            PushDownLimit,
            PushDownPredicates,
        )

        optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [
                        DropRepartition(),
                        PushDownPredicates(),
                        PruneColumns(),
                        FoldProjections(),
                        PushDownClausesIntoScan(),
                    ],
                ),
                RuleBatch(
                    "PushDownLimitsAndRepartitions",
                    FixedPointPolicy(3),
                    [PushDownLimit(), DropRepartition(), DropProjections()],
                ),
            ]
        )
        plan = optimizer.optimize(self._plan)
        return plan.to_builder()

    @classmethod
    def from_in_memory_scan(
        cls, partition: PartitionCacheEntry, schema: Schema, partition_spec: PartitionSpec | None = None
    ) -> PyLogicalPlanBuilder:
        return InMemoryScan(cache_entry=partition, schema=schema, partition_spec=partition_spec).to_builder()

    @classmethod
    def from_tabular_scan(
        cls,
        *,
        file_infos: FileInfos,
        schema: Schema,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> PyLogicalPlanBuilder:
        file_infos_table = Table._from_pytable(file_infos.to_table())
        partition = LocalPartitionSet({0: file_infos_table})
        cache_entry = get_context().runner().put_partition_set_into_cache(partition)
        filepath_plan = InMemoryScan(
            cache_entry=cache_entry,
            schema=file_infos_table.schema(),
            partition_spec=PartitionSpec(PartitionScheme.Unknown, len(file_infos)),
        )

        return TabularFilesScan(
            schema=schema,
            predicate=None,
            columns=None,
            file_format_config=file_format_config,
            storage_config=storage_config,
            filepaths_child=filepath_plan,
            # WARNING: This is currently hardcoded to be the same number of partitions as rows!! This is because we emit
            # one partition per filepath. This will change in the future and our logic here should change accordingly.
            num_partitions=len(file_infos),
        ).to_builder()

    def project(
        self,
        projection: list[Expression],
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> PyLogicalPlanBuilder:
        return Projection(
            self._plan, ExpressionsProjection(projection), custom_resource_request=custom_resource_request
        ).to_builder()

    def filter(self, predicate: Expression):
        return Filter(self._plan, ExpressionsProjection([predicate])).to_builder()

    def limit(self, num_rows: int) -> LogicalPlanBuilder:
        local_limit = LocalLimit(self._plan, num=num_rows)
        plan = GlobalLimit(local_limit, num=num_rows)
        return plan.to_builder()

    def explode(self, explode_expressions: list[Expression]) -> PyLogicalPlanBuilder:
        return Explode(self._plan, ExpressionsProjection(explode_expressions)).to_builder()

    def count(self) -> LogicalPlanBuilder:
        local_count_op = LocalCount(self._plan)
        coalease_op = Coalesce(local_count_op, 1)
        local_sum_op = LocalAggregate(coalease_op, [col("count")._sum()])
        return local_sum_op.to_builder()

    def distinct(self) -> PyLogicalPlanBuilder:
        all_exprs = ExpressionsProjection.from_schema(self._plan.schema())
        plan: LogicalPlan = LocalDistinct(self._plan, all_exprs)
        if self.num_partitions() > 1:
            plan = Repartition(
                plan,
                partition_by=all_exprs,
                num_partitions=self.num_partitions(),
                scheme=PartitionScheme.Hash,
            )
            plan = LocalDistinct(plan, all_exprs)
        return plan.to_builder()

    def sort(self, sort_by: list[Expression], descending: list[bool] | bool = False) -> PyLogicalPlanBuilder:
        return Sort(self._plan, sort_by=ExpressionsProjection(sort_by), descending=descending).to_builder()

    def repartition(
        self, num_partitions: int, partition_by: list[Expression], scheme: PartitionScheme
    ) -> PyLogicalPlanBuilder:
        return Repartition(
            self._plan, num_partitions=num_partitions, partition_by=ExpressionsProjection(partition_by), scheme=scheme
        ).to_builder()

    def coalesce(self, num_partitions: int) -> PyLogicalPlanBuilder:
        return Coalesce(self._plan, num_partitions).to_builder()

    def join(  # type: ignore[override]
        self,
        right: PyLogicalPlanBuilder,
        left_on: list[Expression],
        right_on: list[Expression],
        how: JoinType = JoinType.Inner,
    ) -> PyLogicalPlanBuilder:
        return Join(
            self._plan,
            right._plan,
            left_on=ExpressionsProjection(left_on),
            right_on=ExpressionsProjection(right_on),
            how=how,
        ).to_builder()

    def concat(self, other: PyLogicalPlanBuilder) -> PyLogicalPlanBuilder:  # type: ignore[override]
        return Concat(self._plan, other._plan).to_builder()

    def agg(
        self,
        to_agg: list[tuple[Expression, str]],
        group_by: list[Expression] | None,
    ) -> PyLogicalPlanBuilder:
        agg_builder = AggregationPlanBuilder(
            self._plan, group_by=ExpressionsProjection(group_by) if group_by is not None else None
        )
        for expr, op in to_agg:
            if op == "sum":
                agg_builder.add_sum(expr.name(), expr)
            elif op == "min":
                agg_builder.add_min(expr.name(), expr)
            elif op == "max":
                agg_builder.add_max(expr.name(), expr)
            elif op == "count":
                agg_builder.add_count(expr.name(), expr)
            elif op == "list":
                agg_builder.add_list(expr.name(), expr)
            elif op == "mean":
                agg_builder.add_mean(expr.name(), expr)
            elif op == "concat":
                agg_builder.add_concat(expr.name(), expr)
            else:
                raise NotImplementedError(f"LogicalPlan construction for operation not implemented: {op}")

        return agg_builder.build().to_builder()

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: list[Expression] | None = None,
        compression: str | None = None,
    ) -> PyLogicalPlanBuilder:
        return FileWrite(
            self._plan,
            root_dir=root_dir,
            partition_cols=ExpressionsProjection(partition_cols) if partition_cols is not None else None,
            file_format=file_format,
            compression=compression,
        ).to_builder()


class LogicalPlan(TreeNode["LogicalPlan"]):
    id_iter = itertools.count()

    def __init__(
        self,
        schema: Schema,
        partition_spec: PartitionSpec,
        op_level: OpLevel,
    ) -> None:
        super().__init__()
        if not isinstance(schema, Schema):
            raise ValueError(f"expected Schema Object for LogicalPlan but got {type(schema)}")
        self._schema = schema
        self._op_level = op_level
        self._partition_spec = partition_spec
        self._id = next(LogicalPlan.id_iter)

    def schema(self) -> Schema:
        return self._schema

    def resource_request(self) -> ResourceRequest:
        """Returns a custom ResourceRequest if one has been attached to this LogicalPlan

        Implementations should override this if they allow for customized ResourceRequests.
        """
        return ResourceRequest()

    def num_partitions(self) -> int:
        return self._partition_spec.num_partitions

    def to_builder(self) -> PyLogicalPlanBuilder:
        return PyLogicalPlanBuilder(self)

    @abstractmethod
    def required_columns(self) -> list[set[str]]:
        raise NotImplementedError()

    @abstractmethod
    def input_mapping(self) -> list[dict[str, str]]:
        raise NotImplementedError()

    @abstractmethod
    def _local_eq(self, other: Any) -> bool:
        raise NotImplementedError()

    def is_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LogicalPlan)
            and self._local_eq(other)
            and self.schema() == other.schema()
            and self.partition_spec() == other.partition_spec()
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

    @abstractmethod
    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        raise NotImplementedError()

    def pretty_print(self, simple: bool = False) -> str:
        builder: list[str] = []

        def helper(node: LogicalPlan, depth: int = 0, index: int = 0, prefix: str = "", header: str = ""):
            children: list[LogicalPlan] = node._children()
            if simple:
                obj_repr_lines = [node.__class__.__name__]
            else:
                obj_repr_lines = repr(node).splitlines()
            builder.append(f"{header}{obj_repr_lines[0]}\n")

            if len(children) > 0:
                body_prefix = prefix + "│"
            else:
                body_prefix = prefix + " "

            for line in obj_repr_lines[1:]:
                builder.append(f"{body_prefix}{line}\n")
            builder.append(f"{body_prefix}\n")

            if len(children) < 2:
                for child in children:
                    has_grandchild = len(child._children()) > 0

                    if has_grandchild:
                        header = prefix + "├──"
                    else:
                        header = prefix + "└──"

                    helper(child, depth=depth, index=index + 1, prefix=prefix, header=header)
            else:
                connector = "└─"
                middle_child_header = "─┬─"

                for i, child in enumerate(children):
                    has_grandchild = len(child._children()) > 0
                    if has_grandchild:
                        final_header = "─┬─"
                    else:
                        final_header = "───"

                    position = len(children) - i
                    if i != len(children) - 1:
                        next_child_prefix = prefix + ("   │  " * (position - 1))
                    else:
                        next_child_prefix = prefix + "      "
                    header = (
                        next_child_prefix[: -3 * position]
                        + connector
                        + (middle_child_header * (position - 1))
                        + final_header
                    )

                    helper(child, depth=depth + 1, index=i, prefix=next_child_prefix, header=header)

        helper(self, 0, 0, header="┌─")
        return "".join(builder)

    def _repr_helper(self, **fields: Any) -> str:
        fields_to_print: dict[str, Any] = {}
        if "output" not in fields:
            fields_to_print["output"] = self.schema()

        fields_to_print.update(fields)
        fields_to_print["partitioning"] = self.partition_spec()
        reduced_types = {}
        for k, v in fields_to_print.items():
            if isinstance(v, ExpressionsProjection):
                v = list(v)
            elif isinstance(v, Schema):
                v = list([col(field.name) for field in v])
            elif isinstance(v, PartitionSpec):
                v = {"scheme": v.scheme, "num_partitions": v.num_partitions, "by": v.by}
                if isinstance(v["by"], ExpressionsProjection):
                    v["by"] = list(v["by"])
            reduced_types[k] = v
        to_render: list[str] = [f"{self.__class__.__name__}\n"]
        space = "    "
        for key, value in reduced_types.items():
            repr_ed = pformat(value, width=80, compact=True).splitlines()
            to_render.append(f"{space}{key}={repr_ed[0]}\n")
            for line in repr_ed[1:]:
                to_render.append(f"{space*2}{line}\n")

        return "".join(to_render)


class UnaryNode(LogicalPlan):
    ...


class BinaryNode(LogicalPlan):
    ...


class TabularFilesScan(UnaryNode):
    def __init__(
        self,
        *,
        schema: Schema,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
        predicate: ExpressionsProjection | None = None,
        columns: list[str] | None = None,
        filepaths_child: LogicalPlan,
        num_partitions: int | None = None,
        limit_rows: int | None = None,
    ) -> None:
        if num_partitions is None:
            num_partitions = filepaths_child.num_partitions()
        pspec = PartitionSpec(scheme=PartitionScheme.Unknown, num_partitions=num_partitions)
        super().__init__(schema, partition_spec=pspec, op_level=OpLevel.PARTITION)

        if predicate is not None:
            self._predicate = predicate
        else:
            self._predicate = ExpressionsProjection([])

        if columns is not None:
            self._output_schema = Schema._from_field_name_and_types(
                [(schema[col].name, schema[col].dtype) for col in columns]
            )
        else:
            self._output_schema = schema

        self._column_names = columns
        self._columns = self._schema
        self._file_format_config = file_format_config
        self._storage_config = storage_config
        self._limit_rows = limit_rows

        self._register_child(filepaths_child)

    @property
    def _filepaths_child(self) -> LogicalPlan:
        child = self._children()[0]
        return child

    def schema(self) -> Schema:
        return self._output_schema

    def __repr__(self) -> str:
        return self._repr_helper(
            columns_pruned=len(self._columns) - len(self.schema()), file_format_config=self._file_format_config
        )

    def required_columns(self) -> list[set[str]]:
        return [{"path"} | self._predicate.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [dict()]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, TabularFilesScan)
            and self.schema() == other.schema()
            and self._predicate == other._predicate
            and self._columns == other._columns
            and self._file_format_config == other._file_format_config
        )

    def rebuild(self) -> LogicalPlan:
        child = self._filepaths_child.rebuild()
        return TabularFilesScan(
            schema=self.schema(),
            file_format_config=self._file_format_config,
            storage_config=self._storage_config,
            predicate=self._predicate if self._predicate is not None else None,
            columns=self._column_names,
            filepaths_child=child,
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return TabularFilesScan(
            schema=self.schema(),
            file_format_config=self._file_format_config,
            storage_config=self._storage_config,
            predicate=self._predicate,
            columns=self._column_names,
            filepaths_child=new_children[0],
        )


class InMemoryScan(UnaryNode):
    def __init__(
        self, cache_entry: PartitionCacheEntry, schema: Schema, partition_spec: PartitionSpec | None = None
    ) -> None:
        if partition_spec is None:
            partition_spec = PartitionSpec(scheme=PartitionScheme.Unknown, num_partitions=1)

        super().__init__(schema=schema, partition_spec=partition_spec, op_level=OpLevel.GLOBAL)
        self._cache_entry = cache_entry

    def __repr__(self) -> str:
        return self._repr_helper(cache_id=self._cache_entry.key)

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, InMemoryScan)
            and self._cache_entry == other._cache_entry
            and self.schema() == other.schema()
        )

    def required_columns(self) -> list[set[str]]:
        return [set()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [dict()]

    def rebuild(self) -> LogicalPlan:
        # if we are rebuilding, this will be cached when this is ran
        return InMemoryScan(
            cache_entry=self._cache_entry,
            schema=self.schema(),
            partition_spec=self.partition_spec(),
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 0
        return self


class FileWrite(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: ExpressionsProjection | None = None,
        compression: str | None = None,
    ) -> None:
        if file_format != FileFormat.Parquet and file_format != FileFormat.Csv:
            raise ValueError(f"Writing is only supported for Parquet and CSV file formats, but got: {file_format}")
        self._file_format = file_format
        self._root_dir = root_dir
        self._compression = compression
        if partition_cols is not None:
            self._partition_cols = partition_cols
        else:
            self._partition_cols = ExpressionsProjection([])

        schema = Schema._from_field_name_and_types([("file_path", DataType.string())])

        super().__init__(schema, input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)

    def __repr__(self) -> str:
        return self._repr_helper()

    def required_columns(self) -> list[set[str]]:
        return [self._partition_cols.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [dict()]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, FileWrite)
            and self.schema() == other.schema()
            and self._file_format == other._file_format
            and self._root_dir == other._root_dir
            and self._compression == other._compression
        )

    def rebuild(self) -> LogicalPlan:
        raise NotImplementedError("We can not rebuild a filewrite due to side effects")

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return FileWrite(
            new_children[0],
            root_dir=self._root_dir,
            file_format=self._file_format,
            partition_cols=self._partition_cols,
            compression=self._compression,
        )


class Filter(UnaryNode):
    """Which rows to keep"""

    def __init__(self, input: LogicalPlan, predicate: ExpressionsProjection) -> None:
        super().__init__(input.schema(), partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)

        self._predicate = predicate
        predicate_schema = predicate.resolve_schema(input.schema())

        for resolved_field, predicate_expr in zip(predicate_schema, predicate):
            resolved_type = resolved_field.dtype
            if resolved_type != DataType.bool():
                raise ValueError(
                    f"Expected expression {predicate_expr} to resolve to type Boolean, but received: {resolved_type}"
                )

    def __repr__(self) -> str:
        return self._repr_helper(predicate=self._predicate)

    def required_columns(self) -> list[set[str]]:
        return [self._predicate.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, Filter) and self.schema() == other.schema() and self._predicate == other._predicate

    def rebuild(self) -> LogicalPlan:
        return Filter(input=self._children()[0].rebuild(), predicate=self._predicate)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Filter(input=new_children[0], predicate=self._predicate)


class Projection(UnaryNode):
    """Which columns to keep"""

    def __init__(
        self,
        input: LogicalPlan,
        projection: ExpressionsProjection,
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> None:
        schema = projection.resolve_schema(input.schema())
        super().__init__(schema, partition_spec=input.partition_spec(), op_level=OpLevel.ROW)
        self._register_child(input)
        self._projection = projection
        self._custom_resource_request = custom_resource_request

    def resource_request(self) -> ResourceRequest:
        return self._custom_resource_request

    def __repr__(self) -> str:
        return self._repr_helper(output=list(self._projection))

    def required_columns(self) -> list[set[str]]:
        return [self._projection.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [self._projection.input_mapping()]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Projection) and self.schema() == other.schema() and self._projection == other._projection
        )

    def rebuild(self) -> LogicalPlan:
        return Projection(
            input=self._children()[0].rebuild(),
            projection=self._projection,
            custom_resource_request=self.resource_request(),
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Projection(new_children[0], self._projection, custom_resource_request=self.resource_request())


class Sort(UnaryNode):
    def __init__(
        self, input: LogicalPlan, sort_by: ExpressionsProjection, descending: list[bool] | bool = False
    ) -> None:
        pspec = PartitionSpec(
            scheme=PartitionScheme.Range,
            num_partitions=input.num_partitions(),
            by=sort_by.to_inner_py_exprs(),
        )
        super().__init__(input.schema(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._sort_by = sort_by

        resolved_sort_by_schema = self._sort_by.resolve_schema(input.schema())
        for f, sort_by_expr in zip(resolved_sort_by_schema, self._sort_by):
            if f.dtype == DataType.null() or f.dtype == DataType.binary() or f.dtype == DataType.bool():
                raise ExpressionTypeError(f"Cannot sort on expression {sort_by_expr} with type: {f.dtype}")

        if isinstance(descending, bool):
            self._descending = [descending for _ in self._sort_by]
        else:
            self._descending = descending

    def __repr__(self) -> str:
        return self._repr_helper(sort_by=self._sort_by, desc=self._descending)

    def required_columns(self) -> list[set[str]]:
        return [self._sort_by.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, Sort)
            and self.schema() == other.schema()
            and self._sort_by == other._sort_by
            and self._descending == other._descending
        )

    def rebuild(self) -> LogicalPlan:
        return Sort(input=self._children()[0].rebuild(), sort_by=self._sort_by, descending=self._descending)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Sort(new_children[0], sort_by=self._sort_by, descending=self._descending)


TMapPartitionOp = TypeVar("TMapPartitionOp", bound=MapPartitionOp)


class MapPartition(UnaryNode, Generic[TMapPartitionOp]):
    def __init__(self, input: LogicalPlan, map_partition_op: TMapPartitionOp) -> None:
        self._map_partition_op = map_partition_op
        super().__init__(
            self._map_partition_op.get_output_schema(),
            partition_spec=input.partition_spec(),
            op_level=OpLevel.PARTITION,
        )
        self._register_child(input)

    def __repr__(self) -> str:
        return self._repr_helper(op=self._map_partition_op)

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, MapPartition)
            and self.schema() == other.schema()
            and self._map_partition_op == other._map_partition_op
        )

    def eval_partition(self, partition: Table) -> Table:
        return self._map_partition_op.run(partition)


class Explode(MapPartition[ExplodeOp]):
    def __init__(self, input: LogicalPlan, explode_expressions: ExpressionsProjection):
        map_partition_op = ExplodeOp(input.schema(), explode_columns=explode_expressions)
        super().__init__(
            input,
            map_partition_op,
        )

    def __repr__(self) -> str:
        return self._repr_helper()

    def required_columns(self) -> list[set[str]]:
        return [self._map_partition_op.explode_columns.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        explode_columns = self._map_partition_op.explode_columns.input_mapping().keys()
        return [{name: name for name in self.schema().column_names() if name not in explode_columns}]

    def rebuild(self) -> LogicalPlan:
        return Explode(
            self._children()[0].rebuild(),
            self._map_partition_op.explode_columns,
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Explode(new_children[0], explode_expressions=self._map_partition_op.explode_columns)


class LocalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return self._repr_helper(num=self._num)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return LocalLimit(new_children[0], self._num)

    def required_columns(self) -> list[set[str]]:
        return [set()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, LocalLimit) and self.schema() == other.schema() and self._num == other._num

    def rebuild(self) -> LogicalPlan:
        return LocalLimit(input=self._children()[0].rebuild(), num=self._num)


class GlobalLimit(UnaryNode):
    def __init__(self, input: LogicalPlan, num: int) -> None:
        super().__init__(input.schema(), partition_spec=input.partition_spec(), op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._num = num

    def __repr__(self) -> str:
        return self._repr_helper(num=self._num)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return GlobalLimit(new_children[0], self._num)

    def required_columns(self) -> list[set[str]]:
        return [set()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, GlobalLimit) and self.schema() == other.schema() and self._num == other._num

    def rebuild(self) -> LogicalPlan:
        return GlobalLimit(input=self._children()[0].rebuild(), num=self._num)


class LocalCount(UnaryNode):
    def __init__(self, input: LogicalPlan) -> None:
        schema = Schema._from_field_name_and_types([("count", DataType.int64())])
        super().__init__(schema, partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)

    def __repr__(self) -> str:
        return self._repr_helper()

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return LocalCount(new_children[0])

    def required_columns(self) -> list[set[str]]:
        # HACK: Arbitrarily return the first column in the child to ensure that
        # at least one column is computed by the optimizer
        return [{self._children()[0].schema().column_names()[0]}]

    def input_mapping(self) -> list[dict[str, str]]:
        return []

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, LocalCount) and self.schema() == other.schema()

    def rebuild(self) -> LogicalPlan:
        return LocalCount(input=self._children()[0].rebuild())


class Repartition(UnaryNode):
    def __init__(
        self, input: LogicalPlan, partition_by: ExpressionsProjection, num_partitions: int, scheme: PartitionScheme
    ) -> None:
        pspec = PartitionSpec(
            scheme=scheme,
            num_partitions=num_partitions,
            by=partition_by.to_inner_py_exprs() if len(partition_by) > 0 else None,
        )
        super().__init__(input.schema(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._partition_by = partition_by
        self._scheme = scheme
        if scheme in (PartitionScheme.Random, PartitionScheme.Unknown) and len(partition_by.to_name_set()) > 0:
            raise ValueError(f"Can not pass in {scheme} and partition_by args")

    def __repr__(self) -> str:
        return self._repr_helper(
            partition_by=self._partition_by, num_partitions=self.num_partitions(), scheme=self._scheme
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Repartition(
            input=new_children[0],
            partition_by=self._partition_by,
            num_partitions=self.num_partitions(),
            scheme=self._scheme,
        )

    def required_columns(self) -> list[set[str]]:
        return [self._partition_by.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

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
            partition_by=self._partition_by,
            num_partitions=self.num_partitions(),
            scheme=self._scheme,
        )


class Coalesce(UnaryNode):
    def __init__(self, input: LogicalPlan, num_partitions: int) -> None:
        pspec = PartitionSpec(
            scheme=PartitionScheme.Unknown,
            num_partitions=num_partitions,
        )
        super().__init__(input.schema(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        if num_partitions > input.num_partitions():
            raise ValueError(
                f"Coalesce can only reduce the number of partitions: {num_partitions} vs {input.num_partitions()}"
            )

    def __repr__(self) -> str:
        return self._repr_helper(num_partitions=self.num_partitions())

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return Coalesce(
            input=new_children[0],
            num_partitions=self.num_partitions(),
        )

    def required_columns(self) -> list[set[str]]:
        return [set()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [{name: name for name in self.schema().column_names()}]

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
        agg: list[Expression],
        group_by: ExpressionsProjection | None = None,
    ) -> None:
        self._cols_to_agg = ExpressionsProjection(agg)
        self._group_by = group_by

        if group_by is not None:
            group_and_agg_cols = ExpressionsProjection(list(group_by) + agg)
            schema = group_and_agg_cols.resolve_schema(input.schema())
        else:
            schema = self._cols_to_agg.resolve_schema(input.schema())

        super().__init__(schema, partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)
        self._agg = agg

    def __repr__(self) -> str:
        return self._repr_helper(agg=self._agg, group_by=self._group_by)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return LocalAggregate(new_children[0], agg=self._agg, group_by=self._group_by)

    def required_columns(self) -> list[set[str]]:
        required_cols = set(self._cols_to_agg.required_columns())
        if self._group_by is not None:
            required_cols = required_cols | set(self._group_by.required_columns())
        return [required_cols]

    def input_mapping(self) -> list[dict[str, str]]:
        if self._group_by is not None:
            return [self._group_by.input_mapping()]
        else:
            return []

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LocalAggregate)
            and self.schema() == other.schema()
            and all(expr_structurally_equal(l, r) for l, r in zip(self._agg, other._agg))
            and self._group_by == other._group_by
        )

    def rebuild(self) -> LogicalPlan:
        return LocalAggregate(
            input=self._children()[0].rebuild(),
            agg=self._agg,
            group_by=self._group_by if self._group_by is not None else None,
        )


class LocalDistinct(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
        group_by: ExpressionsProjection,
    ) -> None:
        self._group_by = group_by
        schema = group_by.resolve_schema(input.schema())
        super().__init__(schema, partition_spec=input.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(input)

    def __repr__(self) -> str:
        return self._repr_helper(group_by=self._group_by)

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return LocalDistinct(new_children[0], group_by=self._group_by)

    def required_columns(self) -> list[set[str]]:
        return [self._group_by.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [self._group_by.input_mapping()]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, LocalDistinct) and self.schema() == other.schema() and self._group_by == other._group_by
        )

    def rebuild(self) -> LogicalPlan:
        return LocalDistinct(input=self._children()[0].rebuild(), group_by=self._group_by)


class HTTPRequest(LogicalPlan):
    def __init__(
        self,
        schema: Schema,
    ) -> None:
        self._output_schema = schema
        pspec = PartitionSpec(scheme=PartitionScheme.Unknown, num_partitions=1)
        super().__init__(schema, partition_spec=pspec, op_level=OpLevel.ROW)

    def schema(self) -> Schema:
        return self._output_schema

    def __repr__(self) -> str:
        return self._repr_helper()

    def required_columns(self) -> list[set[str]]:
        raise NotImplementedError()

    def input_mapping(self) -> list[dict[str, str]]:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPRequest) and self.schema() == other.schema()

    def rebuild(self) -> LogicalPlan:
        return HTTPRequest(schema=self.schema())

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 0
        return self


class HTTPResponse(UnaryNode):
    def __init__(
        self,
        input: LogicalPlan,
    ) -> None:
        self._schema = input.schema()
        super().__init__(self._schema, partition_spec=input.partition_spec(), op_level=OpLevel.ROW)

    def schema(self) -> Schema:
        return self._schema

    def __repr__(self) -> str:
        return self._repr_helper()

    def required_columns(self) -> list[set[str]]:
        raise NotImplementedError()

    def input_mapping(self) -> list[dict[str, str]]:
        raise NotImplementedError()

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, HTTPResponse) and self.schema() == other.schema()

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return HTTPResponse(new_children[0])

    def rebuild(self) -> LogicalPlan:
        return HTTPResponse(
            input=self._children()[0].rebuild(),
        )


class Join(BinaryNode):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> None:
        assert len(left_on) == len(right_on), "left_on and right_on must match size"

        if not left.is_disjoint(right):
            right = right.rebuild()
            assert left.is_disjoint(right)
        num_partitions: int
        self._left_on = left_on
        self._right_on = right_on

        for schema, exprs in ((left.schema(), self._left_on), (right.schema(), self._right_on)):
            resolved_schema = exprs.resolve_schema(schema)
            for f, expr in zip(resolved_schema, exprs):
                if f.dtype == DataType.null():
                    raise ExpressionTypeError(f"Cannot join on null type expression: {expr}")

        self._how = how
        output_schema: Schema
        if how == JoinType.Left:
            num_partitions = left.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.Right:
            num_partitions = right.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.Inner:
            num_partitions = max(left.num_partitions(), right.num_partitions())
            right_drop_set = {r.name() for l, r in zip(left_on, right_on) if l.name() == r.name()}
            left_columns = ExpressionsProjection.from_schema(left.schema())
            right_columns = ExpressionsProjection([col(f.name) for f in right.schema() if f.name not in right_drop_set])
            unioned_expressions = left_columns.union(right_columns, rename_dup="right.")
            self._left_columns = left_columns
            self._right_columns = ExpressionsProjection(list(unioned_expressions)[len(self._left_columns) :])
            self._output_projection = unioned_expressions
            output_schema = self._left_columns.resolve_schema(left.schema()).union(
                self._right_columns.resolve_schema(right.schema())
            )

        left_pspec = PartitionSpec(
            scheme=PartitionScheme.Hash, num_partitions=num_partitions, by=self._left_on.to_inner_py_exprs()
        )
        right_pspec = PartitionSpec(
            scheme=PartitionScheme.Hash, num_partitions=num_partitions, by=self._right_on.to_inner_py_exprs()
        )

        new_left = Repartition(
            left, partition_by=self._left_on, num_partitions=num_partitions, scheme=PartitionScheme.Hash
        )

        if num_partitions == 1 and left.num_partitions() == 1:
            left = left
        elif left.partition_spec() != left_pspec:
            left = new_left

        new_right = Repartition(
            right, partition_by=self._right_on, num_partitions=num_partitions, scheme=PartitionScheme.Hash
        )
        if num_partitions == 1 and right.num_partitions() == 1:
            right = right
        elif right.partition_spec() != right_pspec:
            right = new_right

        super().__init__(output_schema, partition_spec=left.partition_spec(), op_level=OpLevel.PARTITION)
        self._register_child(left)
        self._register_child(right)

    def __repr__(self) -> str:
        return self._repr_helper(left_on=self._left_on, right_on=self._right_on, num_partitions=self.num_partitions())

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 2
        return Join(new_children[0], new_children[1], left_on=self._left_on, right_on=self._right_on, how=self._how)

    def required_columns(self) -> list[set[str]]:
        return [self._left_on.required_columns(), self._right_on.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [self._left_columns.input_mapping(), self._right_columns.input_mapping()]

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
            left_on=self._left_on,
            right_on=self._right_on,
            how=self._how,
        )


class Concat(BinaryNode):
    def __init__(self, top: LogicalPlan, bottom: LogicalPlan):
        assert top.schema() == bottom.schema()
        self._top = top
        self._bottom = bottom

        new_partition_spec = PartitionSpec(
            PartitionScheme.Unknown,
            num_partitions=(top.partition_spec().num_partitions + bottom.partition_spec().num_partitions),
            by=None,
        )

        super().__init__(top.schema(), partition_spec=new_partition_spec, op_level=OpLevel.GLOBAL)
        self._register_child(self._top)
        self._register_child(self._bottom)

    def __repr__(self) -> str:
        return self._repr_helper(num_partitions=self.num_partitions())

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 2
        return Concat(new_children[0], new_children[1])

    def required_columns(self) -> list[set[str]]:
        return [set(), set()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [
            {name: name for name in self._top.schema().column_names()},
            {name: name for name in self._bottom.schema().column_names()},
        ]

    def _local_eq(self, other: Any) -> bool:
        return isinstance(other, Concat) and self.schema() == other.schema()

    def rebuild(self) -> LogicalPlan:
        return Concat(
            top=self._children()[0].rebuild(),
            bottom=self._children()[1].rebuild(),
        )
