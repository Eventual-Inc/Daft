from __future__ import annotations

import itertools
import pathlib
from abc import abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum, IntEnum
from pprint import pformat
from typing import Any, Generic, TypeVar

import fsspec

from daft.datasources import SourceInfo, StorageType
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.expressions import Expression, ExpressionsProjection, col
from daft.expressions.testing import expr_structurally_equal
from daft.internal.treenode import TreeNode
from daft.logical.map_partition_ops import ExplodeOp, MapPartitionOp
from daft.logical.schema import Schema
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import PartitionCacheEntry
from daft.table import Table


class OpLevel(IntEnum):
    ROW = 1
    PARTITION = 2
    GLOBAL = 3


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

    @abstractmethod
    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        raise NotImplementedError()

    def pretty_print(
        self,
    ) -> str:
        builder: list[str] = []

        def helper(node: LogicalPlan, depth: int = 0, index: int = 0, prefix: str = "", header: str = ""):
            children: list[LogicalPlan] = node._children()
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
                v = asdict(v)
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
        source_info: SourceInfo,
        fs: fsspec.AbstractFileSystem | None,
        predicate: ExpressionsProjection | None = None,
        columns: list[str] | None = None,
        filepaths_child: LogicalPlan,
        filepaths_column_name: str,
        num_partitions: int | None = None,
        limit_rows: int | None = None,
    ) -> None:
        if num_partitions is None:
            num_partitions = filepaths_child.num_partitions()
        pspec = PartitionSpec(scheme=PartitionScheme.UNKNOWN, num_partitions=num_partitions)
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
        self._source_info = source_info
        self._fs = fs
        self._limit_rows = limit_rows

        # TabularFilesScan has a single child node that provides the filepaths to read from.
        assert (
            filepaths_child.schema()[filepaths_column_name] is not None
        ), f"TabularFileScan requires a child with '{filepaths_column_name}' column"
        self._register_child(filepaths_child)
        self._filepaths_column_name = filepaths_column_name

    @property
    def _filepaths_child(self) -> LogicalPlan:
        child = self._children()[0]
        return child

    def schema(self) -> Schema:
        return self._output_schema

    def __repr__(self) -> str:
        return self._repr_helper(columns_pruned=len(self._columns) - len(self.schema()), source_info=self._source_info)

    def required_columns(self) -> list[set[str]]:
        return [{self._filepaths_column_name} | self._predicate.required_columns()]

    def input_mapping(self) -> list[dict[str, str]]:
        return [dict()]

    def _local_eq(self, other: Any) -> bool:
        return (
            isinstance(other, TabularFilesScan)
            and self.schema() == other.schema()
            and self._predicate == other._predicate
            and self._columns == other._columns
            and self._source_info == other._source_info
            and self._filepaths_column_name == other._filepaths_column_name
        )

    def rebuild(self) -> LogicalPlan:
        child = self._filepaths_child.rebuild()
        return TabularFilesScan(
            schema=self.schema(),
            source_info=self._source_info,
            fs=self._fs,
            predicate=self._predicate if self._predicate is not None else None,
            columns=self._column_names,
            filepaths_child=child,
            filepaths_column_name=self._filepaths_column_name,
        )

    def copy_with_new_children(self, new_children: list[LogicalPlan]) -> LogicalPlan:
        assert len(new_children) == 1
        return TabularFilesScan(
            schema=self.schema(),
            source_info=self._source_info,
            fs=self._fs,
            predicate=self._predicate,
            columns=self._column_names,
            filepaths_child=new_children[0],
            filepaths_column_name=self._filepaths_column_name,
        )


class InMemoryScan(UnaryNode):
    def __init__(
        self, cache_entry: PartitionCacheEntry, schema: Schema, partition_spec: PartitionSpec | None = None
    ) -> None:
        if partition_spec is None:
            partition_spec = PartitionSpec(scheme=PartitionScheme.UNKNOWN, num_partitions=1)

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
        storage_type: StorageType,
        partition_cols: ExpressionsProjection | None = None,
        compression: str | None = None,
    ) -> None:
        assert (
            storage_type == StorageType.PARQUET or storage_type == StorageType.CSV
        ), "only parquet and csv is supported currently"
        self._storage_type = storage_type
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
            and self._storage_type == other._storage_type
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
            storage_type=self._storage_type,
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
        pspec = PartitionSpec(scheme=PartitionScheme.RANGE, num_partitions=input.num_partitions(), by=sort_by)
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


class PartitionScheme(Enum):
    UNKNOWN = "UNKNOWN"
    RANGE = "RANGE"
    HASH = "HASH"
    RANDOM = "RANDOM"

    def __repr__(self) -> str:
        return self.value


@dataclass(frozen=True)
class PartitionSpec:
    scheme: PartitionScheme
    num_partitions: int
    by: ExpressionsProjection | None = None


class Repartition(UnaryNode):
    def __init__(
        self, input: LogicalPlan, partition_by: ExpressionsProjection, num_partitions: int, scheme: PartitionScheme
    ) -> None:
        pspec = PartitionSpec(
            scheme=scheme,
            num_partitions=num_partitions,
            by=partition_by if len(partition_by) > 0 else None,
        )
        super().__init__(input.schema(), partition_spec=pspec, op_level=OpLevel.GLOBAL)
        self._register_child(input)
        self._partition_by = partition_by
        self._scheme = scheme
        if scheme in (PartitionScheme.RANDOM, PartitionScheme.UNKNOWN) and len(partition_by.to_name_set()) > 0:
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
            scheme=PartitionScheme.UNKNOWN,
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
        pspec = PartitionSpec(scheme=PartitionScheme.UNKNOWN, num_partitions=1)
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


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"


class Join(BinaryNode):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.INNER,
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
        if how == JoinType.LEFT:
            num_partitions = left.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.RIGHT:
            num_partitions = right.num_partitions()
            raise NotImplementedError()
        elif how == JoinType.INNER:
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

        left_pspec = PartitionSpec(scheme=PartitionScheme.HASH, num_partitions=num_partitions, by=self._left_on)
        right_pspec = PartitionSpec(scheme=PartitionScheme.HASH, num_partitions=num_partitions, by=self._right_on)

        new_left = Repartition(
            left, partition_by=self._left_on, num_partitions=num_partitions, scheme=PartitionScheme.HASH
        )

        if num_partitions == 1 and left.num_partitions() == 1:
            left = left
        elif left.partition_spec() != left_pspec:
            left = new_left

        new_right = Repartition(
            right, partition_by=self._right_on, num_partitions=num_partitions, scheme=PartitionScheme.HASH
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
