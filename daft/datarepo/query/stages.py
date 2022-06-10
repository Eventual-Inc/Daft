import dataclasses
import uuid
import enum

import networkx as NX
import ray
import pyarrow.dataset as pads
import pyarrow.compute as pc

from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query import functions as F
from daft.datarepo.query.definitions import Comparator, NodeId, QueryColumn, COMPARATOR_MAP, WriteDatarepoStageOutput
from daft.schema import DaftSchema

from typing import Any, Dict, Type, Tuple, cast, Protocol, Callable, Optional, Union, List, Literal, ForwardRef


DEFAULT_ACTOR_STRATEGY: Callable[[], ray.data.ActorPoolStrategy] = lambda: ray.data.ActorPoolStrategy(
    min_size=1,
    max_size=ray.cluster_resources()["CPU"],
)


class StageType(enum.Enum):
    GetDatarepo = "get_datarepo"
    Limit = "limit"
    WithColumn = "with_column"
    Where = "where"
    WriteDatarepo = "write_datarepo"


class QueryStage(Protocol):
    def type(self) -> StageType:
        ...

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        ...

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        ...

    def __repr__(self) -> str:
        ...


def _query_stage_repr(stage_type: StageType, args: Dict[str, Any]) -> str:
    return "\n".join(
        [
            f"* {stage_type.name}",
            *[f"| {arg_key}: {arg}" for arg_key, arg in args.items()],
        ]
    )


@dataclasses.dataclass
class GetDatarepoStage(QueryStage):
    daft_lake_log: DaftLakeLog
    dtype: Type
    read_limit: Optional[int] = None
    # Filters in DNF form, see the `filters` kwarg in:
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    filters: Optional[List[List[Tuple]]] = None

    def __post_init__(self):
        if not dataclasses.is_dataclass(self.dtype):
            raise ValueError(f"{self.dtype} is not a Daft Dataclass")

    def _get_arrow_filter_expression(self) -> Optional[pads.Expression]:
        if self.filters is None:
            return None

        final_expr = None

        for conjunction_filters in self.filters:
            conjunctive_expr = None
            for raw_filter in conjunction_filters:
                col, op, val = raw_filter
                expr = getattr(pc.field(f"root.{col}"), COMPARATOR_MAP[op])(val)
                if conjunctive_expr is None:
                    conjunctive_expr = expr
                else:
                    conjunctive_expr = conjunctive_expr & expr

            if final_expr is None:
                final_expr = conjunctive_expr
            else:
                final_expr = final_expr | conjunctive_expr

        return final_expr

    def type(self) -> StageType:
        return StageType.GetDatarepo

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        assert len(query_tree.nodes) == 0, "can only add _GetDatarepoStage to empty query tree"
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(
            node_id,
            stage=self,
        )
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert len(input_stage_results) == 0, "GetDatarepoStage does not take in inputs"
        files = self.daft_lake_log.file_list()
        daft_schema = cast(DaftSchema, getattr(self.dtype, "_daft_schema", None))
        assert daft_schema is not None, f"{self.dtype} is not a Daft Dataclass"
        ds: ray.data.Dataset = ray.data.read_parquet(
            files,
            schema=self.daft_lake_log.schema(),
            # Dataset kwargs passed to Pyarrow Parquet Dataset
            dataset_kwargs={"filters": self.filters},
            # Reader kwargs passed to Pyarrow Scanner.from_fragment
            filter=self._get_arrow_filter_expression(),
        )

        if self.read_limit is not None:
            ds = ds.limit(self.read_limit)

        # If the cluster has more CPUs available than the number of blocks, repartition the
        # dataset to take advantage of the full parallelism afforded by the cluster
        cluster_cpus = ray.cluster_resources().get("CPU", -1)
        if cluster_cpus != -1 and ds.num_blocks() < cluster_cpus and ds.count() > cluster_cpus:
            ds = ds.repartition(int(cluster_cpus * 2), shuffle=True)

        return ds.map_batches(
            lambda batch: daft_schema.deserialize_batch(batch, self.dtype),
            batch_format="pyarrow",
        )

    def __repr__(self) -> str:
        args = {
            "datarepo": self.daft_lake_log.path,
        }
        if self.read_limit is not None:
            args["read_limit"] = str(self.read_limit)
        if self.filters is not None:
            args["filters"] = " or ".join(
                ["(" + " and ".join([f"{col} {op} {val}" for col, op, val in conj]) + ")" for conj in self.filters]
            )
        return _query_stage_repr(self.type(), args)


@dataclasses.dataclass
class WhereStage(QueryStage):
    column: QueryColumn
    operation: Comparator
    value: Union[str, int, float]

    def type(self) -> StageType:
        return StageType.Where

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"WhereStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.filter(self._get_filter_func())

    def _get_filter_func(self) -> Callable[[Any], bool]:
        """Converts the where clause into a lambda that can be used to filter a dataset"""

        def f(x: Any) -> bool:
            comparator_magic_method = COMPARATOR_MAP[self.operation]
            if dataclasses.is_dataclass(x):
                return cast(bool, getattr(getattr(x, self.column), comparator_magic_method)(self.value))
            return cast(bool, getattr(x[self.column], comparator_magic_method)(self.value))

        return f

    def __repr__(self) -> str:
        args = {
            "predicate": f"{self.column} {self.operation} {self.value}",
        }
        return _query_stage_repr(self.type(), args)


@dataclasses.dataclass
class LimitStage(QueryStage):
    limit: int

    def type(self) -> StageType:
        return StageType.Limit

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"LimitStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.limit(self.limit)

    def __repr__(self) -> str:
        args = {
            "limit": str(self.limit),
        }
        return _query_stage_repr(self.type(), args)


@dataclasses.dataclass
class WithColumnStage(QueryStage):
    new_column: QueryColumn
    expr: F.QueryExpression
    dataclass: Type

    def _get_compute_strategy(self) -> Union[Literal["tasks"], ray.data.ActorPoolStrategy]:
        """Returns the appropriate compute_strategy for the function in this stage's QueryExpression

        Callables can either be a Function, or a Class which defines an .__init__() and a .__call__()
        We handle both cases by constructing the appropriate compute_strategy and passing that to Ray.
        """
        return DEFAULT_ACTOR_STRATEGY() if isinstance(self.expr.func, type) else "tasks"

    def type(self) -> StageType:
        return StageType.WithColumn

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"WithColumnStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]

        compute_strategy = self._get_compute_strategy()
        if self.expr.batch_size is None and compute_strategy == "tasks":
            return prev.map(
                WithColumnStage._get_func_wrapper(self.new_column, self.expr, self.dataclass), compute=compute_strategy
            )
        elif self.expr.batch_size is None:
            assert isinstance(compute_strategy, ray.data.ActorPoolStrategy)
            return prev.map(
                WithColumnStage._get_actor_wrapper(self.new_column, self.expr, self.dataclass),
                compute=compute_strategy,  # type: ignore
            )
        elif compute_strategy == "tasks":
            return prev.map_batches(
                WithColumnStage._get_batched_func_wrapper(self.new_column, self.expr, self.dataclass),  # type: ignore
                compute=compute_strategy,
                batch_size=self.expr.batch_size,
            )
        else:
            assert isinstance(compute_strategy, ray.data.ActorPoolStrategy)
            return prev.map_batches(
                WithColumnStage._get_batched_actor_wrapper(self.new_column, self.expr, self.dataclass),
                compute=compute_strategy,
                batch_size=self.expr.batch_size,
            )

    def __repr__(self) -> str:
        ret_type = self.expr.return_type if self.expr.batch_size is None else self.expr.return_type.__args__[0]
        args_str = tuple(f"`{arg}`" for arg in self.expr.args)
        kwargs_str = tuple(f"{key}=`{col}`" for key, col in self.expr.kwargs.items())
        args = {
            "column": f"`{self.new_column}`",
            "expression": f"{self.expr.func.__name__}({', '.join(args_str + kwargs_str)}) -> {ret_type}",
        }
        return _query_stage_repr(self.type(), args)

    def __eq__(self, other: Any) -> str:
        if not isinstance(other, WithColumnStage):
            return False
        return (
            other.expr == self.expr
            and other.new_column == self.new_column
            and other.dataclass.__name__ == self.dataclass.__name__
            and [(field.name, field.type) for field in dataclasses.fields(other.dataclass)]
            == [(field.name, field.type) for field in dataclasses.fields(self.dataclass)]
        )

    @staticmethod
    def _get_actor_wrapper(
        new_column: QueryColumn, expr: F.QueryExpression, new_dataclass: Type
    ) -> Type[Callable[[Any], Any]]:
        assert isinstance(expr.func, type), "must wrap an actor class"
        old_fields = [f for f in new_dataclass.__dataclass_fields__ if f != new_column]

        class ActorWrapper:
            def __init__(self):
                self._actor = expr.func()

            def __call__(self, item):
                args_batched = [getattr(item, query_column) for query_column in expr.args]
                kwargs_batched = {key: getattr(item, query_column) for key, query_column in expr.kwargs.items()}
                value = self._actor(*args_batched, **kwargs_batched)

                new_item = new_dataclass(
                    **{field: getattr(item, field) for field in old_fields},
                    **{new_column: value},
                )
                return new_item

        return ActorWrapper

    @staticmethod
    def _get_batched_actor_wrapper(
        new_column: QueryColumn, expr: F.QueryExpression, new_dataclass: Type
    ) -> Type[Callable[[List[Any]], List[Any]]]:
        assert isinstance(expr.func, type), "must wrap an actor class"
        old_fields = [f for f in new_dataclass.__dataclass_fields__ if f != new_column]

        class BatchActorWrapper:
            def __init__(self):
                self._actor = expr.func()

            def __call__(self, items):
                args_batched = [[getattr(item, query_column) for item in items] for query_column in expr.args]
                kwargs_batched = {
                    key: [getattr(item, query_column) for item in items] for key, query_column in expr.kwargs.items()
                }
                values = self._actor(*args_batched, **kwargs_batched)
                new_items = [
                    new_dataclass(
                        **{field: getattr(item, field) for field in old_fields},
                        **{new_column: value},
                    )
                    for item, value in zip(items, values)
                ]
                return new_items

        return BatchActorWrapper

    @staticmethod
    def _get_func_wrapper(
        new_column: QueryColumn, expr: F.QueryExpression, new_dataclass: Type
    ) -> Callable[[Any], Any]:
        old_fields = [f for f in new_dataclass.__dataclass_fields__ if f != new_column]

        def with_column_func(item):
            value = expr.func(
                *[getattr(item, query_column) for query_column in expr.args],
                **{key: getattr(item, query_column) for key, query_column in expr.kwargs.items()},
            )
            new_item = new_dataclass(
                **{field: getattr(item, field) for field in old_fields},
                **{new_column: value},
            )
            return new_item

        return with_column_func

    @staticmethod
    def _get_batched_func_wrapper(
        new_column: QueryColumn, expr: F.QueryExpression, new_dataclass: Type
    ) -> Callable[[List[Any]], List[Any]]:
        old_fields = [f for f in new_dataclass.__dataclass_fields__ if f != new_column]

        def with_column_func_batched(items):
            values = expr.func(
                *[[getattr(item, query_column) for item in items] for query_column in expr.args],
                **{key: [getattr(item, query_column) for item in items] for key, query_column in expr.kwargs.items()},
            )
            new_items = [
                new_dataclass(
                    **{field: getattr(item, field) for field in old_fields},
                    **{new_column: value},
                )
                for item, value in zip(items, values)
            ]
            return new_items

        return with_column_func_batched


@dataclasses.dataclass
class WriteDatarepoStage(QueryStage):
    mode: Union[Literal["overwrite"], Literal["append"]]
    datarepo: ForwardRef("DataRepo")
    rows_per_partition: int
    dtype: type

    def type(self) -> StageType:
        return StageType.WriteDatarepo

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"WithColumnStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        field_names = [field for field in self.dtype.__dataclass_fields__]
        prev = prev.map(lambda item: self.dtype(**{field: getattr(item, field) for field in field_names}))

        filepaths: List[str]
        if self.mode == "overwrite":
            filepaths = self.datarepo.overwrite(prev, rows_per_partition=self.rows_per_partition)
        elif self.mode == "append":
            filepaths = self.datarepo.append(prev, rows_per_partition=self.rows_per_partition)
        else:
            raise NotImplementedError(f"Not implemented writing mode {self.mode}")
        return ray.data.from_items([WriteDatarepoStageOutput(filepath=filepath) for filepath in filepaths])

    def __repr__(self) -> str:
        args = {
            "datarepo_name": self.datarepo.name(),
            "mode": self.mode,
            "rows_per_partition": self.rows_per_partition,
        }
        return _query_stage_repr(self.type(), args)
