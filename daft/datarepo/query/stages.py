import dataclasses
import uuid
import enum

import networkx as NX
import ray
import pyarrow.dataset as pads
import pyarrow.compute as pc

from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query import functions as F
from daft.datarepo.query.definitions import Comparator, NodeId, QueryColumn, COMPARATOR_MAP
from daft.schema import DaftSchema

from typing import Any, Dict, Type, Tuple, cast, Protocol, Callable, Optional, Union, List


class StageType(enum.Enum):
    GetDatarepoStageType = "get_datarepo"
    LimitStageType = "limit"
    ApplyStageType = "apply"
    WithColumnStageType = "with_column"
    WhereStageType = "where"


class QueryStage(Protocol):
    def type(self) -> StageType:
        ...

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        ...

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        ...


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
        return StageType.GetDatarepoStageType

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


@dataclasses.dataclass
class WhereStage(QueryStage):
    column: QueryColumn
    operation: Comparator
    value: Union[str, int, float]

    def type(self) -> StageType:
        return StageType.WhereStageType

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


@dataclasses.dataclass
class LimitStage(QueryStage):
    limit: int

    def type(self) -> StageType:
        return StageType.LimitStageType

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


@dataclasses.dataclass
class ApplyStage(QueryStage):
    f: Callable
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]

    def type(self) -> StageType:
        return StageType.ApplyStageType

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"ApplyStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.map(
            lambda x: self.f(
                *[getattr(x, query_column) for query_column in self.args],
                **{key: getattr(x, query_column) for key, query_column in self.kwargs.items()},
            )
        )


@dataclasses.dataclass
class WithColumnStage(QueryStage):
    new_column: QueryColumn
    expr: F.QueryExpression

    def type(self) -> StageType:
        return StageType.WithColumnStageType

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

        def with_column_func(item):
            value = self.expr.func(
                *[getattr(item, query_column) for query_column in self.expr.args],
                **{key: getattr(item, query_column) for key, query_column in self.expr.kwargs.items()},
            )
            setattr(item, self.new_column, value)
            return item

        return prev.map(with_column_func)
