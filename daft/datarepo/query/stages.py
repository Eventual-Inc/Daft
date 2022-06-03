import dataclasses
import uuid

import networkx as NX
import ray

from daft.datarepo.log import DaftLakeLog
from daft.datarepo.query.definitions import NodeId, FilterPredicate, QueryColumn
from daft.schema import DaftSchema

from typing import Dict, Type, Tuple, cast, Protocol, Callable


class QueryStage(Protocol):
    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        ...

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        ...


@dataclasses.dataclass
class GetDatarepoStage(QueryStage):
    daft_lake_log: DaftLakeLog
    dtype: Type

    def __post_init__(self):
        if not dataclasses.is_dataclass(self.dtype):
            raise ValueError(f"{self.dtype} is not a Daft Dataclass")

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
        assert len(input_stage_results) == 0, "_GetDatarepoStage does not take in inputs"
        files = self.daft_lake_log.file_list()
        daft_schema = cast(DaftSchema, getattr(self.dtype, "_daft_schema", None))
        assert daft_schema is not None, f"{self.dtype} is not a Daft Dataclass"
        ds: ray.data.Dataset = ray.data.read_parquet(files, schema=self.daft_lake_log.schema())
        return ds.map_batches(
            lambda batch: daft_schema.deserialize_batch(batch, self.dtype),
            batch_format="pyarrow",
        )


@dataclasses.dataclass
class FilterStage(QueryStage):
    predicate: FilterPredicate

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"_FilterStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.filter(self.predicate.get_callable())


@dataclasses.dataclass
class LimitStage(QueryStage):
    limit: int

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"_LimitStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.limit(self.limit)


@dataclasses.dataclass
class ApplyStage(QueryStage):
    f: Callable
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]

    def add_root(self, query_tree: NX.DiGraph, root_node: NodeId) -> Tuple[NodeId, NX.DiGraph]:
        node_id = str(uuid.uuid4())
        tree_copy = query_tree.copy()
        tree_copy.add_node(node_id, stage=self)
        tree_copy.add_edge(node_id, root_node, key="prev")
        return node_id, tree_copy

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        assert (
            len(input_stage_results) == 1 and "prev" in input_stage_results
        ), f"_ApplyStage.run expects one input named 'prev', found: {input_stage_results.keys()}"
        prev = input_stage_results["prev"]
        return prev.map(
            lambda x: self.f(
                *[getattr(x, qc.name) for qc in self.args],
                **{key: getattr(x, qc.name) for key, qc in self.kwargs.items()},
            )
        )
