from __future__ import annotations

import dataclasses
from daft.datarepo.client import DatarepoClient, get_client
from daft.datarepo.log import DaftLakeLog
from daft.schema import DaftSchema
import networkx as NX
import uuid
import ray

import daft

from typing import Any, Callable, Tuple, Dict, Optional, Protocol, Type, cast


_NodeId = str


@dataclasses.dataclass(frozen=True)
class QueryColumn:
    name: str


class _QueryStage(Protocol):
    def add_root(self, query_tree: NX.DiGraph, root_node: _NodeId) -> Tuple[_NodeId, NX.DiGraph]:
        ...

    def run(self, input_stage_results: Dict[str, ray.data.Dataset]) -> ray.data.Dataset:
        ...


@dataclasses.dataclass
class _GetDatarepoStage(_QueryStage):
    daft_lake_log: DaftLakeLog
    dtype: Type

    def __post_init__(self):
        if not dataclasses.is_dataclass(self.dtype):
            raise ValueError(f"{self.dtype} is not a Daft Dataclass")

    def add_root(self, query_tree: NX.DiGraph, root_node: _NodeId) -> Tuple[_NodeId, NX.DiGraph]:
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
class _FilterStage(_QueryStage):
    predicate: _FilterPredicate

    def add_root(self, query_tree: NX.DiGraph, root_node: _NodeId) -> Tuple[_NodeId, NX.DiGraph]:
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
class _LimitStage(_QueryStage):
    limit: int

    def add_root(self, query_tree: NX.DiGraph, root_node: _NodeId) -> Tuple[_NodeId, NX.DiGraph]:
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
class _ApplyStage(_QueryStage):
    f: Callable
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]

    def add_root(self, query_tree: NX.DiGraph, root_node: _NodeId) -> Tuple[_NodeId, NX.DiGraph]:
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


_COMPARATOR_MAP = {
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "=": "__eq__",
}


@dataclasses.dataclass(frozen=True)
class _FilterPredicate:
    """Predicate describing a condition for operations such as a filter or join"""

    left: str
    comparator: str
    right: str

    def __post_init__(self):
        if self.comparator not in _COMPARATOR_MAP:
            raise ValueError(f"Comparator {self.comparator} not found in accepted comparators {_COMPARATOR_MAP.keys()}")

    def get_callable(self) -> Callable[[Any], bool]:
        def f(x: Any) -> bool:
            comparator_magic_method = _COMPARATOR_MAP[self.comparator]
            if dataclasses.is_dataclass(x):
                return cast(bool, getattr(getattr(x, self.left), comparator_magic_method)(self.right))
            return cast(bool, getattr(x[self.left], comparator_magic_method)(self.right))

        return f


class DatarepoQuery:
    def __init__(
        self,
        query_tree: NX.DiGraph,
        root: str,
    ) -> None:
        # The Query is structured as a Query Tree
        # 1. The root represents the query as a whole
        # 2. Leaf nodes are Datarepos
        # 3. Other nodes are Operations
        self._query_tree = query_tree
        self._root = root

    @classmethod
    def _from_datarepo_log(cls, daft_lake_log: DaftLakeLog, dtype: Type) -> DatarepoQuery:
        tree = NX.DiGraph()
        stage = _GetDatarepoStage(
            daft_lake_log=daft_lake_log,
            dtype=dtype,
        )
        node_id, tree = stage.add_root(tree, "")
        return cls(query_tree=tree, root=node_id)

    def filter(self, predicate: _FilterPredicate) -> DatarepoQuery:
        """Filters the query

        Predicates are provided as simple SQL-compatible strings, for example:

        1. `"id > 5"`: Filters for all data where the ID column is greater than 5
        2. `"id > 5 AND id < 3"`: Filters for all data where the ID column is greater than 5 and smaller than 3
        3. `"(id > 5 AND id < 3) OR id is NULL"`: Same as (2), but includes all results where id is None

        More complex filters requiring processing of data can be achieved using `.apply` to first transform the data
        and then `.filter` to apply a simple filter on transformed data. For example, running an ML model on images and
        running a filter on the classification scores.

        Args:
            predicate (_FilterPredicate): _description_

        Returns:
            DatarepoQuery: _description_
        """
        stage = _FilterStage(predicate=predicate)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQuery(query_tree=tree, root=node_id)

    def apply(self, func: Callable, *args: QueryColumn, **kwargs: QueryColumn) -> DatarepoQuery:
        stage = _ApplyStage(f=func, args=args, kwargs=kwargs)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQuery(query_tree=tree, root=node_id)

    def limit(self, limit: int) -> DatarepoQuery:
        stage = _LimitStage(limit=limit)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQuery(query_tree=tree, root=node_id)

    ###
    # Execution methods: methods that trigger computation
    ###

    def to_daft_dataset(self) -> daft.Dataset:
        ds = _execute_query_tree(self._query_tree, self._root)
        return daft.Dataset(dataset_id="query_results", ray_dataset=ds)


def _execute_query_tree(tree: NX.DiGraph, root: _NodeId) -> ray.data.Dataset:
    """Executes the stages in a query tree recursively in a DFS

    Args:
        tree (NX.DiGraph): query tree to execute
        root (_NodeId): root of the query tree to execute

    Returns:
        ray.data.Dataset: Dataset obtained from the execution
    """
    stage: _QueryStage = tree.nodes[root]["stage"]
    children = tree.out_edges(root)

    # Base case: leaf nodes with no children run with no inputs
    # Recursive case: recursively resolve inputs for each child
    inputs = {tree.edges[root, child]["key"]: _execute_query_tree(tree, child) for _, child in children}
    return stage.run(inputs)
