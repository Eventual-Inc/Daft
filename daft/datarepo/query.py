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


@dataclasses.dataclass(frozen=True)
class QueryColumn:
    name: str


class _QueryStage(Protocol):
    def process(self, ds: Optional[ray.data.Dataset]) -> Optional[ray.data.Dataset]:
        ...


@dataclasses.dataclass
class _GetDatarepoStage(_QueryStage):
    daft_lake_log: DaftLakeLog
    dtype: Type

    def __post_init__(self):
        if not dataclasses.is_dataclass(self.dtype):
            raise ValueError(f"{self.dtype} is not a Daft Dataclass")

    def process(self, ds: Optional[ray.data.Dataset]) -> Optional[ray.data.Dataset]:
        assert ds is None, "_GetDatarepoStage does not take a dataset as input"
        files = self.daft_lake_log.file_list()
        daft_schema = cast(DaftSchema, getattr(self.dtype, "_daft_schema", None))
        assert daft_schema is not None, f"{self.dtype} is not a Daft Dataclass"
        ds = ray.data.read_parquet(files, schema=self.daft_lake_log.schema())
        return ds.map_batches(lambda batch: daft_schema.deserialize_batch(batch, self.dtype), batch_format="pyarrow")


@dataclasses.dataclass
class _FilterStage(_QueryStage):
    predicate: _FilterPredicate

    def process(self, ds: Optional[ray.data.Dataset]) -> Optional[ray.data.Dataset]:
        if ds is None:
            return None
        return ds.filter(self.predicate.get_callable())


@dataclasses.dataclass
class _LimitStage(_QueryStage):
    limit: int

    def process(self, ds: Optional[ray.data.Dataset]) -> Optional[ray.data.Dataset]:
        if ds is None:
            return None
        return ds.limit(self.limit)


@dataclasses.dataclass
class _ApplyStage(_QueryStage):
    f: Callable
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]

    def process(self, ds: Optional[ray.data.Dataset]) -> Optional[ray.data.Dataset]:
        if ds is None:
            return None
        return ds.map(
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
        root: Optional[str],
    ) -> None:
        # The Query is structured as a Query Tree
        # 1. The root represents the query as a whole
        # 2. Leaf nodes are Datarepos
        # 3. Other nodes are Operations
        self._query_tree = query_tree
        self._root = root

    @classmethod
    def _from_datarepo_log(cls, daft_lake_log: DaftLakeLog, dtype: Type) -> DatarepoQuery:
        node_id = str(uuid.uuid4())
        tree = NX.DiGraph()
        tree.add_node(
            node_id,
            stage=_GetDatarepoStage(
                daft_lake_log=daft_lake_log,
                dtype=dtype,
            ),
        )
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
        node_id = str(uuid.uuid4())
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node_id, stage=_FilterStage(predicate=predicate))
        tree_copy.add_edge(node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node_id)

    def apply(self, func: Callable, *args: QueryColumn, **kwargs: QueryColumn) -> DatarepoQuery:
        node_id = str(uuid.uuid4())
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node_id, stage=_ApplyStage(f=func, args=args, kwargs=kwargs))
        tree_copy.add_edge(node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node_id)

    def limit(self, limit: int) -> DatarepoQuery:
        node_id = str(uuid.uuid4())
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node_id, stage=_LimitStage(limit=limit))
        tree_copy.add_edge(node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node_id)

    ###
    # Execution methods: methods that trigger computation
    ###

    def to_daft_dataset(self) -> daft.Dataset:
        # TODO: We currently naively serialize the query into a linear list of stages which
        # can be executed in an eager, serial fashion. Each stage is simply an object that
        # takes a Ray dataset and outputs another Ray dataset. This can be optimized a lot.
        dfs_query_tree = NX.dfs_tree(self._query_tree, source=self._root)
        stages = reversed([self._query_tree.nodes[node_id]["stage"] for node_id in dfs_query_tree.nodes])
        ds = None
        for stage in stages:
            ds = stage.process(ds)
        assert ds is not None
        return daft.Dataset(dataset_id="query_results", ray_dataset=ds)
