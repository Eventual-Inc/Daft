from __future__ import annotations

from daft.datarepo.log import DaftLakeLog
import networkx as NX
import ray

import daft
from daft.datarepo.query import stages
from daft.datarepo.query.definitions import NodeId, FilterPredicate, QueryColumn

from typing import Callable, Type


class DatarepoQueryBuilder:
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
    def _from_datarepo_log(cls, daft_lake_log: DaftLakeLog, dtype: Type) -> DatarepoQueryBuilder:
        tree = NX.DiGraph()
        stage = stages.GetDatarepoStage(
            daft_lake_log=daft_lake_log,
            dtype=dtype,
        )
        node_id, tree = stage.add_root(tree, "")
        return cls(query_tree=tree, root=node_id)

    def filter(self, predicate: FilterPredicate) -> DatarepoQueryBuilder:
        """Filters the query

        Predicates are provided as simple SQL-compatible strings, for example:

        1. `"id > 5"`: Filters for all data where the ID column is greater than 5
        2. `"id > 5 AND id < 3"`: Filters for all data where the ID column is greater than 5 and smaller than 3
        3. `"(id > 5 AND id < 3) OR id is NULL"`: Same as (2), but includes all results where id is None

        More complex filters requiring processing of data can be achieved using `.apply` to first transform the data
        and then `.filter` to apply a simple filter on transformed data. For example, running an ML model on images and
        running a filter on the classification scores.

        Args:
            predicate (FilterPredicate): _description_

        Returns:
            DatarepoQueryBuilder: _description_
        """
        stage = stages.FilterStage(predicate=predicate)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def apply(self, func: Callable, *args: QueryColumn, **kwargs: QueryColumn) -> DatarepoQueryBuilder:
        stage = stages.ApplyStage(f=func, args=args, kwargs=kwargs)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def limit(self, limit: int) -> DatarepoQueryBuilder:
        stage = stages.LimitStage(limit=limit)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    ###
    # Execution methods: methods that trigger computation
    ###

    def to_daft_dataset(self) -> daft.Dataset:
        ds = _execute_query_tree(self._query_tree, self._root)
        return daft.Dataset(dataset_id="query_results", ray_dataset=ds)


def _execute_query_tree(tree: NX.DiGraph, root: NodeId) -> ray.data.Dataset:
    """Executes the stages in a query tree recursively in a DFS

    Args:
        tree (NX.DiGraph): query tree to execute
        root (NodeId): root of the query tree to execute

    Returns:
        ray.data.Dataset: Dataset obtained from the execution
    """
    stage: stages.QueryStage = tree.nodes[root]["stage"]
    children = tree.out_edges(root)

    # Base case: leaf nodes with no children run with no inputs
    # Recursive case: recursively resolve inputs for each child
    inputs = {tree.edges[root, child]["key"]: _execute_query_tree(tree, child) for _, child in children}
    return stage.run(inputs)
