from __future__ import annotations
import dataclasses
from platform import node

from daft.datarepo.log import DaftLakeLog
import networkx as NX
import ray

from daft.datarepo.query import stages, tree_ops
from daft.datarepo.query.definitions import NodeId, QueryColumn, Comparator
from daft.datarepo.query import functions as F

from typing import Callable, Type, Tuple, List, Union, cast


class DatarepoQueryBuilder:
    def __init__(
        self,
        query_tree: NX.DiGraph,
        root: str,
    ) -> None:
        # The Query is structured as a Query Tree
        # 1. There is one root, which holds the entire query
        # 2. Leaf nodes are stages that read from other queries or Datarepos
        # 3. Other nodes are Operations that process data
        # 4. Edges hold a "key" attribute which defines the name of the input to a given operation
        self._query_tree = query_tree
        self._root = root

    @classmethod
    def _from_datarepo_log(cls, daft_lake_log: DaftLakeLog, dtype: Type) -> DatarepoQueryBuilder:
        """Initializes a DatarepoQueryBuilder with the root node being a GetDatarepoStage

        This should not be called by users. Users should access queries through the Datarepo.query() API.

        Args:
            daft_lake_log (DaftLakeLog): log of the Datarepo to query
            dtype (Type): Dataclass to query

        Returns:
            DatarepoQueryBuilder: initialized DatarepoQueryBuilder
        """
        tree = NX.DiGraph()
        stage = stages.GetDatarepoStage(
            daft_lake_log=daft_lake_log,
            dtype=dtype,
            read_limit=None,
            filters=None,
        )
        node_id, tree = stage.add_root(tree, "")
        return cls(query_tree=tree, root=node_id)

    def where(self, column: QueryColumn, operation: Comparator, value: Union[str, float, int]):
        """Filters the query with a simple predicate.

        Args:
            column (QueryColumn): column to filter on
            operation (Comparator): operation to compare the column with
            value (Union[str, float, int]): value to compare the column with
        """
        stage = stages.WhereStage(column, operation, value)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def apply(self, func: Callable, *args: QueryColumn, **kwargs: QueryColumn) -> DatarepoQueryBuilder:
        """Applies a function on specified columns of the data in the query"""
        stage = stages.ApplyStage(f=func, args=args, kwargs=kwargs)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def with_column(self, new_column: QueryColumn, expr: F.QueryExpression) -> DatarepoQueryBuilder:
        """Creates a new column with value derived from the specified expression"""
        stage = stages.WithColumnStage(new_column=new_column, expr=expr)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def with_column_batched(self, new_column: QueryColumn, expr: F.QueryExpression, batch_size: int = 32) -> DatarepoQueryBuilder:
        """Creates a new column with value derived from the specified expression, and executes the function
        encapsulated in the expression on batches of data instead of single items
        """
        stage = stages.WithColumnStage(new_column=new_column, expr=expr, batch_size=batch_size)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def limit(self, limit: int) -> DatarepoQueryBuilder:
        """Limits the number of rows in the query"""
        stage = stages.LimitStage(limit=limit)
        node_id, tree = stage.add_root(self._query_tree, self._root)
        return DatarepoQueryBuilder(query_tree=tree, root=node_id)

    def execute(self) -> ray.data.Dataset:
        """Executes the query and returns it as a Daft dataset"""
        tree, root = self._optimize_query_tree()
        ds = _execute_query_tree(tree, root)
        return ds

    def _optimize_query_tree(self) -> Tuple[NX.DiGraph, NodeId]:
        """Optimize the current query tree and returns the optimized copy"""
        tree, root = self._query_tree, self._root
        tree, root = _limit_pushdown(tree, root)
        tree, root = _where_pushdown(tree, root)
        return tree, root


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


def _where_pushdown(tree: NX.DiGraph, root: NodeId) -> Tuple[NX.DiGraph, NodeId]:
    """Fuses as `WhereStage`s with the lowest possible read operation"""
    tree = tree.copy()

    # Find all WhereStages and the lowest read stage
    where_node_ids = tree_ops.dfs_search(
        tree, root, lambda data: cast(stages.QueryStage, data["stage"]).type() == stages.StageType.WhereStageType
    )
    read_node_ids = tree_ops.dfs_search(
        tree, root, lambda data: cast(stages.QueryStage, data["stage"]).type() == stages.StageType.GetDatarepoStageType
    )

    assert len(read_node_ids) == 1, "where pushdown only supports one read node for now"
    lowest_read_node_id = read_node_ids[0]

    # If no WhereStage was found, no work to be done
    if len(where_node_ids) == 0:
        return tree, root

    # AND all the WhereStages that we found in DNF form, currently no support for OR
    conjuction_predicates = []
    for where_stage in [tree.nodes[where_node_id]["stage"] for where_node_id in where_node_ids]:
        predicate = (where_stage.column, where_stage.operation, where_stage.value)
        conjuction_predicates.append(predicate)
    dnf_filter = [conjuction_predicates]

    # Prune all WhereStages from tree
    for where_node_id in where_node_ids:
        tree, root = tree_ops.prune_singlechild_node(tree, root, where_node_id)

    # Set filter on the lowest read node
    lowest_read_stage: stages.GetDatarepoStage = tree.nodes[lowest_read_node_id]["stage"]
    filtered_read_stage = dataclasses.replace(lowest_read_stage, filters=dnf_filter)
    NX.set_node_attributes(tree, {lowest_read_node_id: {"stage": filtered_read_stage}})

    return tree, root


def _limit_pushdown(tree: NX.DiGraph, root: NodeId) -> Tuple[NX.DiGraph, NodeId]:
    """Fuses any `LimitStage`s with the lowest possible read operation"""
    tree = tree.copy()

    # Find all LimitStages and the lowest read stage
    limit_node_ids = tree_ops.dfs_search(
        tree, root, lambda data: cast(stages.QueryStage, data["stage"]).type() == stages.StageType.LimitStageType
    )
    read_node_ids = tree_ops.dfs_search(
        tree, root, lambda data: cast(stages.QueryStage, data["stage"]).type() == stages.StageType.GetDatarepoStageType
    )

    assert len(read_node_ids) == 1, "limit pushdown only supports one read node for now"
    lowest_read_node_id = read_node_ids[0]

    # If no LimitStage was found, no work to be done
    if len(limit_node_ids) == 0:
        return tree, root

    # Get minimum limit
    min_limit = min([tree.nodes[limit_node_id]["stage"].limit for limit_node_id in limit_node_ids])

    # Prune all LimitStages from tree
    for limit_node_id in limit_node_ids:
        tree, root = tree_ops.prune_singlechild_node(tree, root, limit_node_id)

    # Set limit on the lowest read node
    lowest_read_stage: stages.GetDatarepoStage = tree.nodes[lowest_read_node_id]["stage"]
    limited_read_stage = dataclasses.replace(lowest_read_stage, read_limit=min_limit)
    NX.set_node_attributes(tree, {lowest_read_node_id: {"stage": limited_read_stage}})

    return tree, root
