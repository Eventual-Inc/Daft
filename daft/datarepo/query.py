from __future__ import annotations

import dataclasses
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

import networkx as NX


def query(datarepo_id: str) -> DatarepoQuery:
    """Creates a new Datarepo query

    Args:
        datarepo_id (str): datarepo to query

    Returns:
        DatarepoQuery: new query initialized on the specified Datarepo
    """
    return DatarepoQuery(query_tree=NX.DiGraph(), root=None)._get(datarepo_id)


@dataclasses.dataclass(frozen=True)
class QueryColumn:
    name: str


_QueryTreeNodeType = str
_DatarepoNodeType: _QueryTreeNodeType = "Datarepo"
_FilterOpNodeType: _QueryTreeNodeType = "FilterOp"
_LimitOpNodeType: _QueryTreeNodeType = "LimitOp"
_ApplyOpNodeType: _QueryTreeNodeType = "ApplyOp"


@dataclasses.dataclass(frozen=True)
class _QueryTreeNode:
    """Superclass of all nodes in the QueryTree"""

    type: _QueryTreeNodeType
    data: Dict[str, Any]
    # Globally unique node ID
    node_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def create_datarepo_node(cls, datarepo_id: str) -> _QueryTreeNode:
        return _QueryTreeNode(type=_DatarepoNodeType, data={"datarepo_id": datarepo_id})

    @classmethod
    def create_filter_op_node(cls, predicate: _FilterPredicate) -> _QueryTreeNode:
        return _QueryTreeNode(type=_FilterOpNodeType, data={"predicate": predicate})

    @classmethod
    def create_limit_op_node(cls, limit: int) -> _QueryTreeNode:
        return _QueryTreeNode(type=_LimitOpNodeType, data={"limit": limit})

    @classmethod
    def create_apply_op_node(
        cls, f: Callable, args: Tuple[QueryColumn, ...], kwargs: Dict[str, QueryColumn]
    ) -> _QueryTreeNode:
        return _QueryTreeNode(type=_ApplyOpNodeType, data={"f": f, "args": args, "kwargs": kwargs})


@dataclasses.dataclass(frozen=True)
class _FilterPredicate:
    """Predicate describing a condition for operations such as a filter or join"""

    left: str
    comparator: str
    right: str


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

    def _get(self, datarepo_id: str) -> DatarepoQuery:
        """Creates the leaf node of a query, which is a _DatarepoNode that retrieves a Datarepo by ID

        Users should not call this method directly, and should instead use the wrapper `daft.datarepo.query`

        Args:
            datarepo_id (str): ID of the datarepo to retrieve

        Returns:
            DatarepoQuery: query object
        """
        node = _QueryTreeNode.create_datarepo_node(datarepo_id=datarepo_id)
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node.node_id, type=node.type, data=node.data)
        return DatarepoQuery(query_tree=tree_copy, root=node.node_id)

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
        node = _QueryTreeNode.create_filter_op_node(predicate=predicate)
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node.node_id, type=node.type, data=node.data)
        tree_copy.add_edge(node.node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node.node_id)

    def apply(self, func: Callable, *args: QueryColumn, **kwargs: QueryColumn) -> DatarepoQuery:
        node = _QueryTreeNode.create_apply_op_node(f=func, args=args, kwargs=kwargs)
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node.node_id, type=node.type, data=node.data)
        tree_copy.add_edge(node.node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node.node_id)

    def limit(self, limit: int) -> DatarepoQuery:
        node = _QueryTreeNode.create_limit_op_node(limit=limit)
        tree_copy = self._query_tree.copy()
        tree_copy.add_node(node.node_id, type=node.type, data=node.data)
        tree_copy.add_edge(node.node_id, self._root)
        return DatarepoQuery(query_tree=tree_copy, root=node.node_id)
