from typing import Any, Callable, Dict, List, Tuple

import networkx as NX

from daft.datarepo.query.definitions import NodeId


def prune_singlechild_node(tree: NX.DiGraph, root: NodeId, node_id: NodeId) -> Tuple[NX.DiGraph, NodeId]:
    """Prunes nodes that have a single child, stitching parents together with its child

        Before:
            Parent1 ---e1---> node ---e0---> child
            Parent2 ---e2-----^
        After:
            Parent1 ---e1---> child
            Parent2 ---e2-----^

    Args:
        tree (NX.DiGraph): tree to prune
        root (NodeId): root node ID of the tree
        node_id (NodeId): node to prune

    Returns:
        Tuple[NX.DiGraph, NodeId]: tuple of (pruned_tree, root_node_id)
    """
    tree_copy = tree.copy()
    in_edges = tree_copy.in_edges(node_id)
    out_edges = tree_copy.out_edges(node_id)
    assert len(out_edges) == 1, "_prune_singlechild_node only defined on nodes with one child"
    _, child = list(tree_copy.out_edges(node_id))[0]

    for in_node, _ in in_edges:
        edge_attr = tree_copy.edges[in_node, node_id]
        tree_copy.add_edge(in_node, child, **edge_attr)

    tree_copy.remove_node(node_id)

    if node_id == root:
        return tree_copy, child
    return tree_copy, root


def dfs_search(tree: NX.DiGraph, root: NodeId, condition: Callable[[Dict[str, Any]], bool]) -> List[NodeId]:
    """Searches and returns NodeIds matching a condition with an in-order DFS traversal"""
    node_ids = NX.dfs_tree(tree, source=root)
    return [node_id for node_id in node_ids if condition(tree.nodes[node_id])]
