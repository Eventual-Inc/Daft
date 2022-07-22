from __future__ import annotations

from typing import Generic, List, TypeVar

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")

import pydot


class TreeNode(Generic[TreeNodeType]):
    _registered_children: List[TreeNodeType]

    def __init__(self) -> None:
        self._registered_children: List[TreeNodeType] = []

    def _children(self) -> List[TreeNodeType]:
        return self._registered_children

    def _register_child(self, child: TreeNodeType) -> int:
        self._registered_children.append(child)
        return len(self._registered_children) - 1

    def to_dot(self) -> str:
        graph: pydot.Graph = pydot.Dot("TreeNode", graph_type="digraph", bgcolor="white")  # type: ignore
        counter = 0

        def recurser(node: TreeNode) -> int:
            nonlocal counter
            desc = repr(node)
            my_id = counter
            myself = pydot.Node(my_id, label=f"{desc}")
            graph.add_node(myself)
            counter += 1
            for child in node._children():
                child_id = recurser(child)
                edge = pydot.Edge(str(my_id), str(child_id), color="black")
                graph.add_edge(edge)  # type: ignore
            return my_id

        recurser(self)
        return graph.to_string()  # type: ignore
