from __future__ import annotations

from typing import TYPE_CHECKING, Generic, List, TypeVar

if TYPE_CHECKING:
    from daft.internal.rule import Rule

T = TypeVar("T", bound="TreeNode")

import pydot


class TreeNode(Generic[T]):
    def __init__(self) -> None:
        self._registered_children: List[T] = []

    def _children(self) -> List[T]:
        return self._registered_children

    def _register_child(self, child: T) -> T:
        self._registered_children.append(child)
        return child

    def apply_rule(self, rule: Rule[T]):
        ...

    def to_dot(self) -> str:
        graph: pydot.Graph = pydot.Dot("my_graph", graph_type="digraph", bgcolor="white")  # type: ignore
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
