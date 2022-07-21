from __future__ import annotations

from typing import TYPE_CHECKING, Generic, List, TypeVar, cast

if TYPE_CHECKING:
    from daft.internal.rule import Rule

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")

import pydot


class TreeNode(Generic[TreeNodeType]):
    _registered_children: List[TreeNodeType]

    def __init__(self) -> None:
        self._registered_children: List[TreeNodeType] = []

    def _children(self) -> List[TreeNodeType]:
        return self._registered_children

    def _register_child(self, child: TreeNodeType) -> TreeNodeType:
        self._registered_children.append(child)
        return child

    def apply_and_trickle_down(self, rule: Rule[TreeNodeType]) -> TreeNodeType:
        root = cast(TreeNodeType, self)
        continue_looping = True
        while continue_looping:
            for child in root._children():
                fn = rule.dispatch_fn(root, child)

                if fn is None:
                    continue

                maybe_new_root = fn(root, child)

                if maybe_new_root is not None:
                    root = maybe_new_root
                    break
            else:
                continue_looping = False
        n_children = len(root._children())
        for i in range(n_children):
            root._registered_children[i] = root._registered_children[i].apply_and_trickle_down(rule)
        return root

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
