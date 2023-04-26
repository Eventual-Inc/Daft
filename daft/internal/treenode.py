from __future__ import annotations

import os
import typing
from typing import TYPE_CHECKING, Generic, List, TypeVar, cast

from loguru import logger

if TYPE_CHECKING:
    from daft.internal.rule import Rule

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")


class TreeNode(Generic[TreeNodeType]):
    _registered_children: list[TreeNodeType]

    def __init__(self) -> None:
        self._registered_children: list[TreeNodeType] = []

    def _children(self) -> list[TreeNodeType]:
        return self._registered_children

    def _register_child(self, child: TreeNodeType) -> int:
        self._registered_children.append(child)
        return len(self._registered_children) - 1

    def apply_and_trickle_down(self, rule: Rule[TreeNodeType]) -> TreeNodeType | None:
        root = cast(TreeNodeType, self)
        continue_looping = True
        made_change = False

        # Apply rule to self and its children
        while continue_looping:
            for child in root._children():
                fn = rule.dispatch_fn(root, child)

                if fn is None:
                    continue
                maybe_new_root = fn(root, child)

                if maybe_new_root is not None:
                    root = maybe_new_root
                    made_change = True
                    break
            else:
                continue_looping = False

        # Recursively apply_and_trickle_down to children
        n_children = len(root._children())
        for i in range(n_children):
            maybe_new_child = root._registered_children[i].apply_and_trickle_down(rule)
            if maybe_new_child is not None:
                root._registered_children[i] = maybe_new_child
                made_change = True

        if made_change:
            return root
        else:
            return None

    def to_dot_file(self, filename: str | None = None) -> str:
        dot_data = self.to_dot()
        base_path = "log"
        if filename is None:
            os.makedirs(base_path, exist_ok=True)
            filename = f"{base_path}/{hash(dot_data)}.dot"
        with open(filename, "w") as f:
            f.write(dot_data)
        logger.info(f"Wrote Dot file to {filename}")
        return filename

    def to_dot(self) -> str:
        try:
            import pydot
        except ImportError:
            raise ImportError(
                "Error while importing pydot: please manually install `pip install pydot` for tree visualizations"
            )

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

    def post_order(self) -> list[TreeNodeType]:
        nodes = []

        def helper(curr: TreeNode[TreeNodeType]) -> None:
            for child in curr._children():
                helper(child)
            nodes.append(curr)

        helper(self)
        return typing.cast(List[TreeNodeType], nodes)
