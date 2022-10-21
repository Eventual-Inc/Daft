from __future__ import annotations

from typing import Callable, Generic, Optional, TypeVar

from daft.internal.treenode import TreeNode

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")

RuleFn = Callable[[TreeNodeType, TreeNodeType], Optional[TreeNodeType]]


def get_all_subclasses(input_type: type) -> list[type]:
    result = [input_type]

    def helper(t: type):
        subclasses = t.__subclasses__()
        result.extend(subclasses)
        for sc in subclasses:
            helper(sc)

    helper(input_type)
    return result


class Rule(Generic[TreeNodeType]):
    def __init__(self) -> None:
        self._fn_registry: dict[tuple[type[TreeNodeType], type[TreeNodeType]], RuleFn] = dict()

    def register_fn(self, parent_type: type, child_type: type, fn: RuleFn, override: bool = False) -> None:
        for p_subclass in get_all_subclasses(parent_type):
            for c_subtype in get_all_subclasses(child_type):
                type_tuple = (p_subclass, c_subtype)
                if type_tuple in self._fn_registry:
                    if override:
                        self._fn_registry[type_tuple] = fn
                    else:
                        raise ValueError(f"Rule already registered for {type_tuple}")
                else:
                    self._fn_registry[type_tuple] = fn

    def dispatch_fn(self, parent: TreeNodeType, child: TreeNodeType) -> RuleFn | None:
        type_tuple = (type(parent), type(child))
        if type_tuple not in self._fn_registry:
            return None
        return self._fn_registry.get(type_tuple, None)

    def apply(self, parent: TreeNodeType, child: TreeNodeType) -> TreeNodeType | None:
        fn = self.dispatch_fn(parent, child)
        if fn is None:
            return None
        return fn(parent, child)
