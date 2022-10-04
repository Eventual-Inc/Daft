from collections import defaultdict
from typing import Callable, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from daft.internal.treenode import TreeNode

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")

RuleFn = Callable[[TreeNodeType, TreeNodeType], Optional[TreeNodeType]]


def get_all_subclasses(type: Type) -> List[Type]:
    result = [type]

    def helper(t: Type):
        subclasses = t.__subclasses__()
        result.extend(subclasses)
        for sc in subclasses:
            helper(sc)

    helper(type)
    return result


class Rule(Generic[TreeNodeType]):
    def __init__(self) -> None:
        self._fn_registry: Dict[Tuple[Type[TreeNodeType], Type[TreeNodeType]], List[RuleFn]] = defaultdict(list)

    def register_fn(self, parent_type: Type, child_type: Type, fn: RuleFn) -> None:
        for p_subclass in get_all_subclasses(parent_type):
            for c_subtype in get_all_subclasses(child_type):
                type_tuple = (p_subclass, c_subtype)
                self._fn_registry[type_tuple].append(fn)

    def dispatch_fn(self, parent: TreeNodeType, child: TreeNodeType) -> Optional[List[RuleFn]]:
        type_tuple = (type(parent), type(child))
        if type_tuple not in self._fn_registry:
            return None
        return self._fn_registry.get(type_tuple, None)

    def apply(self, parent: TreeNodeType, child: TreeNodeType) -> Optional[TreeNodeType]:
        fn_list = self.dispatch_fn(parent, child)
        if fn_list is None:
            return None
        val: Optional[TreeNodeType] = None
        for fn in fn_list:
            val = fn(parent, child)
            if val is not None:
                return val  # type: ignore
        return val
