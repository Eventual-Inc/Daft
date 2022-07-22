from typing import Callable, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from daft.internal.treenode import TreeNode

TreeNodeType = TypeVar("TreeNodeType", bound="TreeNode")

RuleFn = Callable[[TreeNodeType, TreeNodeType], Optional[TreeNodeType]]


class Rule(Generic[TreeNodeType]):
    def __init__(self) -> None:
        self._fn_registry: Dict[Tuple[Type[TreeNodeType], Type[TreeNodeType]], RuleFn] = dict()

    def register_fn(self, parent_type: Type[TreeNodeType], child_type: Type[TreeNodeType], fn: RuleFn) -> None:
        type_tuple = (parent_type, child_type)
        assert type_tuple not in self._fn_registry
        self._fn_registry[type_tuple] = fn

    def dispatch_fn(self, parent: TreeNodeType, child: TreeNodeType) -> Optional[RuleFn]:
        type_tuple = (type(parent), type(child))
        return self._fn_registry.get(type_tuple, None)

    def apply(self, parent: TreeNodeType, child: TreeNodeType) -> Optional[TreeNodeType]:
        fn = self.dispatch_fn(parent, child)
        if fn is not None:
            return fn(parent, child)
        else:
            return None


class RuleRunner(Generic[TreeNodeType]):
    def __init__(self, rules: List[Rule[TreeNodeType]]) -> None:
        self._rules = rules

    def run_single_rule(self, root: TreeNodeType, rule: Rule[TreeNodeType]):
        return root.apply_and_trickle_down(rule)
