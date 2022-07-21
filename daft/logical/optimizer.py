from typing import Callable, Generic, Optional, Type, TypeVar

from daft.logical.logical_plan import LogicalPlan, Projection, Selection, Sort


class PushThroughRegistry:
    ...


ParentType = TypeVar("ParentType", bound=LogicalPlan)
ChildType = TypeVar("ChildType", bound=LogicalPlan)


class Rule(Generic[ParentType, ChildType]):
    def __init__(
        self,
        parent_type: Type[ParentType],
        child_type: Type[ChildType],
        function: Callable[[ParentType, ChildType], Optional[ChildType]],
    ) -> None:
        self._parent_type = parent_type
        self._child_type = child_type
        self._function = function

    def apply(self, parent: LogicalPlan, child: LogicalPlan) -> Optional[ChildType]:
        if isinstance(parent, self._parent_type) and isinstance(child, self._child_type):
            return self._function(parent, child)
        return None

    @classmethod
    def register(cls, parent, child):
        ...


class PushDownPredicates(Rule[Selection, LogicalPlan]):
    def __init__(self) -> None:
        self._can_push_through = {Projection, Sort}

    @Rule.register(Selection, Projection)
    def _selection_through_projection(self, parent: Selection, child: Projection) -> Projection:
        grandchild = child._input
        return Projection(input=Selection(grandchild, predicate=parent._predicate), predicate=child._predicate)

    # @Rule.register(Selection, HStack)
    # def _selection_through_hstack(self, parent: Selection, child: Projection) -> Projection:
    #     grandchild = child._input

    #     return Projection(
    #         input=Selection(grandchild, predicate=parent._predicate),
    #         predicate=child._predicate
    #     )
