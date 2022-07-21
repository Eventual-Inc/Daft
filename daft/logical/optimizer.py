from typing import Optional

from daft.internal.rule import Rule
from daft.logical.logical_plan import LogicalPlan, Projection, Selection


class PushDownPredicates(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Selection, Projection, self._selection_through_projection)

    def _selection_through_projection(self, parent: Selection, child: Projection) -> Optional[Projection]:
        ids_needed_for_selection = parent.required_columns().to_id_set()
        grandchild = child._children()[0]
        ids_produced_by_grandchild = grandchild.schema().to_id_set()
        if ids_needed_for_selection.issubset(ids_produced_by_grandchild):
            return Projection(input=Selection(grandchild, predicate=parent._predicate), projection=child._projection)
        else:
            return None
