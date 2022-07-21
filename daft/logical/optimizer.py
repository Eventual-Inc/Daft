from typing import Optional

from daft.internal.rule import Rule
from daft.logical.logical_plan import LogicalPlan, Projection, Selection
from daft.logical.schema import ExpressionList


class PushDownPredicates(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Selection, Projection, self._selection_through_projection)

    def _selection_through_projection(self, parent: Selection, child: Projection) -> Optional[LogicalPlan]:
        # ids_needed_for_selection = parent.required_columns().to_id_set()
        selection_predicate = parent._predicate

        grandchild = child._children()[0]
        ids_produced_by_grandchild = grandchild.schema().to_id_set()
        can_push_down = []
        can_not_push_down = []
        for pred in selection_predicate:
            id_set = ExpressionList(pred.required_columns()).to_id_set()
            if id_set.issubset(ids_produced_by_grandchild):
                can_push_down.append(pred)
            else:
                can_not_push_down.append(pred)
        if len(can_push_down) == 0:
            return None

        pushed_down_selection = Projection(
            input=Selection(grandchild, predicate=ExpressionList(can_push_down)), projection=child._projection
        )

        if len(can_not_push_down) == 0:
            return pushed_down_selection
        else:
            return Selection(pushed_down_selection, ExpressionList(can_not_push_down))
