from typing import Optional

from daft.internal.rule import Rule
from daft.logical.logical_plan import Filter, LogicalPlan, Projection, Scan
from daft.logical.schema import ExpressionList


class PushDownPredicates(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self._combine_filters_rule = CombineFilters()
        self.register_fn(Filter, Filter, self._combine_filters_rule._combine_filters)
        self.register_fn(Filter, Projection, self._filter_through_projection)

    def _filter_through_projection(self, parent: Filter, child: Projection) -> Optional[LogicalPlan]:
        filter_predicate = parent._predicate

        grandchild = child._children()[0]
        ids_produced_by_grandchild = grandchild.schema().to_id_set()
        can_push_down = []
        can_not_push_down = []
        for pred in filter_predicate:
            id_set = ExpressionList(pred.required_columns()).to_id_set()
            if id_set.issubset(ids_produced_by_grandchild):
                can_push_down.append(pred)
            else:
                can_not_push_down.append(pred)
        if len(can_push_down) == 0:
            return None

        pushed_down_filter = Projection(
            input=Filter(grandchild, predicate=ExpressionList(can_push_down)), projection=child._projection
        )

        if len(can_not_push_down) == 0:
            return pushed_down_filter
        else:
            return Filter(pushed_down_filter, ExpressionList(can_not_push_down))


class CombineFilters(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Filter, self._combine_filters)

    def _combine_filters(self, parent: Filter, child: Filter) -> Filter:
        new_predicate = parent._predicate.union(child._predicate)
        grand_child = child._children()[0]
        return Filter(grand_child, new_predicate)


class PushDownClausesIntoScan(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Scan, self._push_down_predicates_into_scan)
        self.register_fn(Projection, Scan, self._push_down_projections_into_scan)

    def _push_down_predicates_into_scan(self, parent: Filter, child: Scan) -> Scan:
        new_predicate = parent._predicate.union(child._predicate)
        child_schema = child.schema()
        assert new_predicate.required_columns().to_id_set().issubset(child_schema.to_id_set())
        return Scan(child._schema, new_predicate, child_schema.names)

    def _push_down_projections_into_scan(self, parent: Projection, child: Scan) -> Optional[LogicalPlan]:
        required_columns = parent.schema().required_columns()
        scan_columns = child.schema()
        if required_columns.to_id_set() == scan_columns.to_id_set():
            return None

        new_scan = Scan(child._schema, child._predicate, columns=required_columns.names)
        projection_required = any(e.has_call() for e in parent._projection)
        if projection_required:
            return Projection(new_scan, parent._projection)
        else:
            return new_scan
