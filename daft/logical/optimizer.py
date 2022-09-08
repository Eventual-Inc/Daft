from typing import Optional, Set, Type

from loguru import logger

from daft.internal.rule import Rule
from daft.logical.logical_plan import (
    Coalesce,
    Filter,
    GlobalLimit,
    LocalLimit,
    LogicalPlan,
    PartitionScheme,
    Projection,
    Repartition,
    Scan,
    Sort,
    UnaryNode,
)
from daft.logical.schema import ExpressionList


class PushDownPredicates(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self._combine_filters_rule = CombineFilters()
        self.register_fn(Filter, Filter, self._combine_filters_rule._combine_filters)
        self.register_fn(Filter, Projection, self._filter_through_projection)
        for op in self._supported_unary_nodes:
            self.register_fn(Filter, op, self._filter_through_unary_node)

    def _filter_through_projection(self, parent: Filter, child: Projection) -> Optional[LogicalPlan]:
        filter_predicate = parent._predicate

        grandchild = child._children()[0]
        ids_produced_by_grandchild = grandchild.schema().to_id_set()
        can_push_down = []
        can_not_push_down = []
        for pred in filter_predicate:
            id_set = {e.get_id() for e in pred.required_columns()}
            if id_set.issubset(ids_produced_by_grandchild):
                can_push_down.append(pred)
            else:
                can_not_push_down.append(pred)
        if len(can_push_down) == 0:
            return None
        logger.debug(f"Pushing down Filter predicate {can_push_down} into {child}")
        pushed_down_filter = Projection(
            input=Filter(grandchild, predicate=ExpressionList(can_push_down)), projection=child._projection
        )

        if len(can_not_push_down) == 0:
            return pushed_down_filter
        else:
            return Filter(pushed_down_filter, ExpressionList(can_not_push_down))

    def _filter_through_unary_node(self, parent: Filter, child: UnaryNode) -> Optional[UnaryNode]:
        assert type(child) in self._supported_unary_nodes
        grandchild = child._children()[0]
        logger.debug(f"Pushing Filter {parent} through {child}")
        return child.copy_with_new_input(Filter(grandchild, parent._predicate))

    @property
    def _supported_unary_nodes(self) -> Set[Type[UnaryNode]]:
        return {Sort, Repartition, Coalesce}


class CombineFilters(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Filter, self._combine_filters)

    def _combine_filters(self, parent: Filter, child: Filter) -> Filter:
        logger.debug(f"combining {parent} into {child}")
        new_predicate = parent._predicate.union(child._predicate, strict=False)
        grand_child = child._children()[0]
        return Filter(grand_child, new_predicate)


class DropRepartition(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Repartition, LogicalPlan, self._drop_repartition_if_same_spec)
        self.register_fn(Repartition, Repartition, self._drop_double_repartition, override=True)

    def _drop_repartition_if_same_spec(self, parent: Repartition, child: LogicalPlan) -> Optional[LogicalPlan]:
        if (
            parent.partition_spec() == child.partition_spec()
            and parent.partition_spec().scheme != PartitionScheme.RANGE
        ) or ((parent.num_partitions() == 1 and child.num_partitions() == 1)):
            logger.debug(f"Dropping: {parent}")
            return child
        return None

    def _drop_double_repartition(self, parent: Repartition, child: Repartition) -> Repartition:
        grandchild = child._children()[0]
        logger.debug(f"Dropping: {child}")
        return Repartition(
            grandchild,
            partition_by=parent._partition_by,
            num_partitions=parent.num_partitions(),
            scheme=parent.partition_spec().scheme,
        )


class PushDownClausesIntoScan(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Scan, self._push_down_predicates_into_scan)
        self.register_fn(Projection, Scan, self._push_down_projections_into_scan)

    def _push_down_predicates_into_scan(self, parent: Filter, child: Scan) -> Scan:
        new_predicate = parent._predicate.union(child._predicate, strict=False)
        child_schema = child.schema()
        assert new_predicate.required_columns().to_id_set().issubset(child_schema.to_id_set())
        return Scan(
            schema=child._schema, predicate=new_predicate, columns=child_schema.names, source_info=child._source_info
        )

    def _push_down_projections_into_scan(self, parent: Projection, child: Scan) -> Optional[LogicalPlan]:
        required_columns = parent.schema().required_columns()
        scan_columns = child.schema()
        if required_columns.to_id_set() == scan_columns.to_id_set():
            return None

        new_scan = Scan(
            schema=child._schema,
            predicate=child._predicate,
            columns=required_columns.names,
            source_info=child._source_info,
        )
        projection_required = any(e.has_call() for e in parent._projection)
        if projection_required:
            return Projection(new_scan, parent._projection)
        else:
            return new_scan


class FoldProjections(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Projection, Projection, self._push_down_predicates_into_scan)

    def _push_down_predicates_into_scan(self, parent: Projection, child: Projection) -> Optional[Projection]:
        required_columns = parent._projection.required_columns()
        grandchild = child._children()[0]
        grandchild_output = grandchild.schema()
        grandchild_ids = grandchild_output.to_id_set()

        can_skip_child = required_columns.to_id_set().issubset(grandchild_ids)

        if can_skip_child:
            logger.debug(f"Folding: {parent} into {child}")
            return Projection(grandchild, parent._projection)
        else:
            return None


class PushDownLimit(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        for op in self._supported_unary_nodes:
            self.register_fn(LocalLimit, op, self._push_down_local_limit_into_unary_node)
            self.register_fn(GlobalLimit, op, self._push_down_global_limit_into_unary_node)

    def _push_down_local_limit_into_unary_node(self, parent: LocalLimit, child: UnaryNode) -> Optional[UnaryNode]:
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_input(LocalLimit(grandchild, num=parent._num))

    def _push_down_global_limit_into_unary_node(self, parent: GlobalLimit, child: UnaryNode) -> Optional[UnaryNode]:
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_input(GlobalLimit(grandchild, num=parent._num))

    @property
    def _supported_unary_nodes(self) -> Set[Type[UnaryNode]]:
        return {Repartition, Coalesce, Projection}
