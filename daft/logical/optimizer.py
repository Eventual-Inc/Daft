from __future__ import annotations

from loguru import logger

from daft.daft import PartitionScheme, ResourceRequest
from daft.expressions import ExpressionsProjection, col
from daft.internal.rule import Rule
from daft.logical.logical_plan import (
    Coalesce,
    Concat,
    Filter,
    GlobalLimit,
    Join,
    LocalAggregate,
    LocalLimit,
    LogicalPlan,
    Projection,
    Repartition,
    Sort,
    TabularFilesScan,
    UnaryNode,
)


class PushDownPredicates(Rule[LogicalPlan]):
    """Push Filter nodes down through its children when possible - run filters early to reduce amount of data processed"""

    def __init__(self) -> None:
        super().__init__()
        self._combine_filters_rule = CombineFilters()
        self.register_fn(Filter, Filter, self._combine_filters_rule._combine_filters)
        self.register_fn(Filter, Projection, self._filter_through_projection)
        for op in self._supported_unary_nodes:
            self.register_fn(Filter, op, self._filter_through_unary_node)
        self.register_fn(Filter, Join, self._filter_through_join)
        self.register_fn(Filter, Concat, self._filter_through_concat)

    def _filter_through_projection(self, parent: Filter, child: Projection) -> LogicalPlan | None:
        """Pushes Filter through Projections, only if filter does not rely on any projected columns

        Filter-Projection-* -> Projection-Filter-*
        """
        filter_predicate = parent._predicate
        grandchild = child._children()[0]
        child_input_mapping = child._projection.input_mapping()

        can_push_down = []
        can_not_push_down = []
        for pred in filter_predicate:
            required_names = {e for e in pred._required_columns()}
            if all(name in child_input_mapping for name in required_names):
                for name in required_names:
                    pred = pred._replace_column_with_expression(name, col(child_input_mapping[name]))
                can_push_down.append(pred)
            else:
                can_not_push_down.append(pred)
        if len(can_push_down) == 0:
            return None
        logger.debug(f"Pushing down Filter predicate {can_push_down} into {child}")
        pushed_down_filter = Projection(
            input=Filter(grandchild, predicate=ExpressionsProjection(can_push_down)),
            projection=child._projection,
            custom_resource_request=child.resource_request(),
        )

        if len(can_not_push_down) == 0:
            return pushed_down_filter
        else:
            return Filter(pushed_down_filter, ExpressionsProjection(can_not_push_down))

    def _filter_through_unary_node(self, parent: Filter, child: UnaryNode) -> LogicalPlan | None:
        """Pushes Filter through "supported" UnaryNodes (see: self._supported_unary_nodes)

        Filter-Unary-* -> Unary-Filter-*
        """
        assert type(child) in self._supported_unary_nodes
        grandchild = child._children()[0]
        logger.debug(f"Pushing Filter {parent} through {child}")
        return child.copy_with_new_children([Filter(grandchild, parent._predicate)])

    def _filter_through_concat(self, parent: Filter, child: Concat) -> LogicalPlan | None:
        """Pushes a Filter through a Concat to its left/right children

        Filter-Concat-Bottom-* -> Concat-Filter-Bottom-*
        Filter-Concat-Top-* -> Concat-Filter-Top-*
        """
        top = child._children()[0]
        bottom = child._children()[1]
        return Concat(
            top=Filter(top, parent._predicate),
            bottom=Filter(bottom, parent._predicate),
        )

    def _filter_through_join(self, parent: Filter, child: Join) -> LogicalPlan | None:
        """Pushes Filter through a Join to its left/right children

        Filter-Join-Left-* -> Join-Filter-Left-*
        Filter-Join-Right-* -> Join-Filter-Right-*
        """
        left = child._children()[0]
        right = child._children()[1]

        left_input_mapping, right_input_mapping = child.input_mapping()

        filter_predicate = parent._predicate
        can_not_push_down = []
        left_push_down = []
        right_push_down = []
        for pred in filter_predicate:
            required_names = pred._required_columns()
            if all(name in left_input_mapping for name in required_names):
                for name in required_names:
                    pred = pred._replace_column_with_expression(name, col(left_input_mapping[name]))
                left_push_down.append(pred)
            elif all(name in right_input_mapping for name in required_names):
                for name in required_names:
                    pred = pred._replace_column_with_expression(name, col(right_input_mapping[name]))
                right_push_down.append(pred)
            else:
                can_not_push_down.append(pred)

        if len(left_push_down) == 0 and len(right_push_down) == 0:
            logger.debug(f"Could not push down Filter predicates into Join")
            return None

        if len(left_push_down) > 0:
            logger.debug(f"Pushing down Filter predicate left side: {left_push_down} into Join")
            left = Filter(left, predicate=ExpressionsProjection(left_push_down))
        if len(right_push_down) > 0:
            logger.debug(f"Pushing down Filter predicate right side: {right_push_down} into Join")
            right = Filter(right, predicate=ExpressionsProjection(right_push_down))

        new_join = child.copy_with_new_children([left, right])
        if len(can_not_push_down) == 0:
            return new_join
        else:
            return Filter(new_join, ExpressionsProjection(can_not_push_down))

    @property
    def _supported_unary_nodes(self) -> set[type[UnaryNode]]:
        return {Sort, Repartition, Coalesce}


class PruneColumns(Rule[LogicalPlan]):
    """Inserts Projection nodes to prune columns that are unnecessary"""

    def __init__(self) -> None:
        super().__init__()

        self.register_fn(Projection, LogicalPlan, self._projection_logical_plan)
        self.register_fn(Projection, Projection, self._projection_projection, override=True)
        self.register_fn(Projection, LocalAggregate, self._projection_aggregate, override=True)
        self.register_fn(LocalAggregate, LogicalPlan, self._aggregate_logical_plan)

    def _projection_projection(self, parent: Projection, child: Projection) -> LogicalPlan | None:
        """Prunes columns in the child Projection if they are not required by the parent

        Projection-Projection-* -> Projection-<Projection with pruned expressions>-*
        """
        parent_required_set = parent.required_columns()[0]
        child_output_set = child.schema().to_name_set()
        if child_output_set.issubset(parent_required_set):
            return None

        logger.debug(f"Pruning Columns: {child_output_set - parent_required_set} in projection projection")

        child_projections = child._projection
        new_child_exprs = [e for e in child_projections if e.name() in parent_required_set]
        grandchild = child._children()[0]

        return parent.copy_with_new_children(
            [
                Projection(
                    grandchild,
                    projection=ExpressionsProjection(new_child_exprs),
                    custom_resource_request=child.resource_request(),
                )
            ]
        )

    def _projection_aggregate(self, parent: Projection, child: LocalAggregate) -> LogicalPlan | None:
        """Prunes columns in the child LocalAggregate if they are not required by the parent

        Projection-LocalAggregate-* -> Projection-<LocalAggregate with pruned columns>-*
        """
        parent_required_set = parent.required_columns()[0]
        agg_exprs = child._agg
        agg_ids = {e.name() for e in agg_exprs}
        if agg_ids.issubset(parent_required_set):
            return None

        new_agg_exprs = [e for e in agg_exprs if e.name() in parent_required_set]
        grandchild = child._children()[0]

        logger.debug(f"Pruning Columns:  {agg_ids - parent_required_set} in projection aggregate")
        return parent.copy_with_new_children(
            [LocalAggregate(input=grandchild, agg=new_agg_exprs, group_by=child._group_by)]
        )

    def _aggregate_logical_plan(self, parent: LocalAggregate, child: LogicalPlan) -> LogicalPlan | None:
        """Adds an intermediate Projection to prune columns in the child plan if they are not required by the parent LocalAggregate

        LocalAggregate-* -> LocalAggregate-Projection-*
        """
        parent_required_set = parent.required_columns()[0]
        child_output_set = child.schema().to_name_set()
        if child_output_set.issubset(parent_required_set):
            return None

        logger.debug(f"Pruning Columns: {child_output_set - parent_required_set} in aggregate logical plan")

        return parent.copy_with_new_children([self._create_pruning_child(child, parent_required_set)])

    def _projection_logical_plan(self, parent: Projection, child: LogicalPlan) -> LogicalPlan | None:
        """Adds Projection children to the child to prune columns if they are not required by both the parent and child nodes

        Projection-Any-* -> Projection-Any-<New Projection>-*
        """
        if isinstance(child, Projection) or isinstance(child, LocalAggregate) or isinstance(child, TabularFilesScan):
            return None
        if len(child._children()) == 0:
            return None

        parent_required = parent.required_columns()[0]
        child_output_set = child.schema().to_name_set()
        if child_output_set.issubset(parent_required):
            return None
        new_grandchildren = []
        need_new_parent = False
        for i, (in_map, child_req_cols) in enumerate(zip(child.input_mapping(), child.required_columns())):
            required_from_grandchild = {
                in_map[parent_col] for parent_col in parent_required if parent_col in in_map
            } | child_req_cols

            current_grandchild_ids = child._children()[i].schema().to_name_set()
            logger.debug(
                f"Pruning Columns: {current_grandchild_ids - required_from_grandchild} in projection logical plan from child {i}"
            )
            if len(current_grandchild_ids - required_from_grandchild) != 0:
                need_new_parent = True
            new_grandchild = self._create_pruning_child(child._children()[i], required_from_grandchild)
            new_grandchildren.append(new_grandchild)
        if need_new_parent:
            return parent.copy_with_new_children([child.copy_with_new_children(new_grandchildren)])
        else:
            return None

    def _create_pruning_child(self, child: LogicalPlan, parent_name_set: set[str]) -> LogicalPlan:
        child_ids = child.schema().to_name_set()
        assert (
            len(parent_name_set - child_ids) == 0
        ), f"trying to prune columns that aren't produced by child {parent_name_set} vs {child_ids}"
        if child_ids.issubset(parent_name_set):
            return child

        return Projection(
            child,
            projection=ExpressionsProjection([col(f.name) for f in child.schema() if f.name in parent_name_set]),
        )


class CombineFilters(Rule[LogicalPlan]):
    """Combines two Filters into one"""

    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Filter, self._combine_filters)

    def _combine_filters(self, parent: Filter, child: Filter) -> Filter:
        """Combines two Filter nodes

        Filter-Filter-* -> <New Combined Filter>-*
        """
        logger.debug(f"combining {parent} into {child}")
        new_predicate = parent._predicate.union(child._predicate, rename_dup="copy.")
        grand_child = child._children()[0]
        return Filter(grand_child, new_predicate)


class DropRepartition(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Repartition, LogicalPlan, self._drop_repartition_if_same_spec)
        self.register_fn(Repartition, Repartition, self._drop_double_repartition, override=True)

    def _drop_repartition_if_same_spec(self, parent: Repartition, child: LogicalPlan) -> LogicalPlan | None:
        """Drops a Repartition node if it is the same as its child's partition spec

        Repartition-Any-* -> Any-*
        """
        if (
            parent.partition_spec() == child.partition_spec()
            and parent.partition_spec().scheme != PartitionScheme.Range
        ):
            logger.debug(f"Dropping Repartition due to same spec: {parent} ")
            return child

        if parent.num_partitions() == 1 and child.num_partitions() == 1:
            logger.debug(f"Dropping Repartition due to single partition: {parent}")
            return child

        return None

    def _drop_double_repartition(self, parent: Repartition, child: Repartition) -> LogicalPlan:
        """Drops any two repartitions in a row

        Repartition1-Repartition2-* -> Repartition1-*
        """
        grandchild = child._children()[0]
        logger.debug(f"Dropping: {child}")
        return parent.copy_with_new_children([grandchild])


class PushDownClausesIntoScan(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        # self.register_fn(Filter, TabularFilesScan, self._push_down_predicates_into_scan)
        self.register_fn(Projection, TabularFilesScan, self._push_down_projections_into_scan)
        self.register_fn(LocalLimit, TabularFilesScan, self._push_down_local_limit_into_scan)

    def _push_down_local_limit_into_scan(self, parent: LocalLimit, child: TabularFilesScan) -> LogicalPlan | None:
        """Pushes LocalLimit into the limit_rows option of a TabularFilesScan.

        LocalLimit(n)-TabularFilesScan-* -> TabularFilesScan(limit_rows=n)-*
        """
        if child._limit_rows is not None:
            new_limit_rows = min(child._limit_rows, parent._num)
        else:
            new_limit_rows = parent._num

        new_scan = TabularFilesScan(
            schema=child._schema,
            predicate=child._predicate,
            columns=child._column_names,
            file_format_config=child._file_format_config,
            storage_config=child._storage_config,
            filepaths_child=child._filepaths_child,
            limit_rows=new_limit_rows,
        )
        return new_scan

    def _push_down_projections_into_scan(self, parent: Projection, child: TabularFilesScan) -> LogicalPlan | None:
        """Pushes Projections into a scan as selected columns. Retains the Projection if there are non-column expressions.

        Projection-TabularFilesScan-* -> <TabularFilesScan with selected columns>-*
        Projection-TabularFilesScan-* -> Projection-<TabularFilesScan with selected columns>-*
        """
        required_columns = parent._projection.required_columns()
        scan_columns = child.schema()
        if required_columns == scan_columns.to_name_set():
            return None
        ordered_required_columns = [f.name for f in scan_columns if f.name in required_columns]

        new_scan = TabularFilesScan(
            schema=child._schema,
            predicate=child._predicate,
            columns=ordered_required_columns,
            file_format_config=child._file_format_config,
            storage_config=child._storage_config,
            filepaths_child=child._filepaths_child,
        )
        if any(not e._is_column() for e in parent._projection):
            return parent.copy_with_new_children([new_scan])
        else:
            return new_scan


class FoldProjections(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Projection, Projection, self._drop_double_projection)

    def _drop_double_projection(self, parent: Projection, child: Projection) -> LogicalPlan | None:
        """Folds two projections into one if the parent's expressions depend only on no-computation columns of the child.

        Projection-Projection-* -> <Projection with combined expressions and resource requests>-*
        """
        required_columns = parent._projection.required_columns()

        parent_projection = parent._projection
        child_projection = child._projection
        grandchild = child._children()[0]

        child_mapping = child_projection.input_mapping()
        can_skip_child = required_columns.issubset(child_mapping.keys())

        if can_skip_child:
            logger.debug(f"Folding: {parent}\ninto {child}")

            new_exprs = []
            for e in parent_projection:
                if e._is_column():
                    name = e.name()
                    assert name is not None
                    e = child_projection.get_expression_by_name(name)
                else:
                    to_replace = e._required_columns()
                    for name in to_replace:
                        e = e._replace_column_with_expression(name, child_projection.get_expression_by_name(name))
                new_exprs.append(e)
            return Projection(
                grandchild,
                ExpressionsProjection(new_exprs),
                custom_resource_request=ResourceRequest.max_resources(
                    [parent.resource_request(), child.resource_request()]
                ),
            )
        else:
            return None


class DropProjections(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Projection, LogicalPlan, self._drop_unneeded_projection)

    def _drop_unneeded_projection(self, parent: Projection, child: LogicalPlan) -> LogicalPlan | None:
        """Drops a Projection if it is exactly the same as its child's output

        Projection-Any-* -> Any-*
        """
        parent_projection = parent._projection
        child_output = child.schema()
        if (
            all(expr._is_column() for expr in parent_projection)
            and len(parent_projection) == len(child_output)
            and all(p.name() == c.name for p, c in zip(parent_projection, child_output))
        ):
            logger.debug(f"Dropping no-op: {parent}\nas parent of: {child}")
            return child
        else:
            return None


class PushDownLimit(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        for op in self._supported_unary_nodes:
            self.register_fn(LocalLimit, op, self._push_down_local_limit_into_unary_node)
            self.register_fn(GlobalLimit, op, self._push_down_global_limit_into_unary_node)

    def _push_down_local_limit_into_unary_node(self, parent: LocalLimit, child: UnaryNode) -> LogicalPlan | None:
        """Pushes a LocalLimit past a UnaryNode if it is "supported" (see self._supported_unary_nodes)

        LocalLimit-UnaryNode-* -> UnaryNode-LocalLimit-*
        """
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_children([LocalLimit(grandchild, num=parent._num)])

    def _push_down_global_limit_into_unary_node(self, parent: GlobalLimit, child: UnaryNode) -> LogicalPlan | None:
        """Pushes a GlobalLimit past a UnaryNode if it is "supported" (see self._supported_unary_nodes)

        GlobalLimit-UnaryNode-* -> UnaryNode-GlobalLimit-*
        """
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_children([GlobalLimit(grandchild, num=parent._num)])

    @property
    def _supported_unary_nodes(self) -> set[type[LogicalPlan]]:
        return {Repartition, Coalesce, Projection}
