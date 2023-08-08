from __future__ import annotations

from daft.daft import PartitionScheme
from daft.expressions import Expression, ExpressionsProjection, col
from daft.logical import logical_plan

AggregationOp = str
ColName = str


def _agg_tuple_to_expr(child_ex: Expression, agg_str: str) -> Expression:
    """Helper method that converts the user-facing tuple API for aggregations (Expression, str)
    to our internal representation of aggregations which is just an Expression
    """
    if agg_str == "sum":
        return child_ex._sum()
    elif agg_str == "count":
        return child_ex._count()
    elif agg_str == "min":
        return child_ex._min()
    elif agg_str == "max":
        return child_ex._max()
    elif agg_str == "mean":
        return child_ex._mean()
    elif agg_str == "list":
        return child_ex._agg_list()
    elif agg_str == "concat":
        return child_ex._agg_concat()
    raise NotImplementedError(f"Aggregation {agg_str} not implemented.")


class AggregationPlanBuilder:
    """Builder class to build the appropriate LogicalPlan tree for aggregations

    See: `AggregationPlanBuilder.build()` for the high level logic on how this LogicalPlan is put together
    """

    def __init__(self, plan: logical_plan.LogicalPlan, group_by: ExpressionsProjection | None):
        self._plan = plan
        self.group_by = group_by

        # Aggregations to perform if the plan is just a single-partition
        self._single_partition_shortcut_aggs: dict[ColName, tuple[Expression, AggregationOp]] = {}

        # Aggregations to perform locally on each partition before the shuffle
        self._preshuffle_aggs: dict[ColName, tuple[Expression, AggregationOp]] = {}

        # Aggregations to perform locally on each partition after the shuffle
        # NOTE: These are "global" aggregations, since the shuffle performs a global gather
        self._postshuffle_aggs: dict[ColName, tuple[Expression, AggregationOp]] = {}

        # Parameters for a final projection that is performed after "global" aggregations
        self._needs_final_projection = False
        self._final_projection: dict[ColName, Expression] = (
            {} if self.group_by is None else {e.name(): e for e in self.group_by}
        )
        self._final_projection_excludes: set[ColName] = set()

    def build(self) -> logical_plan.LogicalPlan:
        """Builds a LogicalPlan for all the aggregations that have been added into the builder"""
        if self._plan.num_partitions() == 1:
            return self._build_for_single_partition_plan()
        return self._build_for_multi_partition_plan()

    def _build_for_single_partition_plan(self) -> logical_plan.LogicalPlan:
        """Special-case for when the LogicalPlan has only one partition - there is no longer a need for
        a shuffle step and everything can happen in a single LocalAggregate.
        """
        aggs = [
            _agg_tuple_to_expr(ex.alias(colname), op)
            for colname, (ex, op) in self._single_partition_shortcut_aggs.items()
        ]
        return logical_plan.LocalAggregate(self._plan, agg=aggs, group_by=self.group_by)

    def _build_for_multi_partition_plan(self) -> logical_plan.LogicalPlan:
        # 1. Pre-shuffle aggregations to reduce the size of the data before the shuffle
        pre_shuffle_aggregations = [
            _agg_tuple_to_expr(ex.alias(colname), op) for colname, (ex, op) in self._preshuffle_aggs.items()
        ]
        preshuffle_agg_plan = logical_plan.LocalAggregate(
            self._plan, agg=pre_shuffle_aggregations, group_by=self.group_by
        )

        # 2. Shuffle gather of all rows with the same key to the same partition
        shuffle_plan: logical_plan.LogicalPlan
        if self.group_by is None:
            shuffle_plan = logical_plan.Coalesce(preshuffle_agg_plan, 1)
        else:
            shuffle_plan = logical_plan.Repartition(
                preshuffle_agg_plan,
                num_partitions=self._plan.num_partitions(),
                partition_by=self.group_by,
                scheme=PartitionScheme.Hash,
            )

        # 3. Perform post-shuffle aggregations (this is effectively now global aggregation)
        post_shuffle_aggregations = [
            _agg_tuple_to_expr(ex.alias(colname), op) for colname, (ex, op) in self._postshuffle_aggs.items()
        ]
        postshuffle_agg_plan = logical_plan.LocalAggregate(
            shuffle_plan, agg=post_shuffle_aggregations, group_by=self.group_by
        )

        # 4. Perform post-shuffle projections if necessary
        postshuffle_projection_plan: logical_plan.LogicalPlan
        if self._needs_final_projection:
            final_expressions = ExpressionsProjection(
                [expr.alias(colname) for colname, expr in self._final_projection.items()]
            )
            final_expressions = ExpressionsProjection(
                [e for e in final_expressions if e.name() not in self._final_projection_excludes]
            )
            postshuffle_projection_plan = logical_plan.Projection(postshuffle_agg_plan, final_expressions)
        else:
            postshuffle_projection_plan = postshuffle_agg_plan

        return postshuffle_projection_plan

    def _add_single_partition_shortcut_agg(
        self,
        result_colname: ColName,
        expr: Expression,
        op: AggregationOp,
    ) -> None:
        self._single_partition_shortcut_aggs[result_colname] = (expr, op)

    def _add_2phase_agg(
        self,
        result_colname: ColName,
        expr: Expression,
        local_op: AggregationOp,
        global_op: AggregationOp,
    ) -> None:
        """Add a simple 2-phase aggregation:

        1. Aggregate using local_op to produce an intermediate column
        2. Shuffle
        3. Aggregate using global_op on the intermediate column to produce result column
        """
        intermediate_colname = f"{result_colname}:_local_{local_op}"
        self._preshuffle_aggs[intermediate_colname] = (expr, local_op)
        self._postshuffle_aggs[result_colname] = (col(intermediate_colname), global_op)
        self._final_projection[result_colname] = col(result_colname)

    def add_sum(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "sum")
        self._add_2phase_agg(result_colname, expr, "sum", "sum")
        return self

    def add_min(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "min")
        self._add_2phase_agg(result_colname, expr, "min", "min")
        return self

    def add_max(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "max")
        self._add_2phase_agg(result_colname, expr, "max", "max")
        return self

    def add_count(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "count")
        self._add_2phase_agg(result_colname, expr, "count", "sum")
        return self

    def add_list(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "list")
        self._add_2phase_agg(result_colname, expr, "list", "concat")
        return self

    def add_concat(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "concat")
        self._add_2phase_agg(result_colname, expr, "concat", "concat")
        return self

    def add_mean(self, result_colname: ColName, expr: Expression) -> AggregationPlanBuilder:
        self._add_single_partition_shortcut_agg(result_colname, expr, "mean")

        # Calculate intermediate sum and count
        intermediate_sum_colname = f"{result_colname}:_sum_for_mean"
        intermediate_count_colname = f"{result_colname}:_count_for_mean"
        self._add_2phase_agg(intermediate_sum_colname, expr, "sum", "sum")
        self._add_2phase_agg(intermediate_count_colname, expr, "count", "sum")

        # Run projection to get mean using intermediate sun and count
        self._needs_final_projection = True
        self._final_projection[result_colname] = col(intermediate_sum_colname) / col(intermediate_count_colname)

        self._final_projection_excludes.add(intermediate_sum_colname)
        self._final_projection_excludes.add(intermediate_count_colname)

        return self
