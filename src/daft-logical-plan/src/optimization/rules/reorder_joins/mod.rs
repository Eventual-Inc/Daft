mod brute_force_join_order;
mod join_graph;
#[cfg(test)]
mod naive_left_deep_join_order;

#[derive(Default, Debug)]
pub struct ReorderJoins {}

impl ReorderJoins {
    pub fn new() -> Self {
        Self {}
    }
}
use std::sync::Arc;

use brute_force_join_order::BruteForceJoinOrderer;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use join_graph::JoinGraphBuilder;

use crate::{
    optimization::rules::{reorder_joins::join_graph::JoinOrderer, OptimizerRule},
    LogicalPlan,
};

// Reorder joins in a query tree.
impl OptimizerRule for ReorderJoins {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        if let LogicalPlan::Join(_) = &*plan {
            let mut join_graph = JoinGraphBuilder::from_logical_plan(plan.clone()).build();
            // Return early if the join graph won't reorder joins.
            // TODO(desmond): We also want to check if we can potentially reorder joins within relations themselves.
            // E.g., we might have a query plan like:
            //              Join
            //             /    \
            //         Join      Join
            //        /    \     /   \
            //      ...    ...  Agg  ...
            //                   |
            //         More reorderable joins
            //
            // In this example, the Agg is considered non-reorderable, so we treat it as a relation and reorder
            // the top 3 joins. In theory, below the Agg, there could be more joins to reorder. In this case
            // we would need to reorder the nodes below the Agg then reorder/reconstruct the logical plan with
            // this reordered relation. We don't consider this case for now.
            if !join_graph.could_reorder() {
                return Ok(Transformed::no(plan));
            }
            let orderer = BruteForceJoinOrderer {};
            let join_order = orderer.order(&join_graph);
            join_graph
                .build_logical_plan(join_order)
                .map(Transformed::yes)
        } else {
            rewrite_children(self, plan)
        }
    }
}

fn rewrite_children(
    optimizer: &impl OptimizerRule,
    plan: Arc<LogicalPlan>,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    plan.map_children(|input| optimizer.try_optimize(input))
}
