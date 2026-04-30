mod brute_force_join_order;
mod dp_ccp_join_order;
mod join_graph;
#[cfg(test)]
mod naive_left_deep_join_order;
mod relation_set;

use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use join_graph::JoinGraphBuilder;

use crate::{
    LogicalPlan,
    optimization::rules::{OptimizerRule, reorder_joins::join_graph::JoinOrderer},
};

/// Maximum number of relations for brute force (O(n!) enumeration).
const BRUTE_FORCE_MAX_RELATIONS: usize = 7;

/// Maximum number of relations for DP-ccp (O(3^n) enumeration).
const DP_CCP_MAX_RELATIONS: usize = 12;

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ReorderJoins {
    cfg: Arc<DaftExecutionConfig>,
    use_dp_ccp: bool,
}

impl ReorderJoins {
    pub fn new(cfg: Option<Arc<DaftExecutionConfig>>, use_dp_ccp: bool) -> Self {
        Self {
            cfg: cfg.unwrap_or_default(),
            use_dp_ccp,
        }
    }
}

// Reorder joins in a query tree.
impl OptimizerRule for ReorderJoins {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        if let LogicalPlan::Join(_) = &*plan {
            let mut join_graph =
                JoinGraphBuilder::from_logical_plan(plan.clone(), self.cfg.clone()).build();
            let max_relations = if self.use_dp_ccp {
                DP_CCP_MAX_RELATIONS
            } else {
                BRUTE_FORCE_MAX_RELATIONS
            };
            if !join_graph.could_reorder(max_relations) {
                return Ok(Transformed::no(plan));
            }
            let join_order: join_graph::JoinOrderTree = if self.use_dp_ccp {
                dp_ccp_join_order::DpCcpJoinOrderer {}.order(&join_graph)
            } else {
                brute_force_join_order::BruteForceJoinOrderer {}.order(&join_graph)
            };
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
