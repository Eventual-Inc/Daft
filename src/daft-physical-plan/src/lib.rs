#![feature(assert_matches)]

mod display;
pub mod ops;
mod optimization;
mod physical_planner;
mod plan;
mod treenode;

#[cfg(test)]
mod test;

pub use physical_planner::{
    AdaptivePlanner, MaterializedResults, QueryStageOutput, StageStats, extract_agg_expr,
    logical_to_physical, populate_aggregation_stages, populate_aggregation_stages_bound,
    populate_aggregation_stages_bound_with_schema,
};
pub use plan::{PhysicalPlan, PhysicalPlanRef};
