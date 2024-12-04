#![feature(assert_matches)]
#![feature(let_chains)]

mod display;
pub mod ops;
mod optimization;
mod physical_planner;
mod plan;
mod treenode;

#[cfg(test)]
mod test;

pub use physical_planner::{
    extract_agg_expr, logical_to_physical, populate_aggregation_stages, AdaptivePlanner,
    MaterializedResults, QueryStageOutput,
};
pub use plan::{PhysicalPlan, PhysicalPlanRef};
