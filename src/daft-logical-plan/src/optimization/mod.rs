pub(crate) mod join_key_set;
mod logical_plan_tracker;
mod optimizer;
mod rules;
#[cfg(test)]
mod test;

pub use optimizer::{Optimizer, OptimizerBuilder, OptimizerConfig};
// Re-exported for the distributed planner's grid spatial-join rewrite, which
// builds a fresh logical subtree at translation time and must enrich it with
// stats before the spatial hash-join path reads them.
pub use rules::{EnrichWithStats, OptimizerRule};
