use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for eliminating common subplans.
///
/// This rule identifies duplicate subplans (subtrees that are structurally identical)
/// and eliminates redundant computation by sharing the same Arc reference.
///
/// For example, if the same computation appears in both sides of a Union:
///   Union(
///     Scan -> Filter(A) -> ExpensiveComputation,
///     Scan -> Filter(B) -> ExpensiveComputation
///   )
/// This optimization ensures ExpensiveComputation is only computed once by sharing
/// the Arc reference, which the physical executor can then recognize and optimize.
///
/// Note: structurally equal subplans are guaranteed to be in different branches
/// because a finite tree cannot contain a proper ancestor that is structurally
/// identical to its descendant (that would imply infinite recursion).
#[derive(Default, Debug)]
pub struct EliminateCommonSubplans {}

impl EliminateCommonSubplans {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateCommonSubplans {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // First pass: collect all subplans and their hashes
        let mut subplan_counts: HashMap<u64, Vec<Arc<LogicalPlan>>> = HashMap::new();

        plan.apply(|node| {
            let hash = hash_plan(node);
            subplan_counts
                .entry(hash)
                .or_default()
                .push(Arc::clone(node));
            Ok(TreeNodeRecursion::Continue)
        })?;

        // Second pass: identify duplicate subplans with equality verification.
        // Hash alone is not sufficient due to potential collisions, so we also
        // verify structural equality via PartialEq.
        let mut canonical_plans: HashMap<u64, Arc<LogicalPlan>> = HashMap::new();
        for (hash, plans) in &subplan_counts {
            if plans.len() > 1
                && let Some(first) = plans.first()
            {
                // Verify all plans in this hash bucket are structurally equal
                if plans.iter().skip(1).all(|p| **p == **first) {
                    canonical_plans
                        .entry(*hash)
                        .or_insert_with(|| Arc::clone(first));
                }
            }
        }

        // If no duplicates found, return unchanged
        if canonical_plans.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Third pass: replace duplicates with canonical version
        plan.transform_down(|current| {
            let hash = hash_plan(&current);

            if let Some(canonical) = canonical_plans.get(&hash)
                && *current == **canonical
                && !Arc::ptr_eq(&current, canonical)
            {
                return Ok(Transformed::yes(Arc::clone(canonical)));
            }

            Ok(Transformed::no(current))
        })
    }
}

/// Compute a hash for a logical plan
fn hash_plan(plan: &Arc<LogicalPlan>) -> u64 {
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    hasher.finish()
}
