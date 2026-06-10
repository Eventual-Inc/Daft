use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode, TreeNodeRecursion};

use super::OptimizerRule;
use crate::{LogicalPlan, ops::CommonSubplan};

/// Global counter for assigning unique IDs to each common subplan group.
static NEXT_CSE_ID: AtomicUsize = AtomicUsize::new(0);

fn next_cse_id() -> usize {
    NEXT_CSE_ID.fetch_add(1, Ordering::Relaxed)
}

/// Optimization rule that identifies duplicate subplans and wraps them in
/// explicit [`CommonSubplan`] nodes with the same unique `id`.
///
/// This rule runs as the very last optimization pass, after all other rules
/// have canonicalized the plan. It performs three passes:
///
/// 1. **Hash collection**: traverse the tree and hash every subtree.
/// 2. **Equality grouping**: within each hash bucket, verify structural
///    equality via `PartialEq` and assign a canonical version + unique id.
/// 3. **Top-down replacement**: recursively walk the tree; when a node
///    matches a canonical duplicate (same structure, different Arc), wrap it
///    in `CommonSubplan { id, subplan: canonical }`.
///
/// The top-down approach ensures that deeper duplicates are wrapped first,
/// so parent nodes whose children change (wrapped → CommonSubplan) won't
/// be mistakenly treated as duplicates of the original unwrapped parents.
#[derive(Default, Debug)]
pub struct EliminateCommonSubplans {}

impl EliminateCommonSubplans {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateCommonSubplans {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // --- Pass 1: collect hashes for every subtree ---
        let mut subplan_counts: HashMap<u64, Vec<Arc<LogicalPlan>>> = HashMap::new();

        plan.apply(|node| {
            let hash = hash_plan(node);
            subplan_counts
                .entry(hash)
                .or_default()
                .push(Arc::clone(node));
            Ok(TreeNodeRecursion::Continue)
        })?;

        // --- Pass 2: identify duplicate groups (hash + PartialEq) ---
        // Key: hash. Value: (canonical plan, globally unique id).
        let mut duplicates: HashMap<u64, (Arc<LogicalPlan>, usize)> = HashMap::new();

        for (hash, plans) in &subplan_counts {
            if plans.len() <= 1 {
                continue;
            }
            // Group plans by structural equality (handles hash collisions)
            let mut groups: Vec<Vec<&Arc<LogicalPlan>>> = Vec::new();
            for plan in plans {
                let mut found_group = false;
                for group in &mut groups {
                    if **plan == **group[0] {
                        group.push(plan);
                        found_group = true;
                        break;
                    }
                }
                if !found_group {
                    groups.push(vec![plan]);
                }
            }

            // For each group of size ≥ 2, select a canonical and assign an id.
            // Note: if multiple groups share the same hash (collision), only
            // the first group is stored. Collided groups are missed (not a
            // correctness bug, just a missed optimization opportunity).
            for group in groups {
                if group.len() > 1 {
                    let id = next_cse_id();
                    duplicates
                        .entry(*hash)
                        .or_insert_with(|| (Arc::clone(group[0]), id));
                }
            }
        }

        if duplicates.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // --- Pass 3: top-down replacement ---
        // Manual recursion instead of TreeNode::transform_down to avoid
        // stack overflow on deep plans and to control traversal order.
        let result = replace_duplicates(plan, &duplicates);
        Ok(Transformed::yes(result))
    }
}

/// Top-down manual recursion to wrap duplicate subplans in CommonSubplan nodes.
///
/// For each node:
/// 1. If it matches a known duplicate (same hash + PartialEq, different Arc),
///    wrap it in `CommonSubplan { id, subplan: canonical }` and stop recursing
///    (the subplan is the canonical which was already processed).
/// 2. If it IS the canonical itself, recurse into its children.
/// 3. Otherwise, recurse into children normally.
fn replace_duplicates(
    plan: Arc<LogicalPlan>,
    duplicates: &HashMap<u64, (Arc<LogicalPlan>, usize)>,
) -> Arc<LogicalPlan> {
    let hash = hash_plan(&plan);

    if let Some((canonical, id)) = duplicates.get(&hash) {
        if Arc::ptr_eq(&plan, canonical) {
            // This IS the canonical — recurse into children.
            return replace_children(plan, duplicates);
        }
        if *plan == **canonical {
            // Duplicate (same structure, different Arc) — wrap in CommonSubplan.
            return Arc::new(LogicalPlan::CommonSubplan(CommonSubplan::new(
                Arc::clone(canonical),
                *id,
            )));
        }
    }

    // Not a duplicate — recurse into children normally.
    replace_children(plan, duplicates)
}

/// Compute a hash for a logical plan.
fn hash_plan(plan: &Arc<LogicalPlan>) -> u64 {
    let mut hasher = DefaultHasher::new();
    plan.hash(&mut hasher);
    hasher.finish()
}

/// Recurse into children of `plan`, replacing duplicates along the way.
fn replace_children(
    plan: Arc<LogicalPlan>,
    duplicates: &HashMap<u64, (Arc<LogicalPlan>, usize)>,
) -> Arc<LogicalPlan> {
    let children = plan.arc_children();
    if children.is_empty() {
        return plan;
    }
    let new_children: Vec<Arc<LogicalPlan>> = children
        .into_iter()
        .map(|child| replace_duplicates(child, duplicates))
        .collect();
    plan.with_new_children(&new_children).into()
}
