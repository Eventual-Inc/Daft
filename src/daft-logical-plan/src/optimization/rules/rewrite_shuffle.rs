use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_functions::random::random_int_expr;

use crate::{LogicalPlan, ops::Sort, optimization::rules::OptimizerRule};

/// Expands [`LogicalPlan::Shuffle`] into `Project` (random_int key) → `Sort` → `Project` (drop key).
#[derive(Default, Debug)]
pub struct RewriteShuffle {}

impl RewriteShuffle {
    pub fn new() -> Self {
        Self {}
    }

    fn expand_shuffle(input: Arc<LogicalPlan>, seed: Option<u64>) -> DaftResult<Arc<LogicalPlan>> {
        let key_name = format!("__daft_shuffle_{}", uuid::Uuid::new_v4());
        let sort_by = vec![random_int_expr(i64::MIN, i64::MAX, seed).alias(key_name.as_str())];
        let sorted = Sort::try_new(input, sort_by, vec![false], vec![false])?;
        Ok(Arc::new(sorted.into()))
    }
}

impl OptimizerRule for RewriteShuffle {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            if let LogicalPlan::Shuffle(shuffle) = node.as_ref() {
                let expanded = Self::expand_shuffle(shuffle.input.clone(), shuffle.seed)?;
                Ok(Transformed::yes(expanded))
            } else {
                Ok(Transformed::no(node))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use common_treenode::{TreeNode, TreeNodeRecursion};
    use daft_scan::Pushdowns;
    use daft_schema::{dtype::DataType, field::Field};

    use crate::{
        LogicalPlan,
        optimization::rules::{OptimizerRule, RewriteShuffle},
        test::{dummy_scan_node_with_pushdowns, dummy_scan_operator},
    };

    #[test]
    fn rewrite_shuffle_expands_to_random_int_and_sort() -> DaftResult<()> {
        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("a", DataType::Int64)]),
            Pushdowns::default(),
        )
        .shuffle(None)?
        .build();

        let out = RewriteShuffle::new().try_optimize(plan)?;
        assert!(out.transformed);
        let ascii = out.data.repr_ascii(true);
        assert!(ascii.contains("Sort"), "expected Sort in plan:\n{ascii}");

        let mut found_random_int = false;
        out.data.apply(|node| {
            if let LogicalPlan::Project(p) = node.as_ref() {
                for e in &p.projection {
                    if e.to_string().contains("random_int") {
                        found_random_int = true;
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(found_random_int, "expected random_int in a projection expr");
        Ok(())
    }
}
