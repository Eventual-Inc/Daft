use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, ScanState};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_algebra::simplify_expr;
use log::Log;

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for automatically re-partitioning the inputs to a query.
/// Only works with a map-only pipeline when there is one input partition and
/// multiple workers.
#[derive(Default, Debug)]
pub struct AutomaticRepartitionRule {}

impl AutomaticRepartitionRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl TreeNodeVisitor for LogicalPlan {
    type Node = Self;

    // calls this first on the node, before visiting children
    fn f_down(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        match node {}

        Ok(TreeNodeRecursion::Continue)
    }

    // calls this on the node after the node's children have been visisted
    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

fn is_ok(plan: Arc<LogicalPlan>) -> bool {
    // plan.visit(visitor)
    let mut n_sources = 0;
    let mut non_map_node_found = false;
    let mut n_partitions = 0;

    plan.apply(|node| match node.as_ref() {
        // sources
        (
            LogicalPlan::Source(_)
            | LogicalPlan::Shard(_)
        ) => {
            n_sources += 1;
            if n_sources > 1 {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        },
        // map-only nodes
        (
            LogicalPlan::Project(_)
            | LogicalPlan::UDFProject(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Explode(_)
            | LogicalPlan::Limit(_)
            // MAYBE these???
            | LogicalPlan::Offset(_)
            | LogicalPlan::Concat(_)
            | LogicalPlan::Sink(_)
            | LogicalPlan::Sample(_)
            | LogicalPlan::MonotonicallyIncreasingId(_)
            | LogicalPlan::SubqueryAlias(_)
            // | LogicalPlan::Window(_)
            | LogicalPlan::TopN(_)
        ) => Ok(TreeNodeRecursion::Continue),
        // NOT a map-only node => we cannot perform this optimization!
        (
            LogicalPlan::Unpivot(_)
            | LogicalPlan::IntoBatches(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Pivot(_)

            | LogicalPlan::Intersect(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Window(_)
        ) => Ok(TreeNodeRecursion::Stop)
    });

    panic!()
}

fn is_single_source_with_single_partition(plan: Arc<LogicalPlan>) -> bool {
    todo!()
}

fn is_map_only(plan: Arc<LogicalPlan>) -> bool {
    todo!()
}

fn in_distributed_context_with_multiple_workers() -> bool {
    todo!()
}

fn insert_repartition_after_source(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    plan.transform_up(f)
}

impl OptimizerRule for AutomaticRepartitionRule {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        if !(is_single_source_with_single_partition(&plan)
            && is_map_only(&plan)
            && in_distributed_context_with_multiple_workers())
        {
            return Ok(Transformed::no(plan));
        }

        // call daft.infer or get runner context

        let updated = insert_repartition_after_source(plan);
        Ok(Transformed::yes(updated))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use daft_core::prelude::Schema;
    use daft_dsl::{lit, resolved_col, unresolved_col};
    use daft_schema::{dtype::DataType, field::Field};

    use super::AutomaticRepartitionRule;
    use crate::{
        ops::{Filter, Project, Source},
        optimization::rules::OptimizerRule,
        source_info::PlaceHolderInfo,
        stats::StatsState,
        ClusteringSpec, LogicalPlan, LogicalPlanBuilder, SourceInfo,
    };

    fn make_source() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bool", DataType::Boolean),
            Field::new("int", DataType::Int32),
        ]));
        LogicalPlanBuilder::from(
            LogicalPlan::Source(Source {
                plan_id: None,
                node_id: None,
                output_schema: schema.clone(),
                source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                    source_schema: schema,
                    clustering_spec: Arc::new(ClusteringSpec::unknown()),
                })),
                stats_state: StatsState::NotMaterialized,
            })
            .arced(),
        )
    }

    #[test]
    fn test_nested_plan() {
        let source = make_source()
            .filter(unresolved_col("int").between(lit(1), lit(10)))
            .unwrap()
            .select(vec![unresolved_col("int").add(lit(0))])
            .unwrap()
            .build();
        let optimizer = SimplifyExpressionsRule::new();
        let optimized = optimizer.try_optimize(source).unwrap();

        let LogicalPlan::Project(Project {
            projection, input, ..
        }) = optimized.data.as_ref()
        else {
            panic!("Expected Filter, got {:?}", optimized.data)
        };

        let LogicalPlan::Filter(Filter { predicate, .. }) = input.as_ref() else {
            panic!("Expected Filter, got {:?}", optimized.data)
        };

        let projection = projection.first().unwrap();

        // make sure the expression is simplified
        assert!(optimized.transformed);

        assert_eq!(projection, &resolved_col("int"));

        // make sure the predicate is simplified
        assert_eq!(
            predicate,
            &resolved_col("int")
                .lt_eq(lit(10))
                .and(resolved_col("int").gt_eq(lit(1)))
        );
    }
}
