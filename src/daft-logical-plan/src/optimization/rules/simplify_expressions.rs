use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, ScanState};
use common_treenode::{Transformed, TreeNode};
use daft_algebra::simplify_expr;

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for simplifying expressions
#[derive(Default, Debug)]
pub struct SimplifyExpressionsRule {}

impl SimplifyExpressionsRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SimplifyExpressionsRule {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        if plan.exists(|p| match p.as_ref() {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                crate::SourceInfo::Physical(PhysicalScanInfo { scan_state: ScanState::Operator(scan_op), .. })
                    // TODO: support simplify expressions for SQLScanOperator
                    if scan_op.0.name() == "SQLScanOperator" =>
                {
                    true
                }
                _ => false,
            },
            _ => false,
        }) {
            return Ok(Transformed::no(plan));
        }

        let schema = plan.schema();
        plan.transform(|plan| {
            Ok(Arc::unwrap_or_clone(plan)
                .map_expressions(|expr| simplify_expr(Arc::unwrap_or_clone(expr), &schema))?
                .update_data(Arc::new))
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use daft_core::prelude::Schema;
    use daft_dsl::{col, lit};
    use daft_schema::{dtype::DataType, field::Field};

    use super::SimplifyExpressionsRule;
    use crate::{
        ops::{Filter, Project, Source},
        optimization::rules::OptimizerRule,
        source_info::PlaceHolderInfo,
        stats::StatsState,
        ClusteringSpec, LogicalPlan, LogicalPlanBuilder, SourceInfo,
    };

    fn make_source() -> LogicalPlanBuilder {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("bool", DataType::Boolean),
                Field::new("int", DataType::Int32),
            ])
            .unwrap(),
        );
        LogicalPlanBuilder::from(
            LogicalPlan::Source(Source {
                output_schema: schema.clone(),
                source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                    source_schema: schema,
                    clustering_spec: Arc::new(ClusteringSpec::unknown()),
                    source_id: 0,
                })),
                stats_state: StatsState::NotMaterialized,
            })
            .arced(),
        )
    }

    #[test]
    fn test_nested_plan() {
        let source = make_source()
            .filter(col("int").between(lit(1), lit(10)))
            .unwrap()
            .select(vec![col("int").add(lit(0))])
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

        assert_eq!(projection, &col("int"));

        // make sure the predicate is simplified
        assert_eq!(
            predicate,
            &col("int").lt_eq(lit(10)).and(col("int").gt_eq(lit(1)))
        );
    }
}
