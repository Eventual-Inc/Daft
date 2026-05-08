#[derive(Default, Debug)]
pub struct MaterializeScans {}

impl MaterializeScans {
    pub fn new() -> Self {
        Self {}
    }
}
use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_scan::ScanState;

use super::OptimizerRule;
use crate::{LogicalPlan, SourceInfo};

// Materialize scan tasks from scan operators for all physical scans.
impl OptimizerRule for MaterializeScans {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node| self.try_optimize_node(node))
    }
}

impl MaterializeScans {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match &*plan {
            LogicalPlan::Source(source) => match &*source.source_info {
                SourceInfo::Physical(physical_scan_info) => match &physical_scan_info.scan_state {
                    ScanState::Operator(_) => {
                        let source_plan = Arc::unwrap_or_clone(plan);
                        if let LogicalPlan::Source(source) = source_plan {
                            Ok(Transformed::yes(
                                source.build_materialized_scan_source()?.into(),
                            ))
                        } else {
                            unreachable!("This logical plan was already matched as a Source node")
                        }
                    }
                    ScanState::Tasks(_) => Ok(Transformed::no(plan)),
                },
                _ => Ok(Transformed::no(plan)),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_treenode::TransformedResult;
    use daft_scan::{
        Precision, Pushdowns, ScanOperatorRef, Statistics, test_utils::DummyScanOperator,
    };
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;
    use crate::{
        stats::StatsState,
        test::{dummy_scan_node_with_pushdowns, dummy_scan_operator},
    };

    fn scan_op_with_stats(stats: Option<Statistics>) -> ScanOperatorRef {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        ScanOperatorRef(Arc::new(DummyScanOperator {
            schema,
            num_scan_tasks: 1,
            num_rows_per_task: None,
            supports_count_pushdown_flag: false,
            stats,
        }))
    }

    fn run_materialize(scan_op: ScanOperatorRef) -> Arc<LogicalPlan> {
        let plan = dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default()).build();
        MaterializeScans::new()
            .try_optimize(plan)
            .data()
            .expect("materialize_scans succeeds")
    }

    #[test]
    fn exact_num_rows_short_circuits_stats() {
        let scan_op = scan_op_with_stats(Some(Statistics {
            num_rows: Precision::Exact(42),
            size_bytes: Precision::Exact(1024),
            column_statistics: vec![],
        }));
        let plan = run_materialize(scan_op);
        let LogicalPlan::Source(source) = &*plan else {
            panic!("expected Source at root")
        };
        let stats = match &source.stats_state {
            StatsState::Materialized(s) => s,
            StatsState::NotMaterialized => {
                panic!("Exact source stats should materialize PlanStats during MaterializeScans")
            }
        };
        assert_eq!(stats.approx_stats.num_rows, 42);
        assert_eq!(stats.approx_stats.size_bytes, 1024);
    }

    #[test]
    fn inexact_num_rows_falls_back_to_task_iteration() {
        let scan_op = scan_op_with_stats(Some(Statistics {
            num_rows: Precision::Inexact(42),
            size_bytes: Precision::Absent,
            column_statistics: vec![],
        }));
        let plan = run_materialize(scan_op);
        let LogicalPlan::Source(source) = &*plan else {
            panic!("expected Source at root")
        };
        assert!(
            matches!(source.stats_state, StatsState::NotMaterialized),
            "Inexact stats should not short-circuit — EnrichWithStats runs next"
        );
    }

    #[test]
    fn no_source_stats_preserves_existing_behavior() {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);
        let plan = run_materialize(scan_op);
        let LogicalPlan::Source(source) = &*plan else {
            panic!("expected Source at root")
        };
        assert!(matches!(source.stats_state, StatsState::NotMaterialized));
    }
}
