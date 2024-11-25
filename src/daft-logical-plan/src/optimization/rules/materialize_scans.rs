#[derive(Default, Debug)]
pub struct MaterializeScans {
    execution_config: Option<Arc<DaftExecutionConfig>>,
}

impl MaterializeScans {
    pub fn new(execution_config: Option<Arc<DaftExecutionConfig>>) -> Self {
        Self { execution_config }
    }
}
use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

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
                SourceInfo::Physical(_) => {
                    let source_plan = Arc::unwrap_or_clone(plan);
                    if let LogicalPlan::Source(source) = source_plan {
                        Ok(Transformed::yes(
                            source
                                .build_materialized_scan_source(self.execution_config.as_deref())
                                .into(),
                        ))
                    } else {
                        unreachable!("This logical plan was already matched as a Source node")
                    }
                }
                _ => Ok(Transformed::no(plan)),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}
