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

// Add stats to all logical plan nodes in a bottom up fashion.
impl OptimizerRule for MaterializeScans {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node| self.try_optimize_node(Arc::unwrap_or_clone(node)))
    }
}

impl MaterializeScans {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(&self, plan: LogicalPlan) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan {
            LogicalPlan::Source(source) => match &*source.source_info {
                SourceInfo::Physical(_) => Ok(Transformed::yes(
                    source
                        .build_materialized_scan_source(self.execution_config.as_deref())
                        .into(),
                )),
                _ => Ok(Transformed::no(Arc::new(LogicalPlan::Source(source)))),
            },
            _ => Ok(Transformed::no(Arc::new(plan))),
        }
    }
}
