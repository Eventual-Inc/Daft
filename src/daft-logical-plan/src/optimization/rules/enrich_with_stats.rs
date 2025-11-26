use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{LogicalPlan, stats::StatsState};

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct EnrichWithStats {
    cfg: Arc<DaftExecutionConfig>,
}

impl Default for EnrichWithStats {
    fn default() -> Self {
        Self::new(Some(Arc::new(DaftExecutionConfig::default())))
    }
}

impl EnrichWithStats {
    pub fn new(cfg: Option<Arc<DaftExecutionConfig>>) -> Self {
        Self {
            cfg: cfg.unwrap_or_default(),
        }
    }
}

// Add stats to all logical plan nodes in a bottom up fashion.
// All scan nodes MUST be materialized before stats are enriched.
impl OptimizerRule for EnrichWithStats {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let cfg = self.cfg.clone();
        plan.transform_up(move |node: Arc<LogicalPlan>| {
            let node = Arc::unwrap_or_clone(node);
            if matches!(node.stats_state(), StatsState::Materialized(_)) {
                Ok(Transformed::no(node.arced()))
            } else {
                Ok(Transformed::yes(node.with_materialized_stats(&cfg).into()))
            }
        })
    }
}
