mod dashboard;
mod debug;
#[cfg(feature = "python")]
pub mod python;

use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, StatSnapshotView, ops::NodeInfo};
use daft_micropartition::MicroPartitionRef;

#[async_trait]
pub trait QuerySubscriber: Send + Sync + std::fmt::Debug + 'static {
    fn on_query_start(&self, query_id: String, unoptimized_plan: String) -> DaftResult<()>;
    fn on_query_end(&self, query_id: String, results: Vec<MicroPartitionRef>) -> DaftResult<()>;
    fn on_plan_start(&self, query_id: String) -> DaftResult<()>;
    fn on_plan_end(&self, query_id: String, optimized_plan: String) -> DaftResult<()>;
    fn on_exec_start(&self, query_id: String, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()>;
    async fn on_exec_end(&self, query_id: String) -> DaftResult<()>;
    async fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_emit_stats(
        &self,
        query_id: String,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()>;
}

pub fn default_subscribers() -> Vec<Arc<dyn QuerySubscriber>> {
    let mut subscribers: Vec<Arc<dyn QuerySubscriber>> = Vec::new();

    // TODO: Error handling?
    if let Ok(Some(s)) = dashboard::DashboardSubscriber::try_new() {
        subscribers.push(Arc::new(s));
    }

    #[cfg(debug_assertions)]
    if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
        let s = s.to_lowercase();
        match s.as_ref() {
            "1" | "true" => {
                use crate::subscribers::debug::DebugSubscriber;
                subscribers.push(Arc::new(DebugSubscriber));
            }
            _ => {}
        }
    }

    subscribers
}
