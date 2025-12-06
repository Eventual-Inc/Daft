mod dashboard;
mod debug;
#[cfg(feature = "python")]
pub mod python;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, QueryEndState, QueryID, QueryPlan, StatSnapshot};
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartitionRef;

pub struct QueryMetadata {
    pub output_schema: SchemaRef,
    pub unoptimized_plan: QueryPlan,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub end_state: QueryEndState,
    pub error_message: Option<String>,
}

#[async_trait]
pub trait Subscriber: Send + Sync + std::fmt::Debug + 'static {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()>;
    fn on_query_end(&self, query_id: QueryID, result: QueryResult) -> DaftResult<()>;
    fn on_result_out(&self, query_id: QueryID, result: MicroPartitionRef) -> DaftResult<()>;
    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()>;
    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()>;
    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()>;
    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: &[(NodeID, StatSnapshot)],
    ) -> DaftResult<()>;
    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()>;
}

pub fn default_subscribers() -> HashMap<String, Arc<dyn Subscriber>> {
    let mut subscribers: HashMap<String, Arc<dyn Subscriber>> = HashMap::new();

    // Dashboard subscriber
    match dashboard::DashboardSubscriber::try_new() {
        Ok(Some(s)) => {
            subscribers.insert("_dashboard".to_string(), Arc::new(s));
        }
        Err(e) => {
            log::error!("Failed to connect to the daft dashboard: {}", e);
        }
        _ => {}
    }

    #[cfg(debug_assertions)]
    if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
        let s = s.to_lowercase();
        match s.as_ref() {
            "1" | "true" => {
                use crate::subscribers::debug::DebugSubscriber;
                subscribers.insert("_debug".to_string(), Arc::new(DebugSubscriber::new()));
            }
            _ => {}
        }
    }

    subscribers
}
