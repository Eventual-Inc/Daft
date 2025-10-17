mod dashboard;
mod debug;
#[cfg(feature = "python")]
pub mod python;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, QueryID, QueryPlan, StatSnapshotView, ops::NodeInfo};
use daft_core::prelude::SchemaRef;
use daft_micropartition::partitioning::PartitionRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryMetadata {
    pub output_schema: SchemaRef,
    pub unoptimized_plan: QueryPlan,
}

#[async_trait]
pub trait Subscriber:
    Send + Sync + std::fmt::Debug + Serialize + Deserialize<'static> + 'static
{
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()>;
    fn on_query_end(&self, query_id: QueryID) -> DaftResult<()>;
    fn on_result_out(&self, query_id: QueryID, result: PartitionRef) -> DaftResult<()>;
    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()>;
    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()>;
    fn on_exec_start(&self, query_id: QueryID, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()>;
    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()>;
    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()>;
    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Subscribers {
    Dashboard(dashboard::DashboardSubscriber),
    Debug(debug::DebugSubscriber),
    #[cfg(feature = "python")]
    Python(python::PySubscriberWrapper),
}

#[async_trait]
impl Subscriber for Subscribers {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_query_start(query_id, metadata),
            Self::Debug(s) => s.on_query_start(query_id, metadata),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_query_start(query_id, metadata),
        }
    }

    fn on_query_end(&self, query_id: QueryID) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_query_end(query_id),
            Self::Debug(s) => s.on_query_end(query_id),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_query_end(query_id),
        }
    }

    fn on_result_out(&self, query_id: QueryID, result: PartitionRef) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_result_out(query_id, result),
            Self::Debug(s) => s.on_result_out(query_id, result),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_result_out(query_id, result),
        }
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_optimization_start(query_id),
            Self::Debug(s) => s.on_optimization_start(query_id),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_optimization_start(query_id),
        }
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_optimization_end(query_id, optimized_plan),
            Self::Debug(s) => s.on_optimization_end(query_id, optimized_plan),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_optimization_end(query_id, optimized_plan),
        }
    }

    fn on_exec_start(&self, query_id: QueryID, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_exec_start(query_id, node_infos),
            Self::Debug(s) => s.on_exec_start(query_id, node_infos),
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_exec_start(query_id, node_infos),
        }
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_exec_operator_start(query_id, node_id).await,
            Self::Debug(s) => s.on_exec_operator_start(query_id, node_id).await,
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_exec_operator_start(query_id, node_id).await,
        }
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_exec_emit_stats(query_id, stats).await,
            Self::Debug(s) => s.on_exec_emit_stats(query_id, stats).await,
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_exec_emit_stats(query_id, stats).await,
        }
    }

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_exec_operator_end(query_id, node_id).await,
            Self::Debug(s) => s.on_exec_operator_end(query_id, node_id).await,
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_exec_operator_end(query_id, node_id).await,
        }
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        match self {
            Self::Dashboard(s) => s.on_exec_end(query_id).await,
            Self::Debug(s) => s.on_exec_end(query_id).await,
            #[cfg(feature = "python")]
            Self::Python(s) => s.on_exec_end(query_id).await,
        }
    }
}

pub fn default_subscribers() -> HashMap<String, Arc<Subscribers>> {
    let mut subscribers: HashMap<String, Arc<Subscribers>> = HashMap::new();

    // Dashboard subscriber
    match dashboard::DashboardSubscriber::try_new() {
        Ok(Some(s)) => {
            subscribers.insert(
                "_dashboard".to_string(),
                Arc::new(Subscribers::Dashboard(s)),
            );
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
                use debug::DebugSubscriber;
                subscribers.insert(
                    "_debug".to_string(),
                    Arc::new(Subscribers::Debug(DebugSubscriber::new())),
                );
            }
            _ => {}
        }
    }

    subscribers
}
