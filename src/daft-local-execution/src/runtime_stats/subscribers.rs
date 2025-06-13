pub(crate) mod dashboard;
pub(crate) mod opentelemetry;

use std::sync::Arc;

use async_trait::async_trait;

use crate::pipeline::NodeInfo;
#[async_trait]
pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;

    async fn on_rows_received(&self, context: &Arc<NodeInfo>, count: u64);
    async fn on_rows_emitted(&self, context: &Arc<NodeInfo>, count: u64);
    async fn on_cpu_time_elapsed(&self, context: &Arc<NodeInfo>, microseconds: u64);
}
