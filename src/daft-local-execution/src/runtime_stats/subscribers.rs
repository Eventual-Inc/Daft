pub(crate) mod dashboard;
#[cfg(debug_assertions)]
pub(crate) mod debug;
pub(crate) mod opentelemetry;
pub(crate) mod progress_bar;
pub(crate) mod rpc;

use common_error::DaftResult;
use common_metrics::StatSnapshot;

use crate::pipeline::NodeInfo;

#[async_trait::async_trait]
pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    fn initialize_node(&self, node_info: &NodeInfo) -> DaftResult<()>;
    fn finalize_node(&self, node_info: &NodeInfo) -> DaftResult<()>;
    fn handle_event(&self, event: &StatSnapshot, node_info: &NodeInfo) -> DaftResult<()>;
    async fn flush(&self) -> DaftResult<()>;
    fn finish(self: Box<Self>) -> DaftResult<()>;
}
