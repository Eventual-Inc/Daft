pub(crate) mod dashboard;
#[cfg(debug_assertions)]
pub(crate) mod debug;
pub(crate) mod opentelemetry;
pub(crate) mod progress_bar;

use common_error::DaftResult;

use crate::{pipeline::NodeInfo, runtime_stats::values::StatSnapshot};

#[async_trait::async_trait]
pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    fn initialize(&mut self, node_info: &NodeInfo) -> DaftResult<()>;
    fn handle_event(&self, event: &StatSnapshot, node_info: &NodeInfo) -> DaftResult<()>;
    async fn flush(&self) -> DaftResult<()>;
}
