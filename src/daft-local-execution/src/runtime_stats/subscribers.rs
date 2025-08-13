pub(crate) mod dashboard;
#[cfg(debug_assertions)]
pub(crate) mod debug;
pub(crate) mod opentelemetry;
pub(crate) mod progress_bar;

use common_error::DaftResult;
use common_metrics::StatSnapshotSend;

use crate::ops::NodeInfo;

pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    /// Called when a node starts.
    fn initialize_node(&self, node_info: &NodeInfo) -> DaftResult<()>;
    /// Called when a node finishes.
    fn finalize_node(&self, node_info: &NodeInfo) -> DaftResult<()>;
    /// Called each time the manager ticks and when a node finishes.
    fn handle_event(&self, events: &[(&NodeInfo, StatSnapshotSend)]) -> DaftResult<()>;
    /// Called when the entire pipeline finishes.
    fn finish(self: Box<Self>) -> DaftResult<()>;
}
