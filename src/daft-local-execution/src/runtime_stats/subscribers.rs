pub(crate) mod progress_bar;
pub(crate) mod query;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, StatSnapshot};

#[async_trait]
pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    /// Called when a node starts.
    async fn initialize_node(&self, node_id: NodeID) -> DaftResult<()>;
    /// Called when a node finishes.
    async fn finalize_node(&self, node_id: NodeID) -> DaftResult<()>;
    /// Called each time the manager ticks and when a node finishes.
    async fn handle_event(&self, events: &[(NodeID, StatSnapshot)]) -> DaftResult<()>;
    /// Called when the entire pipeline finishes.
    async fn finish(self: Box<Self>) -> DaftResult<()>;
}
