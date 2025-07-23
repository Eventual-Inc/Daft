use common_error::DaftResult;

use crate::{
    pipeline::NodeInfo,
    runtime_stats::{subscribers::RuntimeStatsSubscriber, StatSnapshot},
};

#[derive(Debug)]
/// Simple Debug Subscriber that prints events to stdout.
/// Note: this is feature gated to `#[cfg(debug_assertions)]` and should only be used for debugging purposes.
pub struct DebugSubscriber;

#[async_trait::async_trait]
impl RuntimeStatsSubscriber for DebugSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize(&mut self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn handle_event(&self, event: &StatSnapshot, node_info: &NodeInfo) -> DaftResult<()> {
        println!("{:?} {:#?}", node_info, event);
        Ok(())
    }
    async fn flush(&self) -> DaftResult<()> {
        Ok(())
    }
}
