use common_error::DaftResult;
use common_metrics::StatSnapshotSend;

use crate::{ops::NodeInfo, runtime_stats::subscribers::RuntimeStatsSubscriber};

#[derive(Debug)]
/// Simple Debug Subscriber that prints events to stdout.
/// Note: this is feature gated to `#[cfg(debug_assertions)]` and should only be used for debugging purposes.
pub struct DebugSubscriber;

impl RuntimeStatsSubscriber for DebugSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn finalize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn handle_event(&self, events: &[(&NodeInfo, StatSnapshotSend)]) -> DaftResult<()> {
        for (node_info, event) in events {
            println!("{:?} {:#?}", node_info, event);
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> DaftResult<()> {
        Ok(())
    }
}
