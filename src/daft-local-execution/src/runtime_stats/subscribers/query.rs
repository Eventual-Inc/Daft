use std::sync::Arc;

use common_error::DaftResult;
use daft_context::QuerySubscriber;

use crate::runtime_stats::RuntimeStatsSubscriber;

impl RuntimeStatsSubscriber for Arc<dyn QuerySubscriber> {
    fn initialize_node(&self, node_info: &crate::ops::NodeInfo) -> DaftResult<()> {
        self.on_exec_operator_start(node_info.context["query_id"].clone(), node_info.id)?;
        Ok(())
    }

    fn finalize_node(&self, node_info: &crate::ops::NodeInfo) -> DaftResult<()> {
        self.on_exec_operator_end(node_info.context["query_id"].clone(), node_info.id)?;
        Ok(())
    }

    fn handle_event(
        &self,
        events: &[(&crate::ops::NodeInfo, common_metrics::StatSnapshotSend)],
    ) -> DaftResult<()> {
        let query_id = events[0].0.context["query_id"].clone();

        let all_node_stats = events
            .iter()
            .map(|(node_info, snapshot)| (node_info.id, snapshot.clone().into()))
            .collect::<Vec<_>>();

        self.on_exec_emit_stats(query_id, all_node_stats.as_slice())?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> DaftResult<()> {
        Ok(())
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }
}
