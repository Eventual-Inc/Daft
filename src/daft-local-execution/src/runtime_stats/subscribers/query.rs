use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeInfo;
use daft_context::QuerySubscriber;

use crate::runtime_stats::RuntimeStatsSubscriber;

#[derive(Debug)]
pub(crate) struct QuerySubscriberWrapper {
    inner: Arc<dyn QuerySubscriber>,
    query_id: String,
}

impl QuerySubscriberWrapper {
    pub fn try_new(
        inner: Arc<dyn QuerySubscriber>,
        node_infos: &[Arc<NodeInfo>],
    ) -> DaftResult<Self> {
        let query_id = node_infos[0].context["query_id"].clone();
        inner.on_exec_start(query_id.clone(), node_infos)?;
        Ok(Self { inner, query_id })
    }
}

impl RuntimeStatsSubscriber for QuerySubscriberWrapper {
    fn finish(self: Box<Self>) -> DaftResult<()> {
        self.inner.on_exec_end(self.query_id.clone())?;
        Ok(())
    }

    fn initialize_node(&self, node_info: &NodeInfo) -> DaftResult<()> {
        self.inner
            .on_exec_operator_start(self.query_id.clone(), node_info.id)?;
        Ok(())
    }

    fn finalize_node(&self, node_info: &NodeInfo) -> DaftResult<()> {
        self.inner
            .on_exec_operator_end(self.query_id.clone(), node_info.id)?;
        Ok(())
    }

    fn handle_event(
        &self,
        events: &[(&NodeInfo, common_metrics::StatSnapshotSend)],
    ) -> DaftResult<()> {
        let all_node_stats = events
            .iter()
            .map(|(node_info, snapshot)| (node_info.id, snapshot.clone().into()))
            .collect::<Vec<_>>();

        self.inner
            .on_exec_emit_stats(self.query_id.clone(), all_node_stats.as_slice())?;
        Ok(())
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        unimplemented!("We don't support this for query subscribers right now");
    }
}
