use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, QueryID, QueryPlan};
use daft_context::Subscriber;

use crate::runtime_stats::RuntimeStatsSubscriber;

#[derive(Debug)]
pub(crate) struct SubscriberWrapper {
    inner: Arc<dyn Subscriber>,
    query_id: QueryID,
}

impl SubscriberWrapper {
    pub fn try_new(
        inner: Arc<dyn Subscriber>,
        query_id: QueryID,
        physical_plan: QueryPlan,
    ) -> DaftResult<Self> {
        inner.on_exec_start(query_id.clone(), physical_plan)?;
        Ok(Self { inner, query_id })
    }
}

#[async_trait]
impl RuntimeStatsSubscriber for SubscriberWrapper {
    async fn initialize_node(&self, node_id: NodeID) -> DaftResult<()> {
        self.inner
            .on_exec_operator_start(self.query_id.clone(), node_id)
            .await?;
        Ok(())
    }

    async fn finalize_node(&self, node_id: NodeID) -> DaftResult<()> {
        self.inner
            .on_exec_operator_end(self.query_id.clone(), node_id)
            .await?;
        Ok(())
    }

    async fn handle_event(
        &self,
        events: &[(NodeID, common_metrics::StatSnapshot)],
    ) -> DaftResult<()> {
        let all_node_stats = events
            .iter()
            .map(|(node_id, snapshot)| (*node_id, snapshot.clone().into()))
            .collect::<Vec<_>>();

        self.inner
            .on_exec_emit_stats(self.query_id.clone(), all_node_stats.as_slice())
            .await?;
        Ok(())
    }

    async fn finish(self: Box<Self>) -> DaftResult<()> {
        self.inner.on_exec_end(self.query_id.clone()).await?;
        Ok(())
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        unimplemented!("We don't support this for query subscribers right now");
    }
}
