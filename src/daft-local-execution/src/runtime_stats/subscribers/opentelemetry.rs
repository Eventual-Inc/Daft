use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, Stat, StatSnapshotSend, ops::NodeInfo};
use common_tracing::flush_oltp_metrics_provider;
use opentelemetry::{KeyValue, global, metrics::Counter};

use crate::runtime_stats::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, subscribers::RuntimeStatsSubscriber,
};

#[derive(Debug)]
pub struct OpenTelemetrySubscriber {
    rows_in: Counter<u64>,
    rows_out: Counter<u64>,
    cpu_us: Counter<u64>,
    id_to_info: HashMap<NodeID, Arc<NodeInfo>>,
}

impl OpenTelemetrySubscriber {
    pub fn new(node_infos: &[Arc<NodeInfo>]) -> Self {
        let id_to_info = node_infos
            .iter()
            .map(|node_info| (node_info.id, node_info.clone()))
            .collect();

        let meter = global::meter("runtime_stats");
        Self {
            rows_in: meter.u64_counter("daft.runtime_stats.rows_in").build(),
            rows_out: meter.u64_counter("daft.runtime_stats.rows_out").build(),
            cpu_us: meter.u64_counter("daft.runtime_stats.cpu_us").build(),
            id_to_info,
        }
    }
}

#[async_trait]
impl RuntimeStatsSubscriber for OpenTelemetrySubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn initialize_node(&self, _: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn finalize_node(&self, _: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn handle_event(&self, events: &[(NodeID, StatSnapshotSend)]) -> DaftResult<()> {
        for (node_id, event) in events {
            let mut attributes = vec![
                KeyValue::new(
                    "name",
                    self.id_to_info.get(node_id).unwrap().name.to_string(),
                ),
                KeyValue::new("id", node_id.to_string()),
            ];
            for (k, v) in &self.id_to_info.get(node_id).unwrap().context {
                attributes.push(KeyValue::new(k.clone(), v.clone()));
            }

            for (k, v) in event.iter() {
                match (k, v) {
                    (ROWS_IN_KEY, Stat::Count(v)) => self.rows_in.add(*v, &attributes),
                    (ROWS_OUT_KEY, Stat::Count(v)) => self.rows_out.add(*v, &attributes),
                    (CPU_US_KEY, Stat::Count(v)) => self.cpu_us.add(*v, &attributes),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn finish(self: Box<Self>) -> DaftResult<()> {
        flush_oltp_metrics_provider();
        Ok(())
    }
}
