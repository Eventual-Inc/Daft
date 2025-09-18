use common_error::DaftResult;
use common_metrics::{Stat, StatSnapshotSend};
use common_tracing::flush_oltp_metrics_provider;
use opentelemetry::{KeyValue, global, metrics::Counter};

use crate::{
    ops::NodeInfo,
    runtime_stats::{CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, subscribers::RuntimeStatsSubscriber},
};

#[derive(Debug)]
pub struct OpenTelemetrySubscriber {
    rows_in: Counter<u64>,
    rows_out: Counter<u64>,
    cpu_us: Counter<u64>,
}

impl OpenTelemetrySubscriber {
    pub fn new() -> Self {
        let meter = global::meter("runtime_stats");
        Self {
            rows_in: meter.u64_counter("daft.runtime_stats.rows_in").build(),
            rows_out: meter.u64_counter("daft.runtime_stats.rows_out").build(),
            cpu_us: meter.u64_counter("daft.runtime_stats.cpu_us").build(),
        }
    }
}

impl RuntimeStatsSubscriber for OpenTelemetrySubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize_node(&self, _: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn finalize_node(&self, _: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn handle_event(&self, events: &[(&NodeInfo, StatSnapshotSend)]) -> DaftResult<()> {
        for (node_info, event) in events {
            let mut attributes = vec![
                KeyValue::new("name", node_info.name.to_string()),
                KeyValue::new("id", node_info.id.to_string()),
            ];
            for (k, v) in &node_info.context {
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

    fn finish(self: Box<Self>) -> DaftResult<()> {
        flush_oltp_metrics_provider();
        Ok(())
    }
}
