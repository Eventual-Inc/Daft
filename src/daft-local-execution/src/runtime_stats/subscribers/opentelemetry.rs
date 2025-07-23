use common_error::DaftResult;
use common_tracing::flush_oltp_metrics_provider;
use opentelemetry::{global, metrics::Counter, KeyValue};

use crate::{
    pipeline::NodeInfo,
    runtime_stats::{
        subscribers::RuntimeStatsSubscriber, Stat, StatSnapshot, CPU_US_KEY, ROWS_EMITTED_KEY,
        ROWS_RECEIVED_KEY,
    },
};

#[derive(Debug)]
pub struct OpenTelemetrySubscriber {
    rows_received: Counter<u64>,
    rows_emitted: Counter<u64>,
    cpu_us: Counter<u64>,
}

impl OpenTelemetrySubscriber {
    pub fn new() -> Self {
        let meter = global::meter("runtime_stats");
        Self {
            rows_received: meter
                .u64_counter("daft.runtime_stats.rows_received")
                .build(),
            rows_emitted: meter.u64_counter("daft.runtime_stats.rows_emitted").build(),
            cpu_us: meter.u64_counter("daft.runtime_stats.cpu_us").build(),
        }
    }
}
#[async_trait::async_trait]
impl RuntimeStatsSubscriber for OpenTelemetrySubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize(&mut self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn handle_event(&self, event: &StatSnapshot, node_info: &NodeInfo) -> DaftResult<()> {
        let mut attributes = vec![
            KeyValue::new("name", node_info.name.to_string()),
            KeyValue::new("id", node_info.id.to_string()),
        ];
        for (k, v) in &node_info.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }

        if let Some((_, Stat::Count(rows_received))) =
            event.iter().find(|(k, _)| *k == ROWS_RECEIVED_KEY)
        {
            self.rows_received.add(*rows_received, &attributes);
        }
        if let Some((_, Stat::Count(rows_emitted))) =
            event.iter().find(|(k, _)| *k == ROWS_EMITTED_KEY)
        {
            self.rows_emitted.add(*rows_emitted, &attributes);
        }
        if let Some((_, Stat::Count(cpu_us))) = event.iter().find(|(k, _)| *k == CPU_US_KEY) {
            self.cpu_us.add(*cpu_us, &attributes);
        }
        Ok(())
    }

    async fn flush(&self) -> DaftResult<()> {
        flush_oltp_metrics_provider();
        Ok(())
    }
}
