use common_error::DaftResult;
use common_tracing::flush_oltp_metrics_provider;
use opentelemetry::{global, metrics::Counter, KeyValue};

use crate::runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStatsEvent};

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
    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
        let mut attributes = vec![
            KeyValue::new("name", event.node_info.name.to_string()),
            KeyValue::new("id", event.node_info.id.to_string()),
        ];
        for (k, v) in &event.node_info.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }
        self.rows_received.add(event.rows_received, &attributes);
        self.rows_emitted.add(event.rows_emitted, &attributes);
        self.cpu_us.add(event.cpu_us, &attributes);
        Ok(())
    }

    async fn flush(&self) -> DaftResult<()> {
        flush_oltp_metrics_provider();
        Ok(())
    }
}
