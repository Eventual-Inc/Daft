use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::{global, metrics::Counter, KeyValue};

use crate::{pipeline::NodeInfo, runtime_stats::subscribers::RuntimeStatsSubscriber};

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
#[async_trait]
impl RuntimeStatsSubscriber for OpenTelemetrySubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn on_rows_received(&self, context: &Arc<NodeInfo>, count: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }

        self.rows_received.add(count, &attributes);
    }
    async fn on_rows_emitted(&self, context: &Arc<NodeInfo>, count: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }
        self.rows_emitted.add(count, &attributes);
    }

    async fn on_cpu_time_elapsed(&self, context: &Arc<NodeInfo>, microseconds: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }
        self.cpu_us.add(microseconds, &attributes);
    }
}
