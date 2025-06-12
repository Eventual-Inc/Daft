pub(crate) mod opentelemetry;
pub(crate) mod dashboard;

use crate::pipeline::NodeInfo;

pub trait RuntimeStatsSubscriber: Send + Sync {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;

    fn on_rows_received(&self, context: &NodeInfo, count: u64);
    fn on_rows_emitted(&self, context: &NodeInfo, count: u64);
    fn on_cpu_time_elapsed(&self, context: &NodeInfo, microseconds: u64);
}
