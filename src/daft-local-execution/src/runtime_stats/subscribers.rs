pub(crate) mod dashboard;
pub(crate) mod opentelemetry;

use common_error::DaftResult;

use crate::runtime_stats::RuntimeStatsEvent;

pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;
    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()>;
}
