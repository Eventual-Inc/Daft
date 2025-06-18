pub(crate) mod dashboard;
#[cfg(debug_assertions)]
pub(crate) mod debug;
pub(crate) mod opentelemetry;

use common_error::DaftResult;

use crate::runtime_stats::RuntimeStatsEvent;

#[async_trait::async_trait]
pub trait RuntimeStatsSubscriber: Send + Sync + std::fmt::Debug {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()>;
    async fn flush(&self) -> DaftResult<()>;
}
