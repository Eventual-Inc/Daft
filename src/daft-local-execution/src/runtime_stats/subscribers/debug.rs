use common_error::DaftResult;

use crate::runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStatsEvent};

#[derive(Debug)]
/// Simple Debug Subscriber that prints events to stdout.
/// Note: this is feature gated to `#[cfg(debug_assertions)]` and should only be used for debugging purposes.
pub struct DebugSubscriber;

#[async_trait::async_trait]
impl RuntimeStatsSubscriber for DebugSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
        println!("{:#?}", event);
        Ok(())
    }
    async fn flush(&self) -> DaftResult<()> {
        Ok(())
    }
}
