use common_error::DaftResult;

use crate::runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStatsEvent};

#[derive(Debug)]
pub struct DebugSubscriber;
impl RuntimeStatsSubscriber for DebugSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
        println!("{:#?}", event);
        Ok(())
    }
}
