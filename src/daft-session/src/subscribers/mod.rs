mod debug;
#[cfg(feature = "python")]
pub mod python;

use std::sync::Arc;

pub use debug::DebugSubscriber;

use common_error::DaftResult;

// TODO: Make this a global type across all plans
pub(crate) type NodeID = u32;

pub trait QuerySubscriber: std::fmt::Debug + Send + Sync + 'static {
    // Overall query event handlers
    // TODO: Add query_id and unoptimized_plan
    fn on_query_start(&self) -> DaftResult<()>;
    // TODO: Have end event, success, failed, canceled, etc
    fn on_query_end(&self) -> DaftResult<()>;
    // Plan event handlers
    fn on_plan_start(&self) -> DaftResult<()>;
    // TODO: Add optimized_plan
    fn on_plan_end(&self) -> DaftResult<()>;
    // Execution event handlers
    fn on_exec_start(&self) -> DaftResult<()>;
    fn on_exec_operator_start(&self, node_id: NodeID) -> DaftResult<()>;
    // TODO: Add stats as argument
    fn on_exec_emit_stats(&self) -> DaftResult<()>;
    fn on_exec_operator_end(&self, node_id: NodeID) -> DaftResult<()>;
    fn on_exec_end(&self) -> DaftResult<()>;
}

pub fn default_subscribers() -> Vec<Arc<dyn QuerySubscriber>> {
    let mut subscribers: Vec<Arc<dyn QuerySubscriber>> = Vec::new();

    #[cfg(debug_assertions)]
    if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
        let s = s.to_lowercase();
        match s.as_ref() {
            "1" | "true" => {
                subscribers.push(Arc::new(DebugSubscriber));
            }
            _ => {}
        }
    }

    subscribers
}
