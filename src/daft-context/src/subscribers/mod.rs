pub mod dashboard;
mod debug;
pub mod events;
#[cfg(feature = "python")]
pub mod python;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use common_error::{DaftError, DaftResult};
use common_metrics::{QueryEndState, QueryID, QueryPlan};
use daft_core::prelude::SchemaRef;
pub use events::Event;

use crate::subscribers::events::EventHeader;

#[derive(Debug)]
pub struct QueryMetadata {
    pub output_schema: SchemaRef,
    pub unoptimized_plan: QueryPlan,
    pub runner: String,
    pub ray_dashboard_url: Option<String>,
    pub entrypoint: Option<String>,
    pub python_version: Option<String>,
    pub daft_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub end_state: QueryEndState,
    pub error_message: Option<String>,
}

pub fn now_epoch_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_secs_f64()
}

pub fn event_header(query_id: QueryID) -> EventHeader {
    EventHeader {
        query_id,
        timestamp_epoch_secs: now_epoch_secs(),
    }
}

pub trait Subscriber: Send + Sync + std::fmt::Debug + 'static {
    fn on_event(&self, _event: Event) -> DaftResult<()>;
}

pub fn default_subscribers() -> HashMap<String, Arc<dyn Subscriber>> {
    let mut subscribers: HashMap<String, Arc<dyn Subscriber>> = HashMap::new();

    // Dashboard subscriber
    match dashboard::DashboardSubscriber::try_new() {
        Ok(Some(s)) => {
            subscribers.insert("_dashboard".to_string(), Arc::new(s));
        }
        Err(e) => match e {
            DaftError::NotImplemented(msg) => {
                panic!("{}", msg);
            }
            _ => log::error!("Failed to connect to the daft dashboard: {}", e),
        },
        _ => {}
    }

    #[cfg(debug_assertions)]
    if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
        let s = s.to_lowercase();
        match s.as_ref() {
            "1" | "true" => {
                use crate::subscribers::debug::DebugSubscriber;
                subscribers.insert("_debug".to_string(), Arc::new(DebugSubscriber::new()));
            }
            _ => {}
        }
    }

    subscribers
}
