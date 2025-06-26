use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display)]
#[display(
    "GCSConfig
    project_id: {project_id:?}
    anonymous: {anonymous}
    max_connections_per_io_thread: {max_connections_per_io_thread}
    retry_initial_backoff_ms: {retry_initial_backoff_ms}
    connect_timeout_ms: {connect_timeout_ms}
    read_timeout_ms: {read_timeout_ms}
    num_tries: {num_tries}"
)]
pub struct GCSConfig {
    pub project_id: Option<String>,
    pub credentials: Option<ObfuscatedString>,
    pub token: Option<String>,
    pub anonymous: bool,
    pub max_connections_per_io_thread: u32,
    pub retry_initial_backoff_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub num_tries: u32,
}

impl Default for GCSConfig {
    fn default() -> Self {
        Self {
            project_id: None,
            credentials: None,
            token: None,
            anonymous: false,
            max_connections_per_io_thread: 8,
            retry_initial_backoff_ms: 1000,
            connect_timeout_ms: 30_000,
            read_timeout_ms: 30_000,
            num_tries: 5,
        }
    }
}

impl GCSConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(project_id) = &self.project_id {
            res.push(format!("Project ID = {project_id}"));
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res.push(format!(
            "Max connections = {}",
            self.max_connections_per_io_thread
        ));
        res.push(format!(
            "Retry initial backoff ms = {}",
            self.retry_initial_backoff_ms
        ));
        res.push(format!("Connect timeout ms = {}", self.connect_timeout_ms));
        res.push(format!("Read timeout ms = {}", self.read_timeout_ms));
        res.push(format!("Max retries = {}", self.num_tries));
        res
    }
}
