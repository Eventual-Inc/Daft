use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HTTPConfig {
    pub user_agent: String,
    pub bearer_token: Option<ObfuscatedString>,
    pub retry_initial_backoff_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub num_tries: u32,
}

impl Default for HTTPConfig {
    fn default() -> Self {
        Self {
            user_agent: "daft/0.0.1".to_string(), // NOTE: Ideally we grab the version of Daft, but that requires a dependency on daft-core
            bearer_token: None,
            retry_initial_backoff_ms: 1000,
            connect_timeout_ms: 30_000,
            read_timeout_ms: 30_000,
            num_tries: 5,
        }
    }
}

impl HTTPConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let defaults = Self::default();
        let mut res = vec![];
        if let Some(bearer_token) = &self.bearer_token {
            res.push(format!("Bearer token = {bearer_token}"));
        }
        if self.user_agent != defaults.user_agent {
            res.push(format!("User agent = {}", self.user_agent));
        }
        if self.retry_initial_backoff_ms != defaults.retry_initial_backoff_ms {
            res.push(format!(
                "Retry initial backoff ms = {}",
                self.retry_initial_backoff_ms
            ));
        }
        if self.connect_timeout_ms != defaults.connect_timeout_ms {
            res.push(format!("Connect timeout ms = {}", self.connect_timeout_ms));
        }
        if self.read_timeout_ms != defaults.read_timeout_ms {
            res.push(format!("Read timeout ms = {}", self.read_timeout_ms));
        }
        if self.num_tries != defaults.num_tries {
            res.push(format!("Max retries = {}", self.num_tries));
        }
        res
    }
}

impl Display for HTTPConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lines = self.multiline_display();
        if lines.is_empty() {
            write!(f, "HTTPConfig {{}}")
        } else {
            write!(f, "HTTPConfig\n{}", lines.join("\n"))
        }
    }
}
