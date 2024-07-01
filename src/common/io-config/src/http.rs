use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HTTPConfig {
    pub user_agent: String,
}

impl Default for HTTPConfig {
    fn default() -> Self {
        HTTPConfig {
            user_agent: "daft/0.0.1".to_string(), // NOTE: Ideally we grab the version of Daft, but that requires a dependency on daft-core
        }
    }
}

impl HTTPConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("user_agent = {}", self.user_agent)]
    }
}

impl Display for HTTPConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "HTTPConfig
    user_agent: {}",
            self.user_agent,
        )
    }
}
