use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HTTPConfig {
    pub user_agent: String,
    pub bearer_token: Option<ObfuscatedString>,
}

impl Default for HTTPConfig {
    fn default() -> Self {
        HTTPConfig {
            user_agent: "daft/0.0.1".to_string(), // NOTE: Ideally we grab the version of Daft, but that requires a dependency on daft-core
            bearer_token: None,
        }
    }
}

impl HTTPConfig {
    pub fn new<S: Into<ObfuscatedString>>(bearer_token: Option<S>) -> Self {
        HTTPConfig {
            bearer_token: bearer_token.map(|t| t.into()),
            ..Default::default()
        }
    }
}

impl HTTPConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut v = vec![format!("user_agent = {}", self.user_agent)];
        if let Some(bearer_token) = &self.bearer_token {
            v.push(format!("bearer_token = {}", bearer_token));
        }

        v
    }
}

impl Display for HTTPConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "HTTPConfig
    user_agent: {}",
            self.user_agent,
        )?;

        if let Some(bearer_token) = &self.bearer_token {
            write!(
                f,
                "
    bearer_token: {}",
                bearer_token
            )
        } else {
            Ok(())
        }
    }
}
