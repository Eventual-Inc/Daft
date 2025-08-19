use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HuggingFaceConfig {
    pub token: Option<ObfuscatedString>,
    pub anonymous: bool,
    pub endpoint: String,
}

impl Default for HuggingFaceConfig {
    fn default() -> Self {
        Self {
            token: None,
            anonymous: false,
            endpoint: "https://huggingface.co".to_string(),
        }
    }
}

impl HuggingFaceConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(token) = &self.token {
            res.push(format!("Token = {token}"));
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res.push(format!("Endpoint = {}", self.endpoint));
        res
    }
}

impl Display for HuggingFaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "HuggingFaceConfig\n{}",
            self.multiline_display().join("\n")
        )
    }
}
