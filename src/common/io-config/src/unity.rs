use std::{fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UnityConfig {
    pub endpoint: Option<String>,
    pub token: Option<ObfuscatedString>,
}

impl UnityConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(endpoint) = &self.endpoint {
            res.push(format!("Endpoint = {endpoint}"));
        }
        if let Some(token) = &self.token {
            res.push(format!("Token = {token}"));
        }
        res
    }
}

impl Display for UnityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnityConfig
    endpoint: {:?}
    token: {:?}",
            self.endpoint, self.token,
        )
    }
}
