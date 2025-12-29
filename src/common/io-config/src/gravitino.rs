use std::{fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GravitinoConfig {
    pub endpoint: Option<String>,
    pub metalake_name: Option<String>,
    pub auth_type: Option<String>,
    pub username: Option<String>,
    pub password: Option<ObfuscatedString>,
    pub token: Option<ObfuscatedString>,
}

impl GravitinoConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(endpoint) = &self.endpoint {
            res.push(format!("Endpoint = {endpoint}"));
        }
        if let Some(metalake_name) = &self.metalake_name {
            res.push(format!("Metalake = {metalake_name}"));
        }
        if let Some(auth_type) = &self.auth_type {
            res.push(format!("Auth Type = {auth_type}"));
        }
        if let Some(username) = &self.username {
            res.push(format!("Username = {username}"));
        }
        if let Some(password) = &self.password {
            res.push(format!("Password = {password}"));
        }
        if let Some(token) = &self.token {
            res.push(format!("Token = {token}"));
        }
        res
    }
}

impl Display for GravitinoConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GravitinoConfig
    endpoint: {:?}
    metalake_name: {:?}
    auth_type: {:?}
    username: {:?}
    password: {:?}
    token: {:?}",
            self.endpoint,
            self.metalake_name,
            self.auth_type,
            self.username,
            self.password,
            self.token,
        )
    }
}
