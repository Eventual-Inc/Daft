use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AzureConfig {
    pub storage_account: Option<String>,
    pub access_key: Option<ObfuscatedString>,
    pub sas_token: Option<String>,
    pub bearer_token: Option<String>,
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<ObfuscatedString>,
    pub use_fabric_endpoint: bool,
    pub anonymous: bool,
    pub endpoint_url: Option<String>,
    pub use_ssl: bool,
    pub max_connections_per_io_thread: u32,
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            storage_account: None,
            access_key: None,
            sas_token: None,
            bearer_token: None,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            use_fabric_endpoint: false,
            anonymous: false,
            endpoint_url: None,
            use_ssl: true,
            max_connections_per_io_thread: 8,
        }
    }
}

impl AzureConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let defaults = Self::default();
        let mut res = vec![];
        if let Some(storage_account) = &self.storage_account {
            res.push(format!("Storage account = {storage_account}"));
        }
        if let Some(access_key) = &self.access_key {
            res.push(format!("Access key = {access_key}"));
        }
        if let Some(sas_token) = &self.sas_token {
            res.push(format!("Shared Access Signature = {sas_token}"));
        }
        if let Some(bearer_token) = &self.bearer_token {
            res.push(format!("Bearer Token = {bearer_token}"));
        }
        if let Some(tenant_id) = &self.tenant_id {
            res.push(format!("Tenant ID = {tenant_id}"));
        }
        if let Some(client_id) = &self.client_id {
            res.push(format!("Client ID = {client_id}"));
        }
        if let Some(client_secret) = &self.client_secret {
            res.push(format!("Client Secret = {client_secret}"));
        }
        if self.use_fabric_endpoint != defaults.use_fabric_endpoint {
            res.push(format!(
                "Use Fabric Endpoint = {}",
                self.use_fabric_endpoint
            ));
        }
        if self.anonymous != defaults.anonymous {
            res.push(format!("Anonymous = {}", self.anonymous));
        }
        if let Some(endpoint_url) = &self.endpoint_url {
            res.push(format!("Endpoint URL = {endpoint_url}"));
        }
        if self.use_ssl != defaults.use_ssl {
            res.push(format!("Use SSL = {}", self.use_ssl));
        }
        if self.max_connections_per_io_thread != defaults.max_connections_per_io_thread {
            res.push(format!(
                "Max connections = {}",
                self.max_connections_per_io_thread
            ));
        }
        res
    }
}

impl Display for AzureConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lines = self.multiline_display();
        if lines.is_empty() {
            write!(f, "AzureConfig {{}}")
        } else {
            write!(f, "AzureConfig\n    {}", lines.join("\n    "))
        }
    }
}
