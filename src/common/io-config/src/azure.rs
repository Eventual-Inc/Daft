use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AzureConfig {
    pub storage_account: Option<String>,
    pub access_key: Option<String>,
    pub sas_token: Option<String>,
    pub bearer_token: Option<String>,
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub use_fabric_endpoint: bool,
    pub anonymous: bool,
    pub endpoint_url: Option<String>,
    pub use_ssl: bool,
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
        }
    }
}

impl AzureConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(storage_account) = &self.storage_account {
            res.push(format!("Storage account = {}", storage_account));
        }
        if let Some(access_key) = &self.access_key {
            res.push(format!("Access key = {}", access_key));
        }
        if let Some(sas_token) = &self.sas_token {
            res.push(format!("Shared Access Signature = {}", sas_token));
        }
        if let Some(bearer_token) = &self.bearer_token {
            res.push(format!("Bearer Token = {}", bearer_token));
        }
        if let Some(tenant_id) = &self.tenant_id {
            res.push(format!("Tenant ID = {}", tenant_id));
        }
        if let Some(client_id) = &self.client_id {
            res.push(format!("Client ID = {}", client_id));
        }
        if let Some(client_secret) = &self.client_secret {
            res.push(format!("Client Secret = {}", client_secret));
        }
        res.push(format!(
            "Use Fabric Endpoint = {}",
            self.use_fabric_endpoint
        ));
        res.push(format!("Anonymous = {}", self.anonymous));
        if let Some(endpoint_url) = &self.endpoint_url {
            res.push(format!("Endpoint URL = {}", endpoint_url));
        }
        res.push(format!("Use SSL = {}", self.use_ssl));
        res
    }
}

impl Display for AzureConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "AzureConfig
    storage_account: {:?}
    access_key: {:?}
    sas_token: {:?}
    bearer_token: {:?}
    tenant_id: {:?}
    client_id: {:?}
    client_secret: {:?}
    use_fabric_endpoint: {:?}
    anonymous: {:?}
    endpoint_url: {:?}
    use_ssl: {:?}",
            self.storage_account,
            self.access_key,
            self.sas_token,
            self.bearer_token,
            self.tenant_id,
            self.client_id,
            self.client_secret,
            self.use_fabric_endpoint,
            self.anonymous,
            self.endpoint_url,
            self.use_ssl
        )
    }
}
