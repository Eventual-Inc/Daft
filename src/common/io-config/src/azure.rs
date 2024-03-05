use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AzureConfig {
    pub storage_account: Option<String>,
    pub access_key: Option<String>,
    pub anonymous: bool,
    pub endpoint_url: Option<String>,
    pub use_ssl: bool,
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            storage_account: None,
            access_key: None,
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
        res.push(format!("Anoynmous = {}", self.anonymous));
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
    anonymous: {:?}
    endpoint_url: {:?}
    use_ssl: {:?}",
            self.storage_account, self.access_key, self.anonymous, self.endpoint_url, self.use_ssl
        )
    }
}
