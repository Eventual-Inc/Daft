use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AzureConfig {
    pub storage_account: Option<String>,
    pub access_key: Option<String>,
    pub anonymous: bool,
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
    anonymous: {:?}",
            self.storage_account, self.access_key, self.anonymous
        )
    }
}
