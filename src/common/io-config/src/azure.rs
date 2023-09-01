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
