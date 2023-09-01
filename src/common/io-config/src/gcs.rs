use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GCSConfig {
    pub project_id: Option<String>,
    pub anonymous: bool,
}

impl Display for GCSConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "GCSConfig
    project_id: {:?}
    anonymous: {:?}",
            self.project_id, self.anonymous
        )
    }
}
