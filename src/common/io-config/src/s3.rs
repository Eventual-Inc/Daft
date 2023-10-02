use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct S3Config {
    pub region_name: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<String>,
    pub session_token: Option<String>,
    pub access_key: Option<String>,
    pub max_connections: u32,
    pub retry_initial_backoff_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub num_tries: u32,
    pub retry_mode: Option<String>,
    pub anonymous: bool,
    pub verify_ssl: bool,
    pub check_hostname_ssl: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            region_name: None,
            endpoint_url: None,
            key_id: None,
            session_token: None,
            access_key: None,
            max_connections: 64,
            retry_initial_backoff_ms: 1000,
            connect_timeout_ms: 10_000,
            read_timeout_ms: 10_000,
            num_tries: 5,
            retry_mode: Some("adaptive".to_string()),
            anonymous: false,
            verify_ssl: true,
            check_hostname_ssl: true,
        }
    }
}

impl Display for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "S3Config
    region_name: {:?}
    endpoint_url: {:?}
    key_id: {:?}
    session_token: {:?},
    access_key: {:?}
    max_connections: {},
    retry_initial_backoff_ms: {},
    connect_timeout_ms: {},
    read_timeout_ms: {},
    num_tries: {:?},
    retry_mode: {:?},
    anonymous: {},
    verify_ssl: {},
    check_hostname_ssl: {}",
            self.region_name,
            self.endpoint_url,
            self.key_id,
            self.session_token,
            self.access_key,
            self.retry_initial_backoff_ms,
            self.max_connections,
            self.connect_timeout_ms,
            self.read_timeout_ms,
            self.num_tries,
            self.retry_mode,
            self.anonymous,
            self.verify_ssl,
            self.check_hostname_ssl
        )
    }
}
