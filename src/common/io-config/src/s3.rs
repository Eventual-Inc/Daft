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
    pub max_connections_per_io_thread: u32,
    pub retry_initial_backoff_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub num_tries: u32,
    pub retry_mode: Option<String>,
    pub anonymous: bool,
    pub use_ssl: bool,
    pub verify_ssl: bool,
    pub check_hostname_ssl: bool,
    pub requester_pays: bool,
    pub force_virtual_addressing: bool,
    pub profile_name: Option<String>,
}

impl S3Config {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(region_name) = &self.region_name {
            res.push(format!("Region name = {}", region_name));
        }
        if let Some(endpoint_url) = &self.endpoint_url {
            res.push(format!("Endpoint URL = {}", endpoint_url));
        }
        if let Some(key_id) = &self.key_id {
            res.push(format!("Key ID = {}", key_id));
        }
        if let Some(session_token) = &self.session_token {
            res.push(format!("Session token = {}", session_token));
        }
        if let Some(access_key) = &self.access_key {
            res.push(format!("Access key = {}", access_key));
        }
        res.push(format!(
            "Max connections = {}",
            self.max_connections_per_io_thread
        ));
        res.push(format!(
            "Retry initial backoff ms = {}",
            self.retry_initial_backoff_ms
        ));
        res.push(format!("Connect timeout ms = {}", self.connect_timeout_ms));
        res.push(format!("Read timeout ms = {}", self.read_timeout_ms));
        res.push(format!("Max retries = {}", self.num_tries));
        if let Some(retry_mode) = &self.retry_mode {
            res.push(format!("Retry mode = {}", retry_mode));
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res.push(format!("Use SSL = {}", self.use_ssl));
        res.push(format!("Verify SSL = {}", self.verify_ssl));
        res.push(format!("Check hostname SSL = {}", self.check_hostname_ssl));
        res.push(format!("Requester pays = {}", self.requester_pays));
        res.push(format!(
            "Force Virtual Addressing = {}",
            self.force_virtual_addressing
        ));
        if let Some(name) = &self.profile_name {
            res.push(format!("Profile Name = {}", name));
        }
        res
    }
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            region_name: None,
            endpoint_url: None,
            key_id: None,
            session_token: None,
            access_key: None,
            max_connections_per_io_thread: 8,
            retry_initial_backoff_ms: 1000,
            connect_timeout_ms: 30_000,
            read_timeout_ms: 30_000,
            // AWS EMR actually does 100 tries by default for AIMD retries
            // (See [Advanced AIMD retry settings]: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-emrfs-retry.html)
            num_tries: 25,
            retry_mode: Some("adaptive".to_string()),
            anonymous: false,
            use_ssl: true,
            verify_ssl: true,
            check_hostname_ssl: true,
            requester_pays: false,
            force_virtual_addressing: false,
            profile_name: None,
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
    use_ssl: {},
    verify_ssl: {},
    check_hostname_ssl: {}
    requester_pays: {}
    force_virtual_addressing: {}
    profile_name: {:?}",
            self.region_name,
            self.endpoint_url,
            self.key_id,
            self.session_token,
            self.access_key,
            self.max_connections_per_io_thread,
            self.retry_initial_backoff_ms,
            self.connect_timeout_ms,
            self.read_timeout_ms,
            self.num_tries,
            self.retry_mode,
            self.anonymous,
            self.use_ssl,
            self.verify_ssl,
            self.check_hostname_ssl,
            self.requester_pays,
            self.force_virtual_addressing,
            self.profile_name
        )
    }
}
