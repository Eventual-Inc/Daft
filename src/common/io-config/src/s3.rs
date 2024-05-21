use aws_credential_types::provider::ProvideCredentials;
use chrono::offset::Utc;
use chrono::DateTime;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::SystemTime;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct S3Config {
    pub region_name: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<String>,
    pub session_token: Option<String>,
    pub access_key: Option<String>,
    pub credentials_provider: Option<Box<dyn S3CredentialsProvider>>,
    pub buffer_time: Option<u64>,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct S3Credentials {
    pub key_id: String,
    pub access_key: String,
    pub session_token: Option<String>,
    pub expiry: Option<SystemTime>,
}

#[typetag::serde(tag = "type")]
pub trait S3CredentialsProvider: ProvideCredentials + Debug {
    fn as_any(&self) -> &dyn Any;
    fn clone_box(&self) -> Box<dyn S3CredentialsProvider>;
    fn dyn_eq(&self, other: &dyn S3CredentialsProvider) -> bool;
    fn dyn_hash(&self, state: &mut dyn Hasher);
}

impl Clone for Box<dyn S3CredentialsProvider> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn S3CredentialsProvider> {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other.as_ref())
    }
}

impl Eq for Box<dyn S3CredentialsProvider> {}

impl Hash for Box<dyn S3CredentialsProvider> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.dyn_hash(state)
    }
}

impl ProvideCredentials for Box<dyn S3CredentialsProvider> {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        self.as_ref().provide_credentials()
    }
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
        if let Some(credentials_provider) = &self.credentials_provider {
            res.push(format!("Credentials provider = {:?}", credentials_provider));
        }
        if let Some(buffer_time) = &self.buffer_time {
            res.push(format!("Buffer time = {}", buffer_time));
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
            credentials_provider: None,
            buffer_time: None,
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
    credentials_provider: {:?}
    buffer_time: {:?}
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
    force_virtual_addressing: {}",
            self.region_name,
            self.endpoint_url,
            self.key_id,
            self.session_token,
            self.access_key,
            self.credentials_provider,
            self.buffer_time,
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
            self.force_virtual_addressing
        )?;
        Ok(())
    }
}

impl S3Credentials {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Key ID = {}", self.key_id));
        res.push(format!("Access key = {}", self.access_key));

        if let Some(session_token) = &self.session_token {
            res.push(format!("Session token = {}", session_token));
        }
        if let Some(expiry) = &self.expiry {
            let expiry: DateTime<Utc> = (*expiry).into();

            res.push(format!("Expiry = {}", expiry.format("%Y-%m-%dT%H:%M:%S")));
        }
        res
    }
}

impl Display for S3Credentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "S3Credentials
    key_id: {:?}
    session_token: {:?},
    access_key: {:?}
    expiry: {:?}",
            self.key_id, self.session_token, self.access_key, self.expiry,
        )
    }
}
