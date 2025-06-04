use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_context::get_context;
use daft_io::{AzureConfig, GCSConfig, HTTPConfig, IOConfig, S3Config};
use daft_session::Session;
use uuid::Uuid;

#[derive(Clone)]
pub struct ConnectSession {
    /// so order is preserved, and so we can efficiently do a prefix search
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    id: String,
    server_side_session_id: String,
    pub(crate) compute_runtime: RuntimeRef,
    pub session: Arc<RwLock<Session>>,
}

impl ConnectSession {
    pub fn config_values(&self) -> &BTreeMap<String, String> {
        &self.config_values
    }

    pub fn config_values_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.config_values
    }

    pub fn new(id: String) -> Self {
        let server_side_session_id = Uuid::new_v4();
        let server_side_session_id = server_side_session_id.to_string();
        let compute_runtime = common_runtime::get_compute_runtime();
        let session = Arc::new(RwLock::new(Session::default()));

        Self {
            config_values: Default::default(),
            id,
            server_side_session_id,
            compute_runtime,
            session,
        }
    }

    pub fn client_side_session_id(&self) -> &str {
        &self.id
    }

    pub fn server_side_session_id(&self) -> &str {
        &self.server_side_session_id
    }

    /// get a read only reference to the session
    pub fn session(&self) -> RwLockReadGuard<'_, Session> {
        self.session.read().expect("catalog lock poisoned")
    }

    /// get a mutable reference to the session
    pub fn session_mut(&self) -> std::sync::RwLockWriteGuard<'_, Session> {
        self.session.write().expect("catalog lock poisoned")
    }

    pub fn get_io_config(&self) -> DaftResult<IOConfig> {
        let IOConfig {
            mut s3,
            mut azure,
            mut gcs,
            mut http,
            unity,
        } = get_context().io_config();

        self.s3_config_helper(&mut s3)?;
        self.azure_config_helper(&mut azure)?;
        self.gcs_config_helper(&mut gcs)?;
        self.http_config_helper(&mut http)?;

        Ok(IOConfig {
            s3,
            azure,
            gcs,
            http,
            unity,
        })
    }

    fn s3_config_helper(&self, default_s3_conf: &mut S3Config) -> DaftResult<()> {
        macro_rules! set_opt_str {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.s3.", stringify!($field)))
                    .map(|s| s.to_string())
                {
                    default_s3_conf.$field = Some(value);
                }
            };
        }
        macro_rules! set_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.s3.", stringify!($field)))
                    .map(|s| s.parse().ok())
                    .flatten()
                {
                    default_s3_conf.$field = value;
                }
            };
        }
        macro_rules! set_opt_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.s3.", stringify!($field)))
                    .map(|s| s.parse().ok())
                {
                    default_s3_conf.$field = value;
                }
            };
        }

        set_opt_str!(region_name);
        set_opt_str!(endpoint_url);
        set_opt_str!(key_id);
        set_opt_from_config!(session_token);
        set_opt_from_config!(access_key);
        set_opt_from_config!(buffer_time);
        set_from_config!(max_connections_per_io_thread);
        set_from_config!(retry_initial_backoff_ms);
        set_from_config!(connect_timeout_ms);
        set_from_config!(read_timeout_ms);
        set_from_config!(num_tries);
        set_opt_str!(retry_mode);
        set_from_config!(anonymous);
        set_from_config!(use_ssl);
        set_from_config!(verify_ssl);
        set_from_config!(check_hostname_ssl);
        set_from_config!(requester_pays);
        set_from_config!(force_virtual_addressing);
        set_opt_from_config!(profile_name);

        Ok(())
    }

    fn azure_config_helper(&self, azure_conf: &mut AzureConfig) -> DaftResult<()> {
        macro_rules! set_opt_str {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.azure.", stringify!($field)))
                    .map(|s| s.to_string())
                {
                    azure_conf.$field = Some(value);
                }
            };
        }
        macro_rules! set_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.azure.", stringify!($field)))
                    .map(|s| s.parse().ok())
                    .flatten()
                {
                    azure_conf.$field = value;
                }
            };
        }
        macro_rules! set_opt_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.azure.", stringify!($field)))
                    .map(|s| s.parse().ok())
                {
                    azure_conf.$field = value;
                }
            };
        }
        set_opt_str!(storage_account);
        set_opt_from_config!(access_key);
        set_opt_str!(sas_token);
        set_opt_str!(bearer_token);
        set_opt_str!(tenant_id);
        set_opt_str!(client_id);
        set_opt_from_config!(client_secret);
        set_from_config!(use_fabric_endpoint);
        set_from_config!(anonymous);
        set_opt_str!(endpoint_url);
        set_from_config!(use_ssl);

        Ok(())
    }

    fn gcs_config_helper(&self, gcs_conf: &mut GCSConfig) -> DaftResult<()> {
        macro_rules! set_opt_str {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.gcs.", stringify!($field)))
                    .map(|s| s.to_string())
                {
                    gcs_conf.$field = Some(value);
                }
            };
        }
        macro_rules! set_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.gcs.", stringify!($field)))
                    .map(|s| s.parse().ok())
                    .flatten()
                {
                    gcs_conf.$field = value;
                }
            };
        }
        macro_rules! set_opt_from_config {
            ($field:ident) => {
                if let Some(value) = self
                    .config_values
                    .get(concat!("daft.io.gcs.", stringify!($field)))
                    .map(|s| s.parse().ok())
                {
                    gcs_conf.$field = value;
                }
            };
        }
        set_opt_str!(project_id);
        set_opt_from_config!(credentials);
        set_opt_str!(token);
        set_from_config!(anonymous);
        set_from_config!(max_connections_per_io_thread);
        set_from_config!(retry_initial_backoff_ms);
        set_from_config!(connect_timeout_ms);
        set_from_config!(read_timeout_ms);
        set_from_config!(num_tries);

        Ok(())
    }

    fn http_config_helper(&self, http_conf: &mut HTTPConfig) -> DaftResult<()> {
        if let Some(value) = self
            .config_values
            .get("daft.io.http.user_agent")
            .map(|s| s.to_string())
        {
            http_conf.user_agent = value;
        }

        if let Some(value) = self
            .config_values
            .get("daft.io.http.bearer_token")
            .map(|s| s.to_string())
        {
            http_conf.bearer_token = Some(value.into());
        }

        Ok(())
    }
}
