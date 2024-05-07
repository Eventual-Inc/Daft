use std::hash::Hasher;

use common_error::DaftError;
use pyo3::prelude::*;

use crate::config;

/// Create configurations to be used when accessing an S3-compatible system
///
/// Args:
///     region_name: Name of the region to be used (used when accessing AWS S3), defaults to "us-east-1".
///         If wrongly provided, Daft will attempt to auto-detect the buckets' region at the cost of extra S3 requests.
///     endpoint_url: URL to the S3 endpoint, defaults to endpoints to AWS
///     key_id: AWS Access Key ID, defaults to auto-detection from the current environment
///     access_key: AWS Secret Access Key, defaults to auto-detection from the current environment
///     max_connections: Maximum number of connections to S3 at any time, defaults to 64
///     session_token: AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
///     retry_initial_backoff_ms: Initial backoff duration in milliseconds for an S3 retry, defaults to 1000ms
///     connect_timeout_ms: Timeout duration to wait to make a connection to S3 in milliseconds, defaults to 10 seconds
///     read_timeout_ms: Timeout duration to wait to read the first byte from S3 in milliseconds, defaults to 10 seconds
///     num_tries: Number of attempts to make a connection, defaults to 5
///     retry_mode: Retry Mode when a request fails, current supported values are `standard` and `adaptive`, defaults to `adaptive`
///     anonymous: Whether or not to use "anonymous mode", which will access S3 without any credentials
///     use_ssl: Whether or not to use SSL, which require accessing S3 over HTTPS rather than HTTP, defaults to True
///     verify_ssl: Whether or not to verify ssl certificates, which will access S3 without checking if the certs are valid, defaults to True
///     check_hostname_ssl: Whether or not to verify the hostname when verifying ssl certificates, this was the legacy behavior for openssl, defaults to True
///     requester_pays: Whether or not the authenticated user will assume transfer costs, which is required by some providers of bulk data, defaults to False
///     force_virtual_addressing: Force S3 client to use virtual addressing in all cases. If False, virtual addressing will only be used if `endpoint_url` is empty, defaults to False
///     profile_name: Name of AWS_PROFILE to load, defaults to None which will then check the Environment Variable `AWS_PROFILE` then fall back to `default`
///
/// Example:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx"))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct S3Config {
    pub config: crate::S3Config,
}
/// Create configurations to be used when accessing Azure Blob Storage
///
/// Args:
///     storage_account: Azure Storage Account, defaults to reading from `AZURE_STORAGE_ACCOUNT` environment variable.
///     access_key: Azure Secret Access Key, defaults to reading from `AZURE_STORAGE_KEY` environment variable
///     anonymous: Whether or not to use "anonymous mode", which will access Azure without any credentials
///
/// Example:
///     >>> io_config = IOConfig(azure=AzureConfig(storage_account="dafttestdata", access_key="xxx"))
///     >>> daft.read_parquet("az://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct AzureConfig {
    pub config: crate::AzureConfig,
}

/// Create configurations to be used when accessing Google Cloud Storage
///
/// Args:
///     project_id: Google Project ID, defaults to reading credentials file or Google Cloud metadata service
///     anonymous: Whether or not to use "anonymous mode", which will access Google Storage without any credentials
///
/// Example:
///     >>> io_config = IOConfig(gcs=GCSConfig(anonymous=True))
///     >>> daft.read_parquet("gs://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct GCSConfig {
    pub config: crate::GCSConfig,
}

/// Create configurations to be used when accessing storage
///
/// Args:
///     s3: Configuration to use when accessing URLs with the `s3://` scheme
///     azure: Configuration to use when accessing URLs with the `az://` or `abfs://` scheme
///     gcs: Configuration to use when accessing URLs with the `gs://` or `gcs://` scheme
/// Example:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx", num_tries=10), azure=AzureConfig(anonymous=True), gcs=GCSConfig(...))
///     >>> daft.read_parquet(["s3://some-path", "az://some-other-path", "gs://path3"], io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct IOConfig {
    pub config: config::IOConfig,
}

#[pymethods]
impl IOConfig {
    #[new]
    pub fn new(s3: Option<S3Config>, azure: Option<AzureConfig>, gcs: Option<GCSConfig>) -> Self {
        IOConfig {
            config: config::IOConfig {
                s3: s3.unwrap_or_default().config,
                azure: azure.unwrap_or_default().config,
                gcs: gcs.unwrap_or_default().config,
            },
        }
    }

    pub fn replace(
        &self,
        s3: Option<S3Config>,
        azure: Option<AzureConfig>,
        gcs: Option<GCSConfig>,
    ) -> Self {
        IOConfig {
            config: config::IOConfig {
                s3: s3.map(|s3| s3.config).unwrap_or(self.config.s3.clone()),
                azure: azure
                    .map(|azure| azure.config)
                    .unwrap_or(self.config.azure.clone()),
                gcs: gcs.map(|gcs| gcs.config).unwrap_or(self.config.gcs.clone()),
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    /// Configuration to be used when accessing s3 URLs
    #[getter]
    pub fn s3(&self) -> PyResult<S3Config> {
        Ok(S3Config {
            config: self.config.s3.clone(),
        })
    }

    /// Configuration to be used when accessing Azure URLs
    #[getter]
    pub fn azure(&self) -> PyResult<AzureConfig> {
        Ok(AzureConfig {
            config: self.config.azure.clone(),
        })
    }

    /// Configuration to be used when accessing Azure URLs
    #[getter]
    pub fn gcs(&self) -> PyResult<GCSConfig> {
        Ok(GCSConfig {
            config: self.config.gcs.clone(),
        })
    }

    #[staticmethod]
    pub fn from_json(input: &str) -> PyResult<Self> {
        let config: config::IOConfig = serde_json::from_str(input).map_err(DaftError::from)?;
        Ok(config.into())
    }

    pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, (String,))> {
        let io_config_module = py.import("daft.io.config")?;
        let json_string = serde_json::to_string(&self.config).map_err(DaftError::from)?;
        Ok((
            io_config_module
                .getattr("_io_config_from_json")?
                .to_object(py),
            (json_string,),
        ))
    }

    pub fn __hash__(&self) -> PyResult<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;

        let mut hasher = DefaultHasher::new();
        self.config.hash(&mut hasher);
        Ok(hasher.finish())
    }
}

#[pymethods]
impl S3Config {
    #[allow(clippy::too_many_arguments)]
    #[new]
    pub fn new(
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        max_connections: Option<u32>,
        retry_initial_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        num_tries: Option<u32>,
        retry_mode: Option<String>,
        anonymous: Option<bool>,
        use_ssl: Option<bool>,
        verify_ssl: Option<bool>,
        check_hostname_ssl: Option<bool>,
        requester_pays: Option<bool>,
        force_virtual_addressing: Option<bool>,
        profile_name: Option<String>,
    ) -> Self {
        let def = crate::S3Config::default();
        S3Config {
            config: crate::S3Config {
                region_name: region_name.or(def.region_name),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                key_id: key_id.or(def.key_id),
                session_token: session_token.or(def.session_token),
                access_key: access_key.or(def.access_key),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(def.max_connections_per_io_thread),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(def.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(def.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(def.read_timeout_ms),
                num_tries: num_tries.unwrap_or(def.num_tries),
                retry_mode: retry_mode.or(def.retry_mode),
                anonymous: anonymous.unwrap_or(def.anonymous),
                use_ssl: use_ssl.unwrap_or(def.use_ssl),
                verify_ssl: verify_ssl.unwrap_or(def.verify_ssl),
                check_hostname_ssl: check_hostname_ssl.unwrap_or(def.check_hostname_ssl),
                requester_pays: requester_pays.unwrap_or(def.requester_pays),
                force_virtual_addressing: force_virtual_addressing
                    .unwrap_or(def.force_virtual_addressing),
                profile_name: profile_name.or(def.profile_name),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace(
        &self,
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        max_connections: Option<u32>,
        retry_initial_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        num_tries: Option<u32>,
        retry_mode: Option<String>,
        anonymous: Option<bool>,
        use_ssl: Option<bool>,
        verify_ssl: Option<bool>,
        check_hostname_ssl: Option<bool>,
        requester_pays: Option<bool>,
        force_virtual_addressing: Option<bool>,
        profile_name: Option<String>,
    ) -> Self {
        S3Config {
            config: crate::S3Config {
                region_name: region_name.or_else(|| self.config.region_name.clone()),
                endpoint_url: endpoint_url.or_else(|| self.config.endpoint_url.clone()),
                key_id: key_id.or_else(|| self.config.key_id.clone()),
                session_token: session_token.or_else(|| self.config.session_token.clone()),
                access_key: access_key.or_else(|| self.config.access_key.clone()),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(self.config.max_connections_per_io_thread),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(self.config.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(self.config.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(self.config.read_timeout_ms),
                num_tries: num_tries.unwrap_or(self.config.num_tries),
                retry_mode: retry_mode.or_else(|| self.config.retry_mode.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
                use_ssl: use_ssl.unwrap_or(self.config.use_ssl),
                verify_ssl: verify_ssl.unwrap_or(self.config.verify_ssl),
                check_hostname_ssl: check_hostname_ssl.unwrap_or(self.config.check_hostname_ssl),
                requester_pays: requester_pays.unwrap_or(self.config.requester_pays),
                force_virtual_addressing: force_virtual_addressing
                    .unwrap_or(self.config.force_virtual_addressing),
                profile_name: profile_name.or_else(|| self.config.profile_name.clone()),
            },
        }
    }

    /// Creates an S3Config from the current environment, auto-discovering variables such as
    /// credentials, regions and more.
    #[staticmethod]
    pub fn from_env(py: Python) -> PyResult<Self> {
        let io_config_from_env_func = py
            .import(pyo3::intern!(py, "daft"))?
            .getattr(pyo3::intern!(py, "daft"))?
            .getattr(pyo3::intern!(py, "s3_config_from_env"))?;
        io_config_from_env_func.call0().map(|pyany| {
            pyany
                .extract()
                .expect("s3_config_from_env function must return S3Config")
        })
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    /// Region to use when accessing AWS S3
    #[getter]
    pub fn region_name(&self) -> PyResult<Option<String>> {
        Ok(self.config.region_name.clone())
    }

    /// S3-compatible endpoint to use
    #[getter]
    pub fn endpoint_url(&self) -> PyResult<Option<String>> {
        Ok(self.config.endpoint_url.clone())
    }

    /// AWS Access Key ID
    #[getter]
    pub fn key_id(&self) -> PyResult<Option<String>> {
        Ok(self.config.key_id.clone())
    }

    /// AWS Session Token
    #[getter]
    pub fn session_token(&self) -> PyResult<Option<String>> {
        Ok(self.config.session_token.clone())
    }

    /// AWS Secret Access Key
    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self.config.access_key.clone())
    }

    /// AWS max connections per IO thread
    #[getter]
    pub fn max_connections(&self) -> PyResult<u32> {
        Ok(self.config.max_connections_per_io_thread)
    }

    /// AWS Retry Initial Backoff Time in Milliseconds
    #[getter]
    pub fn retry_initial_backoff_ms(&self) -> PyResult<u64> {
        Ok(self.config.retry_initial_backoff_ms)
    }

    /// AWS Connection Timeout in Milliseconds
    #[getter]
    pub fn connect_timeout_ms(&self) -> PyResult<u64> {
        Ok(self.config.connect_timeout_ms)
    }

    /// AWS Read Timeout in Milliseconds
    #[getter]
    pub fn read_timeout_ms(&self) -> PyResult<u64> {
        Ok(self.config.read_timeout_ms)
    }

    /// AWS Number Retries
    #[getter]
    pub fn num_tries(&self) -> PyResult<u32> {
        Ok(self.config.num_tries)
    }

    /// AWS Retry Mode
    #[getter]
    pub fn retry_mode(&self) -> PyResult<Option<String>> {
        Ok(self.config.retry_mode.clone())
    }

    /// AWS Anonymous Mode
    #[getter]
    pub fn anonymous(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.anonymous))
    }

    /// AWS Use SSL
    #[getter]
    pub fn use_ssl(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.use_ssl))
    }

    /// AWS Verify SSL
    #[getter]
    pub fn verify_ssl(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.verify_ssl))
    }

    /// AWS Check SSL Hostname
    #[getter]
    pub fn check_hostname_ssl(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.check_hostname_ssl))
    }

    /// AWS Requester Pays
    #[getter]
    pub fn requester_pays(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.requester_pays))
    }

    /// AWS force virtual addressing
    #[getter]
    pub fn force_virtual_addressing(&self) -> PyResult<Option<bool>> {
        Ok(Some(self.config.force_virtual_addressing))
    }

    /// AWS profile name
    #[getter]
    pub fn profile_name(&self) -> PyResult<Option<String>> {
        Ok(self.config.profile_name.clone())
    }
}

#[pymethods]
impl AzureConfig {
    #[allow(clippy::too_many_arguments)]
    #[new]
    pub fn new(
        storage_account: Option<String>,
        access_key: Option<String>,
        anonymous: Option<bool>,
        endpoint_url: Option<String>,
        use_ssl: Option<bool>,
    ) -> Self {
        let def = crate::AzureConfig::default();
        AzureConfig {
            config: crate::AzureConfig {
                storage_account: storage_account.or(def.storage_account),
                access_key: access_key.or(def.access_key),
                anonymous: anonymous.unwrap_or(def.anonymous),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                use_ssl: use_ssl.unwrap_or(def.use_ssl),
            },
        }
    }

    pub fn replace(
        &self,
        storage_account: Option<String>,
        access_key: Option<String>,
        anonymous: Option<bool>,
        endpoint_url: Option<String>,
        use_ssl: Option<bool>,
    ) -> Self {
        AzureConfig {
            config: crate::AzureConfig {
                storage_account: storage_account.or_else(|| self.config.storage_account.clone()),
                access_key: access_key.or_else(|| self.config.access_key.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
                endpoint_url: endpoint_url.or_else(|| self.config.endpoint_url.clone()),
                use_ssl: use_ssl.unwrap_or(self.config.use_ssl),
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    /// Storage Account to use when accessing Azure Storage
    #[getter]
    pub fn storage_account(&self) -> PyResult<Option<String>> {
        Ok(self.config.storage_account.clone())
    }

    /// Azure Secret Access Key
    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self.config.access_key.clone())
    }

    /// Whether access is anonymous
    #[getter]
    pub fn anonymous(&self) -> PyResult<bool> {
        Ok(self.config.anonymous)
    }

    /// Azure Secret Access Key
    #[getter]
    pub fn endpoint_url(&self) -> PyResult<Option<String>> {
        Ok(self.config.endpoint_url.clone())
    }

    /// Whether SSL (HTTPS) is required
    #[getter]
    pub fn use_ssl(&self) -> PyResult<bool> {
        Ok(self.config.use_ssl)
    }
}

#[pymethods]
impl GCSConfig {
    #[allow(clippy::too_many_arguments)]
    #[new]
    pub fn new(project_id: Option<String>, anonymous: Option<bool>) -> Self {
        let def = crate::GCSConfig::default();
        GCSConfig {
            config: crate::GCSConfig {
                project_id: project_id.or(def.project_id),
                anonymous: anonymous.unwrap_or(def.anonymous),
            },
        }
    }

    pub fn replace(&self, project_id: Option<String>, anonymous: Option<bool>) -> Self {
        GCSConfig {
            config: crate::GCSConfig {
                project_id: project_id.or_else(|| self.config.project_id.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    /// Project ID to use when accessing Google Cloud Storage
    #[getter]
    pub fn project_id(&self) -> PyResult<Option<String>> {
        Ok(self.config.project_id.clone())
    }

    /// Whether to use anonymous mode
    #[getter]
    pub fn anonymous(&self) -> PyResult<bool> {
        Ok(self.config.anonymous)
    }
}

impl From<config::IOConfig> for IOConfig {
    fn from(config: config::IOConfig) -> Self {
        Self { config }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<AzureConfig>()?;
    parent.add_class::<GCSConfig>()?;
    parent.add_class::<S3Config>()?;
    parent.add_class::<IOConfig>()?;
    Ok(())
}
