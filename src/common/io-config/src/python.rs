use std::{
    any::Any,
    hash::{Hash, Hasher},
    time::{Duration, SystemTime},
};

use aws_credential_types::{
    provider::{error::CredentialsError, ProvideCredentials},
    Credentials,
};
use common_error::DaftError;
use common_py_serde::{deserialize_py_object, serialize_py_object};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{config, s3::S3CredentialsProvider};

/// Create configurations to be used when accessing an S3-compatible system
///
/// Args:
///     region_name (str, optional): Name of the region to be used (used when accessing AWS S3), defaults to "us-east-1".
///         If wrongly provided, Daft will attempt to auto-detect the buckets' region at the cost of extra S3 requests.
///     endpoint_url (str, optional): URL to the S3 endpoint, defaults to endpoints to AWS
///     key_id (str, optional): AWS Access Key ID, defaults to auto-detection from the current environment
///     access_key (str, optional): AWS Secret Access Key, defaults to auto-detection from the current environment
///     credentials_provider (Callable[[], S3Credentials], optional): Custom credentials provider function, should return a `S3Credentials` object
///     buffer_time (int, optional): Amount of time in seconds before the actual credential expiration time where credentials given by `credentials_provider` are considered expired, defaults to 10s
///     max_connections (int, optional): Maximum number of connections to S3 at any time, defaults to 64
///     session_token (str, optional): AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
///     retry_initial_backoff_ms (int, optional): Initial backoff duration in milliseconds for an S3 retry, defaults to 1000ms
///     connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to S3 in milliseconds, defaults to 10 seconds
///     read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from S3 in milliseconds, defaults to 10 seconds
///     num_tries (int, optional): Number of attempts to make a connection, defaults to 5
///     retry_mode (str, optional): Retry Mode when a request fails, current supported values are `standard` and `adaptive`, defaults to `adaptive`
///     anonymous (bool, optional): Whether or not to use "anonymous mode", which will access S3 without any credentials
///     use_ssl (bool, optional): Whether or not to use SSL, which require accessing S3 over HTTPS rather than HTTP, defaults to True
///     verify_ssl (bool, optional): Whether or not to verify ssl certificates, which will access S3 without checking if the certs are valid, defaults to True
///     check_hostname_ssl (bool, optional): Whether or not to verify the hostname when verifying ssl certificates, this was the legacy behavior for openssl, defaults to True
///     requester_pays (bool, optional): Whether or not the authenticated user will assume transfer costs, which is required by some providers of bulk data, defaults to False
///     force_virtual_addressing (bool, optional): Force S3 client to use virtual addressing in all cases. If False, virtual addressing will only be used if `endpoint_url` is empty, defaults to False
///     profile_name (str, optional): Name of AWS_PROFILE to load, defaults to None which will then check the Environment Variable `AWS_PROFILE` then fall back to `default`
///
/// Example:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx"))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct S3Config {
    pub config: crate::S3Config,
}

/// Create credentials to be used when accessing an S3-compatible system
///
/// Args:
///     key_id (str): AWS Access Key ID, defaults to auto-detection from the current environment
///     access_key (str): AWS Secret Access Key, defaults to auto-detection from the current environment
///     session_token (str, optional): AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
///     expiry (datetime.datetime, optional): Expiry time of the credentials, credentials are assumed to be permanent if not provided
///
/// Example:
///     >>> get_credentials = lambda: S3Credentials(
///     ...     key_id="xxx",
///     ...     access_key="xxx",
///     ...     expiry=(datetime.datetime.now() + datetime.timedelta(hours=1))
///     ... )
///     >>> io_config = IOConfig(s3=S3Config(credentials_provider=get_credentials))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone)]
#[pyclass]
pub struct S3Credentials {
    pub credentials: crate::S3Credentials,
}

/// Create configurations to be used when accessing Azure Blob Storage.
/// To authenticate with Microsoft Entra ID, `tenant_id`, `client_id`, and `client_secret` must be provided.
/// If no credentials are provided, Daft will attempt to fetch credentials from the environment.
///
/// Args:
///     storage_account (str): Azure Storage Account, defaults to reading from `AZURE_STORAGE_ACCOUNT` environment variable.
///     access_key (str, optional): Azure Secret Access Key, defaults to reading from `AZURE_STORAGE_KEY` environment variable
///     sas_token (str, optional): Shared Access Signature token, defaults to reading from `AZURE_STORAGE_SAS_TOKEN` environment variable
///     bearer_token (str, optional): Bearer Token, defaults to reading from `AZURE_STORAGE_TOKEN` environment variable
///     tenant_id (str, optional): Azure Tenant ID
///     client_id (str, optional): Azure Client ID
///     client_secret (str, optional): Azure Client Secret
///     use_fabric_endpoint (bool, optional): Whether to use Microsoft Fabric, you may want to set this if your URLs are from "fabric.microsoft.com". Defaults to false
///     anonymous (bool, optional): Whether or not to use "anonymous mode", which will access Azure without any credentials
///     endpoint_url (str, optional): Custom URL to the Azure endpoint, e.g. ``https://my-account-name.blob.core.windows.net``. Overrides `use_fabric_endpoint` if set
///     use_ssl (bool, optional): Whether or not to use SSL, which require accessing Azure over HTTPS rather than HTTP, defaults to True
///
/// Example:
///     >>> io_config = IOConfig(azure=AzureConfig(storage_account="dafttestdata", access_key="xxx"))
///     >>> daft.read_parquet("az://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct AzureConfig {
    pub config: crate::AzureConfig,
}

/// Create configurations to be used when accessing Google Cloud Storage.
/// Credentials may be provided directly with the `credentials` parameter, or set with the `GOOGLE_APPLICATION_CREDENTIALS_JSON` or `GOOGLE_APPLICATION_CREDENTIALS` environment variables.
///
/// Args:
///     project_id (str, optional): Google Project ID, defaults to value in credentials file or Google Cloud metadata service
///     credentials (str, optional): Path to credentials file or JSON string with credentials
///     token (str, optional): OAuth2 token to use for authentication. You likely want to use `credentials` instead, since it can be used to refresh the token. This value is used when vended by a data catalog.
///     anonymous (bool, optional): Whether or not to use "anonymous mode", which will access Google Storage without any credentials. Defaults to false
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

/// Create configurations to be used when accessing HTTP URLs.
///
/// Args:
///     user_agent (str, optional): The value for the user-agent header, defaults to "daft/{__version__}" if not provided
///
/// Example:
///     >>> io_config = IOConfig(http=HTTPConfig(user_agent="my_application/0.0.1"))
///     >>> daft.read_parquet("http://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct HTTPConfig {
    pub config: crate::HTTPConfig,
}

#[pymethods]
impl IOConfig {
    #[new]
    pub fn new(
        s3: Option<S3Config>,
        azure: Option<AzureConfig>,
        gcs: Option<GCSConfig>,
        http: Option<HTTPConfig>,
    ) -> Self {
        IOConfig {
            config: config::IOConfig {
                s3: s3.unwrap_or_default().config,
                azure: azure.unwrap_or_default().config,
                gcs: gcs.unwrap_or_default().config,
                http: http.unwrap_or_default().config,
            },
        }
    }

    pub fn replace(
        &self,
        s3: Option<S3Config>,
        azure: Option<AzureConfig>,
        gcs: Option<GCSConfig>,
        http: Option<HTTPConfig>,
    ) -> Self {
        IOConfig {
            config: config::IOConfig {
                s3: s3.map(|s3| s3.config).unwrap_or(self.config.s3.clone()),
                azure: azure
                    .map(|azure| azure.config)
                    .unwrap_or(self.config.azure.clone()),
                gcs: gcs.map(|gcs| gcs.config).unwrap_or(self.config.gcs.clone()),
                http: http
                    .map(|http| http.config)
                    .unwrap_or(self.config.http.clone()),
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

    /// Configuration to be used when accessing Azure URLs
    #[getter]
    pub fn http(&self) -> PyResult<HTTPConfig> {
        Ok(HTTPConfig {
            config: self.config.http.clone(),
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
        py: Python,
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        credentials_provider: Option<&PyAny>,
        buffer_time: Option<u64>,
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
    ) -> PyResult<Self> {
        let def = crate::S3Config::default();
        Ok(S3Config {
            config: crate::S3Config {
                region_name: region_name.or(def.region_name),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                key_id: key_id.or(def.key_id),
                session_token: session_token.or(def.session_token),
                access_key: access_key.or(def.access_key),
                credentials_provider: credentials_provider
                    .map(|p| {
                        Ok::<_, PyErr>(Box::new(PyS3CredentialsProvider::new(py, p)?)
                            as Box<dyn S3CredentialsProvider>)
                    })
                    .transpose()?
                    .or(def.credentials_provider),
                buffer_time: buffer_time.or(def.buffer_time),
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
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace(
        &self,
        py: Python,
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        credentials_provider: Option<&PyAny>,
        buffer_time: Option<u64>,
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
    ) -> PyResult<Self> {
        Ok(S3Config {
            config: crate::S3Config {
                region_name: region_name.or_else(|| self.config.region_name.clone()),
                endpoint_url: endpoint_url.or_else(|| self.config.endpoint_url.clone()),
                key_id: key_id.or_else(|| self.config.key_id.clone()),
                session_token: session_token.or_else(|| self.config.session_token.clone()),
                access_key: access_key.or_else(|| self.config.access_key.clone()),
                credentials_provider: credentials_provider
                    .map(|p| {
                        Ok::<_, PyErr>(Box::new(PyS3CredentialsProvider::new(py, p)?)
                            as Box<dyn S3CredentialsProvider>)
                    })
                    .transpose()?
                    .or_else(|| self.config.credentials_provider.clone()),
                buffer_time: buffer_time.or(self.config.buffer_time),
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
        })
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

    /// Custom credentials provider function
    #[getter]
    pub fn credentials_provider(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        Ok(self.config.credentials_provider.as_ref().and_then(|p| {
            p.as_any()
                .downcast_ref::<PyS3CredentialsProvider>()
                .map(|p| p.provider.as_ref(py).into())
        }))
    }

    /// AWS Buffer Time in Seconds
    #[getter]
    pub fn buffer_time(&self) -> PyResult<Option<u64>> {
        Ok(self.config.buffer_time)
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
impl S3Credentials {
    #[new]
    pub fn new(
        key_id: String,
        access_key: String,
        session_token: Option<String>,
        expiry: Option<&PyAny>,
    ) -> PyResult<Self> {
        // TODO(Kevin): Refactor when upgrading to PyO3 0.21 (https://github.com/Eventual-Inc/Daft/issues/2288)
        let expiry = expiry
            .map(|e| {
                let ts = e.call_method0("timestamp")?.extract()?;

                Ok::<_, PyErr>(SystemTime::UNIX_EPOCH + Duration::from_secs_f64(ts))
            })
            .transpose()?;

        Ok(S3Credentials {
            credentials: crate::S3Credentials {
                key_id,
                access_key,
                session_token,
                expiry,
            },
        })
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.credentials))
    }

    /// AWS Access Key ID
    #[getter]
    pub fn key_id(&self) -> PyResult<String> {
        Ok(self.credentials.key_id.clone())
    }

    /// AWS Secret Access Key
    #[getter]
    pub fn access_key(&self) -> PyResult<String> {
        Ok(self.credentials.access_key.clone())
    }

    /// AWS Session Token
    #[getter]
    pub fn expiry<'a>(&self, py: Python<'a>) -> PyResult<Option<&'a PyAny>> {
        // TODO(Kevin): Refactor when upgrading to PyO3 0.21 (https://github.com/Eventual-Inc/Daft/issues/2288)
        self.credentials
            .expiry
            .map(|e| {
                let datetime = py.import("datetime")?;

                datetime.getattr("datetime")?.call_method1(
                    "fromtimestamp",
                    (e.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64(),),
                )
            })
            .transpose()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PyS3CredentialsProvider {
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub provider: PyObject,
    pub hash: isize,
}

impl PyS3CredentialsProvider {
    pub fn new(py: Python, provider: &PyAny) -> PyResult<Self> {
        Ok(PyS3CredentialsProvider {
            provider: provider.to_object(py),
            hash: provider.hash()?,
        })
    }
}

impl ProvideCredentials for PyS3CredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::ready(
            Python::with_gil(|py| {
                let py_creds = self.provider.call0(py)?;
                py_creds.extract::<S3Credentials>(py)
            })
            .map_err(|e| CredentialsError::provider_error(Box::new(e)))
            .map(|creds| {
                Credentials::new(
                    creds.credentials.key_id,
                    creds.credentials.access_key,
                    creds.credentials.session_token,
                    creds.credentials.expiry,
                    "daft_custom_provider",
                )
            }),
        )
    }
}

impl PartialEq for PyS3CredentialsProvider {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for PyS3CredentialsProvider {}

impl Hash for PyS3CredentialsProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[typetag::serde]
impl S3CredentialsProvider for PyS3CredentialsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn S3CredentialsProvider> {
        Box::new(self.clone())
    }

    fn dyn_eq(&self, other: &dyn S3CredentialsProvider) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map_or(false, |other| self == other)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }
}

#[pymethods]
impl AzureConfig {
    #[allow(clippy::too_many_arguments)]
    #[new]
    pub fn new(
        storage_account: Option<String>,
        access_key: Option<String>,
        sas_token: Option<String>,
        bearer_token: Option<String>,
        tenant_id: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        use_fabric_endpoint: Option<bool>,
        anonymous: Option<bool>,
        endpoint_url: Option<String>,
        use_ssl: Option<bool>,
    ) -> Self {
        let def = crate::AzureConfig::default();
        AzureConfig {
            config: crate::AzureConfig {
                storage_account: storage_account.or(def.storage_account),
                access_key: access_key.or(def.access_key),
                sas_token: sas_token.or(def.sas_token),
                bearer_token: bearer_token.or(def.bearer_token),
                tenant_id: tenant_id.or(def.tenant_id),
                client_id: client_id.or(def.client_id),
                client_secret: client_secret.or(def.client_secret),
                use_fabric_endpoint: use_fabric_endpoint.unwrap_or(def.use_fabric_endpoint),
                anonymous: anonymous.unwrap_or(def.anonymous),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                use_ssl: use_ssl.unwrap_or(def.use_ssl),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replace(
        &self,
        storage_account: Option<String>,
        access_key: Option<String>,
        sas_token: Option<String>,
        bearer_token: Option<String>,
        tenant_id: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        use_fabric_endpoint: Option<bool>,
        anonymous: Option<bool>,
        endpoint_url: Option<String>,
        use_ssl: Option<bool>,
    ) -> Self {
        AzureConfig {
            config: crate::AzureConfig {
                storage_account: storage_account.or_else(|| self.config.storage_account.clone()),
                access_key: access_key.or_else(|| self.config.access_key.clone()),
                sas_token: sas_token.or_else(|| self.config.sas_token.clone()),
                bearer_token: bearer_token.or_else(|| self.config.bearer_token.clone()),
                tenant_id: tenant_id.or_else(|| self.config.tenant_id.clone()),
                client_id: client_id.or_else(|| self.config.client_id.clone()),
                client_secret: client_secret.or_else(|| self.config.client_secret.clone()),
                use_fabric_endpoint: use_fabric_endpoint.unwrap_or(self.config.use_fabric_endpoint),
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

    /// Azure Shared Access Signature token
    #[getter]
    pub fn sas_token(&self) -> PyResult<Option<String>> {
        Ok(self.config.sas_token.clone())
    }

    /// Azure Bearer Token
    #[getter]
    pub fn bearer_token(&self) -> PyResult<Option<String>> {
        Ok(self.config.bearer_token.clone())
    }

    #[getter]
    pub fn tenant_id(&self) -> PyResult<Option<String>> {
        Ok(self.config.tenant_id.clone())
    }

    #[getter]
    pub fn client_id(&self) -> PyResult<Option<String>> {
        Ok(self.config.client_id.clone())
    }

    #[getter]
    pub fn client_secret(&self) -> PyResult<Option<String>> {
        Ok(self.config.client_secret.clone())
    }

    /// Whether to use Microsoft Fabric
    #[getter]
    pub fn use_fabric_endpoint(&self) -> PyResult<bool> {
        Ok(self.config.use_fabric_endpoint)
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
    pub fn new(
        project_id: Option<String>,
        credentials: Option<String>,
        token: Option<String>,
        anonymous: Option<bool>,
    ) -> Self {
        let def = crate::GCSConfig::default();
        GCSConfig {
            config: crate::GCSConfig {
                project_id: project_id.or(def.project_id),
                credentials: credentials.or(def.credentials),
                token: token.or(def.token),
                anonymous: anonymous.unwrap_or(def.anonymous),
            },
        }
    }

    pub fn replace(
        &self,
        project_id: Option<String>,
        credentials: Option<String>,
        token: Option<String>,
        anonymous: Option<bool>,
    ) -> Self {
        GCSConfig {
            config: crate::GCSConfig {
                project_id: project_id.or_else(|| self.config.project_id.clone()),
                credentials: credentials.or_else(|| self.config.credentials.clone()),
                token: token.or_else(|| self.config.token.clone()),
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

    /// Credentials file path or string to use when accessing Google Cloud Storage
    #[getter]
    pub fn credentials(&self) -> PyResult<Option<String>> {
        Ok(self.config.credentials.clone())
    }

    /// OAuth2 token to use when accessing Google Cloud Storage
    #[getter]
    pub fn token(&self) -> PyResult<Option<String>> {
        Ok(self.config.token.clone())
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
    parent.add_class::<HTTPConfig>()?;
    parent.add_class::<S3Credentials>()?;
    parent.add_class::<IOConfig>()?;
    Ok(())
}
