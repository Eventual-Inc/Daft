use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use common_error::DaftResult;
use common_py_serde::{
    deserialize_py_object, impl_bincode_py_state_serialization, serialize_py_object,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    config,
    s3::{S3CredentialsProvider, S3CredentialsProviderWrapper},
};

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
///     max_connections (int, optional): Maximum number of connections to S3 at any time per io thread, defaults to 8
///     session_token (str, optional): AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
///     retry_initial_backoff_ms (int, optional): Initial backoff duration in milliseconds for an S3 retry, defaults to 1000ms
///     connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to S3 in milliseconds, defaults to 30 seconds
///     read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from S3 in milliseconds, defaults to 30 seconds
///     num_tries (int, optional): Number of attempts to make a connection, defaults to 25
///     retry_mode (str, optional): Retry Mode when a request fails, current supported values are `standard` and `adaptive`, defaults to `adaptive`
///     anonymous (bool, optional): Whether or not to use "anonymous mode", which will access S3 without any credentials
///     use_ssl (bool, optional): Whether or not to use SSL, which require accessing S3 over HTTPS rather than HTTP, defaults to True
///     verify_ssl (bool, optional): Whether or not to verify ssl certificates, which will access S3 without checking if the certs are valid, defaults to True
///     check_hostname_ssl (bool, optional): Whether or not to verify the hostname when verifying ssl certificates, this was the legacy behavior for openssl, defaults to True
///     requester_pays (bool, optional): Whether or not the authenticated user will assume transfer costs, which is required by some providers of bulk data, defaults to False
///     force_virtual_addressing (bool, optional): Force S3 client to use virtual addressing in all cases. If False, virtual addressing will only be used if `endpoint_url` is empty, defaults to False
///     profile_name (str, optional): Name of AWS_PROFILE to load, defaults to None which will then check the Environment Variable `AWS_PROFILE` then fall back to `default`
///
/// Examples:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx"))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
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
/// Examples:
///     >>> from datetime import datetime, timedelta, timezone
///     >>> get_credentials = lambda: S3Credentials(
///     ...     key_id="xxx",
///     ...     access_key="xxx",
///     ...     expiry=(datetime.now(timezone.utc) + timedelta(hours=1))
///     ... )
///     >>> io_config = IOConfig(s3=S3Config(credentials_provider=get_credentials))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct S3Credentials {
    pub credentials: crate::S3Credentials,
}

/// Create configurations to be used when accessing Azure Blob Storage.
///
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
/// Examples:
///     >>> io_config = IOConfig(azure=AzureConfig(storage_account="dafttestdata", access_key="xxx"))
///     >>> daft.read_parquet("az://some-path", io_config=io_config)
#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct AzureConfig {
    pub config: crate::AzureConfig,
}

/// Create configurations to be used when accessing Google Cloud Storage.
///
/// Credentials may be provided directly with the `credentials` parameter, or set with the `GOOGLE_APPLICATION_CREDENTIALS_JSON` or `GOOGLE_APPLICATION_CREDENTIALS` environment variables.
///
/// Args:
///     project_id (str, optional): Google Project ID, defaults to value in credentials file or Google Cloud metadata service
///     credentials (str, optional): Path to credentials file or JSON string with credentials
///     token (str, optional): OAuth2 token to use for authentication. You likely want to use `credentials` instead, since it can be used to refresh the token. This value is used when vended by a data catalog.
///     anonymous (bool, optional): Whether or not to use "anonymous mode", which will access Google Storage without any credentials. Defaults to false
///     max_connections (int, optional): Maximum number of connections to GCS at any time per io thread, defaults to 8
///     retry_initial_backoff_ms (int, optional): Initial backoff duration in milliseconds for an GCS retry, defaults to 1000ms
///     connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to GCS in milliseconds, defaults to 30 seconds
///     read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from GCS in milliseconds, defaults to 30 seconds
///     num_tries (int, optional): Number of attempts to make a connection, defaults to 5
///
/// Examples:
///     >>> io_config = IOConfig(gcs=GCSConfig(anonymous=True))
///     >>> daft.read_parquet("gs://some-path", io_config=io_config)
#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct GCSConfig {
    pub config: crate::GCSConfig,
}

/// Create configurations to be used when accessing storage
///
/// Args:
///     s3: Configuration to use when accessing URLs with the `s3://` scheme
///     azure: Configuration to use when accessing URLs with the `az://` or `abfs://` scheme
///     gcs: Configuration to use when accessing URLs with the `gs://` or `gcs://` scheme
///
/// Examples:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx", num_tries=10), azure=AzureConfig(anonymous=True), gcs=GCSConfig(...))
///     >>> daft.read_parquet(["s3://some-path", "az://some-other-path", "gs://path3"], io_config=io_config)
#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct IOConfig {
    pub config: config::IOConfig,
}

/// Create configurations to be used when accessing HTTP URLs.
///
/// Args:
///     user_agent (str, optional): The value for the user-agent header, defaults to "daft/{__version__}" if not provided
///     bearer_token (str, optional): Bearer token to use for authentication. This will be used as the value for the `Authorization` header. such as "Authorization: Bearer xxx"
///     retry_initial_backoff_ms (int, optional): Initial backoff duration in milliseconds for an HTTP retry, defaults to 1000ms
///     connect_timeout_ms (int, optional): Timeout duration to wait to make a connection to HTTP in milliseconds, defaults to 30 seconds
///     read_timeout_ms (int, optional): Timeout duration to wait to read the first byte from HTTP in milliseconds, defaults to 30 seconds
///     num_tries (int, optional): Number of attempts to make a connection, defaults to 5
///
/// Examples:
///     >>> io_config = IOConfig(http=HTTPConfig(user_agent="my_application/0.0.1", bearer_token="xxx"))
///     >>> daft.read_parquet("http://some-path", io_config=io_config)
#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct HTTPConfig {
    pub config: crate::HTTPConfig,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct UnityConfig {
    pub config: crate::UnityConfig,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct HuggingFaceConfig {
    pub config: crate::HuggingFaceConfig,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct TosConfig {
    pub config: crate::TosConfig,
}

#[pymethods]
impl IOConfig {
    #[new]
    #[must_use]
    #[pyo3(signature = (
        s3=None,
        azure=None,
        gcs=None,
        http=None,
        unity=None,
        hf=None,
        disable_suffix_range=None,
        tos=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        s3: Option<S3Config>,
        azure: Option<AzureConfig>,
        gcs: Option<GCSConfig>,
        http: Option<HTTPConfig>,
        unity: Option<UnityConfig>,
        hf: Option<HuggingFaceConfig>,
        disable_suffix_range: Option<bool>,
        tos: Option<TosConfig>,
    ) -> Self {
        Self {
            config: config::IOConfig {
                s3: s3.unwrap_or_default().config,
                azure: azure.unwrap_or_default().config,
                gcs: gcs.unwrap_or_default().config,
                http: http.unwrap_or_default().config,
                unity: unity.unwrap_or_default().config,
                hf: hf.unwrap_or_default().config,
                disable_suffix_range: disable_suffix_range.unwrap_or_default(),
                tos: tos.unwrap_or_default().config,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[must_use]
    #[pyo3(signature = (
        s3=None,
        azure=None,
        gcs=None,
        http=None,
        unity=None,
        hf=None,
        disable_suffix_range=None,
        tos=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn replace(
        &self,
        s3: Option<S3Config>,
        azure: Option<AzureConfig>,
        gcs: Option<GCSConfig>,
        http: Option<HTTPConfig>,
        unity: Option<UnityConfig>,
        hf: Option<HuggingFaceConfig>,
        disable_suffix_range: Option<bool>,
        tos: Option<TosConfig>,
    ) -> Self {
        Self {
            config: config::IOConfig {
                s3: s3
                    .map(|s3| s3.config)
                    .unwrap_or_else(|| self.config.s3.clone()),
                azure: azure
                    .map(|azure| azure.config)
                    .unwrap_or_else(|| self.config.azure.clone()),
                gcs: gcs
                    .map(|gcs| gcs.config)
                    .unwrap_or_else(|| self.config.gcs.clone()),
                http: http
                    .map(|http| http.config)
                    .unwrap_or_else(|| self.config.http.clone()),
                unity: unity
                    .map(|unity| unity.config)
                    .unwrap_or_else(|| self.config.unity.clone()),
                hf: hf
                    .map(|hf| hf.config)
                    .unwrap_or_else(|| self.config.hf.clone()),
                disable_suffix_range: disable_suffix_range
                    .unwrap_or(self.config.disable_suffix_range),
                tos: tos
                    .map(|tos| tos.config)
                    .unwrap_or_else(|| self.config.tos.clone()),
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

    #[getter]
    pub fn unity(&self) -> PyResult<UnityConfig> {
        Ok(UnityConfig {
            config: self.config.unity.clone(),
        })
    }

    #[getter]
    pub fn hf(&self) -> PyResult<HuggingFaceConfig> {
        Ok(HuggingFaceConfig {
            config: self.config.hf.clone(),
        })
    }

    #[getter]
    pub fn tos(&self) -> PyResult<TosConfig> {
        Ok(TosConfig {
            config: self.config.tos.clone(),
        })
    }

    pub fn __hash__(&self) -> PyResult<u64> {
        use std::{collections::hash_map::DefaultHasher, hash::Hash};

        let mut hasher = DefaultHasher::new();
        self.config.hash(&mut hasher);
        Ok(hasher.finish())
    }
}

#[pymethods]
impl S3Config {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (
        region_name=None,
        endpoint_url=None,
        key_id=None,
        session_token=None,
        access_key=None,
        credentials_provider=None,
        buffer_time=None,
        max_connections=None,
        retry_initial_backoff_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        num_tries=None,
        retry_mode=None,
        anonymous=None,
        use_ssl=None,
        verify_ssl=None,
        check_hostname_ssl=None,
        requester_pays=None,
        force_virtual_addressing=None,
        profile_name=None,
        multipart_size=None,
        multipart_max_concurrency=None,
        custom_retry_msgs=None
    ))]
    pub fn new(
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        credentials_provider: Option<Bound<PyAny>>,
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
        multipart_size: Option<u64>,
        multipart_max_concurrency: Option<u32>,
        custom_retry_msgs: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let def = crate::S3Config::default();
        Ok(Self {
            config: crate::S3Config {
                region_name: region_name.or(def.region_name),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                key_id: key_id.or(def.key_id),
                session_token: session_token
                    .map(std::convert::Into::into)
                    .or(def.session_token),
                access_key: access_key.map(std::convert::Into::into).or(def.access_key),
                credentials_provider: credentials_provider
                    .map(|p| {
                        Ok::<_, PyErr>(S3CredentialsProviderWrapper::new(
                            PyS3CredentialsProvider::new(p)?,
                        ))
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
                multipart_size: multipart_size.unwrap_or(def.multipart_size),
                multipart_max_concurrency: multipart_max_concurrency
                    .unwrap_or(def.multipart_max_concurrency),
                custom_retry_msgs: custom_retry_msgs.unwrap_or(def.custom_retry_msgs),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        region_name=None,
        endpoint_url=None,
        key_id=None,
        session_token=None,
        access_key=None,
        credentials_provider=None,
        buffer_time=None,
        max_connections=None,
        retry_initial_backoff_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        num_tries=None,
        retry_mode=None,
        anonymous=None,
        use_ssl=None,
        verify_ssl=None,
        check_hostname_ssl=None,
        requester_pays=None,
        force_virtual_addressing=None,
        profile_name=None,
        multipart_size=None,
        multipart_max_concurrency=None,
        custom_retry_msgs=None
    ))]
    pub fn replace(
        &self,
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        credentials_provider: Option<Bound<PyAny>>,
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
        multipart_size: Option<u64>,
        multipart_max_concurrency: Option<u32>,
        custom_retry_msgs: Option<Vec<String>>,
    ) -> PyResult<Self> {
        Ok(Self {
            config: crate::S3Config {
                region_name: region_name.or_else(|| self.config.region_name.clone()),
                endpoint_url: endpoint_url.or_else(|| self.config.endpoint_url.clone()),
                key_id: key_id.or_else(|| self.config.key_id.clone()),
                session_token: session_token
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.session_token.clone()),
                access_key: access_key
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.access_key.clone()),
                credentials_provider: credentials_provider
                    .map(|p| {
                        Ok::<_, PyErr>(S3CredentialsProviderWrapper::new(
                            PyS3CredentialsProvider::new(p)?,
                        ))
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
                multipart_size: multipart_size.unwrap_or(self.config.multipart_size),
                multipart_max_concurrency: multipart_max_concurrency
                    .unwrap_or(self.config.multipart_max_concurrency),
                custom_retry_msgs: custom_retry_msgs
                    .unwrap_or_else(|| self.config.custom_retry_msgs.clone()),
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
        Ok(self
            .config
            .session_token
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
    }

    /// AWS Secret Access Key
    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self
            .config
            .access_key
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
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
            p.provider
                .as_any()
                .downcast_ref::<PyS3CredentialsProvider>()
                .map(|p| p.provider.clone_ref(py))
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

    /// Custom retry error messages that should trigger retry
    #[getter]
    pub fn custom_retry_msgs(&self) -> PyResult<Vec<String>> {
        Ok(self.config.custom_retry_msgs.clone())
    }

    pub fn provide_cached_credentials(&self) -> PyResult<Option<S3Credentials>> {
        self.config
            .credentials_provider
            .as_ref()
            .map(|provider| {
                Ok(S3Credentials {
                    credentials: provider.get_cached_credentials()?,
                })
            })
            .transpose()
    }
}

#[pymethods]
impl S3Credentials {
    #[new]
    #[pyo3(signature = (key_id, access_key, session_token=None, expiry=None))]
    pub fn new(
        key_id: String,
        access_key: String,
        session_token: Option<String>,
        expiry: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            credentials: crate::S3Credentials {
                key_id,
                access_key,
                session_token,
                expiry,
            },
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{}", self.credentials)
    }

    /// AWS Access Key ID
    #[getter]
    pub fn key_id(&self) -> &str {
        &self.credentials.key_id
    }

    /// AWS Secret Access Key
    #[getter]
    pub fn access_key(&self) -> &str {
        &self.credentials.access_key
    }

    /// AWS Session Token
    #[getter]
    pub fn session_token(&self) -> Option<&str> {
        self.credentials.session_token.as_deref()
    }

    /// AWS Credentials Expiry
    #[getter]
    pub fn expiry(&self) -> Option<DateTime<Utc>> {
        self.credentials.expiry
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PyS3CredentialsProvider {
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub provider: Arc<Py<PyAny>>,
    pub hash: isize,
}

impl PyS3CredentialsProvider {
    pub fn new(provider: Bound<PyAny>) -> PyResult<Self> {
        let hash = provider.hash()?;
        Ok(Self {
            provider: Arc::new(provider.into()),
            hash,
        })
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
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }

    fn provide_credentials(&self) -> DaftResult<crate::S3Credentials> {
        Python::attach(|py| {
            let py_creds = self.provider.call0(py)?;
            Ok(py_creds.extract::<S3Credentials>(py)?.credentials)
        })
    }
}

#[pymethods]
impl AzureConfig {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[must_use]
    #[pyo3(signature = (
        storage_account=None,
        access_key=None,
        sas_token=None,
        bearer_token=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        use_fabric_endpoint=None,
        anonymous=None,
        endpoint_url=None,
        use_ssl=None
    ))]
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
        Self {
            config: crate::AzureConfig {
                storage_account: storage_account.or(def.storage_account),
                access_key: access_key.map(std::convert::Into::into).or(def.access_key),
                sas_token: sas_token.or(def.sas_token),
                bearer_token: bearer_token.or(def.bearer_token),
                tenant_id: tenant_id.or(def.tenant_id),
                client_id: client_id.or(def.client_id),
                client_secret: client_secret
                    .map(std::convert::Into::into)
                    .or(def.client_secret),
                use_fabric_endpoint: use_fabric_endpoint.unwrap_or(def.use_fabric_endpoint),
                anonymous: anonymous.unwrap_or(def.anonymous),
                endpoint_url: endpoint_url.or(def.endpoint_url),
                use_ssl: use_ssl.unwrap_or(def.use_ssl),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[must_use]
    #[pyo3(signature = (
        storage_account=None,
        access_key=None,
        sas_token=None,
        bearer_token=None,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        use_fabric_endpoint=None,
        anonymous=None,
        endpoint_url=None,
        use_ssl=None
    ))]
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
        Self {
            config: crate::AzureConfig {
                storage_account: storage_account.or_else(|| self.config.storage_account.clone()),
                access_key: access_key
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.access_key.clone()),
                sas_token: sas_token.or_else(|| self.config.sas_token.clone()),
                bearer_token: bearer_token.or_else(|| self.config.bearer_token.clone()),
                tenant_id: tenant_id.or_else(|| self.config.tenant_id.clone()),
                client_id: client_id.or_else(|| self.config.client_id.clone()),
                client_secret: client_secret
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.client_secret.clone()),
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
        Ok(self
            .config
            .access_key
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
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
        Ok(self
            .config
            .client_secret
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
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
    #[must_use]
    #[pyo3(signature = (
        project_id=None,
        credentials=None,
        token=None,
        anonymous=None,
        max_connections=None,
        retry_initial_backoff_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        num_tries=None
    ))]
    pub fn new(
        project_id: Option<String>,
        credentials: Option<String>,
        token: Option<String>,
        anonymous: Option<bool>,
        max_connections: Option<u32>,
        retry_initial_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        num_tries: Option<u32>,
    ) -> Self {
        let def = crate::GCSConfig::default();
        Self {
            config: crate::GCSConfig {
                project_id: project_id.or(def.project_id),
                credentials: credentials
                    .map(std::convert::Into::into)
                    .or(def.credentials),
                token: token.or(def.token),
                anonymous: anonymous.unwrap_or(def.anonymous),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(def.max_connections_per_io_thread),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(def.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(def.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(def.read_timeout_ms),
                num_tries: num_tries.unwrap_or(def.num_tries),
            },
        }
    }
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    #[pyo3(signature = (
        project_id=None,
        credentials=None,
        token=None,
        anonymous=None,
        max_connections=None,
        retry_initial_backoff_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        num_tries=None
    ))]
    pub fn replace(
        &self,
        project_id: Option<String>,
        credentials: Option<String>,
        token: Option<String>,
        anonymous: Option<bool>,
        max_connections: Option<u32>,
        retry_initial_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        num_tries: Option<u32>,
    ) -> Self {
        Self {
            config: crate::GCSConfig {
                project_id: project_id.or_else(|| self.config.project_id.clone()),
                credentials: credentials
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.credentials.clone()),
                token: token.or_else(|| self.config.token.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(self.config.max_connections_per_io_thread),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(self.config.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(self.config.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(self.config.read_timeout_ms),
                num_tries: num_tries.unwrap_or(self.config.num_tries),
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
        Ok(self
            .config
            .credentials
            .as_ref()
            .map(|v| v.as_string().clone()))
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

    #[getter]
    pub fn max_connections(&self) -> PyResult<u32> {
        Ok(self.config.max_connections_per_io_thread)
    }

    #[getter]
    pub fn retry_initial_backoff_ms(&self) -> PyResult<u64> {
        Ok(self.config.retry_initial_backoff_ms)
    }

    #[getter]
    pub fn connect_timeout_ms(&self) -> PyResult<u64> {
        Ok(self.config.connect_timeout_ms)
    }

    #[getter]
    pub fn read_timeout_ms(&self) -> PyResult<u64> {
        Ok(self.config.read_timeout_ms)
    }

    #[getter]
    pub fn num_tries(&self) -> PyResult<u32> {
        Ok(self.config.num_tries)
    }
}

impl From<config::IOConfig> for IOConfig {
    fn from(config: config::IOConfig) -> Self {
        Self { config }
    }
}

impl From<IOConfig> for config::IOConfig {
    fn from(value: IOConfig) -> Self {
        value.config
    }
}

#[pymethods]
impl HTTPConfig {
    #[new]
    #[must_use]
    #[pyo3(signature = (bearer_token=None, retry_initial_backoff_ms=None, connect_timeout_ms=None, read_timeout_ms=None, num_tries=None))]
    pub fn new(
        bearer_token: Option<String>,
        retry_initial_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        num_tries: Option<u32>,
    ) -> Self {
        let def = crate::HTTPConfig::default();
        Self {
            config: crate::HTTPConfig {
                bearer_token: bearer_token.map(Into::into).or(def.bearer_token),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(def.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(def.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(def.read_timeout_ms),
                num_tries: num_tries.unwrap_or(def.num_tries),
                ..def
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }
}

#[pymethods]
impl UnityConfig {
    #[new]
    #[pyo3(signature = (endpoint=None, token=None))]
    pub fn new(endpoint: Option<String>, token: Option<String>) -> Self {
        let default = crate::UnityConfig::default();
        Self {
            config: crate::UnityConfig {
                endpoint: endpoint.or(default.endpoint),
                token: token.map(Into::into).or(default.token),
            },
        }
    }

    #[pyo3(signature = (endpoint=None, token=None))]
    pub fn replace(&self, endpoint: Option<String>, token: Option<String>) -> Self {
        Self {
            config: crate::UnityConfig {
                endpoint: endpoint.or_else(|| self.config.endpoint.clone()),
                token: token.map(Into::into).or_else(|| self.config.token.clone()),
            },
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{}", self.config)
    }

    #[getter]
    pub fn endpoint(&self) -> Option<String> {
        self.config.endpoint.clone()
    }

    #[getter]
    pub fn token(&self) -> Option<String> {
        self.config
            .token
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned()
    }
}

#[pymethods]
impl HuggingFaceConfig {
    #[new]
    #[pyo3(signature = (
        token=None,
        anonymous=None,
        use_content_defined_chunking=None,
        row_group_size=None,
        target_filesize=None,
        max_operations_per_commit=None
    ))]
    pub fn new(
        token: Option<String>,
        anonymous: Option<bool>,
        use_content_defined_chunking: Option<bool>,
        row_group_size: Option<usize>,
        target_filesize: Option<usize>,
        max_operations_per_commit: Option<usize>,
    ) -> Self {
        let default = crate::HuggingFaceConfig::default();
        Self {
            config: crate::HuggingFaceConfig {
                token: token.map(Into::into).or(default.token),
                anonymous: anonymous.unwrap_or(default.anonymous),
                use_content_defined_chunking: use_content_defined_chunking
                    .or(default.use_content_defined_chunking),
                row_group_size: row_group_size.or(default.row_group_size),
                target_filesize: target_filesize.unwrap_or(default.target_filesize),
                max_operations_per_commit: max_operations_per_commit
                    .unwrap_or(default.max_operations_per_commit),
            },
        }
    }

    #[pyo3(signature = (
        token=None,
        anonymous=None,
        use_content_defined_chunking=None,
        row_group_size=None,
        target_filesize=None,
        max_operations_per_commit=None
    ))]
    pub fn replace(
        &self,
        token: Option<String>,
        anonymous: Option<bool>,
        use_content_defined_chunking: Option<bool>,
        row_group_size: Option<usize>,
        target_filesize: Option<usize>,
        max_operations_per_commit: Option<usize>,
    ) -> Self {
        Self {
            config: crate::HuggingFaceConfig {
                token: token.map(Into::into).or_else(|| self.config.token.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
                use_content_defined_chunking: use_content_defined_chunking
                    .or(self.config.use_content_defined_chunking),
                row_group_size: row_group_size.or(self.config.row_group_size),
                target_filesize: target_filesize.unwrap_or(self.config.target_filesize),
                max_operations_per_commit: max_operations_per_commit
                    .unwrap_or(self.config.max_operations_per_commit),
            },
        }
    }

    #[getter]
    pub fn token(&self) -> Option<String> {
        self.config
            .token
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned()
    }

    #[getter]
    pub fn anonymous(&self) -> bool {
        self.config.anonymous
    }

    #[getter]
    pub fn use_content_defined_chunking(&self) -> Option<bool> {
        self.config.use_content_defined_chunking
    }

    #[getter]
    pub fn row_group_size(&self) -> Option<usize> {
        self.config.row_group_size
    }

    #[getter]
    pub fn target_filesize(&self) -> usize {
        self.config.target_filesize
    }

    #[getter]
    pub fn max_operations_per_commit(&self) -> usize {
        self.config.max_operations_per_commit
    }
}

#[pymethods]
impl TosConfig {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (
        region=None,
        endpoint=None,
        access_key=None,
        secret_key=None,
        security_token=None,
        anonymous=None,
        max_retries=None,
        retry_timeout_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        max_concurrent_requests=None,
        max_connections=None,
    ))]
    pub fn new(
        region: Option<String>,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        security_token: Option<String>,
        anonymous: Option<bool>,
        max_retries: Option<u32>,
        retry_timeout_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        max_concurrent_requests: Option<u32>,
        max_connections: Option<u32>,
    ) -> PyResult<Self> {
        let def = crate::TosConfig::default();
        Ok(Self {
            config: crate::TosConfig {
                region: region.or(def.region),
                endpoint: endpoint.or(def.endpoint),
                access_key: access_key.or(def.access_key),
                secret_key: secret_key.map(std::convert::Into::into).or(def.secret_key),
                security_token: security_token
                    .map(std::convert::Into::into)
                    .or(def.security_token),
                anonymous: anonymous.unwrap_or(def.anonymous),
                max_retries: max_retries.unwrap_or(def.max_retries),
                retry_timeout_ms: retry_timeout_ms.unwrap_or(def.retry_timeout_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(def.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(def.read_timeout_ms),
                max_concurrent_requests: max_concurrent_requests
                    .unwrap_or(def.max_concurrent_requests),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(def.max_connections_per_io_thread),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        region=None,
        endpoint=None,
        access_key=None,
        secret_key=None,
        security_token=None,
        anonymous=None,
        max_retries=None,
        retry_timeout_ms=None,
        connect_timeout_ms=None,
        read_timeout_ms=None,
        max_concurrent_requests=None,
        max_connections=None,
    ))]
    pub fn replace(
        &self,
        region: Option<String>,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        security_token: Option<String>,
        anonymous: Option<bool>,
        max_retries: Option<u32>,
        retry_timeout_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        read_timeout_ms: Option<u64>,
        max_concurrent_requests: Option<u32>,
        max_connections: Option<u32>,
    ) -> PyResult<Self> {
        Ok(Self {
            config: crate::TosConfig {
                region: region.or_else(|| self.config.region.clone()),
                endpoint: endpoint.or_else(|| self.config.endpoint.clone()),
                access_key: access_key.or_else(|| self.config.access_key.clone()),
                secret_key: secret_key
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.secret_key.clone()),
                security_token: security_token
                    .map(std::convert::Into::into)
                    .or_else(|| self.config.security_token.clone()),
                anonymous: anonymous.unwrap_or(self.config.anonymous),
                max_retries: max_retries.unwrap_or(self.config.max_retries),
                retry_timeout_ms: retry_timeout_ms.unwrap_or(self.config.retry_timeout_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(self.config.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(self.config.read_timeout_ms),
                max_concurrent_requests: max_concurrent_requests
                    .unwrap_or(self.config.max_concurrent_requests),
                max_connections_per_io_thread: max_connections
                    .unwrap_or(self.config.max_connections_per_io_thread),
            },
        })
    }

    #[staticmethod]
    pub fn from_env(_py: Python) -> PyResult<Self> {
        let endpoint = std::env::var("TOS_ENDPOINT").ok();
        let region = std::env::var("TOS_REGION").ok();
        let access_key: Option<String> = std::env::var("TOS_ACCESS_KEY").ok();
        let secret_key: Option<String> = std::env::var("TOS_SECRET_KEY").ok();
        let session_token: Option<String> = std::env::var("TOS_SESSION_TOKEN").ok();
        let anonymous = access_key.is_none();

        Ok(Self {
            config: crate::TosConfig {
                endpoint,
                region,
                access_key,
                secret_key: secret_key.map(|s| s.into()),
                security_token: session_token.map(|s| s.into()),
                anonymous,
                ..Default::default()
            },
        })
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    #[getter]
    pub fn region_name(&self) -> PyResult<Option<String>> {
        Ok(self.config.region.clone())
    }

    #[getter]
    pub fn endpoint(&self) -> PyResult<Option<String>> {
        Ok(self.config.endpoint.clone())
    }

    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self.config.access_key.clone())
    }

    #[getter]
    pub fn secret_key(&self) -> PyResult<Option<String>> {
        Ok(self
            .config
            .secret_key
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
    }

    #[getter]
    pub fn session_token(&self) -> PyResult<Option<String>> {
        Ok(self
            .config
            .security_token
            .as_ref()
            .map(super::ObfuscatedString::as_string)
            .cloned())
    }

    #[getter]
    pub fn max_connections(&self) -> PyResult<u32> {
        Ok(self.config.max_connections_per_io_thread)
    }
}

impl_bincode_py_state_serialization!(IOConfig);
impl_bincode_py_state_serialization!(S3Config);
impl_bincode_py_state_serialization!(S3Credentials);
impl_bincode_py_state_serialization!(AzureConfig);
impl_bincode_py_state_serialization!(GCSConfig);
impl_bincode_py_state_serialization!(HTTPConfig);
impl_bincode_py_state_serialization!(UnityConfig);
impl_bincode_py_state_serialization!(HuggingFaceConfig);
impl_bincode_py_state_serialization!(TosConfig);

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<AzureConfig>()?;
    parent.add_class::<GCSConfig>()?;
    parent.add_class::<S3Config>()?;
    parent.add_class::<HTTPConfig>()?;
    parent.add_class::<S3Credentials>()?;
    parent.add_class::<TosConfig>()?;
    parent.add_class::<UnityConfig>()?;
    parent.add_class::<HuggingFaceConfig>()?;
    parent.add_class::<IOConfig>()?;
    Ok(())
}
