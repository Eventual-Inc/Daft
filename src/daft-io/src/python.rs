use crate::config;
use common_error::DaftError;
use pyo3::prelude::*;

/// Create configurations to be used when accessing an S3-compatible system
///
/// Args:
///     region_name: Name of the region to be used (used when accessing AWS S3), defaults to "us-east-1".
///         If wrongly provided, Daft will attempt to auto-detect the buckets' region at the cost of extra S3 requests.
///     endpoint_url: URL to the S3 endpoint, defaults to endpoints to AWS
///     key_id: AWS Access Key ID, defaults to auto-detection from the current environment
///     access_key: AWS Secret Access Key, defaults to auto-detection from the current environment
///     session_token: AWS Session Token, required only if `key_id` and `access_key` are temporary credentials
///     anonymous: Whether or not to use "anonymous mode", which will access S3 without any credentials
///
/// Example:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx"))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct S3Config {
    pub config: config::S3Config,
}

/// Create configurations to be used when accessing storage
///
/// Args:
///     s3: Configurations to use when accessing URLs with the `s3://` scheme
///
/// Example:
///     >>> io_config = IOConfig(s3=S3Config(key_id="xxx", access_key="xxx"))
///     >>> daft.read_parquet("s3://some-path", io_config=io_config)
#[derive(Clone, Default)]
#[pyclass]
pub struct IOConfig {
    pub config: config::IOConfig,
}

#[pymethods]
impl IOConfig {
    #[new]
    pub fn new(s3: Option<S3Config>) -> Self {
        IOConfig {
            config: config::IOConfig {
                s3: s3.unwrap_or_default().config,
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    /// Configurations to be used when accessing s3 URLs
    #[getter]
    pub fn s3(&self) -> PyResult<S3Config> {
        Ok(S3Config {
            config: self.config.s3.clone(),
        })
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
}

#[pymethods]
impl S3Config {
    #[new]
    pub fn new(
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        anonymous: Option<bool>,
    ) -> Self {
        S3Config {
            config: config::S3Config {
                region_name,
                endpoint_url,
                key_id,
                session_token,
                access_key,
                anonymous: anonymous.unwrap_or(false),
            },
        }
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

    /// AWS Secret Access Key
    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self.config.access_key.clone())
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<S3Config>()?;
    parent.add_class::<IOConfig>()?;
    Ok(())
}
