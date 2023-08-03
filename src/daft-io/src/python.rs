use crate::config::{IOConfig, S3Config};
use common_error::DaftError;
use pyo3::prelude::*;

#[derive(Clone, Default)]
#[pyclass]
pub struct PyS3Config {
    pub config: S3Config,
}

#[derive(Clone, Default)]
#[pyclass]
pub struct PyIOConfig {
    pub config: IOConfig,
}

#[pymethods]
impl PyIOConfig {
    #[new]
    pub fn new(s3: Option<PyS3Config>) -> Self {
        PyIOConfig {
            config: IOConfig {
                s3: s3.unwrap_or_default().config,
            },
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }

    #[getter]
    pub fn s3(&self) -> PyResult<PyS3Config> {
        Ok(PyS3Config {
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
impl PyS3Config {
    #[new]
    pub fn new(
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        session_token: Option<String>,
        access_key: Option<String>,
        anonymous: Option<bool>,
    ) -> Self {
        PyS3Config {
            config: S3Config {
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

    #[getter]
    pub fn region_name(&self) -> PyResult<Option<String>> {
        Ok(self.config.region_name.clone())
    }

    #[getter]
    pub fn endpoint_url(&self) -> PyResult<Option<String>> {
        Ok(self.config.endpoint_url.clone())
    }

    #[getter]
    pub fn key_id(&self) -> PyResult<Option<String>> {
        Ok(self.config.key_id.clone())
    }

    #[getter]
    pub fn access_key(&self) -> PyResult<Option<String>> {
        Ok(self.config.access_key.clone())
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyS3Config>()?;
    parent.add_class::<PyIOConfig>()?;
    Ok(())
}
