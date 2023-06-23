use crate::config::{IOConfig, S3Config};
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
}

#[pymethods]
impl PyS3Config {
    #[new]
    pub fn new(
        region_name: Option<String>,
        endpoint_url: Option<String>,
        key_id: Option<String>,
        access_key: Option<String>,
    ) -> Self {
        PyS3Config {
            config: S3Config {
                region_name,
                endpoint_url,
                key_id,
                access_key,
            },
        }
    }
    // #[getter]
    // pub fn get_region_name(&self) -> PyResult<Option<String>> {
    //     Ok(self.config.region_name.clone())
    // }

    // #[setter]
    // pub fn set_region_name(&mut self, region_name: Option<String>) -> PyResult<()> {
    //     self.config.region_name = region_name;
    //     Ok(())
    // }

    // #[getter]
    // pub fn get_endpoint_url(&self) -> PyResult<Option<String>> {
    //     Ok(self.config.endpoint_url.clone())
    // }

    // #[setter]
    // pub fn set_endpoint_url(&mut self, endpoint_url: Option<String>) -> PyResult<()> {
    //     self.config.endpoint_url = endpoint_url;
    //     Ok(())
    // }

    // #[getter]
    // pub fn get_key_id(&self) -> PyResult<Option<String>> {
    //     Ok(self.config.key_id.clone())
    // }

    // #[setter]
    // pub fn set_key_id(&mut self, key_id: Option<String>) -> PyResult<()> {
    //     self.config.key_id = key_id;
    //     Ok(())
    // }

    // #[getter]
    // pub fn get_access_key(&self) -> PyResult<Option<String>> {
    //     Ok(self.config.access_key.clone())
    // }

    // #[setter]
    // pub fn set_access_key(&mut self, access_key: Option<String>) -> PyResult<()> {
    //     self.config.access_key = access_key;
    //     Ok(())
    // }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyS3Config>()?;
    parent.add_class::<PyIOConfig>()?;
    Ok(())
}
