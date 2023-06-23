use crate::config::S3Config;
use pyo3::prelude::*;

#[derive(Clone)]
#[pyclass]
struct PyS3Config {
    config: S3Config,
}

#[pymethods]
impl PyS3Config {
    #[new]
    pub fn new() -> Self {
        PyS3Config {
            config: S3Config::default(),
        }
    }

    pub fn set_region_name(&mut self, region_name: String) -> PyResult<()> {
        self.config.region_name = Some(region_name);
        Ok(())
    }

    pub fn set_endpoint_url(&mut self, endpoint_url: String) -> PyResult<()> {
        self.config.endpoint_url = Some(endpoint_url);
        Ok(())
    }

    pub fn set_key_id(&mut self, key_id: String) -> PyResult<()> {
        self.config.key_id = Some(key_id);
        Ok(())
    }

    pub fn set_access_key(&mut self, access_key: String) -> PyResult<()> {
        self.config.access_key = Some(access_key);
        Ok(())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.config))
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyS3Config>()?;

    Ok(())
}
