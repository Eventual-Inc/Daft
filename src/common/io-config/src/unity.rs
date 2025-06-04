use std::{hash::Hash, sync::Arc};

use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use pyo3::{intern, prelude::*};

use crate::IOConfig;

/// Wrapper around the Python `daft.unity_catalog.UnityCatalog`
#[cfg(feature = "python")]
#[derive(Debug)]
pub struct UnityCatalog(PyObject);

#[cfg(feature = "python")]
impl PartialEq for UnityCatalog {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.bind(py).is(&other.0))
    }
}

#[cfg(feature = "python")]
impl Eq for UnityCatalog {}

#[cfg(feature = "python")]
impl Hash for UnityCatalog {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Python::with_gil(|py| self.0.bind(py).hash().unwrap().hash(state));
    }
}

#[cfg(not(feature = "python"))]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnityCatalog;

#[derive(Clone)]
pub struct UnityCatalogVolume {
    pub storage_location: String,
    pub io_config: Option<Arc<IOConfig>>,
}

#[cfg(feature = "python")]
impl UnityCatalog {
    pub fn try_new(inner: &PyObject, py: Python) -> DaftResult<Self> {
        let uc_class = py
            .import(intern!(py, "daft.unity_catalog"))?
            .getattr(intern!(py, "UnityCatalog"))?;

        let is_unity_catalog = inner.bind(py).is_instance(&uc_class)?;

        if is_unity_catalog {
            Ok(Self(inner.clone_ref(py)))
        } else {
            Err(DaftError::ValueError("`UnityCatalog` struct can only be constructed from a `daft.unity_catalog.UnityCatalog` object".to_string()))
        }
    }

    pub fn load_volume(&self, name: &str) -> DaftResult<UnityCatalogVolume> {
        Ok(Python::with_gil(|py| {
            use pyo3::intern;

            let py_volume = self
                .0
                .call_method1(py, intern!(py, "load_volume"), (name,))?;

            let storage_location = py_volume
                .getattr(py, intern!(py, "volume_info"))?
                .getattr(py, intern!(py, "storage_location"))?
                .extract::<String>(py)?;

            let py_io_config = py_volume.getattr(py, intern!(py, "io_config"))?;

            let io_config = if py_io_config.is_none(py) {
                None
            } else {
                Some(Arc::new(
                    py_io_config.extract::<crate::python::IOConfig>(py)?.config,
                ))
            };

            Ok::<_, PyErr>(UnityCatalogVolume {
                storage_location,
                io_config,
            })
        })?)
    }
}

#[cfg(not(feature = "python"))]
impl UnityCatalog {
    pub fn load_volume(&self, name: &str) -> DaftResult<UnityCatalogVolume> {
        unimplemented!("UnityCatalog::load_volume requires the Python feature")
    }
}
