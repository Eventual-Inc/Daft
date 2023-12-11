use std::sync::Arc;

use pyo3::{prelude::*, PyTypeInfo};
use serde::{Deserialize, Serialize};

use crate::DaftConfig;

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftConfig {
    pub config: Arc<DaftConfig>,
}

#[pymethods]
impl PyDaftConfig {
    #[new]
    pub fn new() -> Self {
        PyDaftConfig::default()
    }

    fn with_config_values(
        &self,
        merge_scan_tasks_min_size_bytes: Option<usize>,
        merge_scan_tasks_max_size_bytes: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
    ) -> PyResult<PyDaftConfig> {
        let mut config = self.config.as_ref().clone();

        if let Some(merge_scan_tasks_max_size_bytes) = merge_scan_tasks_max_size_bytes {
            config.merge_scan_tasks_max_size_bytes = merge_scan_tasks_max_size_bytes;
        }
        if let Some(merge_scan_tasks_min_size_bytes) = merge_scan_tasks_min_size_bytes {
            config.merge_scan_tasks_min_size_bytes = merge_scan_tasks_min_size_bytes;
        }
        if let Some(broadcast_join_size_bytes_threshold) = broadcast_join_size_bytes_threshold {
            config.broadcast_join_size_bytes_threshold = broadcast_join_size_bytes_threshold;
        }

        Ok(PyDaftConfig {
            config: Arc::new(config),
        })
    }

    #[getter]
    fn get_merge_scan_tasks_min_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_min_size_bytes)
    }

    #[getter]
    fn get_merge_scan_tasks_max_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_max_size_bytes)
    }

    #[getter]
    fn get_broadcast_join_size_bytes_threshold(&self) -> PyResult<usize> {
        Ok(self.config.broadcast_join_size_bytes_threshold)
    }

    fn __reduce__(&self, py: Python) -> PyResult<(PyObject, (Vec<u8>,))> {
        let bin_data = bincode::serialize(self.config.as_ref())
            .expect("DaftConfig should be serializable to bytes");
        Ok((
            Self::type_object(py)
                .getattr("_from_serialized")?
                .to_object(py),
            (bin_data,),
        ))
    }

    #[staticmethod]
    fn _from_serialized(bin_data: Vec<u8>) -> PyResult<PyDaftConfig> {
        let daft_config: DaftConfig = bincode::deserialize(bin_data.as_slice())
            .expect("DaftConfig should be deserializable from bytes");
        Ok(PyDaftConfig {
            config: daft_config.into(),
        })
    }
}
