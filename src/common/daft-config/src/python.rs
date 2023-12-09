use std::sync::Arc;

use pyo3::{prelude::*, PyTypeInfo};
use serde::{Deserialize, Serialize};

use crate::DaftExecutionConfig;

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftExecutionConfig {
    pub config: Arc<DaftExecutionConfig>,
}

#[pymethods]
impl PyDaftExecutionConfig {
    #[new]
    pub fn new() -> Self {
        PyDaftExecutionConfig::default()
    }

    fn with_config_values(
        &self,
        merge_scan_tasks_min_size_bytes: Option<usize>,
        merge_scan_tasks_max_size_bytes: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
    ) -> PyResult<PyDaftExecutionConfig> {
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

        Ok(PyDaftExecutionConfig {
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
            .expect("DaftExecutionConfig should be serializable to bytes");
        Ok((
            Self::type_object(py)
                .getattr("_from_serialized")?
                .to_object(py),
            (bin_data,),
        ))
    }

    #[staticmethod]
    fn _from_serialized(bin_data: Vec<u8>) -> PyResult<PyDaftExecutionConfig> {
        let daft_config: DaftExecutionConfig = bincode::deserialize(bin_data.as_slice())
            .expect("DaftExecutionConfig should be deserializable from bytes");
        Ok(PyDaftExecutionConfig {
            config: daft_config.into(),
        })
    }
}
