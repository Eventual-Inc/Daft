use std::any::Any;

use common_error::DaftResult;
use common_partitioning::Partition;
use pyo3::{pyclass, pymethods, FromPyObject, PyObject, Python};

#[pyclass(module = "daft.daft", name = "RayPartitionRef")]
#[derive(Debug, FromPyObject)]
pub(crate) struct RayPartitionRef {
    pub object_ref: PyObject,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[pymethods]
impl RayPartitionRef {
    #[new]
    pub fn new(object_ref: PyObject, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_ref,
            num_rows,
            size_bytes,
        }
    }

    #[getter]
    pub fn get_object_ref(&self, py: Python) -> PyObject {
        self.object_ref.clone_ref(py)
    }

    #[getter]
    pub fn get_num_rows(&self) -> usize {
        self.num_rows
    }

    #[getter]
    pub fn get_size_bytes(&self) -> usize {
        self.size_bytes
    }
}

impl Partition for RayPartitionRef {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        Ok(Some(self.size_bytes))
    }
    fn num_rows(&self) -> DaftResult<usize> {
        Ok(self.num_rows)
    }
}
