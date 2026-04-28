#[cfg(feature = "python")]
mod python {
    use std::{any::Any, sync::Arc};

    use common_partitioning::Partition;
    use pyo3::{
        Bound, IntoPyObject, Py, PyAny, PyResult, PyTypeInfo, Python, pyclass, pymethods,
        types::PyModuleMethods,
    };

    #[pyclass(module = "daft.daft", name = "RayPartitionRef", frozen, from_py_object)]
    #[derive(Debug, Clone)]
    pub struct RayPartitionRef {
        pub object_ref: Arc<Py<PyAny>>,
        pub num_rows: usize,
        pub size_bytes: usize,
    }

    #[pymethods]
    impl RayPartitionRef {
        #[new]
        pub fn new(object_ref: Py<PyAny>, num_rows: usize, size_bytes: usize) -> Self {
            Self {
                object_ref: Arc::new(object_ref),
                num_rows,
                size_bytes,
            }
        }

        #[getter]
        pub fn get_object_ref(&self, py: Python) -> Py<PyAny> {
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

        fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyAny>)> {
            Ok((
                Self::type_object(py).into(),
                (
                    self.object_ref.clone_ref(py),
                    self.num_rows,
                    self.size_bytes,
                )
                    .into_pyobject(py)?
                    .into_any(),
            ))
        }
    }

    impl Partition for RayPartitionRef {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn size_bytes(&self) -> usize {
            self.size_bytes
        }

        fn num_rows(&self) -> usize {
            self.num_rows
        }
    }

    pub fn register_modules(parent: &Bound<pyo3::types::PyModule>) -> PyResult<()> {
        parent.add_class::<RayPartitionRef>()?;
        Ok(())
    }
}

#[cfg(feature = "python")]
pub use python::{RayPartitionRef, register_modules};
