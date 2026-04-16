use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlightPartitionRef {
    pub shuffle_id: u64,
    pub server_address: String,
    pub partition_ref_id: u64,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[cfg(feature = "python")]
mod python {
    use std::any::Any;

    use common_partitioning::Partition;
    use common_py_serde::impl_bincode_py_state_serialization;
    use pyo3::{Bound, PyResult, Python, pyclass, pymethods, types::PyModuleMethods};
    use serde::{Deserialize, Serialize};

    use crate::flight::FlightPartitionRef;

    #[pyclass(
        module = "daft.daft",
        name = "FlightPartitionRef",
        frozen,
        from_py_object
    )]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyFlightPartitionRef {
        pub inner: FlightPartitionRef,
    }

    impl Partition for PyFlightPartitionRef {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn size_bytes(&self) -> usize {
            self.inner.size_bytes
        }

        fn num_rows(&self) -> usize {
            self.inner.num_rows
        }
    }

    impl_bincode_py_state_serialization!(PyFlightPartitionRef);

    #[pymethods]
    impl PyFlightPartitionRef {
        #[new]
        pub fn new(
            shuffle_id: u64,
            server_address: String,
            partition_ref_id: u64,
            num_rows: usize,
            size_bytes: usize,
        ) -> Self {
            Self {
                inner: FlightPartitionRef {
                    shuffle_id,
                    server_address,
                    partition_ref_id,
                    num_rows,
                    size_bytes,
                },
            }
        }

        #[getter]
        pub fn shuffle_id(&self) -> u64 {
            self.inner.shuffle_id
        }

        #[getter]
        pub fn server_address(&self) -> String {
            self.inner.server_address.clone()
        }

        #[getter]
        pub fn partition_ref_id(&self) -> u64 {
            self.inner.partition_ref_id
        }

        #[getter]
        pub fn num_rows(&self) -> usize {
            self.inner.num_rows
        }

        #[getter]
        pub fn size_bytes(&self) -> usize {
            self.inner.size_bytes
        }
    }

    impl From<FlightPartitionRef> for PyFlightPartitionRef {
        fn from(inner: FlightPartitionRef) -> Self {
            Self { inner }
        }
    }

    impl From<PyFlightPartitionRef> for FlightPartitionRef {
        fn from(py_ref: PyFlightPartitionRef) -> Self {
            py_ref.inner
        }
    }

    pub fn register_modules(parent: &Bound<pyo3::types::PyModule>) -> PyResult<()> {
        parent.add_class::<PyFlightPartitionRef>()?;
        Ok(())
    }
}

#[cfg(feature = "python")]
pub use python::{PyFlightPartitionRef, register_modules};
