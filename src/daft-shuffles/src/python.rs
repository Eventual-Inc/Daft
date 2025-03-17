use std::sync::Arc;

use common_runtime::get_compute_runtime;
use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use pyo3::{
    pyclass, pyfunction, pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

use crate::{
    server::flight_server::{start_flight_server, FlightServerConnectionHandle},
    shuffle_cache::{InProgressShuffleCache, ShuffleCache},
};

#[pyclass(module = "daft.daft", name = "InProgressShuffleCache", frozen)]
pub struct PyInProgressShuffleCache {
    cache: InProgressShuffleCache,
}

#[pymethods]
impl PyInProgressShuffleCache {
    #[staticmethod]
    #[pyo3(signature = (num_partitions, dir, target_filesize, compression=None, partition_by=None))]
    pub fn try_new(
        num_partitions: usize,
        dir: &str,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<PyExpr>>,
    ) -> PyResult<Self> {
        let shuffle_cache = InProgressShuffleCache::try_new(
            num_partitions,
            dir,
            target_filesize,
            compression,
            partition_by.map(|partition_by| partition_by.into_iter().map(|p| p.into()).collect()),
        )?;
        Ok(Self {
            cache: shuffle_cache,
        })
    }

    pub fn push_partition(&self, py: Python, input_partition: PyMicroPartition) -> PyResult<()> {
        py.allow_threads(|| {
            get_compute_runtime()
                .block_on_current_thread(self.cache.push_partition(input_partition.into()))?;
            Ok(())
        })
    }

    pub fn close(&self) -> PyResult<PyShuffleCache> {
        let shuffle_cache = get_compute_runtime().block_on_current_thread(self.cache.close())?;
        Ok(PyShuffleCache {
            cache: Arc::new(shuffle_cache),
        })
    }
}

#[pyclass(module = "daft.daft", name = "ShuffleCache", frozen)]
pub struct PyShuffleCache {
    cache: Arc<ShuffleCache>,
}

#[pymethods]
impl PyShuffleCache {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.cache.schema().into())
    }

    pub fn bytes_per_file(&self, partition_idx: usize) -> PyResult<Vec<usize>> {
        Ok(self.cache.bytes_per_file(partition_idx))
    }

    pub fn file_paths(&self, partition_idx: usize) -> PyResult<Vec<String>> {
        Ok(self.cache.file_paths(partition_idx))
    }
}

#[pyclass(module = "daft.daft", name = "FlightServerConnectionHandle")]
pub struct PyFlightServerConnectionHandle {
    handle: FlightServerConnectionHandle,
}

#[pymethods]
impl PyFlightServerConnectionHandle {
    pub fn shutdown(&mut self) -> PyResult<()> {
        self.handle.shutdown()?;
        Ok(())
    }

    pub fn port(&self) -> PyResult<u16> {
        Ok(self.handle.port())
    }
}

#[pyfunction(name = "start_flight_server")]
pub fn py_start_flight_server(
    shuffle_cache: &PyShuffleCache,
    ip: &str,
) -> PyResult<PyFlightServerConnectionHandle> {
    let handle = start_flight_server(shuffle_cache.cache.clone(), ip).unwrap();
    Ok(PyFlightServerConnectionHandle { handle })
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyInProgressShuffleCache>()?;
    parent.add_class::<PyShuffleCache>()?;
    Ok(())
}
