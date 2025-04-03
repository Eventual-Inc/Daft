use std::sync::Arc;

use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use pyo3::{
    pyclass, pyfunction, pymethods,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult, Python,
};

use crate::{
    server::flight_server::{start_flight_server, FlightServerConnectionHandle},
    shuffle_cache::{get_or_init_shuffle_cache_runtime, InProgressShuffleCache, ShuffleCache},
};

#[pyclass(module = "daft.daft", name = "InProgressShuffleCache", frozen)]
pub struct PyInProgressShuffleCache {
    cache: Arc<InProgressShuffleCache>,
}

#[pymethods]
impl PyInProgressShuffleCache {
    #[staticmethod]
    #[pyo3(signature = (num_partitions, dirs, target_filesize, compression=None, partition_by=None))]
    pub fn try_new(
        num_partitions: usize,
        dirs: Vec<String>,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<PyExpr>>,
    ) -> PyResult<Self> {
        let shuffle_cache = InProgressShuffleCache::try_new(
            num_partitions,
            dirs.as_slice(),
            target_filesize,
            compression,
            partition_by.map(|partition_by| partition_by.into_iter().map(|p| p.into()).collect()),
        )?;
        pyo3_async_runtimes::tokio::init_with_runtime(&get_or_init_shuffle_cache_runtime().runtime)
            .unwrap();
        Ok(Self {
            cache: Arc::new(shuffle_cache),
        })
    }

    pub fn push_partitions<'a>(
        &self,
        py: Python<'a>,
        input_partitions: Vec<PyMicroPartition>,
    ) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let cache = self.cache.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            cache
                .push_partitions(input_partitions.into_iter().map(|p| p.into()).collect())
                .await?;
            Ok(())
        })
    }

    pub fn close<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let cache = self.cache.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let shuffle_cache = cache.close().await?;
            Ok(PyShuffleCache {
                cache: Arc::new(shuffle_cache),
            })
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
    parent.add_class::<PyFlightServerConnectionHandle>()?;
    parent.add_function(wrap_pyfunction!(py_start_flight_server, parent)?)?;
    Ok(())
}
