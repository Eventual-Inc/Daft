use std::sync::Arc;

use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use pyo3::{
    Bound, PyResult, Python, pyclass, pyfunction, pymethods,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};

use crate::{
    client::FlightClientManager,
    server::flight_server::{FlightServerConnectionHandle, start_flight_server},
    shuffle_cache::{InProgressShuffleCache, ShuffleCache, get_or_init_shuffle_cache_runtime},
};

#[pyclass(module = "daft.daft", name = "InProgressShuffleCache", frozen)]
pub struct PyInProgressShuffleCache {
    cache: Arc<InProgressShuffleCache>,
}

#[pymethods]
impl PyInProgressShuffleCache {
    #[staticmethod]
    #[pyo3(signature = (num_partitions, dirs, node_id, shuffle_stage_id, target_filesize, compression=None, partition_by=None))]
    pub fn try_new(
        num_partitions: usize,
        dirs: Vec<String>,
        node_id: String,
        shuffle_stage_id: usize,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<PyExpr>>,
    ) -> PyResult<Self> {
        let shuffle_cache = InProgressShuffleCache::try_new(
            num_partitions,
            dirs.as_slice(),
            node_id,
            shuffle_stage_id,
            target_filesize,
            compression,
            partition_by.map(|partition_by| partition_by.into_iter().map(|p| p.into()).collect()),
        )?;
        let _ = pyo3_async_runtimes::tokio::init_with_runtime(
            &get_or_init_shuffle_cache_runtime().runtime,
        );
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

    pub fn file_paths_for_partition(&self, partition_idx: usize) -> PyResult<Vec<String>> {
        Ok(self.cache.file_paths_for_partition(partition_idx))
    }

    pub fn bytes_per_file_for_partition(&self, partition_idx: usize) -> PyResult<Vec<usize>> {
        Ok(self.cache.bytes_per_file_for_partition(partition_idx))
    }

    pub fn rows_per_partition(&self) -> PyResult<Vec<usize>> {
        Ok(self.cache.rows_per_partition())
    }

    pub fn bytes_per_partition(&self) -> PyResult<Vec<usize>> {
        Ok(self.cache.bytes_per_partition())
    }

    pub fn clear_partition(&self, partition_idx: usize) -> PyResult<()> {
        self.cache.clear_partition(partition_idx)?;
        Ok(())
    }

    pub fn clear_directories(&self) -> PyResult<()> {
        self.cache.clear_directories()?;
        Ok(())
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
    let handle = start_flight_server(shuffle_cache.cache.clone(), ip);
    Ok(PyFlightServerConnectionHandle { handle })
}

#[pyclass(module = "daft.daft", name = "FlightClientManager", frozen)]
pub struct PyFlightClientManager {
    manager: Arc<FlightClientManager>,
}

#[pymethods]
impl PyFlightClientManager {
    #[new]
    pub fn new(
        addresses: Vec<String>,
        num_parallel_fetches: usize,
        schema: &PySchema,
    ) -> PyResult<Self> {
        let _ = pyo3_async_runtimes::tokio::init_with_runtime(
            &get_or_init_shuffle_cache_runtime().runtime,
        );
        Ok(Self {
            manager: Arc::new(FlightClientManager::new(
                addresses,
                num_parallel_fetches,
                schema.schema.clone(),
            )),
        })
    }

    pub fn fetch_partition<'a>(
        &self,
        partition: usize,
        py: Python<'a>,
    ) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let manager = self.manager.clone();
        let res = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let data = manager.fetch_partition(partition).await?;
            Ok(PyMicroPartition::from(data))
        })?;
        Ok(res)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyInProgressShuffleCache>()?;
    parent.add_class::<PyShuffleCache>()?;
    parent.add_class::<PyFlightServerConnectionHandle>()?;
    parent.add_class::<PyFlightClientManager>()?;
    parent.add_function(wrap_pyfunction!(py_start_flight_server, parent)?)?;
    Ok(())
}
