use std::{pin::pin, sync::Arc};

use common_error::DaftResult;
use common_runtime::{get_compute_runtime, get_io_runtime};
use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use futures::StreamExt;
use pyo3::{
    pyclass, pyfunction, pymethods,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyRef, PyRefMut, PyResult, Python,
};

use crate::{
    client::fetch_partitions_from_flight,
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

#[pyclass]
pub struct PartitionIterator {
    rx: tokio::sync::mpsc::Receiver<DaftResult<PyMicroPartition>>,
    task: Option<std::thread::JoinHandle<DaftResult<()>>>,
}

#[pymethods]
impl PartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyMicroPartition>> {
        if slf.task.is_none() {
            return Ok(None);
        }

        let next = slf.rx.blocking_recv();
        match next {
            Some(Ok(partition)) => Ok(Some(partition)),
            Some(Err(e)) => {
                // drop the task
                slf.task = None;
                Err(e.into())
            }
            None => {
                // await the task to complete
                slf.task.take().unwrap().join().unwrap()?;
                Ok(None)
            }
        }
    }
}

#[pyfunction(name = "fetch_partitions_from_flight")]
pub fn py_fetch_partitions_from_flight(
    addresses: Vec<String>,
    partitions: Vec<usize>,
    num_parallel_partitions: usize,
) -> PyResult<PartitionIterator> {
    let runtime = get_io_runtime(true);
    let (tx, rx) = tokio::sync::mpsc::channel(partitions.len());
    let reduce_task = std::thread::spawn(move || {
        runtime.block_on_current_thread(async move {
            let mut partition_stream = pin!(
                fetch_partitions_from_flight(addresses, partitions, num_parallel_partitions)
                    .await?
            );
            while let Some(partition) = partition_stream.next().await {
                if tx
                    .send(partition.map(|p| PyMicroPartition::from(p)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            DaftResult::Ok(())
        })
    });
    Ok(PartitionIterator {
        rx,
        task: Some(reduce_task),
    })
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyInProgressShuffleCache>()?;
    parent.add_class::<PyShuffleCache>()?;
    parent.add_class::<PyFlightServerConnectionHandle>()?;
    parent.add_function(wrap_pyfunction!(py_start_flight_server, parent)?)?;
    parent.add_function(wrap_pyfunction!(py_fetch_partitions_from_flight, parent)?)?;
    Ok(())
}
