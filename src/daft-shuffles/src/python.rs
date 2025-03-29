use std::sync::{Arc, LazyLock};

use common_runtime::RuntimeRef;
use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use pyo3::{
    pyclass, pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

use crate::shuffle_cache::{InProgressShuffleCache, ShuffleCache};

static LOCAL_THREAD_RUNTIME: LazyLock<RuntimeRef> =
    LazyLock::new(|| common_runtime::get_local_thread_runtime());

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
        pyo3_async_runtimes::tokio::init_with_runtime(&LOCAL_THREAD_RUNTIME.runtime).unwrap();
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
                cache: shuffle_cache,
            })
        })
    }
}

#[pyclass(module = "daft.daft", name = "ShuffleCache", frozen)]
pub struct PyShuffleCache {
    cache: ShuffleCache,
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

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyInProgressShuffleCache>()?;
    parent.add_class::<PyShuffleCache>()?;
    Ok(())
}
