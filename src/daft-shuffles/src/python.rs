use common_runtime::get_compute_runtime;
use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use daft_schema::python::schema::PySchema;
use pyo3::{
    pyclass, pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

use crate::shuffle_cache::ShuffleCache;

#[pyclass(module = "daft.daft", name = "ShuffleCache", frozen)]
pub struct PyShuffleCache {
    cache: ShuffleCache,
}

#[pymethods]
impl PyShuffleCache {
    #[staticmethod]
    #[pyo3(signature = (num_partitions, dir, target_filesize, compression=None, partition_by=None))]
    pub fn try_new(
        num_partitions: usize,
        dir: &str,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<PyExpr>>,
    ) -> PyResult<Self> {
        let shuffle_cache = ShuffleCache::try_new(
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

    pub fn close(&self) -> PyResult<()> {
        get_compute_runtime().block_on_current_thread(self.cache.close())?;
        Ok(())
    }

    pub fn schema(&self) -> PyResult<Option<PySchema>> {
        let schema = get_compute_runtime().block_on_current_thread(self.cache.schema());
        Ok(schema.map(|s| s.into()))
    }

    pub fn bytes_per_file(&self, partition_idx: usize) -> PyResult<Vec<usize>> {
        let bytes_per_file = get_compute_runtime()
            .block_on_current_thread(self.cache.bytes_per_file(partition_idx))?;
        Ok(bytes_per_file)
    }

    pub fn file_paths(&self, partition_idx: usize) -> PyResult<Vec<String>> {
        let file_paths =
            get_compute_runtime().block_on_current_thread(self.cache.file_paths(partition_idx))?;
        Ok(file_paths)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyShuffleCache>()?;
    Ok(())
}
