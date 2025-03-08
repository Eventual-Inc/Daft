use common_runtime::get_compute_runtime;
use daft_dsl::python::PyExpr;
use daft_micropartition::python::PyMicroPartition;
use pyo3::{
    pyclass, pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

use crate::shuffle_cache::ShuffleWriter;

#[pyclass(module = "daft.daft", name = "ShuffleWriter", frozen)]
pub struct PyShuffleWriter {
    writer: ShuffleWriter,
}

#[pymethods]
impl PyShuffleWriter {
    #[staticmethod]
    #[pyo3(signature = (num_partitions, dir, target_filesize, compression=None, partition_by=None))]
    pub fn try_new(
        num_partitions: usize,
        dir: &str,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<PyExpr>>,
    ) -> PyResult<Self> {
        let shuffle_writer = ShuffleWriter::try_new(
            num_partitions,
            dir,
            target_filesize,
            compression,
            partition_by.map(|partition_by| partition_by.into_iter().map(|p| p.into()).collect()),
        )?;
        Ok(Self {
            writer: shuffle_writer,
        })
    }

    pub fn push_partition(&self, py: Python, input_partition: PyMicroPartition) -> PyResult<()> {
        py.allow_threads(|| {
            get_compute_runtime()
                .block_on_current_thread(self.writer.push_partition(input_partition.into()))?;
            Ok(())
        })
    }

    pub fn close(&self) -> PyResult<Vec<Vec<usize>>> {
        let bytes_per_file = get_compute_runtime().block_on_current_thread(self.writer.close())?;
        Ok(bytes_per_file)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyShuffleWriter>()?;
    Ok(())
}
