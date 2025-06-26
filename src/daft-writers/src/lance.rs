use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_logical_plan::LanceCatalogInfo;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_recordbatch::{python::PyRecordBatch, RecordBatch};
use pyo3::{types::PyAnyMethods, Python};

use crate::{AsyncFileWriter, WriterFactory};

pub struct LanceWriter {
    is_closed: bool,
    lance_info: LanceCatalogInfo,
    results: Vec<RecordBatch>,
    bytes_written: usize,
}

impl LanceWriter {
    pub fn new(lance_info: LanceCatalogInfo) -> Self {
        Self {
            is_closed: false,
            lance_info,
            results: vec![],
            bytes_written: 0,
        }
    }
}

#[async_trait]
impl AsyncFileWriter for LanceWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        assert!(!self.is_closed, "Cannot write to a closed LanceWriter");
        self.bytes_written += data
            .size_bytes()?
            .expect("MicroPartition should have size_bytes for LanceWriter");
        Python::with_gil(|py| {
            let py_micropartition = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data),))?;
            let written_fragments: PyRecordBatch = py
                .import(pyo3::intern!(py, "daft.recordbatch.recordbatch_io"))?
                .getattr(pyo3::intern!(py, "write_lance"))?
                .call1((
                    py_micropartition,
                    &self.lance_info.path,
                    &self.lance_info.mode,
                    self.lance_info
                        .io_config
                        .as_ref()
                        .map(|cfg| daft_io::python::IOConfig {
                            config: cfg.clone(),
                        }),
                    &self.lance_info.kwargs.clone_ref(py),
                ))?
                .getattr(pyo3::intern!(py, "to_record_batch"))?
                .call0()?
                .getattr(pyo3::intern!(py, "_recordbatch"))?
                .extract()?;
            self.results.push(written_fragments.into());
            Ok(self.bytes_written)
        })
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        self.is_closed = true;
        Ok(std::mem::take(&mut self.results))
    }
}

pub fn make_lance_writer_factory(
    lance_info: LanceCatalogInfo,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>> {
    Arc::new(LanceWriterFactory { lance_info })
}

pub struct LanceWriterFactory {
    pub lance_info: LanceCatalogInfo,
}

impl WriterFactory for LanceWriterFactory {
    type Input = Arc<MicroPartition>;

    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = LanceWriter::new(self.lance_info.clone());
        Ok(Box::new(writer))
    }
}
