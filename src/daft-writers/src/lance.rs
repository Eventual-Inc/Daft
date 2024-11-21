use std::sync::Arc;

use common_error::DaftResult;
use daft_logical_plan::LanceCatalogInfo;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_table::{python::PyTable, Table};
use pyo3::{types::PyAnyMethods, Python};

use crate::{FileWriter, WriterFactory};

pub struct LanceWriter {
    is_closed: bool,
    lance_info: LanceCatalogInfo,
    results: Vec<Table>,
}

impl LanceWriter {
    pub fn new(lance_info: LanceCatalogInfo) -> Self {
        Self {
            is_closed: false,
            lance_info,
            results: vec![],
        }
    }
}

impl FileWriter for LanceWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<Table>;

    fn write(&mut self, data: &Self::Input) -> DaftResult<()> {
        assert!(!self.is_closed, "Cannot write to a closed LanceWriter");
        Python::with_gil(|py| {
            let py_micropartition = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data.clone()),))?;
            let written_fragments: PyTable = py
                .import_bound(pyo3::intern!(py, "daft.table.table_io"))?
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
                    &self.lance_info.kwargs,
                ))?
                .getattr(pyo3::intern!(py, "to_table"))?
                .call0()?
                .getattr(pyo3::intern!(py, "_table"))?
                .extract()?;
            self.results.push(written_fragments.into());
            Ok(())
        })
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        self.is_closed = true;
        Ok(std::mem::take(&mut self.results))
    }
}

pub fn make_lance_writer_factory(
    lance_info: LanceCatalogInfo,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>> {
    Arc::new(LanceWriterFactory { lance_info })
}

pub struct LanceWriterFactory {
    pub lance_info: LanceCatalogInfo,
}

impl WriterFactory for LanceWriterFactory {
    type Input = Arc<MicroPartition>;

    type Result = Vec<Table>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = LanceWriter::new(self.lance_info.clone());
        Ok(Box::new(writer))
    }
}
