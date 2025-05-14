use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_logical_plan::DataSinkInfo;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_recordbatch::{python::PyRecordBatch, RecordBatch};
use pyo3::{types::PyAnyMethods, Python};

use crate::{AsyncFileWriter, WriterFactory};

pub struct DataSinkWriter {
    is_closed: bool,
    data_sink_info: DataSinkInfo,
    results: Vec<RecordBatch>,
    bytes_written: usize,
}

impl DataSinkWriter {
    pub fn new(data_sink_info: DataSinkInfo) -> Self {
        Self {
            is_closed: false,
            data_sink_info,
            results: vec![],
            bytes_written: 0,
        }
    }
}

#[async_trait]
impl AsyncFileWriter for DataSinkWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        let mut bytes_written = 0;
        let mp_result: PyRecordBatch = Python::with_gil(|py| -> pyo3::PyResult<_> {
            // Grab the current micropartition and pass it to the data sink.
            let py_micropartition = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data),))?;
            let py_list = pyo3::types::PyList::new(py, &[py_micropartition])?;
            let py_iter = py_list.try_iter()?;

            let result = self
                .data_sink_info
                .sink
                .call_method(py, "write", (py_iter,), None)?;
            let result_list = py
                .import(pyo3::intern!(py, "builtins"))?
                .call_method1("list", (result,))?;
            for result in result_list.try_iter()? {
                bytes_written += result?.getattr("bytes_written")?.extract::<usize>()?;
            }

            // Save return values into a record batch.
            let results_dict = pyo3::types::PyDict::new(py);
            results_dict.set_item("write_results", result_list)?;
            py.import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "RecordBatch"))?
                .getattr(pyo3::intern!(py, "from_pydict"))?
                .call1((results_dict,))?
                .getattr(pyo3::intern!(py, "_recordbatch"))?
                .extract()
        })?;

        self.results.push(mp_result.into());
        Ok(bytes_written)
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

pub fn make_data_sink_writer_factory(
    data_sink_info: DataSinkInfo,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>> {
    Arc::new(DataSinkWriterFactory { data_sink_info })
}

pub struct DataSinkWriterFactory {
    pub data_sink_info: DataSinkInfo,
}

impl WriterFactory for DataSinkWriterFactory {
    type Input = Arc<MicroPartition>;

    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = DataSinkWriter::new(self.data_sink_info.clone());
        Ok(Box::new(writer))
    }
}
