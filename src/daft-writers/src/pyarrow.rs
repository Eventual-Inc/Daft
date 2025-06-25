use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_recordbatch::{python::PyRecordBatch, RecordBatch};
use pyo3::{types::PyAnyMethods, PyObject, Python};

use crate::AsyncFileWriter;

pub struct PyArrowWriter {
    py_writer: PyObject,
    is_closed: bool,
    bytes_written: usize,
}

impl PyArrowWriter {
    pub fn new_parquet_writer(
        root_dir: &str,
        file_idx: usize,
        compression: Option<&String>,
        io_config: Option<&daft_io::IOConfig>,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("ParquetFileWriter")?;
            let _from_pyrecordbatch = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "RecordBatch"))?
                .getattr(pyo3::intern!(py, "_from_pyrecordbatch"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_recordbatch =
                        _from_pyrecordbatch.call1((PyRecordBatch::from(pv.clone()),))?;
                    Some(py_recordbatch)
                }
                None => None,
            };

            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values,
                compression.map(|c| c.as_str()),
                io_config.map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
                is_closed: false,
                bytes_written: 0,
            })
        })
    }

    pub fn new_csv_writer(
        root_dir: &str,
        file_idx: usize,
        io_config: Option<&daft_io::IOConfig>,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("CSVFileWriter")?;
            let _from_pyrecordbatch = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "RecordBatch"))?
                .getattr(pyo3::intern!(py, "_from_pyrecordbatch"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_recordbatch =
                        _from_pyrecordbatch.call1((PyRecordBatch::from(pv.clone()),))?;
                    Some(py_recordbatch)
                }
                None => None,
            };
            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values,
                io_config.map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
                is_closed: false,
                bytes_written: 0,
            })
        })
    }

    pub fn new_iceberg_writer(
        root_dir: &str,
        file_idx: usize,
        schema: &pyo3::Py<pyo3::PyAny>,
        properties: &pyo3::Py<pyo3::PyAny>,
        partition_spec_id: i64,
        partition_values: Option<&RecordBatch>,
        io_config: Option<&daft_io::IOConfig>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("IcebergWriter")?;
            let _from_pyrecordbatch = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "RecordBatch"))?
                .getattr(pyo3::intern!(py, "_from_pyrecordbatch"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_recordbatch =
                        _from_pyrecordbatch.call1((PyRecordBatch::from(pv.clone()),))?;
                    Some(py_recordbatch)
                }
                None => None,
            };
            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                schema,
                properties,
                partition_spec_id,
                partition_values,
                io_config.map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
                is_closed: false,
                bytes_written: 0,
            })
        })
    }

    pub fn new_deltalake_writer(
        root_dir: &str,
        file_idx: usize,
        version: i32,
        large_dtypes: bool,
        partition_values: Option<&RecordBatch>,
        io_config: Option<&daft_io::IOConfig>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("DeltalakeWriter")?;
            let _from_pyrecordbatch = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "RecordBatch"))?
                .getattr(pyo3::intern!(py, "_from_pyrecordbatch"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_recordbatch =
                        _from_pyrecordbatch.call1((PyRecordBatch::from(pv.clone()),))?;
                    Some(py_recordbatch)
                }
                None => None,
            };
            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                version,
                large_dtypes,
                partition_values,
                io_config.map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
                is_closed: false,
                bytes_written: 0,
            })
        })
    }
}

#[async_trait]
impl AsyncFileWriter for PyArrowWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        assert!(!self.is_closed, "Cannot write to a closed PyArrowWriter");
        let bytes_written = Python::with_gil(|py| {
            let py_micropartition = py
                .import(pyo3::intern!(py, "daft.recordbatch"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data),))?;
            self.py_writer
                .call_method1(py, pyo3::intern!(py, "write"), (py_micropartition,))?
                .extract::<usize>(py)
        })?;
        self.bytes_written += bytes_written;
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
        Python::with_gil(|py| {
            let result = self
                .py_writer
                .call_method0(py, pyo3::intern!(py, "close"))?
                .getattr(py, pyo3::intern!(py, "_recordbatch"))?;
            Ok(Some(result.extract::<PyRecordBatch>(py)?.into()))
        })
    }
}
