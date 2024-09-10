use std::sync::Arc;

use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::{basic::CompareOp, prelude::*};
use serde::{Deserialize, Serialize};

use crate::{
    file_format_config::DatabaseSourceConfig, CsvSourceConfig, FileFormat, FileFormatConfig,
    JsonSourceConfig, ParquetSourceConfig,
};

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "FileFormatConfig")
)]
pub struct PyFileFormatConfig(Arc<FileFormatConfig>);

#[pymethods]
impl PyFileFormatConfig {
    /// Create a Parquet file format config.
    #[staticmethod]
    fn from_parquet_config(config: ParquetSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Parquet(config)))
    }

    /// Create a CSV file format config.
    #[staticmethod]
    fn from_csv_config(config: CsvSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Csv(config)))
    }

    /// Create a JSON file format config.
    #[staticmethod]
    fn from_json_config(config: JsonSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Json(config)))
    }

    /// Create a Database file format config.
    #[staticmethod]
    fn from_database_config(config: DatabaseSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Database(config)))
    }

    /// Get the underlying data source config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        match self.0.as_ref() {
            FileFormatConfig::Parquet(config) => config.clone().into_py(py),
            FileFormatConfig::Csv(config) => config.clone().into_py(py),
            FileFormatConfig::Json(config) => config.clone().into_py(py),
            FileFormatConfig::Database(config) => config.clone().into_py(py),
            FileFormatConfig::PythonFunction => py.None(),
        }
    }

    /// Get the file format for this file format config.
    fn file_format(&self) -> FileFormat {
        self.0.as_ref().into()
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self.0 == other.0,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl_bincode_py_state_serialization!(PyFileFormatConfig);

impl From<PyFileFormatConfig> for Arc<FileFormatConfig> {
    fn from(file_format_config: PyFileFormatConfig) -> Self {
        file_format_config.0
    }
}

impl From<Arc<FileFormatConfig>> for PyFileFormatConfig {
    fn from(file_format_config: Arc<FileFormatConfig>) -> Self {
        Self(file_format_config)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<FileFormat>()?;
    Ok(())
}
