use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(feature = "python")]
use pyo3::{
    pyclass, pyclass::CompareOp, pymethods, types::PyBytes, IntoPy, PyObject, PyResult, PyTypeInfo,
    Python, ToPyObject,
};

/// Format of a file, e.g. Parquet, CSV, JSON.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

impl_bincode_py_state_serialization!(FileFormat);

impl From<&FileFormatConfig> for FileFormat {
    fn from(file_format_config: &FileFormatConfig) -> Self {
        match file_format_config {
            FileFormatConfig::Parquet(_) => Self::Parquet,
            FileFormatConfig::Csv(_) => Self::Csv,
            FileFormatConfig::Json(_) => Self::Json,
        }
    }
}

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
}

impl FileFormatConfig {
    pub fn var_name(&self) -> &'static str {
        use FileFormatConfig::*;

        match self {
            Parquet(_) => "Parquet",
            Csv(_) => "Csv",
            Json(_) => "Json",
        }
    }
}

/// Configuration for a Parquet data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ParquetSourceConfig;

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    /// Create a config for a Parquet data source.
    #[new]
    fn new() -> Self {
        Self {}
    }
}

impl_bincode_py_state_serialization!(ParquetSourceConfig);

/// Configuration for a CSV data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvSourceConfig {
    pub delimiter: String,
    pub has_headers: bool,
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvSourceConfig {
    /// Create a config for a CSV data source.
    ///
    /// # Arguments
    ///
    /// * `delimiter` - The character delmiting individual cells in the CSV data.
    /// * `has_headers` - Whether the CSV has a header row; if so, it will be skipped during data parsing.
    #[new]
    fn new(delimiter: String, has_headers: bool) -> Self {
        Self {
            delimiter,
            has_headers,
        }
    }
}

impl_bincode_py_state_serialization!(CsvSourceConfig);

/// Configuration for a JSON data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonSourceConfig {}

#[cfg(feature = "python")]
#[pymethods]
impl JsonSourceConfig {
    /// Create a config for a JSON data source.
    #[new]
    fn new() -> Self {
        Self {}
    }
}

impl_bincode_py_state_serialization!(JsonSourceConfig);

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "FileFormatConfig")
)]
pub struct PyFileFormatConfig(Arc<FileFormatConfig>);

#[cfg(feature = "python")]
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

    /// Get the underlying data source config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use FileFormatConfig::*;

        match self.0.as_ref() {
            Parquet(config) => config.clone().into_py(py),
            Csv(config) => config.clone().into_py(py),
            Json(config) => config.clone().into_py(py),
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
