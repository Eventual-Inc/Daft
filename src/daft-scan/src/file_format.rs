use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::TimeUnit, impl_bincode_py_state_serialization};
use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc};

#[cfg(feature = "python")]
use {
    daft_core::python::datatype::PyTimeUnit,
    daft_dsl::python::PyExpr,
    pyo3::{
        pyclass, pyclass::CompareOp, pymethods, types::PyBytes, IntoPy, PyObject, PyResult,
        PyTypeInfo, Python, ToPyObject,
    },
};

/// Format of a file, e.g. Parquet, CSV, JSON.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
    Database,
}

impl FromStr for FileFormat {
    type Err = DaftError;

    fn from_str(file_format: &str) -> DaftResult<Self> {
        use FileFormat::*;

        if file_format.trim().eq_ignore_ascii_case("parquet") {
            Ok(Parquet)
        } else if file_format.trim().eq_ignore_ascii_case("csv") {
            Ok(Csv)
        } else if file_format.trim().eq_ignore_ascii_case("json") {
            Ok(Json)
        } else if file_format.trim().eq_ignore_ascii_case("database") {
            Ok(Database)
        } else {
            Err(DaftError::TypeError(format!(
                "FileFormat {} not supported!",
                file_format
            )))
        }
    }
}

impl_bincode_py_state_serialization!(FileFormat);

impl From<&FileFormatConfig> for FileFormat {
    fn from(file_format_config: &FileFormatConfig) -> Self {
        match file_format_config {
            FileFormatConfig::Parquet(_) => Self::Parquet,
            FileFormatConfig::Csv(_) => Self::Csv,
            FileFormatConfig::Json(_) => Self::Json,
            FileFormatConfig::Database(_) => Self::Database,
        }
    }
}

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
    Database(DatabaseSourceConfig),
}

impl FileFormatConfig {
    pub fn var_name(&self) -> &'static str {
        use FileFormatConfig::*;

        match self {
            Parquet(_) => "Parquet",
            Csv(_) => "Csv",
            Json(_) => "Json",
            Database(_) => "Database",
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Parquet(source) => source.multiline_display(),
            Self::Csv(source) => source.multiline_display(),
            Self::Json(source) => source.multiline_display(),
            Self::Database(source) => source.multiline_display(),
        }
    }
}

/// Configuration for a Parquet data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ParquetSourceConfig {
    pub coerce_int96_timestamp_unit: TimeUnit,
}

impl ParquetSourceConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Coerce int96 timestamp unit = {}",
            self.coerce_int96_timestamp_unit
        ));
        res
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    /// Create a config for a Parquet data source.
    #[new]
    fn new(coerce_int96_timestamp_unit: Option<PyTimeUnit>) -> Self {
        Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .unwrap_or(TimeUnit::Nanoseconds.into())
                .into(),
        }
    }

    #[getter]
    fn coerce_int96_timestamp_unit(&self) -> PyResult<PyTimeUnit> {
        Ok(self.coerce_int96_timestamp_unit.into())
    }
}

impl_bincode_py_state_serialization!(ParquetSourceConfig);

/// Configuration for a CSV data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvSourceConfig {
    pub delimiter: Option<char>,
    pub has_headers: bool,
    pub double_quote: bool,
    pub quote: Option<char>,
    pub escape_char: Option<char>,
    pub comment: Option<char>,
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl CsvSourceConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(delimiter) = self.delimiter {
            res.push(format!("Delimiter = {}", delimiter));
        }
        res.push(format!("Has headers = {}", self.has_headers));
        res.push(format!("Double quote = {}", self.double_quote));
        if let Some(quote) = self.quote {
            res.push(format!("Quote = {}", quote));
        }
        if let Some(escape_char) = self.escape_char {
            res.push(format!("Escape char = {}", escape_char));
        }
        if let Some(comment) = self.comment {
            res.push(format!("Comment = {}", comment));
        }
        if let Some(buffer_size) = self.buffer_size {
            res.push(format!("Buffer size = {}", buffer_size));
        }
        if let Some(chunk_size) = self.chunk_size {
            res.push(format!("Chunk size = {}", chunk_size));
        }
        res
    }
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
    /// * `buffer_size` - Size of the buffer (in bytes) used by the streaming reader.
    /// * `chunk_size` - Size of the chunks (in bytes) deserialized in parallel by the streaming reader.
    #[allow(clippy::too_many_arguments)]
    #[new]
    fn new(
        has_headers: bool,
        double_quote: bool,
        delimiter: Option<char>,
        quote: Option<char>,
        escape_char: Option<char>,
        comment: Option<char>,
        buffer_size: Option<usize>,
        chunk_size: Option<usize>,
    ) -> PyResult<Self> {
        Ok(Self {
            delimiter,
            has_headers,
            double_quote,
            quote,
            escape_char,
            comment,
            buffer_size,
            chunk_size,
        })
    }
}

impl_bincode_py_state_serialization!(CsvSourceConfig);

/// Configuration for a JSON data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonSourceConfig {
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl JsonSourceConfig {
    pub fn new_internal(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(buffer_size) = self.buffer_size {
            res.push(format!("Buffer size = {}", buffer_size));
        }
        if let Some(chunk_size) = self.chunk_size {
            res.push(format!("Chunk size = {}", chunk_size));
        }
        res
    }
}

impl Default for JsonSourceConfig {
    fn default() -> Self {
        Self::new_internal(None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl JsonSourceConfig {
    /// Create a config for a JSON data source.
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - Size of the buffer (in bytes) used by the streaming reader.
    /// * `chunk_size` - Size of the chunks (in bytes) deserialized in parallel by the streaming reader.
    #[new]
    fn new(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self::new_internal(buffer_size, chunk_size)
    }
}

impl_bincode_py_state_serialization!(JsonSourceConfig);

/// Configuration for a Database data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct DatabaseSourceConfig {
    pub sql: String,
    pub partition_col: Option<String>,
    pub left_bound: Option<ExprRef>,
    pub right_bound: Option<ExprRef>,
}

impl DatabaseSourceConfig {
    pub fn new_internal(
        sql: String,
        partition_col: Option<String>,
        left_bound: Option<ExprRef>,
        right_bound: Option<ExprRef>,
    ) -> Self {
        Self {
            sql,
            partition_col,
            left_bound,
            right_bound,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("SQL = {}", self.sql));
        if let Some(partition_col) = &self.partition_col {
            res.push(format!("Partition column = {}", partition_col));
        }
        if let Some(left_bound) = &self.left_bound {
            res.push(format!("Left bound = {}", left_bound));
        }
        if let Some(right_bound) = &self.right_bound {
            res.push(format!("Right bound = {}", right_bound));
        }
        res
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl DatabaseSourceConfig {
    /// Create a config for a Database data source.
    #[new]
    fn new(
        sql: &str,
        partition_col: Option<&str>,
        left_bound: Option<PyExpr>,
        right_bound: Option<PyExpr>,
    ) -> Self {
        Self::new_internal(
            sql.to_string(),
            partition_col.map(|s| s.to_string()),
            left_bound.map(|e| e.expr.into()),
            right_bound.map(|e| e.expr.into()),
        )
    }
}

impl_bincode_py_state_serialization!(DatabaseSourceConfig);

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

    /// Create a Database file format config.
    #[staticmethod]
    fn from_database_config(config: DatabaseSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Database(config)))
    }

    /// Get the underlying data source config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use FileFormatConfig::*;

        match self.0.as_ref() {
            Parquet(config) => config.clone().into_py(py),
            Csv(config) => config.clone().into_py(py),
            Json(config) => config.clone().into_py(py),
            Database(config) => config.clone().into_py(py),
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
