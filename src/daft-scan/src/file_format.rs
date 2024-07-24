use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, TimeUnit},
    impl_bincode_py_state_serialization,
};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{collections::BTreeMap, str::FromStr, sync::Arc};
#[cfg(feature = "python")]
use {
    common_py_serde::{deserialize_py_object, serialize_py_object},
    daft_core::python::{datatype::PyTimeUnit, field::PyField},
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
    Python,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileFormat {
    fn ext(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Database => "db",
            Self::Python => "py",
        }
    }
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
            #[cfg(feature = "python")]
            FileFormatConfig::Database(_) => Self::Database,
            #[cfg(feature = "python")]
            FileFormatConfig::PythonFunction => Self::Python,
        }
    }
}

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
    #[cfg(feature = "python")]
    Database(DatabaseSourceConfig),
    #[cfg(feature = "python")]
    PythonFunction,
}

impl FileFormatConfig {
    pub fn var_name(&self) -> &'static str {
        use FileFormatConfig::*;

        match self {
            Parquet(_) => "Parquet",
            Csv(_) => "Csv",
            Json(_) => "Json",
            #[cfg(feature = "python")]
            Database(_) => "Database",
            #[cfg(feature = "python")]
            PythonFunction => "PythonFunction",
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Parquet(source) => source.multiline_display(),
            Self::Csv(source) => source.multiline_display(),
            Self::Json(source) => source.multiline_display(),
            #[cfg(feature = "python")]
            Self::Database(source) => source.multiline_display(),
            #[cfg(feature = "python")]
            Self::PythonFunction => vec![],
        }
    }
}

/// Configuration for a Parquet data source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ParquetSourceConfig {
    pub coerce_int96_timestamp_unit: TimeUnit,

    /// Mapping of field_id to Daft field
    ///
    /// Data Catalogs such as Iceberg rely on Parquet's field_id to identify fields in a Parquet file
    /// in a way that is stable across operations such as column renaming. When reading Parquet files,
    /// if the `field_id_mapping` is provided, we must rename the (potentially stale) Parquet
    /// data according to the provided field_ids.
    ///
    /// See: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L456-L459
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub row_groups: Option<Vec<Option<Vec<i64>>>>,
}

impl ParquetSourceConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Coerce int96 timestamp unit = {}",
            self.coerce_int96_timestamp_unit
        ));
        if let Some(mapping) = &self.field_id_mapping {
            res.push(format!(
                "Field ID to Fields = {{{}}}",
                mapping
                    .iter()
                    .map(|(fid, f)| format!("{fid}: {f}"))
                    .collect::<Vec<String>>()
                    .join(",")
            ));
        }
        if let Some(row_groups) = &self.row_groups {
            res.push(format!(
                "Row Groups = {{{}}}",
                row_groups
                    .iter()
                    .map(|rg| {
                        rg.as_ref()
                            .map(|rg| {
                                rg.iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            })
                            .unwrap_or_else(|| "None".to_string())
                    })
                    .collect::<Vec<String>>()
                    .join(",")
            ));
        }
        res
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    /// Create a config for a Parquet data source.
    #[new]
    fn new(
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        field_id_mapping: Option<BTreeMap<i32, PyField>>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
    ) -> Self {
        Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .unwrap_or(TimeUnit::Nanoseconds.into())
                .into(),
            field_id_mapping: field_id_mapping.map(|map| {
                Arc::new(BTreeMap::from_iter(
                    map.into_iter().map(|(k, v)| (k, v.field)),
                ))
            }),
            row_groups,
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
    pub allow_variable_columns: bool,
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
        res.push(format!(
            "Allow_variable_columns = {}",
            self.allow_variable_columns
        ));
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
        allow_variable_columns: bool,
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
            allow_variable_columns,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct DatabaseSourceConfig {
    pub sql: String,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub conn: PyObject,
}

#[cfg(feature = "python")]
impl PartialEq for DatabaseSourceConfig {
    fn eq(&self, other: &Self) -> bool {
        self.sql == other.sql
            && Python::with_gil(|py| self.conn.as_ref(py).eq(other.conn.as_ref(py)).unwrap())
    }
}

#[cfg(feature = "python")]
impl Eq for DatabaseSourceConfig {}

#[cfg(feature = "python")]
impl Hash for DatabaseSourceConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sql.hash(state);
        let py_obj_hash = Python::with_gil(|py| self.conn.as_ref(py).hash());
        match py_obj_hash {
            Ok(hash) => hash.hash(state),
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

#[cfg(feature = "python")]
impl DatabaseSourceConfig {
    pub fn new_internal(sql: String, conn: PyObject) -> Self {
        Self { sql, conn }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("SQL = \"{}\"", self.sql));
        res
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl DatabaseSourceConfig {
    /// Create a config for a Database data source.
    #[new]
    fn new(sql: &str, conn: PyObject) -> Self {
        Self::new_internal(sql.to_string(), conn)
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
            PythonFunction => py.None(),
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
