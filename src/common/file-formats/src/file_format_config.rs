use std::{collections::BTreeMap, hash::Hash, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use daft_schema::{field::Field, time_unit::TimeUnit};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_py_serde::{deserialize_py_object, serialize_py_object},
    daft_schema::python::{datatype::PyTimeUnit, field::PyField},
    pyo3::{Py, PyAny, PyResult, Python, pyclass, pymethods, types::PyAnyMethods},
};

use crate::FileFormat;

/// Configuration for parsing a particular file format.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum FileFormatConfig {
    Parquet(ParquetSourceConfig),
    Csv(CsvSourceConfig),
    Json(JsonSourceConfig),
    Warc(WarcSourceConfig),
    #[cfg(feature = "python")]
    Database(DatabaseSourceConfig),
    #[cfg(feature = "python")]
    PythonFunction {
        source_type: Option<String>,
        module_name: Option<String>,
        function_name: Option<String>,
    },
}
#[cfg(not(debug_assertions))]
impl std::fmt::Debug for FileFormatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.var_name())
    }
}

impl FileFormatConfig {
    #[must_use]
    pub fn file_format(&self) -> FileFormat {
        self.into()
    }

    #[must_use]
    pub fn var_name(&self) -> String {
        match self {
            Self::Parquet(_) => "Parquet".to_string(),
            Self::Csv(_) => "Csv".to_string(),
            Self::Json(_) => "Json".to_string(),
            Self::Warc(_) => "Warc".to_string(),
            #[cfg(feature = "python")]
            Self::Database(_) => "Database".to_string(),
            #[cfg(feature = "python")]
            Self::PythonFunction {
                source_type,
                module_name,
                function_name: _,
            } => {
                if let Some(source_type) = source_type {
                    format!("{}(Python)", source_type)
                } else if let Some(module_name) = module_name {
                    // Infer type from module name
                    format!("{}(Python)", module_name)
                } else {
                    "PythonFunction".to_string()
                }
            }
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Parquet(source) => source.multiline_display(),
            Self::Csv(source) => source.multiline_display(),
            Self::Json(source) => source.multiline_display(),
            Self::Warc(source) => source.multiline_display(),
            #[cfg(feature = "python")]
            Self::Database(source) => source.multiline_display(),
            #[cfg(feature = "python")]
            Self::PythonFunction { .. } => vec![],
        }
    }
}

/// Configuration for a Parquet data source.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
#[cfg_attr(debug_assertions, derive(Debug))]
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
    pub chunk_size: Option<usize>,
}

impl ParquetSourceConfig {
    #[must_use]
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
                                    .map(std::string::ToString::to_string)
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

impl Default for ParquetSourceConfig {
    fn default() -> Self {
        Self {
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    /// Create a config for a Parquet data source.
    #[new]
    #[pyo3(signature = (coerce_int96_timestamp_unit=None, field_id_mapping=None, row_groups=None, chunk_size=None))]
    fn new(
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        field_id_mapping: Option<BTreeMap<i32, PyField>>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .unwrap_or_else(|| TimeUnit::Nanoseconds.into())
                .into(),
            field_id_mapping: field_id_mapping
                .map(|map| Arc::new(map.into_iter().map(|(k, v)| (k, v.field)).collect())),
            row_groups,
            chunk_size,
        }
    }

    #[getter]
    fn coerce_int96_timestamp_unit(&self) -> PyResult<PyTimeUnit> {
        Ok(self.coerce_int96_timestamp_unit.into())
    }
}

impl_bincode_py_state_serialization!(ParquetSourceConfig);

/// Configuration for a CSV data source.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(debug_assertions, derive(Debug))]
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
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(delimiter) = self.delimiter {
            res.push(format!("Delimiter = {delimiter}"));
        }
        res.push(format!("Has headers = {}", self.has_headers));
        res.push(format!("Double quote = {}", self.double_quote));
        if let Some(quote) = self.quote {
            res.push(format!("Quote = {quote}"));
        }
        if let Some(escape_char) = self.escape_char {
            res.push(format!("Escape char = {escape_char}"));
        }
        if let Some(comment) = self.comment {
            res.push(format!("Comment = {comment}"));
        }
        res.push(format!(
            "Allow_variable_columns = {}",
            self.allow_variable_columns
        ));
        if let Some(buffer_size) = self.buffer_size {
            res.push(format!("Buffer size = {buffer_size}"));
        }
        if let Some(chunk_size) = self.chunk_size {
            res.push(format!("Chunk size = {chunk_size}"));
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
    #[pyo3(signature = (
        has_headers,
        double_quote,
        allow_variable_columns,
        delimiter=None,
        quote=None,
        escape_char=None,
        comment=None,
        buffer_size=None,
        chunk_size=None
    ))]
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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonSourceConfig {
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
    pub skip_empty_files: bool,
}

impl JsonSourceConfig {
    #[must_use]
    pub fn new_internal(
        buffer_size: Option<usize>,
        chunk_size: Option<usize>,
        skip_empty_files: bool,
    ) -> Self {
        Self {
            buffer_size,
            chunk_size,
            skip_empty_files,
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(buffer_size) = self.buffer_size {
            res.push(format!("Buffer size = {buffer_size}"));
        }
        if let Some(chunk_size) = self.chunk_size {
            res.push(format!("Chunk size = {chunk_size}"));
        }
        res
    }
}

impl Default for JsonSourceConfig {
    fn default() -> Self {
        Self::new_internal(None, None, false)
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
    #[pyo3(signature = (buffer_size=None, chunk_size=None, skip_empty_files=false))]
    fn new(buffer_size: Option<usize>, chunk_size: Option<usize>, skip_empty_files: bool) -> Self {
        Self::new_internal(buffer_size, chunk_size, skip_empty_files)
    }
}

impl_bincode_py_state_serialization!(JsonSourceConfig);

/// Configuration for a Database data source.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct DatabaseSourceConfig {
    pub sql: String,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub conn: Arc<Py<PyAny>>,
}

#[cfg(feature = "python")]
impl PartialEq for DatabaseSourceConfig {
    fn eq(&self, other: &Self) -> bool {
        self.sql == other.sql
            && Python::attach(|py| self.conn.bind(py).eq(other.conn.bind(py)).unwrap())
    }
}

#[cfg(feature = "python")]
impl Eq for DatabaseSourceConfig {}

#[cfg(feature = "python")]
impl Hash for DatabaseSourceConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sql.hash(state);
        let py_obj_hash = Python::attach(|py| self.conn.bind(py).hash());
        match py_obj_hash {
            Ok(hash) => hash.hash(state),
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

#[cfg(feature = "python")]
impl DatabaseSourceConfig {
    #[must_use]
    pub fn new_internal(sql: String, conn: Arc<Py<PyAny>>) -> Self {
        Self { sql, conn }
    }

    #[must_use]
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
    fn new(sql: &str, conn: Py<PyAny>) -> Self {
        Self::new_internal(sql.to_string(), Arc::new(conn))
    }
}

impl_bincode_py_state_serialization!(DatabaseSourceConfig);

/// Configuration for a Warc data source.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct WarcSourceConfig {}

impl WarcSourceConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let res = vec![];
        res
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl WarcSourceConfig {
    /// Create a config for a Warc data source.
    #[new]
    #[pyo3(signature = ())]
    fn new() -> PyResult<Self> {
        Ok(Self {})
    }
}

impl_bincode_py_state_serialization!(WarcSourceConfig);
