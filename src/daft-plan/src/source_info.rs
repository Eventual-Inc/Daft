use std::sync::Arc;

use arrow2::array::Array;
use common_error::DaftResult;
use daft_core::{
    impl_bincode_py_state_serialization,
    schema::{Schema, SchemaRef},
    Series,
};
use daft_io::config::IOConfig;
use daft_table::Table;

#[cfg(feature = "python")]
use {
    daft_io::python::IOConfig as PyIOConfig,
    pyo3::{
        exceptions::PyValueError,
        pyclass,
        pyclass::CompareOp,
        pymethods,
        types::{PyBytes, PyTuple},
        IntoPy, PyObject, PyResult, Python, ToPyObject,
    },
};

use serde::de::{Error as DeError, Visitor};
use serde::{ser::Error as SerError, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug)]
pub enum SourceInfo {
    #[cfg(feature = "python")]
    InMemoryInfo(InMemoryInfo),
    ExternalInfo(ExternalInfo),
}

#[cfg(feature = "python")]
fn serialize_py_object<S>(obj: &PyObject, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = Python::with_gil(|py| {
        py.import(pyo3::intern!(py, "ray.cloudpickle"))
            .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
            .and_then(|m| m.getattr(pyo3::intern!(py, "dumps")))
            .and_then(|f| f.call1((obj,)))
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e| SerError::custom(e.to_string()))
    })?;
    s.serialize_bytes(bytes.as_slice())
}

struct PyObjectVisitor;

#[cfg(feature = "python")]
impl<'de> Visitor<'de> for PyObjectVisitor {
    type Value = PyObject;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array containing the pickled partition bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "ray.cloudpickle"))
                .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
                .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
                .and_then(|f| Ok(f.call1((v,))?.to_object(py)))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "ray.cloudpickle"))
                .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
                .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
                .and_then(|f| Ok(f.call1((v,))?.to_object(py)))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }
}

#[cfg(feature = "python")]
fn deserialize_py_object<'de, D>(d: D) -> Result<PyObject, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_bytes(PyObjectVisitor)
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryInfo {
    pub cache_key: String,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub cache_entry: PyObject,
}

#[cfg(feature = "python")]
impl InMemoryInfo {
    pub fn new(cache_key: String, cache_entry: PyObject) -> Self {
        Self {
            cache_key,
            cache_entry,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalInfo {
    pub schema: SchemaRef,
    pub file_info: Arc<FileInfo>,
    pub file_format_config: Arc<FileFormatConfig>,
}

impl ExternalInfo {
    pub fn new(
        schema: SchemaRef,
        file_info: Arc<FileInfo>,
        file_format_config: Arc<FileFormatConfig>,
    ) -> Self {
        Self {
            schema,
            file_info,
            file_format_config,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_paths: Vec<String>,
    pub file_sizes: Option<Vec<i64>>,
    pub num_rows: Option<Vec<i64>>,
    pub file_formats: Option<Vec<FileFormat>>,
}

impl FileInfo {
    pub fn new(
        file_paths: Vec<String>,
        file_sizes: Option<Vec<i64>>,
        num_rows: Option<Vec<i64>>,
        file_formats: Option<Vec<FileFormat>>,
    ) -> Self {
        Self {
            file_paths,
            file_sizes,
            num_rows,
            file_formats,
        }
    }
    pub fn to_table(&self) -> DaftResult<Table> {
        let file_paths: Vec<Option<&str>> =
            self.file_paths.iter().map(|s| Some(s.as_str())).collect();
        let num_files = file_paths.len();
        let columns = vec![
            Series::try_from((
                "path",
                arrow2::array::Utf8Array::<i64>::from(file_paths).to_boxed(),
            ))?,
            Series::try_from((
                "size",
                arrow2::array::PrimitiveArray::<i64>::new_null(
                    arrow2::datatypes::DataType::Int64,
                    num_files,
                )
                .to_boxed(),
            ))?,
            Series::try_from((
                "type",
                arrow2::array::Utf8Array::<i64>::new_null(
                    arrow2::datatypes::DataType::LargeUtf8,
                    num_files,
                )
                .to_boxed(),
            ))?,
            Series::try_from((
                "rows",
                arrow2::array::PrimitiveArray::<i64>::new_null(
                    arrow2::datatypes::DataType::Int64,
                    num_files,
                )
                .to_boxed(),
            ))?,
        ];
        Table::new(
            Schema::new(columns.iter().map(|s| s.field().clone()).collect())?,
            columns,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileFormat {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            // Create dummy variant, to be overridden by __setstate__.
            0 => Ok(Self::Json),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new FileFormat, got : {}",
                args.len()
            ))),
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ParquetSourceConfig {
    pub use_native_downloader: bool,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    #[new]
    fn new(use_native_downloader: bool, io_config: Option<PyIOConfig>) -> Self {
        Self {
            use_native_downloader,
            io_config: io_config.map(|c| c.config),
        }
    }

    #[getter]
    pub fn get_use_native_downloader(&self) -> PyResult<bool> {
        Ok(self.use_native_downloader)
    }

    #[getter]
    fn get_io_config(&self) -> PyResult<Option<PyIOConfig>> {
        Ok(self.io_config.as_ref().map(|c| c.clone().into()))
    }
}

impl_bincode_py_state_serialization!(ParquetSourceConfig);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvSourceConfig {
    pub delimiter: String,
    pub has_headers: bool,
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvSourceConfig {
    #[new]
    fn new(delimiter: String, has_headers: bool) -> Self {
        Self {
            delimiter,
            has_headers,
        }
    }
}

impl_bincode_py_state_serialization!(CsvSourceConfig);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonSourceConfig {}

#[cfg(feature = "python")]
#[pymethods]
impl JsonSourceConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

impl_bincode_py_state_serialization!(JsonSourceConfig);

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
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            // Create dummy inner FileFormatConfig, to be overridden by __setstate__.
            0 => Ok(Arc::new(FileFormatConfig::Json(JsonSourceConfig::new())).into()),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyFileFormatConfig, got : {}",
                args.len()
            ))),
        }
    }

    #[staticmethod]
    fn from_parquet_config(config: ParquetSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Parquet(config)))
    }

    #[staticmethod]
    fn from_csv_config(config: CsvSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Csv(config)))
    }

    #[staticmethod]
    fn from_json_config(config: JsonSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Json(config)))
    }

    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use FileFormatConfig::*;

        match self.0.as_ref() {
            Parquet(config) => config.clone().into_py(py),
            Csv(config) => config.clone().into_py(py),
            Json(config) => config.clone().into_py(py),
        }
    }

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
