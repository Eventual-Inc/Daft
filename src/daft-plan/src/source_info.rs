use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

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
    daft_table::python::PyTable,
    pyo3::{
        exceptions::{PyKeyError, PyValueError},
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

#[derive(Debug, PartialEq, Eq, Hash)]
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
    pub source_schema: SchemaRef,
    pub cache_key: String,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub cache_entry: PyObject,
}

#[cfg(feature = "python")]
impl InMemoryInfo {
    pub fn new(source_schema: SchemaRef, cache_key: String, cache_entry: PyObject) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
        }
    }
}

#[cfg(feature = "python")]
impl PartialEq for InMemoryInfo {
    fn eq(&self, other: &Self) -> bool {
        self.cache_key == other.cache_key
            && Python::with_gil(|py| {
                self.cache_entry
                    .as_ref(py)
                    .eq(other.cache_entry.as_ref(py))
                    .unwrap()
            })
    }
}

#[cfg(feature = "python")]
impl Eq for InMemoryInfo {}

#[cfg(feature = "python")]
impl Hash for InMemoryInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cache_key.hash(state);
        let py_obj_hash = Python::with_gil(|py| self.cache_entry.as_ref(py).hash());
        match py_obj_hash {
            // If Python object is hashable, hash the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExternalInfo {
    pub source_schema: SchemaRef,
    pub file_infos: Arc<FileInfos>,
    pub file_format_config: Arc<FileFormatConfig>,
}

impl ExternalInfo {
    pub fn new(
        source_schema: SchemaRef,
        file_infos: Arc<FileInfos>,
        file_format_config: Arc<FileFormatConfig>,
    ) -> Self {
        Self {
            source_schema,
            file_infos,
            file_format_config,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct FileInfo {
    pub file_path: String,
    pub file_size: Option<i64>,
    pub num_rows: Option<i64>,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileInfo {
    #[new]
    pub fn new(file_path: String, file_size: Option<i64>, num_rows: Option<i64>) -> Self {
        Self::new_internal(file_path, file_size, num_rows)
    }
}

impl FileInfo {
    pub fn new_internal(file_path: String, file_size: Option<i64>, num_rows: Option<i64>) -> Self {
        Self {
            file_path,
            file_size,
            num_rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct FileInfos {
    pub file_paths: Vec<String>,
    pub file_sizes: Vec<Option<i64>>,
    pub num_rows: Vec<Option<i64>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileInfos {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            // Create an empty FileInfos, to be overridden by __setstate__ and/or extended with self.extend().
            0 => Ok(Self::new_internal(vec![], vec![], vec![])),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new FileInfos, got : {}",
                args.len()
            ))),
        }
    }

    #[staticmethod]
    pub fn from_infos(
        file_paths: Vec<String>,
        file_sizes: Vec<Option<i64>>,
        num_rows: Vec<Option<i64>>,
    ) -> Self {
        Self::new_internal(file_paths, file_sizes, num_rows)
    }

    #[staticmethod]
    pub fn from_table(table: PyTable) -> PyResult<Self> {
        Ok(Self::from_table_internal(table.table)?)
    }

    pub fn extend(&mut self, new_infos: Self) {
        self.file_paths.extend(new_infos.file_paths);
        self.file_sizes.extend(new_infos.file_sizes);
        self.num_rows.extend(new_infos.num_rows);
    }

    pub fn __getitem__(&self, idx: isize) -> PyResult<FileInfo> {
        if idx as usize >= self.len() {
            return Err(PyKeyError::new_err(idx));
        }
        Ok(FileInfo::new_internal(
            self.file_paths[0].clone(),
            self.file_sizes[0],
            self.num_rows[0],
        ))
    }

    pub fn to_table(&self) -> PyResult<PyTable> {
        Ok(self.to_table_internal()?.into())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.len())
    }
}

impl_bincode_py_state_serialization!(FileInfos);

impl FileInfos {
    pub fn new_internal(
        file_paths: Vec<String>,
        file_sizes: Vec<Option<i64>>,
        num_rows: Vec<Option<i64>>,
    ) -> Self {
        Self {
            file_paths,
            file_sizes,
            num_rows,
        }
    }

    pub fn from_table_internal(table: Table) -> DaftResult<Self> {
        let file_paths = table
            .get_column("file_paths")?
            .utf8()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Utf8Array<i64>>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect::<Vec<_>>();
        let file_sizes = table
            .get_column("file_sizes")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.cloned())
            .collect::<Vec<_>>();
        let num_rows = table
            .get_column("num_rows")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.cloned())
            .collect::<Vec<_>>();
        Ok(Self::new_internal(file_paths, file_sizes, num_rows))
    }

    pub fn len(&self) -> usize {
        self.file_paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file_paths.is_empty()
    }

    pub fn to_table_internal(&self) -> DaftResult<Table> {
        let columns = vec![
            Series::try_from((
                "file_paths",
                arrow2::array::Utf8Array::<i64>::from_iter_values(self.file_paths.iter())
                    .to_boxed(),
            ))?,
            Series::try_from((
                "file_sizes",
                arrow2::array::PrimitiveArray::<i64>::from(&self.file_sizes).to_boxed(),
            ))?,
            Series::try_from((
                "num_rows",
                arrow2::array::PrimitiveArray::<i64>::from(&self.num_rows).to_boxed(),
            ))?,
        ];
        Table::new(
            Schema::new(columns.iter().map(|s| s.field().clone()).collect())?,
            columns,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ParquetSourceConfig {
    pub use_native_downloader: bool,
    pub io_config: Box<Option<IOConfig>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl ParquetSourceConfig {
    #[new]
    fn new(use_native_downloader: bool, io_config: Option<PyIOConfig>) -> Self {
        Self {
            use_native_downloader,
            io_config: io_config.map(|c| c.config).into(),
        }
    }

    #[getter]
    pub fn get_use_native_downloader(&self) -> PyResult<bool> {
        Ok(self.use_native_downloader)
    }

    #[getter]
    fn get_io_config(&self) -> PyResult<Option<PyIOConfig>> {
        Ok(self.io_config.clone().map(|c| c.into()))
    }
}

impl_bincode_py_state_serialization!(ParquetSourceConfig);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
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
