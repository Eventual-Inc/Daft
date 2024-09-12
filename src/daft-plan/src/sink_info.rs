use std::hash::Hash;

use common_file_formats::FileFormat;
use common_io_config::IOConfig;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use itertools::Itertools;

#[cfg(feature = "python")]
use {
    daft_core::python::PySchema, daft_dsl::python::PyExpr, pyo3::pyclass, pyo3::pymethods,
    pyo3::PyObject, pyo3::PyResult,
};

use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use common_py_serde::{deserialize_py_object, serialize_py_object};

#[derive(Clone)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum PySinkType {
    InMemory,
    FileWrite,
    CatalogWrite,
}

#[derive(Clone)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct PySinkInfo {
    pub sink_type: PySinkType,
    pub file_schema: Option<SchemaRef>,
    pub file_info: Option<OutputFileInfo>,
    // pub catalog_info: Option<CatalogInfo>,
}

impl PySinkInfo {
    pub fn from_output_file_info(file_info: OutputFileInfo, schema: SchemaRef) -> Self {
        Self {
            sink_type: PySinkType::FileWrite,
            file_info: Some(file_info),
            file_schema: Some(schema),
        }
    }

    pub fn from_in_memory() -> Self {
        Self {
            sink_type: PySinkType::InMemory,
            file_info: None,
            file_schema: None,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PySinkInfo {
    #[getter]
    pub fn get_sink_type(&self) -> PySinkType {
        self.sink_type.clone()
    }

    #[getter]
    pub fn get_file_info(&self) -> Option<OutputFileInfo> {
        self.file_info.clone()
    }

    #[getter]
    pub fn get_file_schema(&self) -> Option<PySchema> {
        self.file_schema.clone().map(|schema| schema.into())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SinkInfo {
    OutputFileInfo(OutputFileInfo),
    #[cfg(feature = "python")]
    CatalogInfo(CatalogInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct OutputFileInfo {
    pub root_dir: String,
    pub file_format: FileFormat,
    pub partition_cols: Option<Vec<ExprRef>>,
    pub compression: Option<String>,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
#[pymethods]
impl OutputFileInfo {
    #[getter]
    pub fn get_root_dir(&self) -> PyResult<String> {
        Ok(self.root_dir.clone())
    }
    #[getter]
    pub fn get_file_format(&self) -> PyResult<FileFormat> {
        Ok(self.file_format)
    }
    #[getter]
    pub fn get_partition_cols(&self) -> PyResult<Option<Vec<PyExpr>>> {
        Ok(self
            .partition_cols
            .clone()
            .map(|cols| cols.into_iter().map(|e| e.into()).collect()))
    }
    #[getter]
    pub fn get_compression(&self) -> PyResult<Option<String>> {
        Ok(self.compression.clone())
    }
    #[getter]
    pub fn get_io_config(&self) -> PyResult<Option<common_io_config::python::IOConfig>> {
        Ok(self.io_config.clone().map(|io_config| io_config.into()))
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogInfo {
    pub catalog: CatalogType,
    pub catalog_columns: Vec<String>,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CatalogType {
    Iceberg(IcebergCatalogInfo),
    DeltaLake(DeltaLakeCatalogInfo),
    Lance(LanceCatalogInfo),
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergCatalogInfo {
    pub table_name: String,
    pub table_location: String,
    pub spec_id: i64,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub iceberg_schema: PyObject,

    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub iceberg_properties: PyObject,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl PartialEq for IcebergCatalogInfo {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.table_location == other.table_location
            && self.spec_id == other.spec_id
            && self.io_config == other.io_config
    }
}
#[cfg(feature = "python")]
impl Eq for IcebergCatalogInfo {}

#[cfg(feature = "python")]
impl Hash for IcebergCatalogInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.table_location.hash(state);
        self.spec_id.hash(state);
        self.io_config.hash(state);
    }
}

#[cfg(feature = "python")]
impl IcebergCatalogInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.table_name));
        res.push(format!("Table Location = {}", self.table_location));
        res.push(format!("Spec ID = {}", self.spec_id));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        };
        res
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLakeCatalogInfo {
    pub path: String,
    pub mode: String,
    pub version: i32,
    pub large_dtypes: bool,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl PartialEq for DeltaLakeCatalogInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.mode == other.mode
            && self.version == other.version
            && self.large_dtypes == other.large_dtypes
            && self.io_config == other.io_config
    }
}

#[cfg(feature = "python")]
impl Eq for DeltaLakeCatalogInfo {}

#[cfg(feature = "python")]
impl Hash for DeltaLakeCatalogInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.mode.hash(state);
        self.version.hash(state);
        self.large_dtypes.hash(state);
        self.io_config.hash(state);
    }
}

#[cfg(feature = "python")]
impl DeltaLakeCatalogInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.path));
        res.push(format!("Mode = {}", self.mode));
        res.push(format!("Version = {}", self.version));
        res.push(format!("Large Dtypes = {}", self.large_dtypes));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        };
        res
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceCatalogInfo {
    pub path: String,
    pub mode: String,
    pub io_config: Option<IOConfig>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub kwargs: PyObject,
}

#[cfg(feature = "python")]
impl PartialEq for LanceCatalogInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.mode == other.mode && self.io_config == other.io_config
    }
}

#[cfg(feature = "python")]
impl Eq for LanceCatalogInfo {}

#[cfg(feature = "python")]
impl Hash for LanceCatalogInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.mode.hash(state);
        self.io_config.hash(state);
    }
}

#[cfg(feature = "python")]
impl LanceCatalogInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.path));
        res.push(format!("Mode = {}", self.mode));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        };
        res
    }
}

impl OutputFileInfo {
    pub fn new(
        root_dir: String,
        file_format: FileFormat,
        partition_cols: Option<Vec<ExprRef>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            root_dir,
            file_format,
            partition_cols,
            compression,
            io_config,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(ref partition_cols) = self.partition_cols {
            res.push(format!(
                "Partition cols = {}",
                partition_cols.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        if let Some(ref compression) = self.compression {
            res.push(format!("Compression = {}", compression));
        }
        res.push(format!("Root dir = {}", self.root_dir));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        }
        res
    }
}
