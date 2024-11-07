use std::hash::Hash;

use common_file_formats::FileFormat;
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::{deserialize_py_object, serialize_py_object};
use daft_dsl::ExprRef;
use derivative::Derivative;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::PyObject;
use serde::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SinkInfo {
    OutputFileInfo(OutputFileInfo),
    #[cfg(feature = "python")]
    CatalogInfo(CatalogInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputFileInfo {
    pub root_dir: String,
    pub file_format: FileFormat,
    pub partition_cols: Option<Vec<ExprRef>>,
    pub compression: Option<String>,
    pub io_config: Option<IOConfig>,
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
#[derive(Derivative, Debug, Clone, Serialize, Deserialize)]
#[derivative(PartialEq, Eq, Hash)]
pub struct IcebergCatalogInfo {
    pub table_name: String,
    pub table_location: String,
    pub partition_spec_id: i64,
    pub partition_cols: Vec<ExprRef>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub iceberg_schema: PyObject,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub iceberg_properties: PyObject,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl IcebergCatalogInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.table_name));
        res.push(format!("Table Location = {}", self.table_location));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        };
        res
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeltaLakeCatalogInfo {
    pub path: String,
    pub mode: String,
    pub version: i32,
    pub large_dtypes: bool,
    pub partition_cols: Option<Vec<String>>,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl DeltaLakeCatalogInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.path));
        res.push(format!("Mode = {}", self.mode));
        res.push(format!("Version = {}", self.version));
        res.push(format!("Large Dtypes = {}", self.large_dtypes));
        if let Some(ref partition_cols) = self.partition_cols {
            res.push(format!(
                "Partition cols = {}",
                partition_cols.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        };
        res
    }
}

#[cfg(feature = "python")]
#[derive(Derivative, Debug, Clone, Serialize, Deserialize)]
#[derivative(PartialEq, Eq, Hash)]
pub struct LanceCatalogInfo {
    pub path: String,
    pub mode: String,
    pub io_config: Option<IOConfig>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub kwargs: PyObject,
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
