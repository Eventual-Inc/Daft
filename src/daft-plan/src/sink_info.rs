use std::hash::Hash;

use common_io_config::IOConfig;
use daft_dsl::ExprRef;
use itertools::Itertools;

#[cfg(feature = "python")]
use pyo3::PyObject;

use crate::FileFormat;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use daft_scan::py_object_serde::{deserialize_py_object, serialize_py_object};

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
