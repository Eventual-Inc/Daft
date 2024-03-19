use std::hash::Hash;

use common_io_config::IOConfig;
use daft_core::schema::SchemaRef;
use daft_dsl::Expr;
use itertools::Itertools;
use pyo3::PyObject;

use crate::FileFormat;
use serde::{Deserialize, Serialize};

use daft_scan::py_object_serde::{deserialize_py_object, serialize_py_object};
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SinkInfo {
    OutputFileInfo(OutputFileInfo),
    CatalogInfo(CatalogInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputFileInfo {
    pub root_dir: String,
    pub file_format: FileFormat,
    pub partition_cols: Option<Vec<Expr>>,
    pub compression: Option<String>,
    pub io_config: Option<IOConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogInfo {
    pub catalog: CatalogType,
    pub catalog_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CatalogType {
    Iceberg(IcebergCatalogInfo),
}

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

impl PartialEq for IcebergCatalogInfo {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.table_location == other.table_location
            && self.spec_id == other.spec_id
            && self.io_config == other.io_config
    }
}

impl Eq for IcebergCatalogInfo {}

impl Hash for IcebergCatalogInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.table_location.hash(state);
        self.spec_id.hash(state);
        self.io_config.hash(state);
    }
}

impl CatalogInfo {}

impl OutputFileInfo {
    pub fn new(
        root_dir: String,
        file_format: FileFormat,
        partition_cols: Option<Vec<Expr>>,
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
