use std::{hash::Hash, sync::Arc};

use common_error::DaftResult;
use common_file_formats::{FileFormat, WriteMode};
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::{deserialize_py_object, serialize_py_object};
use daft_core::prelude::Schema;
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use educe::Educe;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::PyObject;
use serde::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkInfo<E = ExprRef> {
    OutputFileInfo(OutputFileInfo<E>),
    #[cfg(feature = "python")]
    CatalogInfo(CatalogInfo<E>),
    #[cfg(feature = "python")]
    DataSinkInfo(DataSinkInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputFileInfo<E = ExprRef> {
    pub root_dir: String,
    pub write_mode: WriteMode,
    pub file_format: FileFormat,
    pub partition_cols: Option<Vec<E>>,
    pub compression: Option<String>,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogInfo<E = ExprRef> {
    pub catalog: CatalogType<E>,
    pub catalog_columns: Vec<String>,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CatalogType<E = ExprRef> {
    Iceberg(IcebergCatalogInfo<E>),
    DeltaLake(DeltaLakeCatalogInfo<E>),
    Lance(LanceCatalogInfo),
}

#[cfg(feature = "python")]
#[derive(Educe, Debug, Clone, Serialize, Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct IcebergCatalogInfo<E = ExprRef> {
    pub table_name: String,
    pub table_location: String,
    pub partition_spec_id: i64,
    pub partition_cols: Vec<E>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub iceberg_schema: Arc<PyObject>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub iceberg_properties: Arc<PyObject>,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl<E> IcebergCatalogInfo<E> {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Table Name = {}", self.table_name));
        res.push(format!("Table Location = {}", self.table_location));
        match &self.io_config {
            None => res.push("IOConfig = None".to_string()),
            Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
        }
        res
    }
}

#[cfg(feature = "python")]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeltaLakeCatalogInfo<E = ExprRef> {
    pub path: String,
    pub mode: String,
    pub version: i32,
    pub large_dtypes: bool,
    pub partition_cols: Option<Vec<E>>,
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
impl<E> DeltaLakeCatalogInfo<E>
where
    E: ToString,
{
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
        }
        res
    }
}

#[cfg(feature = "python")]
#[derive(Educe, Debug, Clone, Serialize, Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct LanceCatalogInfo {
    pub path: String,
    pub mode: String,
    pub io_config: Option<IOConfig>,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub kwargs: Arc<PyObject>,
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
        }
        res
    }
}

#[cfg(feature = "python")]
#[derive(Educe, Debug, Clone, Serialize, Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct DataSinkInfo {
    pub name: String,
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub sink: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl DataSinkInfo {
    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("DataSinkInfo = {}", self.sink)]
    }
}

impl<E> OutputFileInfo<E>
where
    E: ToString,
{
    pub fn new(
        root_dir: String,
        write_mode: WriteMode,
        file_format: FileFormat,
        partition_cols: Option<Vec<E>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            root_dir,
            write_mode,
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

impl SinkInfo {
    pub fn bind(self, schema: &Schema) -> DaftResult<SinkInfo<BoundExpr>> {
        match self {
            Self::OutputFileInfo(output_file_info) => {
                Ok(SinkInfo::OutputFileInfo(output_file_info.bind(schema)?))
            }
            #[cfg(feature = "python")]
            Self::CatalogInfo(catalog_info) => {
                Ok(SinkInfo::CatalogInfo(catalog_info.bind(schema)?))
            }
            #[cfg(feature = "python")]
            Self::DataSinkInfo(data_sink_info) => Ok(SinkInfo::DataSinkInfo(data_sink_info)),
        }
    }
}

impl OutputFileInfo {
    pub fn bind(self, schema: &Schema) -> DaftResult<OutputFileInfo<BoundExpr>> {
        Ok(OutputFileInfo {
            root_dir: self.root_dir,
            write_mode: self.write_mode,
            file_format: self.file_format,
            partition_cols: self
                .partition_cols
                .map(|cols| BoundExpr::bind_all(&cols, schema))
                .transpose()?,
            compression: self.compression,
            io_config: self.io_config,
        })
    }
}

#[cfg(feature = "python")]
impl CatalogInfo {
    pub fn bind(self, schema: &Schema) -> DaftResult<CatalogInfo<BoundExpr>> {
        Ok(CatalogInfo {
            catalog: self.catalog.bind(schema)?,
            catalog_columns: self.catalog_columns,
        })
    }
}

#[cfg(feature = "python")]
impl CatalogType {
    pub fn bind(self, schema: &Schema) -> DaftResult<CatalogType<BoundExpr>> {
        match self {
            Self::Iceberg(iceberg_catalog_info) => {
                Ok(CatalogType::Iceberg(iceberg_catalog_info.bind(schema)?))
            }
            Self::DeltaLake(delta_lake_catalog_info) => Ok(CatalogType::DeltaLake(
                delta_lake_catalog_info.bind(schema)?,
            )),
            Self::Lance(lance_catalog_info) => Ok(CatalogType::Lance(lance_catalog_info)),
        }
    }
}

#[cfg(feature = "python")]
impl IcebergCatalogInfo {
    pub fn bind(self, schema: &Schema) -> DaftResult<IcebergCatalogInfo<BoundExpr>> {
        Ok(IcebergCatalogInfo {
            table_name: self.table_name,
            table_location: self.table_location,
            partition_spec_id: self.partition_spec_id,
            partition_cols: BoundExpr::bind_all(&self.partition_cols, schema)?,
            iceberg_schema: self.iceberg_schema,
            iceberg_properties: self.iceberg_properties,
            io_config: self.io_config,
        })
    }
}

#[cfg(feature = "python")]
impl DeltaLakeCatalogInfo {
    pub fn bind(self, schema: &Schema) -> DaftResult<DeltaLakeCatalogInfo<BoundExpr>> {
        Ok(DeltaLakeCatalogInfo {
            path: self.path,
            mode: self.mode,
            version: self.version,
            large_dtypes: self.large_dtypes,
            partition_cols: self
                .partition_cols
                .map(|cols| BoundExpr::bind_all(&cols, schema))
                .transpose()?,
            io_config: self.io_config,
        })
    }
}
