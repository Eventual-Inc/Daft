pub mod file_format;
pub mod file_info;
#[cfg(feature = "python")]
mod py_object_serde;
pub mod storage_config;

use daft_core::schema::SchemaRef;
pub use file_format::{
    CsvSourceConfig, FileFormat, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig,
    PyFileFormatConfig,
};
pub use file_info::{FileInfo, FileInfos};
use serde::{Deserialize, Serialize};
use std::{hash::Hash, sync::Arc};
#[cfg(feature = "python")]
pub use storage_config::PythonStorageConfig;
pub use storage_config::{NativeStorageConfig, PyStorageConfig, StorageConfig};
#[cfg(feature = "python")]
use {
    py_object_serde::{deserialize_py_object, serialize_py_object},
    pyo3::{PyObject, Python},
    std::hash::Hasher,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SourceInfo {
    #[cfg(feature = "python")]
    InMemoryInfo(InMemoryInfo),
    ExternalInfo(ExternalInfo),
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
    pub storage_config: Arc<StorageConfig>,
}

impl ExternalInfo {
    pub fn new(
        source_schema: SchemaRef,
        file_infos: Arc<FileInfos>,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
    ) -> Self {
        Self {
            source_schema,
            file_infos,
            file_format_config,
            storage_config,
        }
    }
}
