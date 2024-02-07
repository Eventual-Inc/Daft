pub mod file_info;
use daft_core::schema::SchemaRef;
use daft_scan::storage_config::StorageConfig;
use daft_scan::ScanExternalInfo;
use daft_scan::{file_format::FileFormatConfig, Pushdowns};
pub use file_info::{FileInfo, FileInfos};
use serde::{Deserialize, Serialize};
use std::{hash::Hash, sync::Arc};
#[cfg(feature = "python")]
use {
    daft_scan::py_object_serde::{deserialize_py_object, serialize_py_object},
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
    pub num_partitions: usize,
    pub size_bytes: usize,
}

#[cfg(feature = "python")]
impl InMemoryInfo {
    pub fn new(
        source_schema: SchemaRef,
        cache_key: String,
        cache_entry: PyObject,
        num_partitions: usize,
        size_bytes: usize,
    ) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
            num_partitions,
            size_bytes,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExternalInfo {
    Scan(ScanExternalInfo),
    Legacy(LegacyExternalInfo),
}

impl ExternalInfo {
    pub fn pushdowns(&self) -> &Pushdowns {
        match self {
            Self::Scan(ScanExternalInfo { pushdowns, .. })
            | Self::Legacy(LegacyExternalInfo { pushdowns, .. }) => pushdowns,
        }
    }

    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        match self {
            Self::Scan(external_info) => Self::Scan(ScanExternalInfo {
                pushdowns,
                ..external_info.clone()
            }),
            Self::Legacy(external_info) => Self::Legacy(LegacyExternalInfo {
                pushdowns,
                ..external_info.clone()
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LegacyExternalInfo {
    pub source_schema: SchemaRef,
    pub file_infos: Arc<FileInfos>,
    pub file_format_config: Arc<FileFormatConfig>,
    pub storage_config: Arc<StorageConfig>,
    pub pushdowns: Pushdowns,
}

impl LegacyExternalInfo {
    pub fn new(
        source_schema: SchemaRef,
        file_infos: Arc<FileInfos>,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            source_schema,
            file_infos,
            file_format_config,
            storage_config,
            pushdowns,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "File paths = [{}]",
            self.file_infos.file_paths.join(", ")
        ));
        res.push(format!(
            "File schema = {}",
            self.source_schema.short_string()
        ));
        let file_format = self.file_format_config.multiline_display();
        if !file_format.is_empty() {
            res.push(format!(
                "{} config= {}",
                self.file_format_config.var_name(),
                file_format.join(", ")
            ));
        }
        let storage_config = self.storage_config.multiline_display();
        if !storage_config.is_empty() {
            res.push(format!(
                "{} storage config = {{ {} }}",
                self.storage_config.var_name(),
                storage_config.join(", ")
            ));
        }
        res.extend(self.pushdowns.multiline_display());
        res
    }
}
