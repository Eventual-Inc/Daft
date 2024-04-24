pub mod file_info;
use daft_core::schema::SchemaRef;
use daft_scan::ScanExternalInfo;
pub use file_info::{FileInfo, FileInfos};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;

use crate::{partitioning::ClusteringSpecRef, ClusteringSpec};

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
    ExternalInfo(ScanExternalInfo),
    PlaceHolderInfo(PlaceHolderInfo)
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
    pub clustering_spec: Option<ClusteringSpecRef>
}

#[cfg(feature = "python")]
impl InMemoryInfo {
    pub fn new(
        source_schema: SchemaRef,
        cache_key: String,
        cache_entry: PyObject,
        num_partitions: usize,
        size_bytes: usize,
        clustering_spec: Option<ClusteringSpecRef>
    ) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
            num_partitions,
            size_bytes,
            clustering_spec
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

static PLACEHOLDER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PlaceHolderInfo {
    pub source_schema: SchemaRef,
    pub clustering_spec: ClusteringSpecRef,
    pub source_id: usize
}

impl PlaceHolderInfo {
    pub fn new(source_schema: SchemaRef, clustering_spec: ClusteringSpecRef) -> Self {
        PlaceHolderInfo {
            source_schema,
            clustering_spec,
            source_id: PLACEHOLDER_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }
    }
}
