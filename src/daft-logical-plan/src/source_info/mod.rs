use std::{
    hash::{Hash, Hasher},
    sync::atomic::AtomicUsize,
};

use common_scan_info::PhysicalScanInfo;
pub use common_scan_info::{FileInfo, FileInfos};
use daft_schema::schema::SchemaRef;
use daft_table::Table;
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_py_serde::{deserialize_py_object, serialize_py_object},
    pyo3::PyObject,
};

use crate::partitioning::ClusteringSpecRef;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceInfo {
    Python(PythonInfo),
    Physical(PhysicalScanInfo),
    PlaceHolder(PlaceHolderInfo),
    InMemory(Vec<Table>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub source_schema: SchemaRef,
    pub cache_key: String,
    pub num_partitions: usize,
    pub size_bytes: usize,
    pub num_rows: usize,
    pub clustering_spec: Option<ClusteringSpecRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonInfo {
    pub source_schema: SchemaRef,
    pub cache_key: String,
    #[cfg(feature = "python")]
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub cache_entry: PyObject,
    pub num_partitions: usize,
    pub size_bytes: usize,
    pub num_rows: usize,
    pub clustering_spec: Option<ClusteringSpecRef>,
}

#[cfg(feature = "python")]
impl PythonInfo {
    pub fn new(
        source_schema: SchemaRef,
        cache_key: String,
        cache_entry: PyObject,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
        clustering_spec: Option<ClusteringSpecRef>,
    ) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
            num_partitions,
            size_bytes,
            num_rows,
            clustering_spec,
        }
    }
}

impl PartialEq for PythonInfo {
    fn eq(&self, other: &Self) -> bool {
        self.cache_key == other.cache_key
    }
}

impl Eq for PythonInfo {}

impl Hash for PythonInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cache_key.hash(state);
    }
}

static PLACEHOLDER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PlaceHolderInfo {
    pub source_schema: SchemaRef,
    pub clustering_spec: ClusteringSpecRef,
    pub source_id: usize,
}

impl PlaceHolderInfo {
    pub fn new(source_schema: SchemaRef, clustering_spec: ClusteringSpecRef) -> Self {
        Self {
            source_schema,
            clustering_spec,
            source_id: PLACEHOLDER_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        }
    }
}
