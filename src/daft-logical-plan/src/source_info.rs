use std::hash::{Hash, Hasher};

use common_partitioning::PartitionCacheEntry;
use common_scan_info::PhysicalScanInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::partitioning::ClusteringSpecRef;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceInfo {
    InMemory(InMemoryInfo),
    Physical(PhysicalScanInfo),
    PlaceHolder(PlaceHolderInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryInfo {
    pub source_schema: SchemaRef,
    pub cache_key: String,
    pub cache_entry: Option<PartitionCacheEntry>,
    pub num_partitions: usize,
    pub size_bytes: usize,
    pub num_rows: usize,
    pub clustering_spec: Option<ClusteringSpecRef>,
    pub source_stage_id: Option<usize>,
}

impl InMemoryInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_schema: SchemaRef,
        cache_key: String,
        cache_entry: Option<PartitionCacheEntry>,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
        clustering_spec: Option<ClusteringSpecRef>,
        source_stage_id: Option<usize>,
    ) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
            num_partitions,
            size_bytes,
            num_rows,
            clustering_spec,
            source_stage_id,
        }
    }
}

impl PartialEq for InMemoryInfo {
    fn eq(&self, other: &Self) -> bool {
        self.cache_key == other.cache_key
    }
}

impl Eq for InMemoryInfo {}

impl Hash for InMemoryInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cache_key.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlaceHolderInfo {
    pub source_schema: SchemaRef,
    pub clustering_spec: ClusteringSpecRef,
}

impl PlaceHolderInfo {
    pub fn new(source_schema: SchemaRef, clustering_spec: ClusteringSpecRef) -> Self {
        Self {
            source_schema,
            clustering_spec,
        }
    }
}
