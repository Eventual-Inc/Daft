use std::hash::{Hash, Hasher};

use common_partitioning::PartitionCacheEntry;
use common_scan_info::{PhysicalScanInfo, Pushdowns};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::partitioning::ClusteringSpecRef;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceInfo {
    InMemory(InMemoryInfo),
    Physical(PhysicalScanInfo),
    GlobScan(GlobScanInfo),
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobScanInfo {
    pub glob_paths: Vec<String>,
    pub source_schema: SchemaRef,
    pub pushdowns: Pushdowns,
    pub io_config: Option<common_io_config::IOConfig>,
}

impl GlobScanInfo {
    #[must_use]
    pub fn new(
        glob_paths: Vec<String>,
        source_schema: SchemaRef,
        pushdowns: Pushdowns,
        io_config: Option<common_io_config::IOConfig>,
    ) -> Self {
        Self {
            glob_paths,
            source_schema,
            pushdowns,
            io_config,
        }
    }

    #[must_use]
    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        Self {
            glob_paths: self.glob_paths.clone(),
            source_schema: self.source_schema.clone(),
            pushdowns,
            io_config: self.io_config.clone(),
        }
    }
}
