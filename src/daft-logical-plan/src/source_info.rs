use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_io_config::IOConfig;
use common_partitioning::PartitionCacheEntry;
use common_scan_info::{PhysicalScanInfo, Pushdowns};
use daft_core::prelude::Schema;
use daft_schema::{dtype::DataType, field::Field, schema::SchemaRef};
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
    pub glob_paths: Arc<Vec<String>>,
    pub schema: SchemaRef,
    pub pushdowns: Pushdowns,
    pub io_config: Option<Box<IOConfig>>,
}

impl GlobScanInfo {
    #[must_use]
    pub fn new(glob_paths: Vec<String>, io_config: Option<IOConfig>) -> Self {
        let schema = Schema::new([
            Field::new("path", DataType::Utf8),
            Field::new("size", DataType::Int64),
            Field::new("num_rows", DataType::Int64),
        ])
        .into();
        Self {
            glob_paths: Arc::new(glob_paths),
            schema,
            pushdowns: Pushdowns::default(),
            io_config: io_config.map(Box::new),
        }
    }

    #[must_use]
    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        Self {
            glob_paths: self.glob_paths.clone(),
            schema: self.schema.clone(),
            pushdowns,
            io_config: self.io_config.clone(),
        }
    }
}
