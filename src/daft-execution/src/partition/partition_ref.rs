use std::sync::Arc;

use daft_micropartition::MicroPartition;

/// A reference to a partition of the table.
///
/// If using the local executor, this partition lives in local memory.
/// If using the Ray executor, this partition lives in the memory of some worker in the cluster.
pub trait PartitionRef: std::fmt::Debug + Clone + Send + 'static {
    /// Get the metadata for this partition.
    fn metadata(&self) -> PartitionMetadata;

    /// Materialize the partition that underlies the reference.
    fn partition(&self) -> Arc<MicroPartition>;
}

/// Metadata for a partition.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    // Number of rows in partition.
    pub num_rows: Option<usize>,
    // Size of partition in bytes.
    pub size_bytes: Option<usize>,
    // pub part_col_stats: PartitionColumnStats,
    // pub execution_stats: ExecutionStats,
}

impl PartitionMetadata {
    pub fn new(
        num_rows: Option<usize>,
        size_bytes: Option<usize>,
        // part_col_stats: PartitionColumnStats,
        // execution_stats: ExecutionStats,
    ) -> Self {
        Self {
            num_rows,
            size_bytes,
            // part_col_stats,
            // execution_stats,
        }
    }

    pub fn with_num_rows(&self, num_rows: Option<usize>) -> Self {
        Self {
            num_rows,
            size_bytes: self.size_bytes,
        }
    }

    pub fn with_size_bytes(&self, size_bytes: Option<usize>) -> Self {
        Self {
            num_rows: self.num_rows,
            size_bytes,
        }
    }
}

#[derive(Debug)]
pub struct PartitionColumnStats {}

#[derive(Debug)]
pub struct ExecutionStats {
    wall_time_s: f64,
    cpu_time_s: f64,
    max_rss_bytes: usize,
    node_id: String,
    partition_id: String,
}
