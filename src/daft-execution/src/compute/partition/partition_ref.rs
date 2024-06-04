use std::sync::Arc;

use daft_micropartition::MicroPartition;

pub trait PartitionRef: std::fmt::Debug + Clone + Send + 'static {
    fn metadata(&self) -> PartitionMetadata;
    fn partition(&self) -> Arc<MicroPartition>;
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub num_rows: Option<usize>,
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
