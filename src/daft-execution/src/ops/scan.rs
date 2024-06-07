use std::sync::Arc;

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Scan task op, executing provided scan tasks in order to produce eagerly or lazily materialized MicroPartitions.
#[derive(Debug)]
pub struct ScanOp {
    resource_request: ResourceRequest,
}

impl ScanOp {
    pub fn new() -> Self {
        Self {
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for ScanOp {
    type Input = ScanTask;

    fn execute(&self, inputs: &[Arc<ScanTask>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let scan_task = inputs.iter().next().unwrap();
        let io_stats = IOStatsContext::new(format!(
            "MicroPartition::from_scan_task for {:?}",
            scan_task.sources
        ));
        let out = MicroPartition::from_scan_task(scan_task.clone(), io_stats)?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "ScanOp"
    }
}
