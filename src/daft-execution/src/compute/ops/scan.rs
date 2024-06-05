use std::sync::Arc;

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

#[derive(Debug)]
pub struct ScanOp {
    resource_request: ResourceRequest,
}

impl ScanOp {
    pub fn new() -> Self {
        Self {
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for ScanOp {
    type Input = ScanTask;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let scan_task = inputs.into_iter().next().unwrap();
        let io_stats = IOStatsContext::new(format!(
            "MicroPartition::from_scan_task for {:?}",
            scan_task.sources
        ));
        let out = MicroPartition::from_scan_task(scan_task, io_stats)?;
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
