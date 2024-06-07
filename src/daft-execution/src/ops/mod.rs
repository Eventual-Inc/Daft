pub mod filter;
mod fused;
pub mod join;
pub mod limit;
pub mod monotonically_increasing_id;
pub mod project;
pub mod scan;
pub mod shuffle;
pub mod sort;

pub use fused::FusedOpBuilder;

use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

/// Local task operator that takes one or more inputs (either scan tasks or micropartitions) and produces
/// one or more outputs (micropartitions). These are the core compute kernels of the execution model, designed
/// to execute locally over a single partition from each upstream source (single input for unary ops, two inputs for
/// binary ops, n inputs for fan-in ops).
pub trait PartitionTaskOp: std::fmt::Debug + Send + Sync {
    // The type of inputs for execution. Can be either ScanTasks or MicroPartitions.
    type Input;

    /// Execute the underlying op on the provided inputs, producing output micropartitions.
    fn execute(&self, inputs: &[Arc<Self::Input>]) -> DaftResult<Vec<Arc<MicroPartition>>>;

    /// Number of outputs produced by this op. Defaults to 1.
    fn num_outputs(&self) -> usize {
        1
    }

    /// Number of inputs that this op takes. Defaults to 1.
    fn num_inputs(&self) -> usize {
        1
    }

    /// Resource request for a task executing this op; this resource request isn't bound to any input metadata.
    fn resource_request(&self) -> &ResourceRequest;

    /// Resource request for a task executing this op, bound to the metadata for the input that will be provided to
    /// this task.
    fn resource_request_with_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> ResourceRequest {
        self.resource_request()
            .or_memory_bytes(input_meta.iter().map(|m| m.size_bytes).sum())
    }

    /// The partially-specified metadata derived from the input metadata.
    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> PartitionMetadata;

    /// Provides the input metadata for the next task; this allows the task op to internally maintain stateful
    /// configuration for the next task to be submitted (e.g. the partition number for montonically increasing ID).
    fn with_input_metadata(&self, _: &[PartitionMetadata]) {}

    /// Provides the output metadata of the previous task executed for this op; this allows the task op to internally
    /// maintain stateful configuration for the next task to be submitted (e.g. the total number of rows for the row number op).
    fn with_previous_output_metadata(&self, _: &[PartitionMetadata]) {}

    /// A human-readable name for the task op.
    fn name(&self) -> &str;
}
