use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use itertools::Itertools;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Builder for fused task ops. Task ops are only fused if they have compatible resource requests, meaning that their
/// resource requests or homogeneous enough that we don't want to pipeline execution of each task op with the other.
#[derive(Debug)]
pub struct FusedOpBuilder<T> {
    // Task op at the front of the chain.
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    // All task ops that have been fused into chain after source op.
    // The order of this vec indicates the order of chain invocation.
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    // Aggregate resource request for fused task op.
    resource_request: ResourceRequest,
}

impl<T> Clone for FusedOpBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            source_op: self.source_op.clone(),
            fused_ops: self.fused_ops.clone(),
            resource_request: self.resource_request.clone(),
        }
    }
}

impl<T: std::fmt::Debug + 'static> FusedOpBuilder<T> {
    pub fn new(source_op: Arc<dyn PartitionTaskOp<Input = T>>) -> Self {
        let resource_request = source_op.resource_request().clone();
        Self {
            source_op,
            fused_ops: vec![],
            resource_request,
        }
    }

    pub fn add_op(&mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) {
        self.resource_request = self.resource_request.max(op.resource_request());
        self.fused_ops.push(op);
    }

    pub fn can_add_op(&self, op: &dyn PartitionTaskOp<Input = MicroPartition>) -> bool {
        self.resource_request
            .is_pipeline_compatible_with(op.resource_request())
    }

    pub fn build(self) -> Arc<dyn PartitionTaskOp<Input = T>> {
        if self.fused_ops.is_empty() {
            self.source_op
        } else {
            Arc::new(FusedPartitionTaskOp::<T>::new(
                self.source_op,
                self.fused_ops,
                self.resource_request,
            ))
        }
    }
}

/// Finalized fused task op, containing two or more underlying task ops that will be invoked in a chain.
#[derive(Debug)]
pub struct FusedPartitionTaskOp<T> {
    // Task op at the front of the chain.
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    // All task ops that have been fused into chain after source op.
    // The order of this vec indicates the order of chain invocation.
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    // Aggregate resource request for fused task op; this is incrementally built via the above builder.
    resource_request: ResourceRequest,
    // A human-readable name for the fused task op, currently a concatenation of the fused task ops with a hyphen
    // separator, e.g. ScanOp-ProjectOp-FilterOp.
    name: String,
}

impl<T> FusedPartitionTaskOp<T> {
    pub fn new(
        source_op: Arc<dyn PartitionTaskOp<Input = T>>,
        fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        resource_request: ResourceRequest,
    ) -> Self {
        // Concatenate all fused task ops with a hyphen separator.
        let name = std::iter::once(source_op.name())
            .chain(fused_ops.iter().map(|op| op.name()))
            .join("-");
        Self {
            source_op,
            fused_ops,
            resource_request,
            name,
        }
    }
}

impl<T: std::fmt::Debug> PartitionTaskOp for FusedPartitionTaskOp<T> {
    type Input = T;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        // Execute task ops in a chain.
        let mut inputs = self.source_op.execute(inputs)?;
        for op in self.fused_ops.iter() {
            inputs = op.execute(inputs)?;
        }
        Ok(inputs)
    }

    fn num_inputs(&self) -> usize {
        self.source_op.num_inputs()
    }

    fn num_outputs(&self) -> usize {
        // Number of outputs is dictated by the last task op in the fused chain.
        self.fused_ops
            .last()
            .map_or_else(|| self.source_op.num_outputs(), |op| op.num_outputs())
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn resource_request_with_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> ResourceRequest {
        // TODO(Clark): This should eventually take the max of the heap memory estimate for all fused ops in the chain,
        // using max output size estimates from previous ops in the chain. This would require looping in
        // the approximate stats estimate logic that's currently tied to the physical plan, which we previously talked
        // about factoring out.
        self.resource_request
            .or_memory_bytes(input_meta.iter().map(|m| m.size_bytes).sum())
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }
    fn with_input_metadata(&self, input_meta: &[PartitionMetadata]) {
        // TODO(Clark): This should be applied to every task op in the chain, using partial metadata propagation for the
        // intermediate task ops. For now, we can insure that any task op requiring stateful configuration maintenance
        // based on input metadata isn't fused with upstream task ops.
        self.source_op.with_input_metadata(input_meta);
    }
    fn with_previous_output_metadata(&self, output_meta: &[PartitionMetadata]) {
        // TODO(Clark): This can't be applied to every task op in the chain, since we would need to keep track of
        // output metadata for intermediate ops in the chain. For now, we limit stateful configuration maintenance to
        // task ops at the end of a fused chain, and should prevent fusion for such task ops otherwise.
        self.fused_ops.last().map_or_else(
            || self.source_op.with_previous_output_metadata(output_meta),
            |op| op.with_previous_output_metadata(output_meta),
        );
    }

    fn name(&self) -> &str {
        &self.name
    }
}
