use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use itertools::Itertools;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

#[derive(Debug)]
pub struct FusedOpBuilder<T> {
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
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
        Self {
            source_op,
            fused_ops: vec![],
            resource_request: Default::default(),
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

#[derive(Debug)]
pub struct FusedPartitionTaskOp<T> {
    source_op: Arc<dyn PartitionTaskOp<Input = T>>,
    fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    resource_request: ResourceRequest,
    name: String,
}

impl<T> FusedPartitionTaskOp<T> {
    pub fn new(
        source_op: Arc<dyn PartitionTaskOp<Input = T>>,
        fused_ops: Vec<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        resource_request: ResourceRequest,
    ) -> Self {
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
        self.resource_request
            .or_memory_bytes(input_meta.iter().map(|m| m.size_bytes).sum())
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }
    fn with_input_metadata(&self, input_meta: &[PartitionMetadata]) {
        self.source_op.with_input_metadata(input_meta);
    }
    fn with_previous_output_metadata(&self, output_meta: &[PartitionMetadata]) {
        self.source_op.with_previous_output_metadata(output_meta);
    }

    fn name(&self) -> &str {
        &self.name
    }
}
