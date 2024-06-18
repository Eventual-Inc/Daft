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
        assert!(self.can_add_op(op.as_ref()));
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

    fn execute(&self, inputs: &[Arc<T>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        // Execute task ops in a chain.
        let mut inputs = self.source_op.execute(inputs)?;
        for op in self.fused_ops.iter() {
            inputs = op.execute(&inputs)?;
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

#[cfg(test)]
mod tests {
    use std::{
        assert_matches::assert_matches,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use common_error::DaftResult;
    use daft_micropartition::MicroPartition;
    use daft_plan::ResourceRequest;

    use crate::{
        ops::PartitionTaskOp, partition::partition_ref::PartitionMetadata, test::MockInputOutputOp,
    };

    use super::FusedOpBuilder;

    #[derive(Debug)]
    struct MockExecOp {
        exec_log: Arc<Mutex<Vec<String>>>,
        resource_request: ResourceRequest,
        name: String,
    }

    impl MockExecOp {
        fn new(name: impl Into<String>, exec_log: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                exec_log,
                resource_request: Default::default(),
                name: name.into(),
            }
        }
    }

    impl PartitionTaskOp for MockExecOp {
        type Input = MicroPartition;

        fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
            self.exec_log.lock().unwrap().push(self.name.clone());
            Ok(inputs.to_vec())
        }

        fn resource_request(&self) -> &ResourceRequest {
            &self.resource_request
        }

        fn partial_metadata_from_input_metadata(
            &self,
            input_meta: &[crate::partition::partition_ref::PartitionMetadata],
        ) -> crate::partition::partition_ref::PartitionMetadata {
            todo!()
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Tests execution order of fused ops.
    #[test]
    fn exec_order() -> DaftResult<()> {
        let exec_log = Arc::new(Mutex::new(vec![]));
        let op1 = Arc::new(MockExecOp::new("op1", exec_log.clone()));
        let op2 = Arc::new(MockExecOp::new("op2", exec_log.clone()));
        let op3 = Arc::new(MockExecOp::new("op3", exec_log.clone()));

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // Upon execution, fused ops should be called in the order in which they were added to the builder.
        let inputs = vec![];
        fused.execute(&inputs);
        assert_eq!(exec_log.lock().unwrap().clone(), vec!["op1", "op2", "op3"]);
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    #[derive(Debug)]
    struct MockResourceOp {
        resource_request: ResourceRequest,
        name: String,
    }

    impl MockResourceOp {
        fn new(
            name: impl Into<String>,
            num_cpus: Option<f64>,
            num_gpus: Option<f64>,
            memory_bytes: Option<usize>,
        ) -> Self {
            Self {
                resource_request: ResourceRequest::new_internal(num_cpus, num_gpus, memory_bytes),
                name: name.into(),
            }
        }
    }

    impl PartitionTaskOp for MockResourceOp {
        type Input = MicroPartition;

        fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
            Ok(inputs.to_vec())
        }

        fn resource_request(&self) -> &ResourceRequest {
            &self.resource_request
        }

        fn partial_metadata_from_input_metadata(
            &self,
            input_meta: &[PartitionMetadata],
        ) -> PartitionMetadata {
            todo!()
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Tests that tasks requesting CPU compute are fusable.
    #[test]
    fn both_cpu_compute_fusable() -> DaftResult<()> {
        let cpu_op1 = Arc::new(MockResourceOp::new("op1", Some(1.0), None, None));
        let cpu_op2 = Arc::new(MockResourceOp::new("op2", Some(2.0), None, None));

        // Smaller CPU request first, larger second.
        let mut builder = FusedOpBuilder::new(cpu_op1.clone());
        assert!(builder.can_add_op(cpu_op2.as_ref()));
        builder.add_op(cpu_op2.clone());
        let fused = builder.build();
        // CPU resource request should be max.
        assert_matches!(fused.resource_request().num_cpus, Some(2.0));
        assert_eq!(fused.name(), "op1-op2");

        // Larger CPU request first, smaller second.
        let mut builder = FusedOpBuilder::new(cpu_op2.clone());
        assert!(builder.can_add_op(cpu_op1.as_ref()));
        builder.add_op(cpu_op1.clone());
        let fused = builder.build();
        // CPU resource request should be max.
        assert_matches!(fused.resource_request().num_cpus, Some(2.0));
        assert_eq!(fused.name(), "op2-op1");

        Ok(())
    }

    /// Tests that tasks requesting GPU compute are fusable.
    #[test]
    fn both_gpu_compute_fusable() -> DaftResult<()> {
        let gpu_op1 = Arc::new(MockResourceOp::new("op1", None, Some(1.0), None));
        let gpu_op2 = Arc::new(MockResourceOp::new("op2", None, Some(2.0), None));

        // Smaller GPU request first, larger second.
        let mut builder = FusedOpBuilder::new(gpu_op1.clone());
        assert!(builder.can_add_op(gpu_op2.as_ref()));
        builder.add_op(gpu_op2.clone());
        let fused = builder.build();
        // GPU resource request should be max.
        assert_matches!(fused.resource_request().num_gpus, Some(2.0));
        assert_eq!(fused.name(), "op1-op2");

        // Larger GPU request first, smaller second.
        let mut builder = FusedOpBuilder::new(gpu_op2.clone());
        assert!(builder.can_add_op(gpu_op1.as_ref()));
        builder.add_op(gpu_op1.clone());
        let fused = builder.build();
        // GPU resource request should be max.
        assert_matches!(fused.resource_request().num_gpus, Some(2.0));
        assert_eq!(fused.name(), "op2-op1");

        Ok(())
    }

    /// Tests resource request merging, where a max should be taken across each of the resource dimensions.
    #[test]
    fn resource_request_merging() -> DaftResult<()> {
        let op1 = Arc::new(MockResourceOp::new("op1", Some(1.0), None, Some(1024)));
        let op2 = Arc::new(MockResourceOp::new("op2", Some(3.0), None, Some(512)));
        let op3 = Arc::new(MockResourceOp::new("op3", Some(2.0), None, None));

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // Resource requests should contain max across all task ops.
        assert_eq!(
            fused.resource_request(),
            &ResourceRequest::new_internal(Some(3.0), None, Some(1024))
        );
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    /// Tests that input metadata supplies memory bytes request.
    #[test]
    fn input_metadata_memory_bytes() -> DaftResult<()> {
        let op1 = Arc::new(MockResourceOp::new("op1", None, None, None));
        let op2 = Arc::new(MockResourceOp::new("op2", None, None, None));
        let input_metadata = vec![PartitionMetadata::new(None, Some(1024))];

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        let fused = builder.build();
        // Memory bytes request should be equal to size given in input metadata.
        assert_matches!(
            fused
                .resource_request_with_input_metadata(&input_metadata)
                .memory_bytes,
            Some(1024),
        );
        assert_eq!(fused.name(), "op1-op2");

        Ok(())
    }

    /// Tests that an explicit memory bytes request overrides input metadata.
    #[test]
    fn explicit_memory_bytes_overrides_input_metadata() -> DaftResult<()> {
        let op1 = Arc::new(MockResourceOp::new("op1", None, None, None));
        let op2 = Arc::new(MockResourceOp::new("op2", None, None, Some(2048)));
        let op3 = Arc::new(MockResourceOp::new("op3", None, None, None));
        let input_metadata = vec![PartitionMetadata::new(None, Some(1024))];

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // Explicit memory bytes request should override size given in input metadata.
        assert_matches!(
            fused
                .resource_request_with_input_metadata(&input_metadata)
                .memory_bytes,
            Some(2048),
        );
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    /// Tests that CPU and GPU task ops aren't fused.
    #[test]
    fn cpu_and_gpu_task_not_fused() -> DaftResult<()> {
        // CPU -> GPU.
        let cpu_op = Arc::new(MockResourceOp::new("cpu_op", Some(1.0), None, None));
        let gpu_op = Arc::new(MockResourceOp::new("gpu_op", None, Some(1.0), None));
        let builder = FusedOpBuilder::new(cpu_op.clone());
        assert!(!builder.can_add_op(gpu_op.as_ref()));

        // GPU -> CPU.
        let builder = FusedOpBuilder::new(gpu_op);
        assert!(!builder.can_add_op(cpu_op.as_ref()));
        Ok(())
    }

    /// Tests that CPU and GPU task ops aren't fused, even if separated by several non-conflicting tasks.
    #[test]
    fn cpu_and_gpu_task_not_fused_long_chain() -> DaftResult<()> {
        let cpu_op = Arc::new(MockResourceOp::new("cpu_op", Some(1.0), None, None));
        let light_op = Arc::new(MockResourceOp::new("light_op", None, None, None));
        let gpu_op = Arc::new(MockResourceOp::new("gpu_op", None, Some(1.0), None));

        // CPU op as source, lightweight op as intermediate, GPU op as attempted sink op.
        let mut builder = FusedOpBuilder::new(cpu_op.clone());
        assert!(builder.can_add_op(light_op.as_ref()));
        builder.add_op(light_op.clone());
        assert!(!builder.can_add_op(gpu_op.as_ref()));
        assert_eq!(builder.build().name(), "cpu_op-light_op");

        // Lightweight op as source, CPU as intermediate, GPU op as attempted sink op.
        let mut builder = FusedOpBuilder::new(light_op.clone());
        assert!(builder.can_add_op(cpu_op.as_ref()));
        builder.add_op(cpu_op.clone());
        assert!(!builder.can_add_op(gpu_op.as_ref()));
        assert_eq!(builder.build().name(), "light_op-cpu_op");

        // GPU op as source, lightweight op as intermediate, CPU op as attempted sink op.
        let mut builder = FusedOpBuilder::new(gpu_op.clone());
        assert!(builder.can_add_op(light_op.as_ref()));
        builder.add_op(light_op.clone());
        assert!(!builder.can_add_op(cpu_op.as_ref()));
        assert_eq!(builder.build().name(), "gpu_op-light_op");

        // Lightweight op as source, GPU as intermediate, CPU op as attempted sink op.
        let mut builder = FusedOpBuilder::new(light_op.clone());
        assert!(builder.can_add_op(gpu_op.as_ref()));
        builder.add_op(gpu_op);
        assert!(!builder.can_add_op(cpu_op.as_ref()));
        assert_eq!(builder.build().name(), "light_op-gpu_op");

        Ok(())
    }

    /// Tests that the number of inputs is tied to first task op in fused chain.
    #[test]
    fn num_inputs_first_op() -> DaftResult<()> {
        let op1 = Arc::new(MockInputOutputOp::new("op1", 2, 1));
        let op2 = Arc::new(MockInputOutputOp::new("op2", 1, 1));
        let op3 = Arc::new(MockInputOutputOp::new("op3", 3, 1));

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // Number of inputs of fused op should be equal to number of inputs for first op in chain.
        assert_eq!(fused.num_inputs(), op1.num_inputs());
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    /// Tests that the number of outputs is tied to last task op in fused chain.
    #[test]
    fn num_outputs_last_op() -> DaftResult<()> {
        let op1 = Arc::new(MockInputOutputOp::new("op1", 1, 3));
        let op2 = Arc::new(MockInputOutputOp::new("op2", 1, 1));
        let op3 = Arc::new(MockInputOutputOp::new("op3", 1, 2));

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // Number of outputs of fused op should be equal to number of inputs for last op in chain.
        assert_eq!(fused.num_inputs(), op3.num_inputs());
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    #[derive(Debug)]
    struct MockMutableOp {
        num_with_input_metadatas: AtomicUsize,
        num_with_output_metadatas: AtomicUsize,
        resource_request: ResourceRequest,
        name: String,
    }

    impl MockMutableOp {
        fn new(name: impl Into<String>) -> Self {
            Self {
                num_with_input_metadatas: AtomicUsize::new(0),
                num_with_output_metadatas: AtomicUsize::new(0),
                resource_request: Default::default(),
                name: name.into(),
            }
        }
    }

    impl PartitionTaskOp for MockMutableOp {
        type Input = MicroPartition;

        fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
            Ok(inputs.to_vec())
        }

        fn resource_request(&self) -> &ResourceRequest {
            &self.resource_request
        }

        fn partial_metadata_from_input_metadata(
            &self,
            input_meta: &[crate::partition::partition_ref::PartitionMetadata],
        ) -> crate::partition::partition_ref::PartitionMetadata {
            todo!()
        }

        fn with_input_metadata(&self, _: &[PartitionMetadata]) {
            self.num_with_input_metadatas.fetch_add(1, Ordering::SeqCst);
        }

        fn with_previous_output_metadata(&self, _: &[PartitionMetadata]) {
            self.num_with_output_metadatas
                .fetch_add(1, Ordering::SeqCst);
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Tests that the with_input_metadata() is propagated to first task op in fused chain.
    #[test]
    fn with_input_metadata_first_op() -> DaftResult<()> {
        let op1 = Arc::new(MockMutableOp::new("op1"));
        let op2 = Arc::new(MockMutableOp::new("op2"));
        let op3 = Arc::new(MockMutableOp::new("op3"));
        let input_metadata = vec![PartitionMetadata::new(None, Some(1024))];

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // with_input_metadata() should propagate to ONLY the first task op in the fused chain.
        fused.with_input_metadata(&input_metadata);
        assert_eq!(op1.num_with_input_metadatas.load(Ordering::SeqCst), 1);
        assert_eq!(op2.num_with_input_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op3.num_with_input_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op1.num_with_output_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op2.num_with_output_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op3.num_with_output_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }

    /// Tests that the with_previous_output_metadata() is propagated to last task op in fused chain.
    #[test]
    fn with_previous_output_metadata_last_op() -> DaftResult<()> {
        let op1 = Arc::new(MockMutableOp::new("op1"));
        let op2 = Arc::new(MockMutableOp::new("op2"));
        let op3 = Arc::new(MockMutableOp::new("op3"));
        let input_metadata = vec![PartitionMetadata::new(None, Some(1024))];

        let mut builder = FusedOpBuilder::new(op1.clone());
        assert!(builder.can_add_op(op2.as_ref()));
        builder.add_op(op2.clone());
        assert!(builder.can_add_op(op3.as_ref()));
        builder.add_op(op3.clone());
        let fused = builder.build();
        // with_previous_output_metadata() should propagate to ONLY the last task op in the fused chain.
        fused.with_previous_output_metadata(&input_metadata);
        assert_eq!(op1.num_with_output_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op2.num_with_output_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op3.num_with_output_metadatas.load(Ordering::SeqCst), 1);
        assert_eq!(op1.num_with_input_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op2.num_with_input_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(op3.num_with_input_metadatas.load(Ordering::SeqCst), 0);
        assert_eq!(fused.name(), "op1-op2-op3");

        Ok(())
    }
}
