use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::ops::PartitionTaskOp;

use crate::partition::{
    partition_ref::{PartitionMetadata, PartitionRef},
    virtual_partition::VirtualPartition,
};

// TODO(Clark): Scope this to per stage/execution?
static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Executable task, with variants for ScanTask and PartitionRef inputs.
#[derive(Debug)]
pub enum Task<T: PartitionRef> {
    ScanTask(PartitionTask<Arc<ScanTask>>),
    PartitionTask(PartitionTask<T>),
}

impl<T: PartitionRef> Task<T> {
    pub fn execute(self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        match self {
            Self::ScanTask(pt) => pt.execute(),
            Self::PartitionTask(pt) => pt.execute(),
        }
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        match self {
            Self::ScanTask(pt) => pt.resource_request(),
            Self::PartitionTask(pt) => pt.resource_request(),
        }
    }

    pub fn task_id(&self) -> usize {
        match self {
            Self::ScanTask(pt) => pt.task_id(),
            Self::PartitionTask(pt) => pt.task_id(),
        }
    }

    pub fn task_op_name(&self) -> &str {
        match self {
            Self::ScanTask(pt) => pt.task_op.name(),
            Self::PartitionTask(pt) => pt.task_op.name(),
        }
    }
}

/// Executable partition task.
#[derive(Debug)]
pub struct PartitionTask<V: VirtualPartition> {
    // Inputs for task, either ScanTasks or PartitionRefs.
    inputs: Vec<V>,
    // Task op that can execute on either ScanTasks or MicroPartitions as inputs.
    task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
    // Resource request for task.
    resource_request: ResourceRequest,
    // partial_metadata: PartitionMetadata,
    // ID for task.
    task_id: usize,
}

impl<V: VirtualPartition> PartitionTask<V> {
    pub fn new(
        inputs: Vec<V>,
        task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
        resource_request: ResourceRequest,
    ) -> Self {
        let task_id = TASK_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let input_metadata = inputs.iter().map(|vp| vp.metadata()).collect::<Vec<_>>();
        task_op.with_input_metadata(&input_metadata);
        // let partial_metadata = task_op.partial_metadata_from_input_metadata(&input_metadata);
        Self {
            inputs,
            task_op,
            resource_request,
            // partial_metadata,
            task_id,
        }
    }

    /// Repack components into a PartitionTask.
    pub fn repack(
        inputs: Vec<V>,
        task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
        resource_request: ResourceRequest,
        task_id: usize,
    ) -> Self {
        Self {
            inputs,
            task_op,
            resource_request,
            task_id,
        }
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    pub fn partial_metadata(&self) -> &PartitionMetadata {
        todo!()
        // &self.partial_metadata
    }

    pub fn task_id(&self) -> usize {
        self.task_id
    }

    /// Execute task.
    pub fn execute(self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|input| input.partition())
            .collect::<Vec<_>>();
        self.task_op.execute(&inputs)
    }

    /// Unpack components for serialization.
    pub fn unpack(
        self,
    ) -> (
        Vec<V>,
        Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
        ResourceRequest,
        usize,
    ) {
        (
            self.inputs,
            self.task_op,
            self.resource_request,
            self.task_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use common_error::DaftResult;
    use daft_core::{
        datatypes::{Field, UInt64Array},
        schema::Schema,
        DataType, Series,
    };
    use daft_micropartition::MicroPartition;
    use daft_plan::ResourceRequest;
    use daft_table::Table;

    use crate::{
        ops::PartitionTaskOp,
        partition::{partition_ref::PartitionMetadata, VirtualPartition},
    };

    use super::PartitionTask;

    #[derive(Debug)]
    struct MockResourceOp {
        resource_request: ResourceRequest,
        name: String,
        num_with_input_metadatas: AtomicUsize,
    }

    impl MockResourceOp {
        fn new(name: impl Into<String>, resource_request: ResourceRequest) -> Self {
            Self {
                resource_request,
                name: name.into(),
                num_with_input_metadatas: AtomicUsize::new(0),
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

        fn with_input_metadata(&self, _: &[PartitionMetadata]) {
            self.num_with_input_metadatas.fetch_add(1, Ordering::SeqCst);
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    #[derive(Debug, Clone)]
    struct MockVirtualPartition {
        meta: PartitionMetadata,
        data: Arc<MicroPartition>,
    }

    impl MockVirtualPartition {
        fn new(meta: PartitionMetadata, data: MicroPartition) -> Self {
            Self {
                meta,
                data: data.into(),
            }
        }

        fn from_meta(input_meta: PartitionMetadata) -> Self {
            let field = Field::new("a", DataType::Int64);
            let schema = Arc::new(Schema::new(vec![field.clone()]).unwrap());
            let data = MicroPartition::new_loaded(
                schema.clone(),
                Arc::new(vec![Table::new(
                    schema.clone(),
                    vec![Series::from_arrow(
                        field.into(),
                        arrow2::array::Int64Array::from_vec(
                            (0..input_meta.num_rows.unwrap())
                                .map(|n| n as i64)
                                .collect(),
                        )
                        .boxed(),
                    )
                    .unwrap()],
                )
                .unwrap()]),
                None,
            );
            Self::new(input_meta, data)
        }
    }

    impl VirtualPartition for MockVirtualPartition {
        type TaskOpInput = MicroPartition;

        fn metadata(&self) -> PartitionMetadata {
            self.meta.clone()
        }

        fn partition(&self) -> Arc<Self::TaskOpInput> {
            self.data.clone()
        }
    }

    /// Tests PartitionTask construction and execution.
    #[test]
    fn partition_task() -> DaftResult<()> {
        // Setup.
        let num_rows = 10;
        let input_meta = PartitionMetadata::new(Some(num_rows), Some(8 * num_rows));
        let resource_request = ResourceRequest::new_internal(Some(1.0), None, None);
        let op = Arc::new(MockResourceOp::new("op", resource_request));
        let bound_resource_request = op.resource_request_with_input_metadata(&[input_meta.clone()]);
        let input = vec![MockVirtualPartition::from_meta(input_meta)];
        let pt = PartitionTask::new(input.clone(), op.clone(), bound_resource_request.clone());

        // // Task ID should start at 0.
        // assert_eq!(pt.task_id(), 0);
        // Basic pass-through to provided resource request (bound to input metadata).
        assert_eq!(pt.resource_request(), &bound_resource_request);
        // .with_input_metadata() should be called on construction.
        assert_eq!(op.num_with_input_metadatas.load(Ordering::SeqCst), 1);
        // Execution should delegate to execution of partition task op, providing bound inputs.
        let output = pt.execute().unwrap();
        assert_eq!(output.len(), 1);
        assert!(Arc::<MicroPartition>::ptr_eq(
            &output[0],
            &input[0].partition()
        ));

        // Create another partition task.
        let pt2 = PartitionTask::new(input.clone(), op.clone(), bound_resource_request);
        // // Task ID should have been incremented.
        // assert_eq!(pt2.task_id(), 1);
        // .with_input_metadata() should be called (again) on construction.
        assert_eq!(op.num_with_input_metadatas.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
