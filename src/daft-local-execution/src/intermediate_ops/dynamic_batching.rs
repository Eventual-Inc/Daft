use std::{
    sync::{Arc, Mutex},
};

use daft_micropartition::MicroPartition;

use crate::{
    dynamic_batching::BatchingStrategy,
    intermediate_ops::intermediate_op::{
        IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
    },
};

pub struct DynamicBatchingState<S1> {
    inner_state: S1,
    row_offset: usize,
    current_input: Option<Arc<MicroPartition>>,
}

pub struct DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: BatchingStrategy,
{
    inner_op: Arc<Op>,
    dynamic_batcher: Arc<Mutex<DB>>,
}

impl<Op, DB> DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: BatchingStrategy,
{
    pub fn new(inner_op: Arc<Op>, dynamic_batcher: Arc<Mutex<DB>>) -> Self {
        Self {
            inner_op,
            dynamic_batcher,
        }
    }
}

impl<Op, DB> IntermediateOperator for DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator + 'static,
    DB: BatchingStrategy + 'static,
{
    type State = DynamicBatchingState<Op::State>;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &crate::ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        todo!()
    }

    fn name(&self) -> crate::pipeline::NodeName {
        self.inner_op.name()
    }

    fn op_type(&self) -> common_metrics::ops::NodeType {
        self.inner_op.op_type()
    }

    fn multiline_display(&self) -> Vec<String> {
        self.inner_op.multiline_display()
    }

    fn make_state(&self) -> common_error::DaftResult<Self::State> {
        let inner_state = self.inner_op.make_state()?;
        Ok(DynamicBatchingState {
            inner_state,
            row_offset: 0,
            current_input: None,
        })
    }

    fn max_concurrency(&self) -> common_error::DaftResult<usize> {
        self.inner_op.max_concurrency()
    }

    fn morsel_size_requirement(&self) -> Option<crate::pipeline::MorselSizeRequirement> {
        self.inner_op.morsel_size_requirement()
    }

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn crate::runtime_stats::RuntimeStats> {
        self.inner_op.make_runtime_stats(id)
    }
}
