use std::{sync::Arc, time::Instant};

use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::Span;

use crate::{
    dynamic_batching::DynamicBatchingAlgorithm,
    intermediate_ops::intermediate_op::{
        IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
    },
};

pub struct DynamicBatchingState<S1, S2> {
    inner_state: S1,
    row_offset: usize,
    current_input: Option<Arc<RecordBatch>>,
    batch_state: S2,
}

pub struct DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: DynamicBatchingAlgorithm,
{
    inner_op: Arc<Op>,
    dynamic_batcher: Arc<DB>,
}

impl<Op, DB> DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: DynamicBatchingAlgorithm,
{
    pub fn new(inner_op: Arc<Op>, dynamic_batcher: Arc<DB>) -> Self {
        Self {
            inner_op,
            dynamic_batcher,
        }
    }
}

impl<Op, DB> IntermediateOperator for DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator + 'static,
    DB: DynamicBatchingAlgorithm + 'static,
{
    type State = DynamicBatchingState<Op::State, DB::State>;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &crate::ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let inner_op = self.inner_op.clone();
        let dynamic_batcher = self.dynamic_batcher.clone();
        let task_spawner_clone = task_spawner.clone();

        let fut = async move {
            if state.current_input.is_none() {
                let input_tables = input.get_tables()?;
                if let Some(table) = input_tables.first() {
                    state.current_input = Some(Arc::new(table.clone()));
                    state.row_offset = 0;
                } else {
                    return Ok((state, IntermediateOperatorResult::NeedMoreInput(None)));
                }
            }
            if let Some(ref current_table) = state.current_input {
                let total_rows = current_table.num_rows();

                // If we've processed all rows, we need more input
                if state.row_offset >= total_rows {
                    state.current_input = None;
                    state.row_offset = 0;
                    return Ok((state, IntermediateOperatorResult::NeedMoreInput(None)));
                }

                // Process a dynamic batch size
                let batch_size = {
                    dynamic_batcher
                        .current_batch_size(&state.batch_state)
                        .min(total_rows - state.row_offset)
                };

                let sub_batch =
                    current_table.slice(state.row_offset, state.row_offset + batch_size)?;

                let sub_partition = Arc::new(MicroPartition::new_loaded(
                    input.schema(),
                    Arc::new(vec![sub_batch]),
                    None,
                ));

                let start_time = Instant::now();
                let (inner_state, op_result) = inner_op
                    .execute(sub_partition, state.inner_state, &task_spawner_clone)
                    .await??;
                let duration = start_time.elapsed();
                state.inner_state = inner_state;
                state.row_offset += batch_size;

                // Update dynamic batching state
                dynamic_batcher.adjust_batch_size(
                    &mut state.batch_state,
                    task_spawner_clone.runtime_stats.as_ref(),
                    duration,
                );

                Ok((state, op_result))
            } else {
                Ok((state, IntermediateOperatorResult::NeedMoreInput(None)))
            }
        };

        task_spawner.spawn(fut, Span::current()).into()
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
        let batch_state = self.dynamic_batcher.make_state();
        Ok(DynamicBatchingState {
            inner_state,
            row_offset: 0,
            current_input: None,
            batch_state,
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
