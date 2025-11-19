use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::Span;

use crate::{
    dynamic_batching::DynamicBatching,
    intermediate_ops::intermediate_op::{
        IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
    },
};

pub struct DynamicBatchingState<S> {
    inner_state: S,
    row_offset: usize,
    current_input: Option<Arc<RecordBatch>>,
}

pub struct DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: DynamicBatching,
{
    inner_op: Arc<Op>,
    dynamic_batcher: Arc<DB>,
    batch_state: Arc<Mutex<DB::State>>,
}

impl<Op, DB> DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator,
    DB: DynamicBatching,
{
    pub fn new(inner_op: Arc<Op>, dynamic_batcher: Arc<DB>, initial_state: DB::State) -> Self {
        Self {
            inner_op,
            dynamic_batcher,
            batch_state: Arc::new(Mutex::new(initial_state)),
        }
    }
}

impl<Op, DB> IntermediateOperator for DynamicallyBatchedIntermediateOperator<Op, DB>
where
    Op: IntermediateOperator + 'static,
    DB: DynamicBatching + 'static,
{
    type State = DynamicBatchingState<Op::State>;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &crate::ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let inner_op = self.inner_op.clone();
        let batch_state = self.batch_state.clone();
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
                    let batch_state = batch_state.lock().unwrap();
                    dynamic_batcher
                        .current_batch_size(&batch_state)
                        .min(total_rows - state.row_offset)
                };

                let sub_batch =
                    current_table.slice(state.row_offset, state.row_offset + batch_size)?;

                let sub_partition = Arc::new(MicroPartition::new_loaded(
                    input.schema().clone(),
                    Arc::new(vec![sub_batch]),
                    None,
                ));

                let start_time = Instant::now();
                let result = inner_op
                    .execute(sub_partition, state.inner_state, &task_spawner_clone)
                    .await??;
                let duration = start_time.elapsed();
                state.inner_state = result.0;
                state.row_offset += batch_size;
                // Update dynamic batching state
                if let Ok(mut batch_state) = batch_state.lock() {
                    dynamic_batcher.adjust_batch_size(&mut batch_state, duration);
                }

                match result.1 {
                    IntermediateOperatorResult::HasMoreOutput(output) => {
                        return Ok((state, IntermediateOperatorResult::HasMoreOutput(output)));
                    }
                    IntermediateOperatorResult::NeedMoreInput(Some(output)) => {
                        return Ok((state, IntermediateOperatorResult::HasMoreOutput(output)));
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        return Ok((state, IntermediateOperatorResult::NeedMoreInput(None)));
                    }
                }
            }

            Ok((state, IntermediateOperatorResult::NeedMoreInput(None)))
        };

        let fut = task_spawner.spawn(fut, Span::current());
        fut.into()
    }

    fn name(&self) -> crate::pipeline::NodeName {
        format!("DynamicBatching[{}]", self.inner_op.name()).into()
    }

    fn op_type(&self) -> common_metrics::ops::NodeType {
        self.inner_op.op_type()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![format!("DynamicBatching[{}]", self.inner_op.name())];
        display.extend(self.inner_op.multiline_display());
        display
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
