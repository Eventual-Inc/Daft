use std::sync::Arc;

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use tracing::Span;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ops::NodeType, pipeline::NodeName, ExecutionRuntimeContext, ExecutionTaskSpawner};

pub struct IntoBatchesOperator {
    batch_size: usize,
}

impl IntoBatchesOperator {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl IntermediateOperator for IntoBatchesOperator {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        task_spawner
            .spawn(
                async move {
                    let out = match input.concat_or_get(IOStatsContext::new("into_batches"))? {
                        Some(record_batch) => Arc::new(MicroPartition::new_loaded(
                            input.schema(),
                            Arc::new(vec![record_batch]),
                            None,
                        )),
                        None => Arc::new(MicroPartition::empty(Some(input.schema()))),
                    };
                    Ok((state, IntermediateOperatorResult::NeedMoreInput(Some(out))))
                },
                Span::current(),
            )
            .into()
    }
    fn name(&self) -> NodeName {
        "IntoBatches".into()
    }
    fn op_type(&self) -> NodeType {
        NodeType::IntoBatches
    }
    fn multiline_display(&self) -> Vec<String> {
        vec![format!("IntoBatches: {}", self.batch_size)]
    }
    fn morsel_size_range(&self, _runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        (self.batch_size, self.batch_size)
    }
}
