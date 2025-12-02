use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use tracing::Span;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
};

pub struct IntoBatchesOperator {
    batch_size: usize,
    strict: bool,
}

impl IntoBatchesOperator {
    const BATCH_SIZE_LOWER_BOUND_THRESHOLD: f64 = 0.8;

    pub fn new(batch_size: usize, strict: bool) -> Self {
        Self { batch_size, strict }
    }
}

impl IntermediateOperator for IntoBatchesOperator {
    type State = ();
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        task_spawner
            .spawn(
                async move {
                    let out = match input.concat_or_get()? {
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
    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        match self.strict {
            true => Some(MorselSizeRequirement::Strict(self.batch_size)),
            false => {
                let lower_bound =
                    (self.batch_size as f64 * Self::BATCH_SIZE_LOWER_BOUND_THRESHOLD) as usize;
                Some(MorselSizeRequirement::Flexible(
                    lower_bound,
                    self.batch_size,
                ))
            }
        }
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
