use std::sync::Arc;

use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::runtime_stats::ExecutionTaskSpawner;

struct SampleParams {
    fraction: f64,
    with_replacement: bool,
    seed: Option<u64>,
}

pub struct SampleOperator {
    params: Arc<SampleParams>,
}

impl SampleOperator {
    pub fn new(fraction: f64, with_replacement: bool, seed: Option<u64>) -> Self {
        Self {
            params: Arc::new(SampleParams {
                fraction,
                with_replacement,
                seed,
            }),
        }
    }
}

impl IntermediateOperator for SampleOperator {
    #[instrument(skip_all, name = "SampleOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let params = self.params.clone();
        spawner
            .spawn(async move {
                let out = input.sample_by_fraction(
                    params.fraction,
                    params.with_replacement,
                    params.seed,
                )?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "SampleOperator"
    }
}
