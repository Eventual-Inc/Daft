use std::sync::Arc;

use common_error::DaftResult;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

pub struct SampleOperator {
    fraction: f64,
    with_replacement: bool,
    seed: Option<u64>,
}

impl SampleOperator {
    pub fn new(fraction: f64, with_replacement: bool, seed: Option<u64>) -> Self {
        Self {
            fraction,
            with_replacement,
            seed,
        }
    }
}

impl IntermediateOperator for SampleOperator {
    #[instrument(skip_all, name = "SampleOperator::execute")]
    fn execute(
        &self,
        _idx: usize,
        input: &PipelineResultType,
        _state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out =
            input
                .as_data()
                .sample_by_fraction(self.fraction, self.with_replacement, self.seed)?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "SampleOperator"
    }
}
