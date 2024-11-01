use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorResultType,
};

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
        input: &Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime_ref: &RuntimeRef,
    ) -> IntermediateOperatorResult {
        let input = input.clone();
        let fraction = self.fraction;
        let with_replacement = self.with_replacement;
        let seed = self.seed;
        runtime_ref
            .spawn(async move {
                let out = input.sample_by_fraction(fraction, with_replacement, seed)?;
                Ok((
                    state,
                    IntermediateOperatorResultType::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "SampleOperator"
    }
}
