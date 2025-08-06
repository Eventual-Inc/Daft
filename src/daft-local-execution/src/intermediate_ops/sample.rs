use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{pipeline::NodeName, ExecutionTaskSpawner};

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
    type State = ();

    #[instrument(skip_all, name = "SampleOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let params = self.params.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.sample_by_fraction(
                        params.fraction,
                        params.with_replacement,
                        params.seed,
                    )?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Sample: {}", self.params.fraction));
        res.push(format!(
            "With replacement = {}",
            self.params.with_replacement
        ));
        res.push(format!("Seed = {:?}", self.params.seed));
        res
    }

    fn name(&self) -> NodeName {
        "Sample".into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }
}
