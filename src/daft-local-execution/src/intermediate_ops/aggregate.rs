use std::sync::Arc;

use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::runtime_stats::ExecutionTaskSpawner;

struct AggParams {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

pub struct AggregateOperator {
    params: Arc<AggParams>,
}

impl AggregateOperator {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            params: Arc::new(AggParams {
                agg_exprs,
                group_by,
            }),
        }
    }
}

impl IntermediateOperator for AggregateOperator {
    #[instrument(skip_all, name = "AggregateOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let params = self.params.clone();
        spawner
            .spawn(async move {
                let out = input.agg(&params.agg_exprs, &params.group_by)?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
