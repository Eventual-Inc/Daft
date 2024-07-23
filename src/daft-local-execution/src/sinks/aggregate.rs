use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    sink::{SingleInputSink, SinkResultType},
    state::SinkTaskState,
};

#[derive(Clone)]
pub struct AggregateSink {
    sink_agg_exprs: Vec<ExprRef>,
    sink_group_by: Vec<ExprRef>,
    final_agg_exprs: Vec<ExprRef>,
    final_group_by: Vec<ExprRef>,
    final_project_exprs: Vec<ExprRef>,
}

impl AggregateSink {
    pub fn new(
        sink_agg_exprs: Vec<ExprRef>,
        sink_group_by: Vec<ExprRef>,
        final_agg_exprs: Vec<ExprRef>,
        final_group_by: Vec<ExprRef>,
        final_project_exprs: Vec<ExprRef>,
    ) -> Self {
        Self {
            sink_agg_exprs,
            sink_group_by,
            final_agg_exprs,
            final_group_by,
            final_project_exprs,
        }
    }
}

impl SingleInputSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        let agged = input.agg(&self.sink_agg_exprs, &self.sink_group_by)?;
        state.push(Arc::new(agged));
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    fn can_parallelize(&self) -> bool {
        true
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(&self, input: &Arc<MicroPartition>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let agged = input.agg(&self.final_agg_exprs, &self.final_group_by)?;
        let projected = agged.eval_expression_list(&self.final_project_exprs)?;
        Ok(vec![Arc::new(projected)])
    }
}
