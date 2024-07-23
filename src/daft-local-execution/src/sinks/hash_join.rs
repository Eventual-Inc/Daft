use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use tracing::instrument;

use super::{
    sink::{DoubleInputSink, SinkResultType},
    state::SinkTaskState,
};

#[derive(Clone)]
pub struct HashJoinSink {
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
}

impl HashJoinSink {
    pub fn new(left_on: Vec<ExprRef>, right_on: Vec<ExprRef>, join_type: JoinType) -> Self {
        Self {
            left_on,
            right_on,
            join_type,
        }
    }
}

impl DoubleInputSink for HashJoinSink {
    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_left(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        state.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        state.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "HashJoin::finalize")]
    fn finalize(
        &self,
        input_left: &Arc<MicroPartition>,
        input_right: &Arc<MicroPartition>,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let joined =
            input_left.hash_join(input_right, &self.left_on, &self.right_on, self.join_type)?;
        Ok(vec![Arc::new(joined)])
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }
}
