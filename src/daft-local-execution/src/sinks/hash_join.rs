use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use tracing::instrument;

use super::sink::{DoubleInputSink, SinkResultType};

#[derive(Clone)]
pub struct HashJoinSink {
    result_left: Vec<Arc<MicroPartition>>,
    result_right: Vec<Arc<MicroPartition>>,
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
}

impl HashJoinSink {
    pub fn new(left_on: Vec<ExprRef>, right_on: Vec<ExprRef>, join_type: JoinType) -> Self {
        Self {
            result_left: Vec::new(),
            result_right: Vec::new(),
            left_on,
            right_on,
            join_type,
        }
    }
}

impl DoubleInputSink for HashJoinSink {
    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.result_left.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.result_right.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "HashJoin::finalize")]
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let concated_left = MicroPartition::concat(
            &self
                .result_left
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>(),
        )?;
        let concated_right = MicroPartition::concat(
            &self
                .result_right
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>(),
        )?;
        let joined = concated_left.hash_join(
            &concated_right,
            &self.left_on,
            &self.right_on,
            self.join_type,
        )?;
        Ok(vec![Arc::new(joined)])
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }
}
