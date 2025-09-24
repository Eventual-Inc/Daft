use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, ops::NodeType, pipeline::NodeName};

pub(crate) enum SortMergeJoinState {
    Building {
        left_parts: Vec<Arc<MicroPartition>>,
        right_parts: Vec<Arc<MicroPartition>>,
    },
    Done,
}

impl SortMergeJoinState {
    fn push_left(&mut self, part: Arc<MicroPartition>) {
        if let Self::Building { left_parts, .. } = self {
            left_parts.push(part);
        } else {
            panic!("SortMergeJoinSink should be in Building state");
        }
    }

    fn push_right(&mut self, part: Arc<MicroPartition>) {
        if let Self::Building { right_parts, .. } = self {
            right_parts.push(part);
        } else {
            panic!("SortMergeJoinSink should be in Building state");
        }
    }

    fn finalize(&mut self) -> (Vec<Arc<MicroPartition>>, Vec<Arc<MicroPartition>>) {
        let (left_parts, right_parts) = if let Self::Building { left_parts, right_parts } = self {
            (std::mem::take(left_parts), std::mem::take(right_parts))
        } else {
            panic!("SortMergeJoinSink should be in Building state");
        };
        *self = Self::Done;
        (left_parts, right_parts)
    }
}

struct SortMergeJoinParams {
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    is_sorted: bool,
    output_schema: SchemaRef,
}

pub struct SortMergeJoinSink {
    params: Arc<SortMergeJoinParams>,
}

impl SortMergeJoinSink {
    pub fn new(
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        is_sorted: bool,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            params: Arc::new(SortMergeJoinParams {
                left_on,
                right_on,
                is_sorted,
                output_schema,
            }),
        }
    }
}

impl BlockingSink for SortMergeJoinSink {
    type State = SortMergeJoinState;

    #[instrument(skip_all, name = "SortMergeJoinSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        // For now, we'll collect all inputs in the left side
        // In a proper implementation, we would need to distinguish between left and right inputs
        // This would require a more sophisticated approach, possibly using input metadata or multiple sinks
        state.push_left(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "SortMergeJoinSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    // Collect all left and right parts
                    let mut all_left_parts = Vec::new();
                    let mut all_right_parts = Vec::new();

                    for mut state in states {
                        let (left_parts, right_parts) = state.finalize();
                        all_left_parts.extend(left_parts);
                        all_right_parts.extend(right_parts);
                    }

                    // Check if either side is empty before moving
                    if all_left_parts.is_empty() || all_right_parts.is_empty() {
                        // Return empty result if either side is empty
                        let empty_result = MicroPartition::empty(Some(params.output_schema.clone()));
                        return Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(empty_result)]));
                    }

                    // Concatenate left and right sides
                    let left_concat = MicroPartition::concat(all_left_parts.into_iter())?;
                    let right_concat = MicroPartition::concat(all_right_parts.into_iter())?;

                    // Perform sort-merge join
                    let result = left_concat.sort_merge_join(
                        &right_concat,
                        &params.left_on,
                        &params.right_on,
                        params.is_sorted,
                    )?;

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(result)]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "SortMergeJoin".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::SortMergeJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push("SortMergeJoin:".to_string());
        lines.push(format!(
            "Left on: [{}]",
            self.params.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        lines.push(format!(
            "Right on: [{}]",
            self.params.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        lines.push(format!("Is sorted: {}", self.params.is_sorted));
        lines
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(SortMergeJoinState::Building {
            left_parts: Vec::new(),
            right_parts: Vec::new(),
        })
    }
}
