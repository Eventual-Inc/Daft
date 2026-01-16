use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub struct CrossJoinBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) struct CrossJoinProbeState {
    collect_tables: Vec<RecordBatch>,
    stream_idx: usize,
    collect_idx: usize,
}

pub struct CrossJoinOperator {
    output_schema: SchemaRef,
    stream_side: JoinSide,
}

impl CrossJoinOperator {
    pub fn new(output_schema: SchemaRef, stream_side: JoinSide) -> Self {
        Self {
            output_schema,
            stream_side,
        }
    }
}

impl JoinOperator for CrossJoinOperator {
    type BuildState = CrossJoinBuildState;
    type FinalizedBuildState = Vec<RecordBatch>;
    type ProbeState = CrossJoinProbeState;

    fn build(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        if !input.is_empty() {
            state.tables.extend(input.record_batches().iter().cloned());
        }
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(state.tables).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(CrossJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        CrossJoinProbeState {
            collect_tables: finalized_build_state,
            stream_idx: 0,
            collect_idx: 0,
        }
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        // If there are no input tables or collect tables, return an empty output
        if input.is_empty() || state.collect_tables.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }
        // If we've finished processing all stream tables, move to next input
        if state.stream_idx >= input.record_batches().len() {
            // Finished processing all stream tables, move to next input
            state.stream_idx = 0;
            state.collect_idx = 0;
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let output_schema = self.output_schema.clone();
        let stream_side = self.stream_side;

        spawner
            .spawn(
                async move {
                    let stream_tables = input.record_batches();
                    let stream_tbl = &stream_tables[state.stream_idx];
                    let collect_tbl = &state.collect_tables[state.collect_idx];

                    let (left_tbl, right_tbl) = match stream_side {
                        JoinSide::Left => (stream_tbl, collect_tbl),
                        JoinSide::Right => (collect_tbl, stream_tbl),
                    };

                    let output_tbl = left_tbl.cross_join(right_tbl, stream_side)?;

                    let output_morsel = Arc::new(MicroPartition::new_loaded(
                        output_schema,
                        Arc::new(vec![output_tbl]),
                        None,
                    ));

                    // Increment inner loop index
                    state.collect_idx = (state.collect_idx + 1) % state.collect_tables.len();

                    if state.collect_idx == 0 {
                        // Finished the inner loop, increment outer loop index
                        state.stream_idx = (state.stream_idx + 1) % stream_tables.len();
                    }

                    let result = if state.stream_idx == 0 && state.collect_idx == 0 {
                        // Finished the outer loop, move onto next input
                        ProbeOutput::NeedMoreInput(Some(output_morsel))
                    } else {
                        // Still looping through tables
                        ProbeOutput::HasMoreOutput {
                            input,
                            output: output_morsel,
                        }
                    };
                    Ok((state, result))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        _states: Vec<Self::ProbeState>,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        Ok(None).into()
    }

    fn name(&self) -> NodeName {
        "Cross Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::CrossJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "Cross Join".to_string(),
            format!("Stream Side = {:?}", self.stream_side),
        ]
    }

    fn needs_probe_finalization(&self) -> bool {
        false
    }
}
