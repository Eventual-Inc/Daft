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

pub(crate) struct CrossJoinBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) struct CrossJoinProbeState {
    pub(crate) collect_tables: Vec<RecordBatch>,
    pub(crate) stream_idx: usize,
    pub(crate) collect_idx: usize,
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
        let output_schema = self.output_schema.clone();
        let stream_side = self.stream_side;

        spawner
            .spawn(
                async move {
                    if input.is_empty() || state.collect_tables.is_empty() {
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((state, ProbeOutput::NeedMoreInput(Some(empty))));
                    }
                    if state.stream_idx >= input.record_batches().len() {
                        state.stream_idx = 0;
                        state.collect_idx = 0;
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((state, ProbeOutput::NeedMoreInput(Some(empty))));
                    }

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

                    state.collect_idx = (state.collect_idx + 1) % state.collect_tables.len();

                    if state.collect_idx == 0 {
                        state.stream_idx = (state.stream_idx + 1) % stream_tables.len();
                    }

                    let result = if state.stream_idx == 0 && state.collect_idx == 0 {
                        ProbeOutput::NeedMoreInput(Some(output_morsel))
                    } else {
                        ProbeOutput::HasMoreOutput {
                            input,
                            output: Some(output_morsel),
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
}
