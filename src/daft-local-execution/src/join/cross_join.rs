use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{Span, info_span};

use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::StaticBatchingStrategy,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeOutput, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub struct CrossJoinBuildState {
    tables: Vec<RecordBatch>,
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
    type BatchingStrategy = StaticBatchingStrategy;

    fn build_state(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        spawner
            .spawn(
                async move {
                    if !input.is_empty() {
                        state.tables.extend(input.record_batches().iter().cloned());
                    }
                    Ok(state)
                },
                info_span!("CrossJoinOperator::build_state"),
            )
            .into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(Arc::new(state.tables)).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(CrossJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Arc<Self::FinalizedBuildState>,
    ) -> DaftResult<Self::ProbeState> {
        Ok(CrossJoinProbeState {
            collect_tables: finalized_build_state,
            stream_idx: 0,
            collect_idx: 0,
        })
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let output_schema = self.output_schema.clone();
        let stream_side = self.stream_side;

        spawner
            .spawn(
                async move {
                    let collect_tables = &state.collect_tables;
                    if collect_tables.is_empty() {
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((state, ProbeOutput::NeedMoreInput(Some(empty))));
                    }

                    let stream_tables = input.record_batches();
                    if state.stream_idx >= stream_tables.len() {
                        // Finished processing all stream tables, move to next input
                        state.stream_idx = 0;
                        state.collect_idx = 0;
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((state, ProbeOutput::NeedMoreInput(Some(empty))));
                    }

                    let stream_tbl = &stream_tables[state.stream_idx];
                    let collect_tbl = &collect_tables[state.collect_idx];

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
                    state.collect_idx = (state.collect_idx + 1) % collect_tables.len();

                    if state.collect_idx == 0 {
                        // Finished the inner loop, increment outer loop index
                        state.stream_idx = (state.stream_idx + 1) % stream_tables.len();
                    }

                    let result = if state.stream_idx == 0 && state.collect_idx == 0 {
                        // Finished the outer loop, move onto next input
                        ProbeOutput::NeedMoreInput(Some(output_morsel))
                    } else {
                        // Still looping through tables
                        ProbeOutput::HasMoreOutput(output_morsel)
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
    ) -> crate::join::join_operator::ProbeFinalizeResult<Self> {
        Ok(ProbeFinalizeOutput::Finished(None)).into()
    }

    fn name(&self) -> NodeName {
        "CrossJoin".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::CrossJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "CrossJoin:".to_string(),
            format!("Stream Side = {:?}", self.stream_side),
        ]
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }

    fn needs_probe_finalization(&self) -> bool {
        false
    }
}

pub(crate) struct CrossJoinProbeState {
    collect_tables: Arc<Vec<RecordBatch>>,
    stream_idx: usize,
    collect_idx: usize,
}
