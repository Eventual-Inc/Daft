use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
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

pub(crate) struct NestedLoopJoinBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) struct NestedLoopJoinProbeState {
    collect_tables: RecordBatch,
    stream_idx: usize,
}

pub struct NestedLoopJoinOperator {
    predicate: Vec<BoundExpr>,
    build_schema: SchemaRef,
    output_schema: SchemaRef,
    /// Which side is the streaming (probe) side. Used to correctly order left/right
    /// when calling `nested_loop_join` so the output schema column order is preserved.
    stream_side: JoinSide,
}

impl NestedLoopJoinOperator {
    pub fn new(
        predicate: Vec<BoundExpr>,
        build_schema: SchemaRef,
        output_schema: SchemaRef,
        stream_side: JoinSide,
    ) -> Self {
        Self {
            predicate,
            build_schema,
            output_schema,
            stream_side,
        }
    }
}

impl JoinOperator for NestedLoopJoinOperator {
    type BuildState = NestedLoopJoinBuildState;
    type FinalizedBuildState = RecordBatch;
    type ProbeState = NestedLoopJoinProbeState;

    fn build(
        &self,
        input: MicroPartition,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        if !input.is_empty() {
            state.tables.extend(input.record_batches().iter().cloned());
        }
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        RecordBatch::concat_or_empty(&state.tables, Some(self.build_schema.clone()))
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(NestedLoopJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        NestedLoopJoinProbeState {
            collect_tables: finalized_build_state,
            stream_idx: 0,
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() || state.collect_tables.is_empty() {
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        if state.stream_idx >= input.record_batches().len() {
            // Finished processing all stream tables, move to next input
            state.stream_idx = 0;
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let output_schema = self.output_schema.clone();
        let predicate = self.predicate.clone();
        let stream_side = self.stream_side;

        spawner
            .spawn(
                async move {
                    let stream_tables = input.record_batches();
                    let stream_tbl = &stream_tables[state.stream_idx];
                    let collect_tbl = &state.collect_tables;

                    let (left_tbl, right_tbl) = match stream_side {
                        JoinSide::Left => (stream_tbl, collect_tbl),
                        JoinSide::Right => (collect_tbl, stream_tbl),
                    };

                    let output_tbl = left_tbl.nested_loop_join(right_tbl, &predicate)?;

                    let output_morsel =
                        MicroPartition::new_loaded(output_schema, Arc::new(vec![output_tbl]), None);

                    state.stream_idx += 1;

                    let result = if state.stream_idx >= stream_tables.len() {
                        ProbeOutput::NeedMoreInput(Some(output_morsel))
                    } else {
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
        "Nested Loop Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::NestedLoopJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec!["Nested Loop Join".to_string()];
        display.push(format!("Stream Side = {:?}", self.stream_side));
        for pred in &self.predicate {
            display.push(format!("Predicate = {}", pred.inner()));
        }
        display
    }

    fn needs_probe_finalization(&self) -> bool {
        false
    }
}
