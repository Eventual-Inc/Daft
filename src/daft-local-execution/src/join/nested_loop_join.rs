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
    collect_tables: Vec<RecordBatch>,
}

pub struct NestedLoopJoinOperator {
    predicate: Vec<BoundExpr>,
    output_schema: SchemaRef,
    /// Which side is the streaming (probe) side. Used to correctly order left/right
    /// when calling `nested_loop_join` so the output schema column order is preserved.
    stream_side: JoinSide,
}

impl NestedLoopJoinOperator {
    pub fn new(predicate: Vec<BoundExpr>, output_schema: SchemaRef, stream_side: JoinSide) -> Self {
        Self {
            predicate,
            output_schema,
            stream_side,
        }
    }
}

impl JoinOperator for NestedLoopJoinOperator {
    type BuildState = NestedLoopJoinBuildState;
    type FinalizedBuildState = Vec<RecordBatch>;
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
        Ok(state.tables).into()
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
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() || state.collect_tables.is_empty() {
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let output_schema = self.output_schema.clone();
        let predicate = self.predicate.clone();
        let stream_side = self.stream_side;

        spawner
            .spawn(
                async move {
                    let mut results = Vec::new();
                    for stream_tbl in input.record_batches() {
                        for collect_tbl in &state.collect_tables {
                            // Ensure left/right order matches the logical schema so the output
                            // column order is correct regardless of which side is streaming.
                            let (left_tbl, right_tbl) = match stream_side {
                                JoinSide::Left => (stream_tbl, collect_tbl),
                                JoinSide::Right => (collect_tbl, stream_tbl),
                            };
                            let output_tbl = left_tbl.nested_loop_join(right_tbl, &predicate)?;
                            if !output_tbl.is_empty() {
                                results.push(output_tbl);
                            }
                        }
                    }
                    let output_morsel =
                        MicroPartition::new_loaded(output_schema, Arc::new(results), None);
                    Ok((state, ProbeOutput::NeedMoreInput(Some(output_morsel))))
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
