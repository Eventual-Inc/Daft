use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tokio::sync::broadcast;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeResult,
    },
    pipeline::NodeName,
    pipeline_execution::OperatorExecutionOutput,
};

pub(crate) struct CrossJoinBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) enum CrossJoinProbeState {
    Uninitialized(broadcast::Receiver<Vec<RecordBatch>>),
    Initialized {
        collect_tables: Vec<RecordBatch>,
        stream_idx: usize,
        collect_idx: usize,
    },
}

impl CrossJoinProbeState {
    /// Initialize the state by awaiting the receiver if uninitialized.
    /// Returns the initialized state.
    pub(crate) async fn initialize(self) -> common_error::DaftResult<Self> {
        match self {
            CrossJoinProbeState::Uninitialized(mut receiver) => {
                let finalized = receiver.recv().await.map_err(|e| {
                    common_error::DaftError::ValueError(format!(
                        "Failed to receive finalized build state: {}",
                        e
                    ))
                })?;

                Ok(CrossJoinProbeState::Initialized {
                    collect_tables: finalized,
                    stream_idx: 0,
                    collect_idx: 0,
                })
            }
            CrossJoinProbeState::Initialized { .. } => Ok(self),
        }
    }

    /// Extract all fields from an Initialized state.
    /// Panics if the state is Uninitialized.
    pub(crate) fn into_initialized(self) -> (Vec<RecordBatch>, usize, usize) {
        match self {
            CrossJoinProbeState::Initialized {
                collect_tables,
                stream_idx,
                collect_idx,
            } => (collect_tables, stream_idx, collect_idx),
            CrossJoinProbeState::Uninitialized(_) => {
                panic!("State must be initialized before extracting fields")
            }
        }
    }

    /// Get a reference to collect_tables if initialized.
    pub(crate) fn collect_tables(&self) -> Option<&Vec<RecordBatch>> {
        match self {
            CrossJoinProbeState::Initialized { collect_tables, .. } => Some(collect_tables),
            CrossJoinProbeState::Uninitialized(_) => None,
        }
    }

    /// Get stream_idx if initialized.
    pub(crate) fn stream_idx(&self) -> Option<usize> {
        match self {
            CrossJoinProbeState::Initialized { stream_idx, .. } => Some(*stream_idx),
            CrossJoinProbeState::Uninitialized(_) => None,
        }
    }
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
        receiver: broadcast::Receiver<Self::FinalizedBuildState>,
    ) -> Self::ProbeState {
        CrossJoinProbeState::Uninitialized(receiver)
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        let output_schema = self.output_schema.clone();
        let stream_side = self.stream_side;

        // Move state into async closure where we can await initialization
        spawner
            .spawn(
                async move {
                    // Initialize state if needed
                    let state = state.initialize().await?;

                    // Extract fields from Initialized state for early returns
                    let collect_tables_empty = state
                        .collect_tables()
                        .map(|tables| tables.is_empty())
                        .unwrap_or(true);

                    // If there are no input tables or collect tables, return an empty output
                    if input.is_empty() || collect_tables_empty {
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((state, OperatorExecutionOutput::NeedMoreInput(Some(empty))));
                    }

                    // Check if we've finished processing all stream tables
                    let stream_idx = state.stream_idx().unwrap_or(0);
                    if stream_idx >= input.record_batches().len() {
                        // Finished processing all stream tables, move to next input
                        let (collect_tables, _, _) = state.into_initialized();
                        let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
                        return Ok((
                            CrossJoinProbeState::Initialized {
                                collect_tables,
                                stream_idx: 0,
                                collect_idx: 0,
                            },
                            OperatorExecutionOutput::NeedMoreInput(Some(empty)),
                        ));
                    }

                    let (collect_tables, mut stream_idx, mut collect_idx) =
                        state.into_initialized();

                    let stream_tables = input.record_batches();
                    let stream_tbl = &stream_tables[stream_idx];
                    let collect_tbl = &collect_tables[collect_idx];

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
                    collect_idx = (collect_idx + 1) % collect_tables.len();

                    if collect_idx == 0 {
                        // Finished the inner loop, increment outer loop index
                        stream_idx = (stream_idx + 1) % stream_tables.len();
                    }

                    let result = if stream_idx == 0 && collect_idx == 0 {
                        // Finished the outer loop, move onto next input
                        OperatorExecutionOutput::NeedMoreInput(Some(output_morsel))
                    } else {
                        // Still looping through tables
                        OperatorExecutionOutput::HasMoreOutput {
                            input,
                            output: Some(output_morsel),
                        }
                    };
                    Ok((
                        CrossJoinProbeState::Initialized {
                            collect_tables,
                            stream_idx,
                            collect_idx,
                        },
                        result,
                    ))
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
