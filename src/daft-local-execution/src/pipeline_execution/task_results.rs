use std::{sync::Arc, time::Duration};

use daft_micropartition::MicroPartition;

use crate::pipeline_message::InputId;

/// Unified operator execution output enum.
///
/// This consolidates the output types across all operators (IntermediateOp, StreamingSink, Probe).
/// It represents the result of executing an operator on a single partition.
#[derive(Debug, Clone)]
pub(crate) enum OperatorExecutionOutput {
    /// Operator needs more input to continue processing.
    /// May optionally return output produced from the input.
    NeedMoreInput(Option<Arc<MicroPartition>>),

    /// Operator has more output to produce using the same input.
    /// The input partition is returned along with the current output.
    /// The operator will be called again with the same input to produce more output.
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },

    /// Operator is finished processing (used by StreamingSink).
    /// May optionally return final output.
    Finished(Option<Arc<MicroPartition>>),
}

impl OperatorExecutionOutput {
    /// Get the output partition if present.
    pub(crate) fn result(&self) -> &Option<Arc<MicroPartition>> {
        match self {
            OperatorExecutionOutput::NeedMoreInput(mp) => mp,
            OperatorExecutionOutput::HasMoreOutput { output, .. } => output,
            OperatorExecutionOutput::Finished(mp) => mp,
        }
    }
}

/// Unified operator finalize output enum.
///
/// This represents the result of finalizing an operator (used by operators with finalization).
#[derive(Debug)]
pub(crate) enum OperatorFinalizeOutput<State> {
    /// More finalization work is needed.
    /// Returns states to continue finalization and optional output.
    HasMoreOutput {
        states: Vec<State>,
        output: Option<Arc<MicroPartition>>,
    },

    /// Finalization is complete.
    /// May optionally return final output.
    Finished(Option<Arc<MicroPartition>>),
}

/// Unified task result for all pipeline node handlers.
///
/// This replaces the per-handler TaskResult types with a single unified type
/// that can represent both execution and finalization results.
#[derive(Debug)]
pub(crate) enum UnifiedTaskResult<State> {
    /// Result from an execution task.
    Execution {
        input_id: InputId,
        state: State,
        elapsed: Duration,
        output: OperatorExecutionOutput,
    },
    /// Result from a finalization task.
    Finalize {
        input_id: InputId,
        output: UnifiedFinalizeOutput<State>,
    },
}

/// Unified finalize output for all pipeline node handlers.
///
/// This consolidates the various finalize output types into a single enum
/// that can represent all finalization scenarios.
#[derive(Debug)]
pub(crate) enum UnifiedFinalizeOutput<State> {
    /// Finalization complete with output partitions (BlockingSink, Probe).
    Done(Vec<Arc<MicroPartition>>),
    /// Need to continue finalization (StreamingSink only).
    /// Returns states and optional output to spawn another finalize task.
    Continue {
        states: Vec<State>,
        output: Option<Arc<MicroPartition>>,
    },
    /// Finalization complete with no output (Build).
    NoOutput,
}
