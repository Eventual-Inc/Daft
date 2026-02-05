use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::OrderingAwareJoinSet;
use daft_micropartition::MicroPartition;

use crate::{
    ExecutionTaskSpawner, pipeline_execution::UnifiedTaskResult, pipeline_message::InputId,
};

/// Trait for node-specific execution behaviors.
///
/// This trait abstracts the differences between different pipeline node types
/// (intermediate ops, build, probe, blocking sink, streaming sink) while allowing
/// PipelineNodeExecutor to provide common execution logic.
///
/// Handlers only need to implement task spawning logic - result handling is
/// unified in PipelineNodeExecutor.
pub(crate) trait NodeExecutionHandler<State>: Send + Sync {
    /// Spawn a single task for the given partition and state.
    ///
    /// This is the node-specific task spawning logic that each handler must implement.
    /// The method should spawn a task on `task_set` to process the partition with the state.
    /// The spawned task should return a `UnifiedTaskResult<State>`.
    fn spawn_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<State>>>,
        task_spawner: ExecutionTaskSpawner,
        partition: Arc<MicroPartition>,
        state: State,
        input_id: InputId,
    );

    /// Spawn a finalization task for the given input_id with the collected states.
    ///
    /// This is called when all states are available and ready for finalization
    /// (all states returned, no buffered partitions, and input completed).
    ///
    /// Handlers that need finalization (sinks, probe) should implement this to spawn
    /// async finalization tasks. Handlers that don't finalize (intermediate ops) can
    /// use the default empty implementation.
    ///
    /// The spawned task should return a `UnifiedTaskResult<State>` with a Finalize variant.
    ///
    /// Default implementation does nothing (for handlers without finalization).
    fn spawn_finalize_task(
        &self,
        _task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<State>>>,
        _finalize_spawner: Option<ExecutionTaskSpawner>,
        _input_id: InputId,
        _states: Vec<State>,
    ) {
        // Default: no finalization needed
    }
}
