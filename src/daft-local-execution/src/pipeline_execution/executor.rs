use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::OrderingAwareJoinSet;
use daft_micropartition::MicroPartition;

use crate::{
    ControlFlow, ExecutionTaskSpawner,
    channel::{Receiver, Sender},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, UnifiedFinalizeOutput, UnifiedTaskResult,
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

/// Base execution context that stores common fields shared across all pipeline node execution contexts.
///
/// This struct contains fields that are present in all or most execution contexts:
/// - `task_spawner`: Used to spawn execution tasks
/// - `runtime_stats`: Runtime statistics tracking
/// - `max_concurrency`: Maximum number of concurrent tasks
/// - `stats_manager`: Optional stats manager (only some contexts have this)
/// - `finalize_spawner`: Optional spawner for finalize tasks (only some contexts have this)
/// - `output_sender`: Optional sender for pipeline messages (only some contexts have this)
/// - `task_set`: Join set for managing concurrent tasks
/// - `input_state_tracker`: Tracker for managing input states and buffered partitions
/// - `batch_manager`: Optional batch manager for dynamic batching (only some contexts have this)
pub(crate) struct PipelineNodeExecutor<State, Strategy: BatchingStrategy> {
    pub(crate) task_spawner: ExecutionTaskSpawner,
    pub(crate) runtime_stats: Arc<dyn RuntimeStats>,
    pub(crate) max_concurrency: usize,
    /// Optional stats manager - only some execution contexts have this
    pub(crate) stats_manager: RuntimeStatsManagerHandle,
    /// Optional finalize spawner - only some execution contexts have this
    pub(crate) finalize_spawner: Option<ExecutionTaskSpawner>,
    /// Optional output sender - only some execution contexts have this
    pub(crate) output_sender: Option<Sender<PipelineMessage>>,
    /// Task set for managing concurrent tasks
    pub(crate) task_set: OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<State>>>,
    /// State tracker for managing input states and buffered partitions
    pub(crate) input_state_tracker: InputStateTracker<State>,
    /// Optional batch manager for dynamic batching
    pub(crate) batch_manager: Option<Arc<BatchManager<Strategy>>>,
}

impl<State: Send + 'static, Strategy: BatchingStrategy + 'static>
    PipelineNodeExecutor<State, Strategy>
{
    /// Create a new PipelineNodeExecutor with the required fields.
    pub(crate) fn new(
        task_spawner: ExecutionTaskSpawner,
        runtime_stats: Arc<dyn RuntimeStats>,
        max_concurrency: usize,
        task_set: OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<State>>>,
        input_state_tracker: InputStateTracker<State>,
        stats_manager: RuntimeStatsManagerHandle,
    ) -> Self {
        Self {
            task_spawner,
            runtime_stats,
            max_concurrency,
            stats_manager,
            finalize_spawner: None,
            output_sender: None,
            task_set,
            input_state_tracker,
            batch_manager: None,
        }
    }
    /// Add a finalize spawner to the base context.
    pub(crate) fn with_finalize_spawner(mut self, spawner: ExecutionTaskSpawner) -> Self {
        self.finalize_spawner = Some(spawner);
        self
    }

    /// Add an output sender to the base context.
    pub(crate) fn with_output_sender(mut self, sender: Sender<PipelineMessage>) -> Self {
        self.output_sender = Some(sender);
        self
    }

    /// Add a batch manager to the base context.
    pub(crate) fn with_batch_manager(mut self, batch_manager: Arc<BatchManager<Strategy>>) -> Self {
        self.batch_manager = Some(batch_manager);
        self
    }

    /// Send a single message to the output channel.
    /// Returns ControlFlow::Stop if send fails (receiver closed).
    #[inline]
    async fn send(&self, msg: PipelineMessage) -> ControlFlow {
        if let Some(sender) = &self.output_sender {
            if sender.send(msg).await.is_err() {
                return ControlFlow::Stop;
            }
        }
        ControlFlow::Continue
    }

    /// Check if processing should continue based on common conditions.
    ///
    /// Returns true if:
    /// - Input is not closed, OR
    /// - There are tasks still running, OR
    /// - There are buffered partitions, OR
    /// - There are unfinalized states
    pub(crate) fn should_continue_processing(&self, input_closed: bool) -> bool {
        !input_closed
            || !self.task_set.is_empty()
            || self.input_state_tracker.has_buffered_partitions()
            || self.input_state_tracker.has_unfinalized_states()
    }

    /// Try to spawn tasks for all input_ids using the provided spawning function.
    ///
    /// This implements the common pattern of iterating through all input_ids and
    /// spawning tasks while capacity is available. The loop continues until no
    /// more tasks can be spawned.
    ///
    /// The `spawn_fn` should attempt to spawn tasks for the given input_id and
    /// return `Ok(true)` if any tasks were spawned, `Ok(false)` otherwise.
    pub(crate) fn try_spawn_tasks_for_all_inputs<F>(&mut self, mut spawn_fn: F) -> DaftResult<()>
    where
        F: FnMut(&mut Self, InputId) -> DaftResult<bool>,
    {
        loop {
            let input_ids = self.input_state_tracker.input_ids();
            let mut spawned_any = false;

            for input_id in input_ids {
                if self.task_set.len() >= self.max_concurrency {
                    return Ok(());
                }
                // Try to spawn tasks for this input_id
                if spawn_fn(self, input_id)? {
                    spawned_any = true;
                }
            }

            // If we didn't spawn any task, no more work can be done
            if !spawned_any {
                break;
            }
        }
        Ok(())
    }

    /// Try to spawn a finalize task for the given input_id.
    ///
    /// This checks if states are ready for finalization and spawns a finalize task if they are.
    ///
    /// Returns true if finalization was triggered, false otherwise.
    pub(crate) fn try_spawn_finalize_task<Handler>(
        &mut self,
        handler: &Handler,
        input_id: InputId,
    ) -> DaftResult<bool>
    where
        Handler: NodeExecutionHandler<State>,
    {
        if let Some(states) = self
            .input_state_tracker
            .try_take_states_for_finalize(input_id)
        {
            handler.spawn_finalize_task(
                &mut self.task_set,
                self.finalize_spawner.clone(),
                input_id,
                states,
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Handle input closure by marking all trackers as completed and attempting
    /// finalization for each input_id.
    ///
    /// This implements the common pattern in `handle_input_closed` implementations:
    /// - Marks all trackers as completed
    /// - Iterates through all input_ids
    /// - Attempts finalization using the handler's `spawn_finalize_task` method
    /// - Sends flush messages directly when finalization occurs
    async fn handle_input_closed<Handler>(&mut self, handler: &Handler) -> DaftResult<ControlFlow>
    where
        Handler: NodeExecutionHandler<State>,
    {
        // Mark all trackers as completed so they can be finalized/cleaned up
        self.input_state_tracker.mark_all_completed();

        // Try to finalize each input_id
        let input_ids = self.input_state_tracker.input_ids();
        for input_id in input_ids {
            let finalized = self.try_spawn_finalize_task(handler, input_id)?;

            // If finalization was triggered, send flush message directly
            if finalized && self.send(PipelineMessage::Flush(input_id)).await == ControlFlow::Stop {
                return Ok(ControlFlow::Stop);
            }
        }

        Ok(ControlFlow::Continue)
    }

    /// Try to spawn tasks for the given input_id using the handler's spawn_task method.
    ///
    /// This implements the common pattern of looping while capacity is available and
    /// spawning tasks for buffered partitions with available states.
    pub(crate) fn try_spawn_tasks_for_input<Handler>(
        &mut self,
        handler: &Handler,
        input_id: InputId,
    ) -> DaftResult<()>
    where
        Handler: super::NodeExecutionHandler<State>,
    {
        while self.task_set.len() < self.max_concurrency {
            let Some(next_partition_and_state) = self
                .input_state_tracker
                .get_next_partition_for_execute(input_id)
            else {
                break;
            };
            let (partition, state) = next_partition_and_state?;
            handler.spawn_task(
                &mut self.task_set,
                self.task_spawner.clone(),
                partition,
                state,
                input_id,
            );
        }
        Ok(())
    }

    /// Handle morsel and attempt to spawn tasks for the input.
    ///
    /// This combines the common morsel handling pattern with task spawning:
    /// - Handles the morsel (activates node, records stats, buffers partition)
    /// - Attempts to spawn tasks for the input if capacity is available
    pub(crate) fn handle_morsel_and_spawn_tasks<Handler>(
        &mut self,
        handler: &Handler,
        input_id: InputId,
        partition: Arc<MicroPartition>,
        node_id: usize,
        node_initialized: &mut bool,
    ) -> DaftResult<()>
    where
        Handler: super::NodeExecutionHandler<State>,
    {
        if !*node_initialized {
            self.stats_manager.activate_node(node_id);
            *node_initialized = true;
        }
        self.runtime_stats.add_rows_in(partition.len() as u64);
        self.input_state_tracker
            .buffer_partition(input_id, partition)?;
        self.try_spawn_tasks_for_input(handler, input_id)?;
        Ok(())
    }

    /// Handle a flush message with consolidated logic.
    ///
    /// This implements the common flush pattern across all handlers:
    /// - If no tracker exists: forward flush immediately (no morsels received for this input_id)
    /// - If tracker exists: mark as completed and attempt finalization or check readiness
    ///
    /// For handlers with finalize_spawner (sinks, build, probe):
    /// - Attempts finalization using handler's spawn_finalize_task
    /// - Flush message comes from finalize task completion
    ///
    /// For handlers without finalize_spawner (IntermediateOp):
    /// - Checks if ready to flush (completed && all states available && no buffered partitions)
    /// - Sends flush message directly if ready
    async fn handle_flush<Handler>(
        &mut self,
        handler: &Handler,
        input_id: InputId,
    ) -> DaftResult<ControlFlow>
    where
        Handler: super::NodeExecutionHandler<State>,
    {
        // If tracker doesn't exist, forward flush immediately (no morsels received for this input_id)
        if !self.input_state_tracker.contains_key(input_id) {
            return Ok(self.send(PipelineMessage::Flush(input_id)).await);
        }

        // Mark as completed
        self.input_state_tracker.mark_completed(input_id);

        // Distinguish behavior based on whether handler has finalize_spawner
        if self.finalize_spawner.is_some() {
            // Handlers with finalize_spawner (sinks, build, probe): try to finalize
            // Flush message will come from finalize task completion
            self.try_spawn_finalize_task(handler, input_id)?;
        } else {
            // Handlers without finalize_spawner (IntermediateOp): check if ready to flush
            // Check if ready: completed && all states available && no buffered partitions
            if self
                .input_state_tracker
                .try_take_states_for_finalize(input_id)
                .is_some()
            {
                // Tracker already removed, send flush message directly
                return Ok(self.send(PipelineMessage::Flush(input_id)).await);
            }
        }

        Ok(ControlFlow::Continue)
    }

    /// Handle a unified task result (either execution or finalization).
    ///
    /// This method consolidates all result handling logic that was previously
    /// scattered across individual handler implementations. Messages are sent
    /// directly to the output channel.
    async fn handle_task_result<Handler>(
        &mut self,
        result: UnifiedTaskResult<State>,
        handler: &Handler,
    ) -> DaftResult<ControlFlow>
    where
        Handler: NodeExecutionHandler<State>,
    {
        use crate::pipeline_execution::OperatorExecutionOutput;

        match result {
            UnifiedTaskResult::Execution {
                input_id,
                state,
                elapsed,
                output,
            } => {
                // Record CPU time
                self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                // Send output message if present and record stats
                if let Some(mp) = output.result() {
                    self.runtime_stats.add_rows_out(mp.len() as u64);
                    if let Some(batch_mgr) = &self.batch_manager {
                        batch_mgr.record_execution_stats(
                            self.runtime_stats.as_ref(),
                            mp.len(),
                            elapsed,
                        );
                    }
                    if self
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: mp.clone(),
                        })
                        .await
                        == ControlFlow::Stop
                    {
                        return Ok(ControlFlow::Stop);
                    }
                }

                // Update batch bounds if batch manager is present
                if let Some(batch_mgr) = &self.batch_manager {
                    let new_requirements = batch_mgr.calculate_batch_size();
                    self.input_state_tracker.update_all_bounds(new_requirements);
                }

                // Handle different output types
                match output {
                    OperatorExecutionOutput::NeedMoreInput(_) => {
                        // Return state to tracker
                        self.input_state_tracker.return_state(input_id, state);

                        // Try to flush if ready (for IntermediateOp without finalization)
                        if self.finalize_spawner.is_none()
                            && self
                                .input_state_tracker
                                .try_take_states_for_finalize(input_id)
                                .is_some()
                        {
                            if self.send(PipelineMessage::Flush(input_id)).await
                                == ControlFlow::Stop
                            {
                                return Ok(ControlFlow::Stop);
                            }
                        }

                        // Try to spawn more tasks
                        self.try_spawn_tasks_for_input(handler, input_id)?;

                        // Try to finalize if ready (for handlers with finalization)
                        if self.finalize_spawner.is_some() {
                            self.try_spawn_finalize_task(handler, input_id)?;
                        }

                        // Try to spawn tasks for all inputs
                        self.try_spawn_tasks_for_all_inputs(|base, input_id| {
                            base.try_spawn_tasks_for_input(handler, input_id)?;
                            Ok(false)
                        })?;

                        Ok(ControlFlow::Continue)
                    }
                    OperatorExecutionOutput::HasMoreOutput { input, .. } => {
                        // Spawn another execution with same input and state
                        handler.spawn_task(
                            &mut self.task_set,
                            self.task_spawner.clone(),
                            input,
                            state,
                            input_id,
                        );

                        // Try to spawn tasks for all inputs (except streaming sink)
                        if self.batch_manager.is_some() {
                            self.try_spawn_tasks_for_all_inputs(|base, input_id| {
                                base.try_spawn_tasks_for_input(handler, input_id)?;
                                Ok(false)
                            })?;
                        }

                        Ok(ControlFlow::Continue)
                    }
                    OperatorExecutionOutput::Finished(_) => {
                        // Return state to tracker
                        self.input_state_tracker.return_state(input_id, state);

                        // Finished means we should flush all active input_ids and exit
                        let active_input_ids: Vec<InputId> = self.input_state_tracker.input_ids();
                        for flush_input_id in active_input_ids {
                            if self.send(PipelineMessage::Flush(flush_input_id)).await
                                == ControlFlow::Stop
                            {
                                return Ok(ControlFlow::Stop);
                            }
                        }
                        Ok(ControlFlow::Stop)
                    }
                }
            }
            UnifiedTaskResult::Finalize { input_id, output } => {
                match output {
                    UnifiedFinalizeOutput::Done(partitions) => {
                        // Send all output morsels
                        for partition in partitions {
                            self.runtime_stats.add_rows_out(partition.len() as u64);
                            if self
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition,
                                })
                                .await
                                == ControlFlow::Stop
                            {
                                return Ok(ControlFlow::Stop);
                            }
                        }

                        // Finalization complete, send flush
                        if self.send(PipelineMessage::Flush(input_id)).await == ControlFlow::Stop {
                            return Ok(ControlFlow::Stop);
                        }
                    }
                    UnifiedFinalizeOutput::Continue { states, output } => {
                        // Send finalize output if present
                        if let Some(mp) = output {
                            self.runtime_stats.add_rows_out(mp.len() as u64);
                            if self
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition: mp,
                                })
                                .await
                                == ControlFlow::Stop
                            {
                                return Ok(ControlFlow::Stop);
                            }
                        }

                        // Spawn another finalize task with the returned states
                        handler.spawn_finalize_task(
                            &mut self.task_set,
                            self.finalize_spawner.clone(),
                            input_id,
                            states,
                        );
                    }
                    UnifiedFinalizeOutput::NoOutput => {
                        // Finalization complete with no output (Build side)
                        // No flush message needed for Build
                    }
                }

                Ok(ControlFlow::Continue)
            }
        }
    }

    /// Process input messages using the common tokio::select! loop pattern.
    ///
    /// This implements the standard two-branch select loop:
    /// - Branch 1: Join completed tasks (if tasks exist)
    /// - Branch 2: Receive input messages (if can spawn task and receiver open)
    ///
    /// The handler parameter provides node-specific behavior for:
    /// - Spawning tasks
    /// - Spawning finalize tasks
    ///
    /// Returns `ControlFlow::Continue` on normal completion, `ControlFlow::Stop` on early exit.
    pub(crate) async fn process_input<Handler>(
        &mut self,
        mut receiver: Receiver<PipelineMessage>,
        node_id: usize,
        handler: &mut Handler,
    ) -> DaftResult<ControlFlow>
    where
        Handler: NodeExecutionHandler<State>,
    {
        let mut input_closed = false;
        let mut node_initialized = false;

        // Main processing loop
        // Continue while: input not closed, or tasks are running, or partitions are buffered, or states need finalization
        while self.should_continue_processing(input_closed) {
            tokio::select! {
                biased;

                // Branch 1: Join completed task (only if tasks exist)
                Some(task_result) = self.task_set.join_next(), if !self.task_set.is_empty() => {
                    if self.handle_task_result(task_result??, handler).await? == ControlFlow::Stop {
                        return Ok(ControlFlow::Stop);
                    }
                }

                // Branch 2: Receive input (only if we can spawn task and receiver open)
                msg = receiver.recv(), if self.task_set.len() < self.max_concurrency && !input_closed => {
                    match msg {
                        Some(PipelineMessage::Morsel { input_id, partition }) => {
                            self.handle_morsel_and_spawn_tasks(handler, input_id, partition, node_id, &mut node_initialized)?;
                        }
                        Some(PipelineMessage::Flush(input_id)) => {
                            if self.handle_flush(handler, input_id).await? == ControlFlow::Stop {
                                return Ok(ControlFlow::Stop);
                            }
                        }
                        None => {
                            input_closed = true;
                            if self.handle_input_closed(handler).await? == ControlFlow::Stop {
                                return Ok(ControlFlow::Stop);
                            }
                        }
                    }
                }
            }
        }

        self.stats_manager.finalize_node(node_id);

        Ok(ControlFlow::Continue)
    }
}
