use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

use common_error::DaftResult;
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;
use tokio::sync::broadcast;

use crate::{
    ExecutionTaskSpawner,
    channel::Receiver,
    dynamic_batching::StaticBatchingStrategy,
    join::join_operator::JoinOperator,
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, PipelineNodeExecutor, StateTracker,
        UnifiedFinalizeOutput, UnifiedTaskResult,
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

/// Bridge for communicating finalized build state between build and probe sides
pub(crate) struct BuildStateBridge<Op: JoinOperator> {
    channels: Mutex<HashMap<InputId, broadcast::Sender<Op::FinalizedBuildState>>>,
}

impl<Op: JoinOperator> BuildStateBridge<Op> {
    pub(crate) fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Called by build side after finalizing. Sends finalized state to probe side via broadcast.
    pub(crate) fn send_finalized_build_state(
        &self,
        input_id: InputId,
        finalized: Op::FinalizedBuildState,
    ) {
        let mut channels = self.channels.lock().unwrap();
        // Get or create sender for this input_id
        let sender = channels.entry(input_id).or_insert_with(|| {
            let (sender, _receiver) = broadcast::channel(1);
            sender
        });
        // Broadcast the finalized state (ignore send errors - receivers may have lagged, but that's fine)
        let _ = sender.send(finalized);
    }

    /// Called by probe side to subscribe to finalized build state updates.
    /// Returns a receiver that can be awaited independently for each ProbeState.
    pub(crate) fn subscribe(
        &self,
        input_id: InputId,
    ) -> broadcast::Receiver<Op::FinalizedBuildState> {
        let mut channels = self.channels.lock().unwrap();
        // Get or create sender for this input_id
        let sender = channels.entry(input_id).or_insert_with(|| {
            let (sender, _receiver) = broadcast::channel(1);
            sender
        });
        // Subscribe to get a new receiver
        sender.subscribe()
    }
}

struct BuildNodeHandler<Op: JoinOperator> {
    op: Arc<Op>,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
}

impl<Op: JoinOperator + 'static> NodeExecutionHandler<Op::BuildState> for BuildNodeHandler<Op> {
    fn spawn_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::BuildState>>>,
        task_spawner: ExecutionTaskSpawner,
        partition: Arc<MicroPartition>,
        state: Op::BuildState,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.build(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(UnifiedTaskResult::Execution {
                input_id,
                state: new_state,
                elapsed,
                output: crate::pipeline_execution::OperatorExecutionOutput::NeedMoreInput(None),
            })
        });
    }

    fn spawn_finalize_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::BuildState>>>,
        _finalize_spawner: Option<ExecutionTaskSpawner>,
        input_id: InputId,
        states: Vec<Op::BuildState>,
    ) {
        let op = self.op.clone();
        let build_state_bridge = self.build_state_bridge.clone();
        task_set.spawn(async move {
            // Build side uses single state, so take first
            let state = states.into_iter().next().expect("Should have state");
            let finalized = op.finalize_build(state)?;
            build_state_bridge.send_finalized_build_state(input_id, finalized);

            Ok(UnifiedTaskResult::Finalize {
                input_id,
                output: UnifiedFinalizeOutput::NoOutput,
            })
        });
    }
}

pub(crate) struct BuildExecutionContext<Op: JoinOperator> {
    base: PipelineNodeExecutor<Op::BuildState, StaticBatchingStrategy>,
    handler: BuildNodeHandler<Op>,
}

impl<Op: JoinOperator + 'static> BuildExecutionContext<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        stats_manager: RuntimeStatsManagerHandle,
    ) -> Self {
        let max_concurrency = get_compute_pool_num_threads();
        let task_set = OrderingAwareJoinSet::new(false);

        let op_for_state_creator = op.clone();
        let build_state_tracker = InputStateTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::BuildState>> {
                let state = op_for_state_creator.make_build_state()?;
                // Build side uses single state with bounds 0 and usize::MAX (no batching needed)
                Ok(StateTracker::new(
                    vec![state],
                    0,
                    NonZeroUsize::new(usize::MAX).unwrap(),
                ))
            },
        ));

        let base = PipelineNodeExecutor::new(
            task_spawner,
            runtime_stats,
            max_concurrency,
            task_set,
            build_state_tracker,
            stats_manager,
        );

        let handler = BuildNodeHandler {
            op,
            build_state_bridge,
        };

        Self { base, handler }
    }

    pub(crate) async fn process_build_input(
        &mut self,
        node_id: usize,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        self.base
            .process_input(receiver, node_id, &mut self.handler)
            .await?;
        Ok(())
    }
}
