use std::{sync::Arc, time::Instant};

use common_error::DaftResult;
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;

use crate::{
    ExecutionTaskSpawner,
    channel::{Receiver, Sender},
    dynamic_batching::{BatchManager, BatchingStrategy},
    join::{build::BuildStateBridge, join_operator::JoinOperator},
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, PipelineNodeExecutor, StateTracker,
        UnifiedFinalizeOutput, UnifiedTaskResult,
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

struct ProbeNodeHandler<Op: JoinOperator> {
    op: Arc<Op>,
}

impl<Op: JoinOperator + 'static> NodeExecutionHandler<Op::ProbeState> for ProbeNodeHandler<Op> {
    fn spawn_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::ProbeState>>>,
        task_spawner: ExecutionTaskSpawner,
        partition: Arc<MicroPartition>,
        state: Op::ProbeState,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.probe(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(UnifiedTaskResult::Execution {
                input_id,
                state: new_state,
                elapsed,
                output: result,
            })
        });
    }

    fn spawn_finalize_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::ProbeState>>>,
        finalize_spawner: Option<ExecutionTaskSpawner>,
        input_id: InputId,
        states: Vec<Op::ProbeState>,
    ) {
        let op = self.op.clone();
        let finalize_spawner = finalize_spawner.expect("finalize_spawner must be set");
        task_set.spawn(async move {
            let output = op.finalize_probe(states, &finalize_spawner).await??;

            Ok(UnifiedTaskResult::Finalize {
                input_id,
                output: UnifiedFinalizeOutput::Done(output.into_iter().collect()),
            })
        });
    }
}

pub(crate) struct ProbeExecutionContext<Op: JoinOperator, Strategy: BatchingStrategy + 'static> {
    base: PipelineNodeExecutor<Op::ProbeState, Strategy>,
    handler: ProbeNodeHandler<Op>,
}

impl<Op: JoinOperator + 'static, Strategy: BatchingStrategy + 'static>
    ProbeExecutionContext<Op, Strategy>
{
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_sender: Sender<PipelineMessage>,
        batch_manager: Arc<BatchManager<Strategy>>,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        maintain_order: bool,
        stats_manager: RuntimeStatsManagerHandle,
    ) -> Self {
        // Capture values needed for state creation
        let op_for_state_creator = op.clone();
        let build_state_bridge_for_state_creator = build_state_bridge.clone();
        let batch_manager_for_state_creator = batch_manager.clone();

        // State creator that subscribes to build state bridge for the given input_id
        let state_creator = Box::new(
            move |input_id| -> DaftResult<StateTracker<Op::ProbeState>> {
                // Subscribe receivers for each probe state - each will await independently in probe()
                let max_probe_concurrency = op_for_state_creator.max_probe_concurrency();
                let probe_states: Vec<_> = (0..max_probe_concurrency)
                    .map(|_| {
                        let receiver = build_state_bridge_for_state_creator.subscribe(input_id);
                        op_for_state_creator.make_probe_state(receiver)
                    })
                    .collect();
                let (lower, upper) = batch_manager_for_state_creator
                    .initial_requirements()
                    .values();
                Ok(StateTracker::new(probe_states, lower, upper))
            },
        );
        let max_total_concurrency = get_compute_pool_num_threads();
        let task_set = OrderingAwareJoinSet::new(maintain_order);
        let input_state_tracker = InputStateTracker::new(state_creator);
        let base = PipelineNodeExecutor::new(
            task_spawner,
            runtime_stats,
            max_total_concurrency,
            task_set,
            input_state_tracker,
            stats_manager,
        )
        .with_finalize_spawner(finalize_spawner)
        .with_output_sender(output_sender)
        .with_batch_manager(batch_manager);

        let handler = ProbeNodeHandler { op };

        Self { base, handler }
    }

    pub(crate) async fn process_probe_input(
        node_id: usize,
        receiver: Receiver<PipelineMessage>,
        ctx: &mut ProbeExecutionContext<Op, Strategy>,
    ) -> DaftResult<()> {
        ctx.base
            .process_input(receiver, node_id, &mut ctx.handler)
            .await?;
        Ok(())
    }
}
