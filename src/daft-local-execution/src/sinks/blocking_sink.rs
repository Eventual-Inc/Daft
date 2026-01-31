use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    channel::{Receiver, Sender, create_channel},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub enum BlockingSinkFinalizeOutput<Op: BlockingSink> {
    #[allow(dead_code)]
    HasMoreOutput {
        states: Vec<Op::State>,
        output: Vec<Arc<MicroPartition>>,
    },
    Finished(Vec<Arc<MicroPartition>>),
}

pub(crate) type BlockingSinkSinkResult<Op> =
    OperatorOutput<DaftResult<<Op as BlockingSink>::State>>;
pub(crate) type BlockingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<BlockingSinkFinalizeOutput<Op>>>;
pub(crate) trait BlockingSink: Send + Sync {
    type State: Send + Sync + Unpin;

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self>
    where
        Self: Sized;
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self>
    where
        Self: Sized;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Self::State>;
    fn make_runtime_stats(&self, name: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(name))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

type StateId = usize;

struct ExecutionTaskResult<S> {
    state_id: StateId,
    state: S,
    elapsed: Duration,
}

struct ExecutionContext<Op: BlockingSink> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
    state_pool: HashMap<StateId, Op::State>,
    output_sender: Sender<Arc<MicroPartition>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    stats_manager: RuntimeStatsManagerHandle,
}

pub struct BlockingSinkNode<Op: BlockingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
}

impl<Op: BlockingSink + 'static> BlockingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(
            name,
            op.op_type(),
            NodeCategory::BlockingSink,
            output_schema,
            context,
        );
        let runtime_stats = op.make_runtime_stats(node_info.id);

        Self {
            op,
            child,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // ========== Helper Functions ==========

    fn spawn_execution_task(
        ctx: &mut ExecutionContext<Op>,
        input: Arc<MicroPartition>,
        state: Op::State,
        state_id: StateId,
    ) {
        let op = ctx.op.clone();
        let task_spawner = ctx.task_spawner.clone();
        ctx.task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.sink(input, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(ExecutionTaskResult {
                state_id,
                state: new_state,
                elapsed,
            })
        });
    }

    async fn process_input(
        node_id: usize,
        mut receiver: Receiver<Arc<MicroPartition>>,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<()> {
        let mut input_closed = false;
        let mut node_initialized = false;

        // Main processing loop
        while !input_closed || !ctx.task_set.is_empty() {
            tokio::select! {
                biased;

                // Branch 1: Join completed task (only if tasks exist)
                Some(result) = ctx.task_set.join_next(), if !ctx.task_set.is_empty() => {
                    // Record execution stats
                    let ExecutionTaskResult { state_id, state, elapsed } = result??;
                    ctx.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                    // Return state to pool
                    ctx.state_pool.insert(state_id, state);
                }

                // Branch 2: Receive input (only if states available and receiver open)
                morsel = receiver.recv(), if !ctx.state_pool.is_empty() && !input_closed => {
                    match morsel {
                        Some(morsel) => {
                            if !node_initialized {
                                ctx.stats_manager.activate_node(node_id);
                                node_initialized = true;
                            }
                            ctx.runtime_stats.add_rows_in(morsel.len() as u64);
                            let state_id = *ctx
                                .state_pool
                                .keys()
                                .next()
                                .expect("State pool should have states when it is not empty");
                            let state = ctx
                                .state_pool
                                .remove(&state_id)
                                .expect("State pool should have states when it is not empty");

                            Self::spawn_execution_task(ctx, morsel, state, state_id);
                        }
                        None => {
                            input_closed = true;
                        }
                    }
                }
            }
        }

        // After loop exits, verify invariants
        debug_assert_eq!(ctx.task_set.len(), 0, "TaskSet should be empty after loop");
        debug_assert!(input_closed, "Receiver should be closed after loop");

        Ok(())
    }
}

impl<Op: BlockingSink + 'static> TreeDisplay for BlockingSinkNode<Op> {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.op.name()).unwrap();
            }
            level => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                if matches!(level, DisplayLevel::Verbose) {
                    let rt_result = self.runtime_stats.snapshot();
                    for (name, value) in rt_result.to_stats() {
                        writeln!(display, "{} = {}", name.as_ref().capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        let children: Vec<serde_json::Value> = self
            .get_children()
            .iter()
            .map(|child| child.repr_json())
            .collect();

        serde_json::json!({
            "id": self.node_id(),
            "category": "BlockingSink",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl<Op: BlockingSink + 'static> PipelineNode for BlockingSinkNode<Op> {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.child]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        _downstream_requirement: MorselSizeRequirement,
        default_morsel_size: MorselSizeRequirement,
    ) {
        self.child
            .propagate_morsel_size_requirement(default_morsel_size, default_morsel_size);
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let child_results_receiver = self.child.start(false, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        let op = self.op.clone();
        let max_concurrency = op.max_concurrency();

        // Create task spawners
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("BlockingSink::Sink"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("BlockingSink::Finalize"),
        );

        // Spawn process_input task
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();

        // Create task set
        let task_set = OrderingAwareJoinSet::new(maintain_order);

        // Create context (state_pool will be initialized inside async move)
        let mut ctx = ExecutionContext {
            op,
            task_spawner,
            task_set,
            state_pool: HashMap::new(),
            output_sender: destination_sender,
            runtime_stats: runtime_stats.clone(),
            stats_manager: stats_manager.clone(),
        };

        runtime_handle.spawn(
            async move {
                // Initialize state pool with max_concurrency states
                for i in 0..max_concurrency {
                    ctx.state_pool.insert(
                        i,
                        ctx.op.make_state().context(PipelineExecutionSnafu {
                            node_name: ctx.op.name().to_string(),
                        })?,
                    );
                }
                Self::process_input(node_id, child_results_receiver, &mut ctx).await?;

                // Collect finished states
                let mut finished_states: Vec<_> =
                    ctx.state_pool.drain().map(|(_, state)| state).collect();

                // Finalize
                loop {
                    let finalized_result = ctx
                        .op
                        .finalize(finished_states, &finalize_spawner)
                        .await??;
                    match finalized_result {
                        BlockingSinkFinalizeOutput::HasMoreOutput { states, output } => {
                            for output in output {
                                ctx.runtime_stats.add_rows_out(output.len() as u64);
                                if ctx.output_sender.send(output).await.is_err() {
                                    break;
                                }
                            }
                            finished_states = states;
                        }
                        BlockingSinkFinalizeOutput::Finished(output) => {
                            for output in output {
                                ctx.runtime_stats.add_rows_out(output.len() as u64);
                                let _ = ctx.output_sender.send(output).await;
                            }
                            break;
                        }
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );

        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
    fn node_id(&self) -> usize {
        self.node_info.id
    }
    fn plan_id(&self) -> Arc<str> {
        Arc::from(self.node_info.context.get("plan_id").unwrap().clone())
    }
    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.runtime_stats.clone()
    }
}
