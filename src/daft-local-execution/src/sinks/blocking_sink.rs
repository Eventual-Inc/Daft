use std::{collections::HashMap, sync::Arc};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, Sender, create_channel},
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    plan_input::{InputId, PipelineMessage},
    resource_manager::MemoryManager,
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
};

pub(crate) type BlockingSinkSinkResult<Op> =
    OperatorOutput<DaftResult<<Op as BlockingSink>::State>>;
pub(crate) type BlockingSinkFinalizeResult = OperatorOutput<DaftResult<Vec<Arc<MicroPartition>>>>;
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
    ) -> BlockingSinkFinalizeResult
    where
        Self: Sized;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Self::State>;
    fn make_runtime_stats(&self, name: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(name))
    }
    fn dispatch_spawner(
        &self,
        _morsel_size_requirement: Option<MorselSizeRequirement>,
    ) -> Arc<dyn DispatchSpawner> {
        Arc::new(UnorderedDispatcher::unbounded())
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
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

    async fn finalize_and_send_output(
        op: Arc<Op>,
        states: Vec<Op::State>,
        input_id: InputId,
        memory_manager: Arc<MemoryManager>,
        runtime_stats: Arc<dyn RuntimeStats>,
        counting_sender: &CountingSender,
    ) -> DaftResult<bool> {
        let compute_runtime = get_compute_runtime();
        let spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_manager,
            runtime_stats,
            info_span!("BlockingSink::Finalize"),
        );

        let finalized_result = op.finalize(states, &spawner).await??;
        println!("[Blocking Sink {}] Finalized result num rows: {:#?} for input_id: {}", op.name(), finalized_result.len(), input_id);
        for output_partition in finalized_result {
            let input_mp = PipelineMessage::Morsel {
                input_id,
                partition: output_partition,
            };
            if counting_sender.send(input_mp).await.is_err() {
                println!("[Blocking Sink {}] Error sending morsel for input_id: {:?}", op.name(), input_id);
                return Ok(false);
            }
        }
        println!("[Blocking Sink {}] Sending flush message for input_id: {:?}", op.name(), input_id);
        if counting_sender
            .send(PipelineMessage::Flush(input_id))
            .await
            .is_err()
        {
            println!("[Blocking Sink {}] Error sending flush message for input_id: {:?}", op.name(), input_id);
            return Ok(false);
        }
        Ok(true)
    }

    #[instrument(level = "info", skip_all, name = "BlockingSink::run_worker")]
    async fn run_worker(
        op: Arc<Op>,
        mut input_receiver: Receiver<PipelineMessage>,
        flush_state_sender: Sender<(InputId, Option<Op::State>)>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<()> {
        let span = info_span!("BlockingSink::Sink");
        let compute_runtime = get_compute_runtime();
        let spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats, span);
        let mut states: HashMap<InputId, Op::State> = HashMap::new();
        while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    // Get or create state for this input ID
                    let state = match states.remove(&input_id) {
                        Some(state) => {
                            state
                        }
                        None => {
                            op.make_state()?
                        }
                    };
                    let result = op.sink(partition, state, &spawner).await??;
                    states.insert(input_id, result);
                }
                PipelineMessage::Flush(input_id) => {
                    // Send state back to coordinator when flush is called
                    // Even if worker didn't have state for this input_id, we still need to
                    // propagate the flush so the coordinator knows this worker has flushed
                    let state = states.remove(&input_id);
                    if flush_state_sender.send((input_id, state)).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        // Input receiver is exhausted, send all remaining states to flush_state_sender
        for (input_id, state) in states {
            if flush_state_sender
                .send((input_id, Some(state)))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    fn spawn_workers(
        op: Arc<Op>,
        input_receivers: Vec<Receiver<PipelineMessage>>,
        runtime_handle: &mut ExecutionRuntimeContext,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        flush_state_sender: Sender<(InputId, Option<Op::State>)>,
    ) -> usize {
        let num_workers = input_receivers.len();
        for input_receiver in input_receivers {
            runtime_handle.spawn(
                Self::run_worker(
                    op.clone(),
                    input_receiver,
                    flush_state_sender.clone(),
                    runtime_stats.clone(),
                    memory_manager.clone(),
                ),
                &op.name(),
            );
        }

        num_workers
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
                    for (name, value) in rt_result {
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
        &self,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let child_results_receiver = self.child.start(runtime_handle)?;

        let counting_receiver = InitializingCountingReceiver::new(
            child_results_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();

        let dispatch_spawner = op.dispatch_spawner(None);

        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            counting_receiver,
            num_workers,
            &mut runtime_handle.handle(),
        );

        runtime_handle.spawn(
            async move {
                spawned_dispatch_result.spawned_dispatch_task.await?
            },
            &self.name(),
        );

        // Create a single shared channel for all workers
        let (flush_state_sender, mut flush_state_receiver) =
            create_channel::<(InputId, Option<Op::State>)>(1);

        // Spawn workers on runtime_handle
        let num_workers_spawned = Self::spawn_workers(
            op.clone(),
            spawned_dispatch_result.worker_receivers,
            runtime_handle,
            self.runtime_stats.clone(),
            runtime_handle.memory_manager(),
            flush_state_sender.clone(),
        );

        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();

        let sink_name = self.name().clone();
        runtime_handle.spawn(
            async move {
                // Track states and flush counts per input_id
                let mut all_states: HashMap<InputId, Vec<Op::State>> = HashMap::new();
                let mut flush_counts: HashMap<InputId, usize> = HashMap::new();

                while let Some((input_id, state_opt)) = flush_state_receiver.recv().await {
                    // Received a flush state from a worker
                    // Only add state if it exists (Some), but always count the flush
                    if let Some(state) = state_opt {
                        all_states
                            .entry(input_id)
                            .or_insert_with(Vec::new)
                            .push(state);
                    }
                    let count = flush_counts.entry(input_id).or_insert(0);
                    *count += 1;
                    // Once all active workers have flushed for this input_id, finalize it
                    if *count == num_workers_spawned {
                        let states = all_states.remove(&input_id).unwrap_or_default();
                        flush_counts.remove(&input_id);

                        if !Self::finalize_and_send_output(
                            op.clone(),
                            states,
                            input_id,
                            memory_manager.clone(),
                            runtime_stats.clone(),
                            &counting_sender,
                        )
                        .await?
                        {
                            break;
                        }
                    }
                }

                // Finish up finalizing remaining states here.
                // At this point, there may still be input_ids that have not been fully flushed/finalized
                // because their workers exited early. For any such input_id, finalize the accumulated states.

                for (input_id, states) in all_states.drain() {
                    if !Self::finalize_and_send_output(
                        op.clone(),
                        states,
                        input_id,
                        memory_manager.clone(),
                        runtime_stats.clone(),
                        &counting_sender,
                    )
                    .await?
                    {
                        break;
                    }
                }

                println!("[Blocking Sink {}] Finalizing node", sink_name);
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
