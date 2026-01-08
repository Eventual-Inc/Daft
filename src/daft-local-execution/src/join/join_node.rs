use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo};
use common_runtime::get_compute_runtime;
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tokio::sync::broadcast;
use tracing::{info_span, instrument};

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner,
    channel::{Receiver, Sender, create_channel, create_ordering_aware_receiver_channel, OrderingAwareReceiver},
    dispatcher::{DispatchSpawner, DynamicUnorderedDispatcher, InputIdDispatcher, RoundRobinDispatcher},
    dynamic_batching::BatchManager,
    join::join_operator::{JoinOperator, ProbeFinalizeOutput, ProbeOutput},
    pipeline::{MorselSizeRequirement, PipelineNode, RuntimeContext},
    plan_input::{InputId, PipelineMessage},
    resource_manager::MemoryManager,
    runtime_stats::{CountingSender, InitializingCountingReceiver, RuntimeStats},
};

/// Unified message type for probe worker output
#[derive(Debug, Clone)]
enum ProbeWorkerOutput<Op: JoinOperator> {
    /// Output morsel to be forwarded downstream
    OutputMorsel(InputId, Arc<MicroPartition>),
    /// Flush state that needs to be collected and finalized (may be None if worker has no state for this input_id)
    FlushState(InputId, Option<Op::ProbeState>),
}

/// State of the build channel for a given input ID
enum BuildChannelState<Op: JoinOperator> {
    /// Waiting for build state to be finalized - only sender is available
    Waiting(broadcast::Sender<Arc<Op::FinalizedBuildState>>),
    /// Build state is ready - both sender and state are available
    Ready(Arc<Op::FinalizedBuildState>),
}

pub struct JoinNode<Op: JoinOperator> {
    op: Arc<Op>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

impl<Op: JoinOperator + 'static> JoinNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(
            name,
            op.op_type(),
            NodeCategory::Join,
            output_schema,
            context,
        );
        let runtime_stats = op.make_runtime_stats(node_info.id);

        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
            left,
            right,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(node_info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    /// Run a build worker: processes build messages for specific input IDs
    /// Broadcasts finalized build states to waiting probe workers
    #[instrument(level = "info", skip_all, name = "JoinNode::run_build_worker")]
    async fn run_build_worker(
        op: Arc<Op>,
        mut build_receiver: Receiver<PipelineMessage>,
        build_channels: Arc<Mutex<HashMap<InputId, BuildChannelState<Op>>>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<()> {
        let span = info_span!("JoinNode::Build");
        let compute_runtime = get_compute_runtime();
        let spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats, span);

        // Maintain build state per input ID (this worker handles specific input IDs)
        let mut build_states: HashMap<InputId, Op::BuildState> = HashMap::new();

        while let Some(msg) = build_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    // Get or create build state for this input ID
                    let build_state = match build_states.remove(&input_id) {
                        Some(build_state) => Ok(build_state),
                        None => op.make_build_state(),
                    }?;

                    let result = op.build_state(partition, build_state, &spawner).await??;
                    build_states.insert(input_id, result);
                }
                PipelineMessage::Flush(input_id) => {
                    // When we receive a flush for an input_id, finalize its build state
                    if let Some(build_state) = build_states.remove(&input_id) {
                        let finalized_build_state = op.finalize_build(build_state).await??;
                        // Get or create broadcast channel for this input_id and update to Ready state
                        {
                            let mut channels = build_channels.lock().unwrap();
                            match channels.entry(input_id) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    match entry.get() {
                                        BuildChannelState::Waiting(s) => {
                                            let _ = s.send(finalized_build_state.clone());
                                            entry.insert(BuildChannelState::Ready(
                                                finalized_build_state,
                                            ));
                                        }
                                        BuildChannelState::Ready(_) => debug_assert!(
                                            false,
                                            "Build channel should not be ready yet"
                                        ),
                                    }
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    entry.insert(BuildChannelState::Ready(finalized_build_state));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Finalize any remaining build states for each input ID
        for (input_id, build_state) in build_states {
            let finalized_build_state = op.finalize_build(build_state).await??;
            // Get or create broadcast channel for this input_id and update to Ready state
            let mut channels = build_channels.lock().unwrap();
            match channels.entry(input_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get() {
                    BuildChannelState::Waiting(s) => {
                        let _ = s.send(finalized_build_state.clone());
                        entry.insert(BuildChannelState::Ready(finalized_build_state));
                    }
                    BuildChannelState::Ready(_) => {
                        debug_assert!(false, "Build channel should not be ready yet")
                    }
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(BuildChannelState::Ready(finalized_build_state));
                }
            }
        }
        Ok(())
    }

    /// Run a probe worker
    #[instrument(level = "info", skip_all, name = "JoinNode::run_probe_worker")]
    async fn run_probe_worker(
        op: Arc<Op>,
        mut input_receiver: Receiver<PipelineMessage>,
        output_sender: Sender<ProbeWorkerOutput<Op>>,
        build_channels: Arc<Mutex<HashMap<InputId, BuildChannelState<Op>>>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> DaftResult<()> {
        let span = info_span!("JoinNode::Probe");
        let compute_runtime = get_compute_runtime();
        let spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats.clone(), span);

        // Maintain probe state per input ID
        let mut probe_states: HashMap<InputId, Op::ProbeState> = HashMap::new();

        while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    // Only wait for finalized build state if we don't already have a probe state
                    if !probe_states.contains_key(&input_id) {
                        // Wait for the finalized build state for this input ID to be ready
                        let finalized_build_state = {
                            // Get or create broadcast channel and check if state is already available
                            let (maybe_state, maybe_receiver) = {
                                let mut channels = build_channels.lock().unwrap();
                                match channels.entry(input_id) {
                                    std::collections::hash_map::Entry::Occupied(entry) => {
                                        match entry.get() {
                                            BuildChannelState::Waiting(sender) => {
                                                // State not ready yet, subscribe and wait
                                                let receiver = sender.subscribe();
                                                (None, Some(receiver))
                                            }
                                            BuildChannelState::Ready(state) => {
                                                // State is ready, use it directly
                                                (Some(state.clone()), None)
                                            }
                                        }
                                    }
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        // Channel doesn't exist yet, create it and wait
                                        let (tx, _) = broadcast::channel(1);
                                        let receiver = tx.subscribe();
                                        entry.insert(BuildChannelState::Waiting(tx));
                                        (None, Some(receiver))
                                    }
                                }
                            };
                            // If we have a state, use it; otherwise wait on the receiver
                            match (maybe_state, maybe_receiver) {
                                (Some(state), _) => state,
                                (_, Some(mut receiver)) => receiver.recv().await.map_err(|_| {
                                    common_error::DaftError::ValueError(
                                        "Build channel closed unexpectedly".to_string(),
                                    )
                                })?,
                                (None, None) => {
                                    return Err(common_error::DaftError::ValueError(
                                        "Invalid build channel state".to_string(),
                                    ));
                                }
                            }
                        };
                        let probe_state = op.make_probe_state(finalized_build_state)?;
                        probe_states.insert(input_id, probe_state);
                    }

                    // Get the probe state (we know it exists now)

                    loop {
                        let probe_state = probe_states.remove(&input_id).unwrap();
                        let result = op.probe(partition.clone(), probe_state, &spawner).await??;
                        let new_probe_state = result.0;
                        probe_states.insert(input_id, new_probe_state);

                        match result.1 {
                            ProbeOutput::NeedMoreInput(mp) => {
                                batch_manager.record_execution_stats(
                                    runtime_stats.clone(),
                                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                                    std::time::Instant::now().elapsed(),
                                );
                                if let Some(mp) = mp
                                    && output_sender
                                        .send(ProbeWorkerOutput::OutputMorsel(input_id, mp))
                                        .await
                                        .is_err()
                                {
                                    return Ok(());
                                }

                                break;
                            }
                            ProbeOutput::HasMoreOutput(mp) => {
                                batch_manager.record_execution_stats(
                                    runtime_stats.clone(),
                                    mp.len(),
                                    std::time::Instant::now().elapsed(),
                                );
                                if output_sender
                                    .send(ProbeWorkerOutput::OutputMorsel(input_id, mp))
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Send state back to coordinator when flush is called
                    let probe_state = probe_states.remove(&input_id);
                    if output_sender
                        .send(ProbeWorkerOutput::FlushState(input_id, probe_state))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
        }

        // Input receiver is exhausted, send all remaining states to output_sender
        for (input_id, probe_state) in probe_states {
            if output_sender
                .send(ProbeWorkerOutput::FlushState(input_id, Some(probe_state)))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        Ok(())
    }

    fn spawn_build_workers(
        op: Arc<Op>,
        input_receivers: Vec<Receiver<PipelineMessage>>,
        build_channels: Arc<Mutex<HashMap<InputId, BuildChannelState<Op>>>>,
        runtime_handle: &tokio::runtime::Handle,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
    ) {
        for input_receiver in input_receivers {
            runtime_handle.spawn(Self::run_build_worker(
                op.clone(),
                input_receiver,
                build_channels.clone(),
                runtime_stats.clone(),
                memory_manager.clone(),
            ));
        }
    }

    fn spawn_probe_workers(
        op: Arc<Op>,
        input_receivers: Vec<Receiver<PipelineMessage>>,
        build_channels: Arc<Mutex<HashMap<InputId, BuildChannelState<Op>>>>,
        runtime_handle: &tokio::runtime::Handle,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
        maintain_order: bool,
    ) -> OrderingAwareReceiver<ProbeWorkerOutput<Op>> {
        let (output_senders, output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, input_receivers.len());
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_senders) {
            runtime_handle.spawn(Self::run_probe_worker(
                op.clone(),
                input_receiver,
                output_sender,
                build_channels.clone(),
                runtime_stats.clone(),
                memory_manager.clone(),
                batch_manager.clone(),
            ));
        }
        output_receiver
    }
}

impl<Op: JoinOperator + 'static> TreeDisplay for JoinNode<Op> {
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
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
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
            "category": "Join",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl<Op: JoinOperator + 'static> PipelineNode for JoinNode<Op> {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.left, &self.right]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();

        // Left side: behave like BlockingSinkNode - ignore downstream_requirement,
        // use operator requirement or default
        let left_morsel_size_requirement = match operator_morsel_size_requirement {
            Some(requirement) => requirement,
            None => default_requirement,
        };

        // Right side: behave like IntermediateNode - combine operator requirement
        // with downstream requirement
        let right_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );

        // Use the right side requirement for the join node itself
        // (since joins typically propagate from right side in the pipeline)
        self.morsel_size_requirement = right_morsel_size_requirement;

        self.left
            .propagate_morsel_size_requirement(left_morsel_size_requirement, default_requirement);
        self.right
            .propagate_morsel_size_requirement(right_morsel_size_requirement, default_requirement);
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        // Start build-side child (index 0)
        let build_child_receiver: Receiver<PipelineMessage> = self.left.start(false,runtime_handle)?;
        let build_counting_receiver = InitializingCountingReceiver::new(
            build_child_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        // Start probe-side child (index 1) - will be consumed by multiple workers
        let probe_child_receiver: Receiver<PipelineMessage> = self.right.start(maintain_order, runtime_handle)?;
        let probe_counting_receiver = InitializingCountingReceiver::new(
            probe_child_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let op_name = self.op.name().to_string();

        // Broadcast channels for finalized build states - probe workers subscribe to these
        let build_channels: Arc<Mutex<HashMap<InputId, BuildChannelState<Op>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        runtime_handle.spawn(
            async move {
                use crate::RuntimeHandle;
                let current_handle = tokio::runtime::Handle::current();

                // Set up build workers - route by input_id
                let num_build_workers = op.max_probe_concurrency(); // Use same number as probe workers
                let build_dispatcher = InputIdDispatcher::new();
                let mut build_runtime_handle = RuntimeHandle(current_handle.clone());
                let build_dispatch_result = build_dispatcher.spawn_dispatch(
                    build_counting_receiver,
                    num_build_workers,
                    &mut build_runtime_handle,
                );

                // Spawn build dispatch task
                let build_dispatch_task = build_dispatch_result.spawned_dispatch_task;
                current_handle.spawn(async move {
                    let _ = build_dispatch_task.await?;
                    Ok::<(), crate::Error>(())
                });

                // Set up probe workers - can process any input_id once build state is ready
                let num_probe_workers = op.max_probe_concurrency();
                let strategy = op.batching_strategy()?;
                let batch_manager = Arc::new(BatchManager::new(strategy));

                // Create dispatcher for probe side and spawn dispatch
                let batch_manager_for_dispatcher = batch_manager.clone();
                let maintain_order_for_probe = maintain_order;
                let spawned_probe_dispatch_result = {
                    let mut probe_runtime_handle = RuntimeHandle(current_handle.clone());
                    let dispatcher: Arc<dyn DispatchSpawner> = if maintain_order_for_probe {
                        Arc::new(RoundRobinDispatcher::new(batch_manager_for_dispatcher))
                    } else {
                        Arc::new(DynamicUnorderedDispatcher::new(batch_manager_for_dispatcher))
                    };
                    dispatcher.spawn_dispatch(
                        probe_counting_receiver,
                        num_probe_workers,
                        &mut probe_runtime_handle,
                    )
                };

                // Spawn the probe dispatch task
                let probe_dispatch_task = spawned_probe_dispatch_result.spawned_dispatch_task;
                current_handle.spawn(async move {
                    let _ = probe_dispatch_task.await?;
                    Ok::<(), crate::Error>(())
                });

                // Spawn build and probe workers concurrently
                Self::spawn_build_workers(
                    op.clone(),
                    build_dispatch_result.worker_receivers,
                    build_channels.clone(),
                    &current_handle,
                    runtime_stats.clone(),
                    memory_manager.clone(),
                );
                let num_probe_workers = spawned_probe_dispatch_result.worker_receivers.len();
                let mut output_receiver = Self::spawn_probe_workers(
                    op.clone(),
                    spawned_probe_dispatch_result.worker_receivers,
                    build_channels.clone(),
                    &current_handle,
                    runtime_stats.clone(),
                    memory_manager.clone(),
                    batch_manager,
                    maintain_order,
                );

                // Track states and flush counts per input_id
                let mut all_probe_states: HashMap<InputId, Vec<Op::ProbeState>> = HashMap::new();
                let mut flush_counts: HashMap<InputId, usize> = HashMap::new();

                // Stream probe results and handle flush states
                while let Some(msg) = output_receiver.recv().await {
                    match msg {
                        ProbeWorkerOutput::OutputMorsel(input_id, partition) => {
                            let pipeline_msg = PipelineMessage::Morsel {
                                input_id,
                                partition,
                            };
                            if counting_sender.send(pipeline_msg).await.is_err() {
                                return Ok(());
                            }
                        }
                        ProbeWorkerOutput::FlushState(input_id, probe_state) => {
                            // Received a flush state from a worker
                            if let Some(state) = probe_state {
                                all_probe_states
                                    .entry(input_id)
                                    .or_insert_with(Vec::new)
                                    .push(state);
                            }
                            let count = flush_counts.entry(input_id).or_insert(0);
                            *count += 1;
                            // Once all active workers have flushed for this input_id, finalize it if needed
                            if *count == num_probe_workers {
                                let states = all_probe_states.remove(&input_id).unwrap_or_default();
                                flush_counts.remove(&input_id);

                                if op.needs_probe_finalization() {
                                    let compute_runtime = get_compute_runtime();
                                    let spawner = ExecutionTaskSpawner::new(
                                        compute_runtime,
                                        memory_manager.clone(),
                                        runtime_stats.clone(),
                                        info_span!("JoinNode::FinalizeProbe"),
                                    );

                                    let mut current_states = states;
                                    loop {
                                        let finalized_result =
                                            op.finalize_probe(current_states, &spawner).await??;
                                        match finalized_result {
                                            ProbeFinalizeOutput::HasMoreOutput {
                                                states,
                                                output,
                                            } => {
                                                if let Some(mp) = output {
                                                    let input_mp = PipelineMessage::Morsel {
                                                        input_id,
                                                        partition: mp,
                                                    };
                                                    if counting_sender.send(input_mp).await                                                    .is_err()
                                                    {
                                                        return Ok(());
                                                    }
                                                }
                                                current_states = states;
                                            }
                                            ProbeFinalizeOutput::Finished(output) => {
                                                if let Some(mp) = output {
                                                    let input_mp = PipelineMessage::Morsel {
                                                        input_id,
                                                        partition: mp,
                                                    };
                                                    if counting_sender.send(input_mp).await                                                    .is_err()
                                                    {
                                                        return Ok(());
                                                    }
                                                }
                                                // Send flush signal after finalizing this input_id
                                                if counting_sender
                                                    .send(PipelineMessage::Flush(input_id))
                                                    .await
                                                    .is_err()
                                                {
                                                    return Ok(());
                                                }
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    // No finalization needed, just send flush signal
                                    if counting_sender
                                        .send(PipelineMessage::Flush(input_id))
                                        .await
                                        .is_err()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }

                // Finish up finalizing remaining states here.
                // At this point, there may still be input_ids that have not been fully flushed/finalized
                // because their workers exited early. For any such input_id, finalize the accumulated states.
                if op.needs_probe_finalization() {
                    let compute_runtime = get_compute_runtime();
                    let spawner = ExecutionTaskSpawner::new(
                        compute_runtime,
                        memory_manager.clone(),
                        runtime_stats.clone(),
                        info_span!("JoinNode::FinalizeProbe"),
                    );

                    for (input_id, mut probe_states) in all_probe_states.drain() {
                        loop {
                            let finalized_result =
                                op.finalize_probe(probe_states, &spawner).await??;
                            match finalized_result {
                                ProbeFinalizeOutput::HasMoreOutput { states, output } => {
                                    if let Some(mp) = output {
                                        let input_mp = PipelineMessage::Morsel {
                                            input_id,
                                            partition: mp,
                                        };
                                        if counting_sender.send(input_mp).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    probe_states = states;
                                }
                                ProbeFinalizeOutput::Finished(output) => {
                                    if let Some(mp) = output {
                                        let input_mp = PipelineMessage::Morsel {
                                            input_id,
                                            partition: mp,
                                        };
                                        if counting_sender.send(input_mp).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    // Send flush signal after finalizing this input_id
                                    if counting_sender
                                        .send(PipelineMessage::Flush(input_id))
                                        .await
                                        .is_err()
                                    {
                                        return Ok(());
                                    }
                                    break;
                                }
                            }
                        }
                    }
                } else {
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &op_name,
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
