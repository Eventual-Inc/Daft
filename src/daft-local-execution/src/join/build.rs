use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{Meter, ops::NodeInfo};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;
use tokio::sync::oneshot;

use crate::{
    ExecutionTaskSpawner,
    channel::Receiver,
    join::{join_operator::JoinOperator, stats::JoinStats},
    pipeline::{InputId, PipelineEvent, PipelineMessage, next_event},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

enum BuildStateSlot<T> {
    Pending(Vec<oneshot::Sender<T>>),
    Ready(T),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BuildKey {
    Input(InputId),
    Shared(String),
}

impl BuildKey {
    pub(crate) fn for_input(shared_build_key: Option<&str>, input_id: InputId) -> Self {
        shared_build_key
            .map(|key| Self::Shared(key.to_string()))
            .unwrap_or(Self::Input(input_id))
    }
}

pub(crate) enum FinalizedBuildStateReceiver<Op: JoinOperator> {
    Receiver(oneshot::Receiver<Op::FinalizedBuildState>),
    Ready(Op::FinalizedBuildState),
}

pub(crate) struct BuildStateBridge<Op: JoinOperator> {
    channels: Mutex<HashMap<BuildKey, BuildStateSlot<Op::FinalizedBuildState>>>,
}

impl<Op: JoinOperator> BuildStateBridge<Op> {
    pub(crate) fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn send_finalized_build_state(
        &self,
        build_key: BuildKey,
        finalized: Op::FinalizedBuildState,
    ) {
        let mut channels = self.channels.lock().unwrap();
        match channels.entry(build_key) {
            Entry::Vacant(e) => {
                e.insert(BuildStateSlot::Ready(finalized));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                BuildStateSlot::Pending(waiters) => {
                    for waiter in waiters.drain(..) {
                        let _ = waiter.send(finalized.clone());
                    }
                    e.insert(BuildStateSlot::Ready(finalized));
                }
                BuildStateSlot::Ready(slot) => {
                    *slot = finalized;
                }
            },
        }
    }

    pub(crate) fn subscribe(&self, build_key: BuildKey) -> FinalizedBuildStateReceiver<Op> {
        let mut channels = self.channels.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        match channels.entry(build_key) {
            Entry::Vacant(e) => {
                e.insert(BuildStateSlot::Pending(vec![tx]));
                FinalizedBuildStateReceiver::Receiver(rx)
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                BuildStateSlot::Ready(v) => FinalizedBuildStateReceiver::Ready(v.clone()),
                BuildStateSlot::Pending(waiters) => {
                    waiters.push(tx);
                    FinalizedBuildStateReceiver::Receiver(rx)
                }
            },
        }
    }
}

type BuildTaskResult<Op> = DaftResult<(BuildKey, <Op as JoinOperator>::BuildState)>;

struct PerBuildInput<Op: JoinOperator> {
    owner_input_id: InputId,
    state: Option<Op::BuildState>,
    pending: VecDeque<MicroPartition>,
    flushed: bool,
    runtime_stats: Arc<JoinStats>,
}

impl<Op: JoinOperator + 'static> PerBuildInput<Op> {
    fn new(owner_input_id: InputId, state: Op::BuildState, runtime_stats: Arc<JoinStats>) -> Self {
        Self {
            owner_input_id,
            state: Some(state),
            pending: VecDeque::new(),
            flushed: false,
            runtime_stats,
        }
    }

    /// If the state is idle and there is pending work, concat all pending
    /// partitions and spawn a single build task.
    fn flush_pending(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<BuildTaskResult<Op>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        build_key: BuildKey,
    ) -> DaftResult<()> {
        if self.pending.is_empty() || self.state.is_none() {
            return Ok(());
        }
        let state = self.state.take().unwrap();
        let partition = if self.pending.len() == 1 {
            self.pending.pop_front().unwrap()
        } else {
            MicroPartition::concat(self.pending.drain(..).collect::<Vec<_>>())?
        };
        let op = op.clone();
        let spawner = spawner.clone();
        tasks.spawn(async move {
            let state = op.build(partition, state, &spawner).await??;
            Ok((build_key, state))
        });
        Ok(())
    }

    fn is_idle(&self) -> bool {
        self.state.is_some()
    }

    fn ready_to_finalize(&self) -> bool {
        self.flushed && self.is_idle()
    }
}

pub(crate) struct BuildExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    shared_build_key: Option<String>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    meter: Meter,
    node_info: Arc<NodeInfo>,
}

impl<Op: JoinOperator + 'static> BuildExecutionContext<Op> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        shared_build_key: Option<String>,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
        meter: Meter,
        node_info: Arc<NodeInfo>,
    ) -> Self {
        Self {
            op,
            task_spawner,
            build_state_bridge,
            shared_build_key,
            stats_manager,
            node_id,
            meter,
            node_info,
        }
    }

    fn build_key(&self, input_id: InputId) -> BuildKey {
        BuildKey::for_input(self.shared_build_key.as_deref(), input_id)
    }

    fn try_finalize(&self, per_input: PerBuildInput<Op>, build_key: BuildKey) {
        let state = per_input.state.expect("must be idle when finalizing");
        if let Ok(finalized) = self.op.finalize_build(state) {
            self.build_state_bridge
                .send_finalized_build_state(build_key, finalized);
        }
    }

    pub(crate) async fn process_build_input(
        &self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut inputs: HashMap<BuildKey, PerBuildInput<Op>> = HashMap::new();
        let mut completed_shared_build_keys = HashSet::new();
        let mut tasks: OrderingAwareJoinSet<BuildTaskResult<Op>> = OrderingAwareJoinSet::new(false);
        let mut node_initialized = false;
        let mut child_closed = false;

        while let Some(event) = next_event(
            &mut tasks,
            get_compute_pool_num_threads(),
            &mut receiver,
            &mut child_closed,
        )
        .await?
        {
            match event {
                PipelineEvent::TaskCompleted((build_key, state)) => {
                    let per_input = inputs.get_mut(&build_key).unwrap();
                    per_input.state = Some(state);
                    per_input.flush_pending(
                        &mut tasks,
                        &self.op,
                        &self.task_spawner,
                        build_key.clone(),
                    )?;

                    if inputs
                        .get(&build_key)
                        .is_some_and(|p| p.ready_to_finalize())
                    {
                        let per_input = inputs.remove(&build_key).unwrap();
                        self.try_finalize(per_input, build_key.clone());
                        if matches!(build_key, BuildKey::Shared(_)) {
                            completed_shared_build_keys.insert(build_key);
                        }
                    }
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    if !node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        node_initialized = true;
                    }

                    let build_key = self.build_key(input_id);
                    if completed_shared_build_keys.contains(&build_key) {
                        continue;
                    }
                    let per_input = match inputs.entry(build_key.clone()) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let runtime_stats =
                                Arc::new(JoinStats::new(&self.meter, &self.node_info));
                            self.stats_manager.register_runtime_stats(
                                self.node_id,
                                input_id,
                                runtime_stats.clone(),
                            );
                            let state = self.op.make_build_state()?;
                            e.insert(PerBuildInput::new(input_id, state, runtime_stats))
                        }
                    };
                    if per_input.owner_input_id != input_id {
                        continue;
                    }
                    per_input
                        .runtime_stats
                        .add_build_rows_inserted(partition.len() as u64);
                    per_input.pending.push_back(partition);
                    per_input.flush_pending(&mut tasks, &self.op, &self.task_spawner, build_key)?;
                }
                PipelineEvent::Flush(input_id) => {
                    let build_key = self.build_key(input_id);
                    if completed_shared_build_keys.contains(&build_key) {
                        continue;
                    }
                    if let Some(p) = inputs.get_mut(&build_key)
                        && p.owner_input_id == input_id
                    {
                        p.flushed = true;
                    }
                    if inputs
                        .get(&build_key)
                        .is_some_and(|p| p.ready_to_finalize())
                    {
                        let per_input = inputs.remove(&build_key).unwrap();
                        self.try_finalize(per_input, build_key.clone());
                        if matches!(build_key, BuildKey::Shared(_)) {
                            completed_shared_build_keys.insert(build_key);
                        }
                    }
                }
                PipelineEvent::InputClosed => {
                    for p in inputs.values_mut() {
                        p.flushed = true;
                    }
                    let ready_keys: Vec<_> = inputs
                        .iter()
                        .filter(|(_, p)| p.ready_to_finalize())
                        .map(|(build_key, _)| build_key.clone())
                        .collect();
                    for build_key in ready_keys {
                        let per_input = inputs.remove(&build_key).unwrap();
                        self.try_finalize(per_input, build_key.clone());
                        if matches!(build_key, BuildKey::Shared(_)) {
                            completed_shared_build_keys.insert(build_key);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use common_display::{DisplayLevel, tree::TreeDisplay};
    use common_metrics::{
        Meter, QueryEndState,
        ops::{NodeCategory, NodeInfo, NodeType},
    };
    use common_runtime::get_compute_runtime;
    use daft_micropartition::MicroPartition;
    use tokio::time::{Duration, timeout};
    use tracing::Span;

    use super::*;
    use crate::{
        ExecutionRuntimeContext,
        channel::{Receiver, create_channel},
        join::{join_operator::ProbeOutput, probe::ProbeExecutionContext},
        pipeline::{MorselSizeRequirement, NodeName, PipelineNode},
        resource_manager::get_or_init_memory_manager,
        runtime_stats::RuntimeStatsManager,
    };

    #[derive(Default)]
    struct TestJoinCounters {
        make_build_state: AtomicUsize,
        build: AtomicUsize,
        finalize_build: AtomicUsize,
        probe: AtomicUsize,
    }

    #[derive(Clone)]
    struct TestJoinOperator {
        counters: Arc<TestJoinCounters>,
    }

    impl JoinOperator for TestJoinOperator {
        type BuildState = usize;
        type FinalizedBuildState = usize;
        type ProbeState = usize;

        fn build(
            &self,
            input: MicroPartition,
            state: Self::BuildState,
            _spawner: &ExecutionTaskSpawner,
        ) -> crate::join::join_operator::BuildStateResult<Self> {
            self.counters.build.fetch_add(1, Ordering::SeqCst);
            Ok(state + input.len()).into()
        }

        fn finalize_build(
            &self,
            state: Self::BuildState,
        ) -> crate::join::join_operator::FinalizeBuildResult<Self> {
            self.counters.finalize_build.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        }

        fn make_build_state(&self) -> DaftResult<Self::BuildState> {
            self.counters
                .make_build_state
                .fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }

        fn make_probe_state(
            &self,
            finalized_build_state: Self::FinalizedBuildState,
        ) -> Self::ProbeState {
            finalized_build_state
        }

        fn probe(
            &self,
            _input: MicroPartition,
            state: Self::ProbeState,
            _spawner: &ExecutionTaskSpawner,
        ) -> crate::join::join_operator::ProbeResult<Self> {
            self.counters.probe.fetch_add(1, Ordering::SeqCst);
            Ok((state, ProbeOutput::NeedMoreInput(None))).into()
        }

        fn finalize_probe(
            &self,
            _states: Vec<Self::ProbeState>,
            _spawner: &ExecutionTaskSpawner,
        ) -> crate::join::join_operator::ProbeFinalizeResult {
            Ok(None).into()
        }

        fn name(&self) -> NodeName {
            "TestJoin".into()
        }

        fn op_type(&self) -> NodeType {
            NodeType::HashJoin
        }

        fn multiline_display(&self) -> Vec<String> {
            vec!["TestJoin".to_string()]
        }

        fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
            Some(MorselSizeRequirement::Flexible(
                0,
                NonZeroUsize::new(1).unwrap(),
            ))
        }

        fn needs_probe_finalization(&self) -> bool {
            false
        }
    }

    struct DummyPipelineNode {
        node_info: Arc<NodeInfo>,
    }

    impl TreeDisplay for DummyPipelineNode {
        fn display_as(&self, _level: DisplayLevel) -> String {
            "DummyPipelineNode".to_string()
        }

        fn repr_json(&self) -> serde_json::Value {
            serde_json::json!({
                "id": self.node_id(),
                "name": self.name(),
            })
        }

        fn get_children(&self) -> Vec<&dyn TreeDisplay> {
            vec![]
        }
    }

    impl PipelineNode for DummyPipelineNode {
        fn children(&self) -> Vec<&dyn PipelineNode> {
            vec![]
        }

        fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
            vec![]
        }

        fn name(&self) -> Arc<str> {
            self.node_info.name.clone()
        }

        fn propagate_morsel_size_requirement(
            &mut self,
            _downstream_requirement: crate::pipeline::MorselSizeRequirement,
            _default_requirement: crate::pipeline::MorselSizeRequirement,
        ) {
        }

        fn start(
            self: Box<Self>,
            _maintain_order: bool,
            _runtime_handle: &mut ExecutionRuntimeContext,
        ) -> crate::Result<Receiver<PipelineMessage>> {
            panic!("DummyPipelineNode::start should not be called in tests")
        }

        fn as_tree_display(&self) -> &dyn TreeDisplay {
            self
        }

        fn node_id(&self) -> usize {
            self.node_info.id
        }

        fn node_info(&self) -> Arc<NodeInfo> {
            self.node_info.clone()
        }
    }

    struct TestRunResult {
        counters: Arc<TestJoinCounters>,
    }

    fn make_node_info() -> Arc<NodeInfo> {
        Arc::new(NodeInfo {
            id: 0,
            name: Arc::from("TestJoin"),
            node_type: NodeType::HashJoin,
            node_category: NodeCategory::Intermediate,
            ..Default::default()
        })
    }

    fn make_stats_manager(node_info: Arc<NodeInfo>) -> RuntimeStatsManager {
        let pipeline: Box<dyn PipelineNode> = Box::new(DummyPipelineNode { node_info });
        RuntimeStatsManager::try_new(
            &tokio::runtime::Handle::current(),
            &pipeline,
            vec![],
            "".into(),
        )
        .unwrap()
    }

    fn make_task_spawner() -> ExecutionTaskSpawner {
        ExecutionTaskSpawner::new(
            get_compute_runtime(),
            get_or_init_memory_manager().clone(),
            Span::none(),
        )
    }

    async fn wait_for_flushes(
        receiver: &mut Receiver<PipelineMessage>,
        expected_input_ids: &[InputId],
    ) {
        let mut received = Vec::with_capacity(expected_input_ids.len());
        timeout(Duration::from_secs(2), async {
            while received.len() < expected_input_ids.len() {
                match receiver.recv().await {
                    Some(PipelineMessage::Flush(input_id)) => received.push(input_id),
                    Some(PipelineMessage::Morsel { .. }) => {}
                    None => panic!("output channel closed before all flushes were received"),
                }
            }
        })
        .await
        .unwrap();

        let mut expected = expected_input_ids.to_vec();
        expected.sort_unstable();
        received.sort_unstable();
        assert_eq!(received, expected);
    }

    async fn send_input(sender: &crate::channel::Sender<PipelineMessage>, input_id: InputId) {
        sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: MicroPartition::empty(None),
            })
            .await
            .unwrap();
        sender.send(PipelineMessage::Flush(input_id)).await.unwrap();
    }

    async fn run_join_scenario(
        shared_build_key: Option<String>,
        phases: Vec<Vec<InputId>>,
    ) -> TestRunResult {
        let counters = Arc::new(TestJoinCounters::default());
        let op = Arc::new(TestJoinOperator {
            counters: counters.clone(),
        });
        let build_state_bridge = Arc::new(BuildStateBridge::new());
        let node_info = make_node_info();
        let meter = Meter::test_scope("broadcast_join_shared_build");
        let stats_manager = make_stats_manager(node_info.clone());
        let stats_handle = stats_manager.handle();

        let build_ctx = BuildExecutionContext::new(
            op.clone(),
            make_task_spawner(),
            build_state_bridge.clone(),
            shared_build_key.clone(),
            stats_handle.clone(),
            node_info.id,
            meter.clone(),
            node_info.clone(),
        );
        let (build_tx, build_rx) = create_channel(16);

        let (output_tx, mut output_rx) = create_channel(16);
        let probe_ctx = ProbeExecutionContext::new(
            op,
            make_task_spawner(),
            make_task_spawner(),
            output_tx,
            build_state_bridge,
            shared_build_key,
            false,
            stats_handle,
            node_info.id,
            meter,
            node_info,
        );
        let (probe_tx, probe_rx) = create_channel(16);

        let build_task = tokio::spawn(async move { build_ctx.process_build_input(build_rx).await });
        let probe_task = tokio::spawn(async move { probe_ctx.process_probe_input(probe_rx).await });

        for phase in &phases {
            for &input_id in phase {
                send_input(&probe_tx, input_id).await;
            }
            for &input_id in phase {
                send_input(&build_tx, input_id).await;
            }
            wait_for_flushes(&mut output_rx, phase).await;
        }

        drop(build_tx);
        drop(probe_tx);

        build_task.await.unwrap().unwrap();
        probe_task.await.unwrap().unwrap();
        stats_manager.finish(QueryEndState::Finished).await;

        TestRunResult { counters }
    }

    #[tokio::test]
    async fn test_shared_build_key_reuses_build_state_across_inputs() {
        let result =
            run_join_scenario(Some("broadcast_join:test".to_string()), vec![vec![1, 2]]).await;

        assert_eq!(result.counters.make_build_state.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.build.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.finalize_build.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.probe.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_late_subscriber_uses_ready_build_state() {
        let result = run_join_scenario(
            Some("broadcast_join:test".to_string()),
            vec![vec![1], vec![2]],
        )
        .await;

        assert_eq!(result.counters.make_build_state.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.build.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.finalize_build.load(Ordering::SeqCst), 1);
        assert_eq!(result.counters.probe.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_without_shared_build_key_builds_per_input() {
        let result = run_join_scenario(None, vec![vec![1, 2]]).await;

        assert_eq!(result.counters.make_build_state.load(Ordering::SeqCst), 2);
        assert_eq!(result.counters.build.load(Ordering::SeqCst), 2);
        assert_eq!(result.counters.finalize_build.load(Ordering::SeqCst), 2);
        assert_eq!(result.counters.probe.load(Ordering::SeqCst), 2);
    }
}
