mod process_stats;
mod progress_bar;
mod values;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use daft_common::error::DaftResult;
use daft_common::metrics::{
    NodeID, QueryEndState, QueryID, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl,
};
use daft_common::runtime::RuntimeTask;
use daft_context::{
    Subscriber,
    subscribers::{
        event_header,
        events::{
            Event, ExecEndEvent, ExecStartEvent, OperatorEndEvent, OperatorMeta,
            OperatorStartEvent, ProcessStatsEvent, StatsEvent,
        },
    },
};
use daft_common::treenode::{TreeNode, TreeNodeRecursion};
use daft_local_plan::{ExecutionStats, InputId};
use progress_bar::{ProgressBar, make_progress_bar_manager};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    time::interval,
};
pub use values::{DefaultRuntimeStats, RuntimeStats};

use crate::pipeline::PipelineNode;

/// Message type for the stats manager channel.
#[allow(dead_code)]
pub enum StatsManagerMessage {
    NodeEvent(usize, bool),
    RegisterRuntimeStats(NodeID, InputId, Arc<dyn RuntimeStats>),
    TakeInputSnapshot(InputId, oneshot::Sender<ExecutionStats>),
}

fn should_enable_process_monitor() -> bool {
    if let Ok(val) = std::env::var("DAFT_PROCESS_MONITOR_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_PROCESS_MONITOR_ENABLED=true
    }
}

enum ProgressBarMode {
    Disabled,
    Enabled,
    Persist,
}

fn progress_bar_mode(is_flotilla_worker: bool) -> ProgressBarMode {
    if is_flotilla_worker {
        return ProgressBarMode::Disabled;
    }
    match std::env::var("DAFT_PROGRESS_BAR")
        .as_deref()
        .map(str::trim)
        .unwrap_or("true")
    {
        val if val.eq_ignore_ascii_case("persist") => ProgressBarMode::Persist,
        val if matches!(val.to_lowercase().as_str(), "1" | "true") => ProgressBarMode::Enabled,
        _ => ProgressBarMode::Disabled,
    }
}

#[derive(Clone)]
pub struct RuntimeStatsManagerHandle {
    tx: Arc<mpsc::UnboundedSender<StatsManagerMessage>>,
    // Final per-input snapshots are retained here after the stats manager exits so
    // callers can still retrieve metrics during executor teardown.
    finished_snapshots: Arc<Mutex<Option<HashMap<InputId, ExecutionStats>>>>,
}

impl RuntimeStatsManagerHandle {
    pub fn activate_node(&self, node_id: usize) {
        if let Err(e) = self.tx.send(StatsManagerMessage::NodeEvent(node_id, true)) {
            log::warn!(
                "Unable to activate node: {node_id} because RuntimeStatsManager was already finished: {e}"
            );
        }
    }

    pub fn finalize_node(&self, node_id: usize) {
        if let Err(e) = self.tx.send(StatsManagerMessage::NodeEvent(node_id, false)) {
            log::warn!(
                "Unable to finalize node: {node_id} because RuntimeStatsManager was already finished: {e}"
            );
        }
    }

    /// Register a RuntimeStats with the stats manager for a given node and input.
    pub fn register_runtime_stats(
        &self,
        node_id: NodeID,
        input_id: InputId,
        stats: Arc<dyn RuntimeStats>,
    ) {
        if let Err(e) = self.tx.send(StatsManagerMessage::RegisterRuntimeStats(
            node_id, input_id, stats,
        )) {
            log::warn!(
                "Unable to register runtime stats for node {node_id}, input {input_id}: {e}"
            );
        }
    }

    /// Take a snapshot scoped to the given input_id.
    ///
    /// If the stats manager has already exited, fall back to the finalized snapshot
    /// that was captured during shutdown. The snapshot is removed on read so cached
    /// plans do not retain metrics for completed inputs indefinitely.
    #[allow(dead_code)]
    pub async fn take_input_snapshot(&self, input_id: InputId) -> DaftResult<ExecutionStats> {
        if let Some(stats) = self
            .finished_snapshots
            .lock()
            .expect("finished_snapshots lock poisoned")
            .as_mut()
            .and_then(|snapshots| snapshots.remove(&input_id))
        {
            return Ok(stats);
        }

        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StatsManagerMessage::TakeInputSnapshot(input_id, tx))
            .map_err(|_| {
                daft_common::error::DaftError::InternalError(
                    "RuntimeStatsManager was already finished; cannot take input snapshot"
                        .to_string(),
                )
            })?;
        rx.await.map_err(|_| {
            daft_common::error::DaftError::InternalError(
                "RuntimeStatsManager task ended before responding to snapshot request".to_string(),
            )
        })
    }
}

/// Event handler for RuntimeStats
/// The event handler contains a vector of subscribers
/// When a new event is broadcast, `RuntimeStatsEventHandler` manages notifying the subscribers.
///
/// For a given event, the event handler ensures that the subscribers only get the latest event at a frequency of once every 500ms
/// This prevents the subscribers from being overwhelmed by too many events.
pub struct RuntimeStatsManager {
    handle: RuntimeStatsManagerHandle,
    finish_tx: oneshot::Sender<QueryEndState>,
    stats_manager_task: RuntimeTask<()>,
}

impl std::fmt::Debug for RuntimeStatsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeStatsEventHandler")
    }
}

impl RuntimeStatsManager {
    fn flush_and_finalize_node(
        query_id: &QueryID,
        node_id: NodeID,
        input_stats: &HashMap<(NodeID, InputId), Arc<dyn RuntimeStats>>,
        progress_bar: Option<&dyn ProgressBar>,
        subscribers: &[Arc<dyn Subscriber>],
        err_context: &'static str,
        operators: &HashMap<NodeID, Arc<OperatorMeta>>,
    ) {
        let snapshot = aggregate_node_stats(input_stats, node_id);

        if let Some(progress_bar) = &progress_bar
            && let Some(snapshot) = &snapshot
        {
            progress_bar.finalize_node(node_id, snapshot);
        }

        let Some(operator_meta) = operators.get(&node_id) else {
            log::warn!("Unknown node_id {node_id} in operators during {err_context}, skipping");
            return;
        };

        if let Some(snapshot) = snapshot {
            let stats_event = Event::Stats(StatsEvent {
                header: event_header(query_id.clone()),
                stats: Arc::new(vec![(node_id, snapshot.to_stats())]),
            });
            dispatch_event(subscribers, &stats_event, "flush node final stats");

            let end_event = Event::OperatorEnd(OperatorEndEvent {
                header: event_header(query_id.clone()),
                operator: operator_meta.clone(),
            });
            dispatch_event(subscribers, &end_event, "flush node operator end");
        } else {
            let end_event = Event::OperatorEnd(OperatorEndEvent {
                header: event_header(query_id.clone()),
                operator: operator_meta.clone(),
            });
            dispatch_event(subscribers, &end_event, "flush node operator end");
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn try_new(
        handle: &Handle,
        pipeline: &Box<dyn PipelineNode>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        query_id: QueryID,
        is_flotilla_worker: bool,
    ) -> DaftResult<Self> {
        // Construct mapping between node id and their node info
        let mut node_info_map = HashMap::new();
        let mut operator_meta_map: HashMap<NodeID, Arc<OperatorMeta>> = HashMap::new();
        let _ = pipeline.apply(|node| {
            let node_info = node.node_info();
            node_info_map.insert(node_info.id, node_info.clone());
            operator_meta_map.insert(
                node_info.id,
                Arc::new(OperatorMeta::from(node_info.as_ref())),
            );
            Ok(TreeNodeRecursion::Continue)
        });

        let query_plan = pipeline.repr_json();
        let serialized_plan: Arc<str> = serde_json::to_string(&query_plan)
            .expect("Failed to serialize physical plan")
            .into();

        let exec_start_event = Event::ExecStart(ExecStartEvent {
            header: event_header(query_id.clone()),
            physical_plan: serialized_plan,
        });
        dispatch_event(&subscribers, &exec_start_event, "notify execution start");

        let progress_bar = match progress_bar_mode(is_flotilla_worker) {
            ProgressBarMode::Disabled => None,
            ProgressBarMode::Enabled => Some(make_progress_bar_manager(&node_info_map, false)),
            ProgressBarMode::Persist => Some(make_progress_bar_manager(&node_info_map, true)),
        };

        let throttle_interval = Duration::from_millis(200);
        let enable_process_monitor = should_enable_process_monitor();
        Ok(Self::new_impl(
            handle,
            query_id,
            query_plan,
            subscribers,
            progress_bar,
            node_info_map,
            throttle_interval,
            enable_process_monitor,
            operator_meta_map,
        ))
    }

    // Mostly used for testing purposes so we can inject our own subscribers and throttling interval
    #[allow(clippy::too_many_arguments)]
    fn new_impl(
        handle: &Handle,
        query_id: QueryID,
        query_plan: serde_json::Value,
        subscribers: Vec<Arc<dyn Subscriber>>,
        progress_bar: Option<Box<dyn ProgressBar>>,
        node_info_map: HashMap<NodeID, Arc<NodeInfo>>,
        throttle_interval: Duration,
        enable_process_monitor: bool,
        operators: HashMap<NodeID, Arc<OperatorMeta>>,
    ) -> Self {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<StatsManagerMessage>();
        let node_tx = Arc::new(node_tx);

        // Preserve finalized per-input stats after shutdown so executor teardown can
        // still retrieve metrics even after the manager task has exited.
        let finished_snapshots = Arc::new(Mutex::new(None));
        let (finish_tx, mut finish_rx) = oneshot::channel::<QueryEndState>();

        let mut process_stats = if enable_process_monitor {
            let meter = daft_common::metrics::Meter::global_scope("daft-process-monitor");
            process_stats::ProcessStatsCollector::new(&meter)
        } else {
            None
        };

        let finished_snapshots_for_task = finished_snapshots.clone();
        let event_loop = async move {
            let mut interval = interval(throttle_interval);
            let mut active_nodes = HashSet::with_capacity(node_info_map.len());
            // Per-input_id stats: (node_id, input_id) -> RuntimeStats
            let mut input_stats: HashMap<(NodeID, InputId), Arc<dyn RuntimeStats>> = HashMap::new();
            // Reuse container for ticks
            let mut snapshot_container = Vec::with_capacity(node_info_map.len());

            loop {
                tokio::select! {
                    biased;
                    Some(msg) = node_rx.recv() => {
                        match msg {
                            StatsManagerMessage::NodeEvent(node_id, is_initialize) => {
                                if is_initialize && active_nodes.insert(node_id) {
                                    if let Some(progress_bar) = &progress_bar {
                                        progress_bar.initialize_node(node_id);
                                    }

                                    let Some(operator_meta) = operators.get(&node_id) else {
                                        log::warn!("Unknown node_id {node_id} in operators during on_start, skipping subscriber notification");
                                        continue;
                                    };

                                    let event = Event::OperatorStart(OperatorStartEvent {
                                        header: event_header(query_id.clone()),
                                        operator: operator_meta.clone(),
                                    });
                                    dispatch_event(&subscribers, &event, "notify operator start");
                                } else if !is_initialize && active_nodes.remove(&node_id) {
                                    Self::flush_and_finalize_node(
                                        &query_id,
                                        node_id,
                                        &input_stats,
                                        progress_bar.as_deref(),
                                    &subscribers,
                                    "finalize node",
                                    &operators,
                                    );
                                }
                            }
                            StatsManagerMessage::RegisterRuntimeStats(node_id, input_id, stats) => {
                                input_stats.insert((node_id, input_id), stats);
                            }
                            StatsManagerMessage::TakeInputSnapshot(input_id, respond_tx) => {
                                let mut result = Vec::new();
                                for (&(node_id, iid), stats) in &input_stats {
                                    if iid == input_id
                                        && let Some(node_info) = node_info_map.get(&node_id)
                                    {
                                        result.push((node_info.clone(), stats.flush()));
                                    }
                                }
                                let _ = respond_tx.send(ExecutionStats::new(query_id.clone(), result).with_query_plan(query_plan.clone()));
                            }
                        }
                    }

                    _ = &mut finish_rx => {
                        // Queries that terminate early (e.g. LIMIT) may still have active upstream nodes.
                        // Flush and finalize those nodes so subscribers and progress bars end in a consistent state.
                        for node_id in active_nodes.drain() {
                            Self::flush_and_finalize_node(
                                &query_id,
                                node_id,
                                &input_stats,
                                progress_bar.as_deref(),
                                &subscribers,
                                "finalize node during shutdown",
                                &operators,
                            );
                        }
                        break;
                    }

                    _ = interval.tick() => {
                        if let Some(ps) = &mut process_stats {
                            let ps_stats = ps.sample();
                            let event = Event::ProcessStats(ProcessStatsEvent {
                                header: event_header(query_id.clone()),
                                stats: ps_stats,
                            });
                            dispatch_event(&subscribers, &event, "notify process stats");
                        }

                        if active_nodes.is_empty() {
                            continue;
                        }

                        for node_id in &active_nodes {
                            if let Some(snapshot) = aggregate_node_stats(&input_stats, *node_id) {
                                if let Some(progress_bar) = &progress_bar {
                                    progress_bar.handle_event(*node_id, &snapshot);
                                }
                                snapshot_container.push((*node_id, snapshot.to_stats()));
                            }
                        }

                        if !snapshot_container.is_empty() {
                            let snapshot_container = Arc::new(std::mem::take(&mut snapshot_container));
                            let event = Event::Stats(StatsEvent {
                                header: event_header(query_id.clone()),
                                stats: snapshot_container.clone(),
                            });
                            dispatch_event(&subscribers, &event, "notify runtime stats");
                        }
                    }
                }
            }

            if let Some(progress_bar) = progress_bar
                && let Err(e) = progress_bar.finish()
            {
                log::warn!("Failed to finish progress bar: {}", e);
            }

            let mut snapshots_by_input: HashMap<InputId, Vec<(Arc<NodeInfo>, StatSnapshot)>> =
                HashMap::new();

            for ((node_id, input_id), stats) in input_stats {
                if let Some(node_info) = node_info_map.get(&node_id) {
                    snapshots_by_input
                        .entry(input_id)
                        .or_default()
                        .push((node_info.clone(), stats.flush()));
                }
            }

            let finished = snapshots_by_input
                .into_iter()
                .map(|(input_id, mut nodes)| {
                    nodes.sort_by_key(|(node_info, _)| node_info.id);
                    (
                        input_id,
                        ExecutionStats::new(query_id.clone(), nodes)
                            .with_query_plan(query_plan.clone()),
                    )
                })
                .collect();

            // Publish finalized snapshots before exiting so callers can retrieve per-input
            // metrics after the stats manager task has shut down.
            *finished_snapshots_for_task
                .lock()
                .expect("finished_snapshots lock poisoned") = Some(finished);

            let exec_end_event = Event::ExecEnd(ExecEndEvent {
                header: event_header(query_id.clone()),
                duration_ms: None,
            });
            dispatch_event(&subscribers, &exec_end_event, "notify execution end");
        };

        let task_handle = RuntimeTask::new(handle, event_loop);
        Self {
            handle: RuntimeStatsManagerHandle {
                tx: node_tx,
                finished_snapshots,
            },
            finish_tx,
            stats_manager_task: task_handle,
        }
    }

    pub fn handle(&self) -> RuntimeStatsManagerHandle {
        self.handle.clone()
    }

    pub async fn finish(self, status: QueryEndState) {
        self.finish_tx
            .send(status)
            .expect("The finish_tx channel was closed");
        self.stats_manager_task
            .await
            .expect("The stats_manager_task panicked");
    }
}

fn dispatch_event(subscribers: &[Arc<dyn Subscriber>], event: &Event, err_context: &'static str) {
    for subscriber in subscribers {
        if let Err(e) = subscriber.on_event(event.clone()) {
            log::error!("Failed to {}: {}", err_context, e);
        }
    }
}

/// Aggregate stats for a given node_id across all input_ids.
fn aggregate_node_stats(
    input_stats: &HashMap<(NodeID, InputId), Arc<dyn RuntimeStats>>,
    node_id: NodeID,
) -> Option<StatSnapshot> {
    let mut aggregated: Option<StatSnapshot> = None;
    for ((nid, _), stats) in input_stats {
        if *nid == node_id {
            let snapshot = stats.snapshot();
            aggregated = Some(match aggregated {
                Some(acc) => acc.merge(&snapshot),
                None => snapshot,
            });
        }
    }
    aggregated
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, atomic::AtomicU64},
    };

    use daft_common::error::DaftResult;
    use daft_common::metrics::{
        DURATION_KEY, Meter, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, Stats,
    };
    use daft_context::Subscriber;
    use tokio::time::{Duration, sleep};

    use super::*;

    fn make_stats_manager(
        subscribers: Vec<Arc<dyn Subscriber>>,
        node_stat: Arc<dyn RuntimeStats>,
        throttle_interval: Duration,
        query_id: &str,
        enable_process_monitor: bool,
    ) -> RuntimeStatsManager {
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            query_id.into(),
            serde_json::Value::Null,
            subscribers,
            None,
            HashMap::from([(0, Arc::new(NodeInfo::default()))]),
            throttle_interval,
            enable_process_monitor,
            HashMap::from([(0, Arc::new(OperatorMeta::from(&NodeInfo::default())))]),
        );
        // Register stats via the handle (mimics what nodes do in start())
        stats_manager
            .handle()
            .register_runtime_stats(0, 0, node_stat);
        stats_manager
    }

    #[derive(Debug)]
    struct MockState {
        total_calls: AtomicU64,
        event: Mutex<Option<Stats>>,
    }

    impl MockState {
        fn get_total_calls(&self) -> u64 {
            self.total_calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn get_latest_event(&self) -> Stats {
            self.event.lock().unwrap().clone().expect("No event")
        }
    }

    #[derive(Debug)]
    struct MockSubscriber {
        pub state: Arc<MockState>,
    }

    impl MockSubscriber {
        fn new() -> Self {
            Self {
                state: Arc::new(MockState {
                    total_calls: AtomicU64::new(0),
                    event: Mutex::new(None),
                }),
            }
        }
    }

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    impl Subscriber for MockSubscriber {
        fn on_event(&self, event: Event) -> DaftResult<()> {
            if let Event::Stats(e) = event {
                self.state
                    .total_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for (_, snapshot) in e.stats.iter() {
                    *self.state.event.lock().unwrap() = Some(snapshot.clone());
                }
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_interval_respected() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let mock_state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = make_stats_manager(
            vec![mock_subscriber],
            node_stat.clone(),
            throttle_interval,
            "test_ps_disabled",
            false,
        );
        let handle = stats_manager.handle();

        // Activate the node
        handle.activate_node(0);

        // Send first event
        node_stat.add_rows_in(100);
        assert_eq!(
            mock_state.get_total_calls(),
            0,
            "No materialized events should be sent yet"
        );

        // Send second event rapidly (within throttle interval)
        node_stat.add_rows_in(100);
        sleep(Duration::from_millis(50)).await;

        // Should only get 1 call due to throttling
        assert_eq!(
            mock_state.get_total_calls(),
            1,
            "Rapid events should be throttled to a single call"
        );
        assert_eq!(
            mock_state.get_latest_event()[1],
            (ROWS_IN_KEY.into(), Stat::Count(200))
        );

        // Wait for throttle interval to pass, then send another event
        node_stat.add_rows_in(300);
        sleep(Duration::from_millis(50)).await;

        // Should now get a second call
        assert_eq!(
            mock_state.get_total_calls(),
            2,
            "Event after throttle interval should trigger a new call"
        );
        assert_eq!(
            mock_state.get_latest_event()[1],
            (ROWS_IN_KEY.into(), Stat::Count(500))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_multiple_subscribers_all_receive_events() {
        let subscriber1 = Arc::new(MockSubscriber::new());
        let subscriber2 = Arc::new(MockSubscriber::new());
        let state1 = subscriber1.state.clone();
        let state2 = subscriber2.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = make_stats_manager(
            vec![subscriber1, subscriber2],
            node_stat.clone(),
            throttle_interval,
            "test_query_id",
            false,
        );
        let handle = stats_manager.handle();

        handle.activate_node(0);

        node_stat.add_rows_in(100);
        sleep(Duration::from_millis(50)).await;

        // Both subscribers should receive the event
        assert_eq!(state1.get_total_calls(), 1);
        assert_eq!(state2.get_total_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_subscriber_error_doesnt_affect_others() {
        #[derive(Debug)]
        struct FailingSubscriber;

        impl Subscriber for FailingSubscriber {
            fn on_event(&self, event: Event) -> DaftResult<()> {
                if let Event::Stats(_) = event {
                    return Err(daft_common::error::DaftError::InternalError(
                        "Test error".to_string(),
                    ));
                }
                Ok(())
            }
        }

        let failing_subscriber = Arc::new(FailingSubscriber);
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = make_stats_manager(
            vec![failing_subscriber, mock_subscriber],
            node_stat.clone(),
            throttle_interval,
            "test_query_id",
            false,
        );
        let handle = stats_manager.handle();

        handle.activate_node(0);
        node_stat.add_rows_in(100);
        sleep(Duration::from_millis(50)).await;

        // Mock subscriber should still receive event despite other failing
        assert_eq!(state.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_runtime_stats_context_operations() {
        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        ));

        // Test initial state
        let StatSnapshot::Default(stats) = node_stat.snapshot() else {
            panic!("Expected DefaultSnapshot");
        };
        assert_eq!(stats.rows_in, 0);
        assert_eq!(stats.rows_out, 0);

        // Test incremental updates
        node_stat.add_rows_in(100);
        node_stat.add_rows_in(50);
        let StatSnapshot::Default(stats) = node_stat.snapshot() else {
            panic!("Expected DefaultSnapshot");
        };
        assert_eq!(stats.rows_in, 150);
        assert_eq!(stats.rows_out, 0);

        node_stat.add_rows_out(75);
        let StatSnapshot::Default(stats) = node_stat.snapshot() else {
            panic!("Expected DefaultSnapshot");
        };
        assert_eq!(stats.rows_in, 150);
        assert_eq!(stats.rows_out, 75);
    }

    #[tokio::test(start_paused = true)]
    async fn test_events_without_init() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = make_stats_manager(
            vec![mock_subscriber],
            node_stat.clone(),
            throttle_interval,
            "test_ps_disabled",
            false,
        );
        let handle = stats_manager.handle();

        // No events yet because no nodes are initialized
        node_stat.add_rows_in(100);
        sleep(Duration::from_millis(50)).await;
        assert_eq!(state.get_total_calls(), 0);

        // Activate the node
        handle.activate_node(0);
        sleep(Duration::from_millis(50)).await;

        // Now we should get an event
        assert_eq!(state.get_total_calls(), 1);
        let event = state.get_latest_event();
        assert_eq!(event[1], (ROWS_IN_KEY.into(), Stat::Count(100)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_final_event_before_interval() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        // Use 500ms for the throttle interval.
        let throttle_interval = Duration::from_millis(500);
        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;
        let stats_manager = make_stats_manager(
            vec![mock_subscriber],
            node_stat.clone(),
            throttle_interval,
            "test_ps_disabled",
            false,
        );
        let handle = stats_manager.handle();

        handle.activate_node(0);

        // Simulate a fast query that completes within the throttle interval (500ms)
        node_stat.add_rows_in(100);
        node_stat.add_rows_out(50);
        node_stat.add_duration_us(1000);

        handle.finalize_node(0);

        // Wait less than throttle interval (500ms) but enough for processing (1ms)
        sleep(Duration::from_millis(10)).await;

        // The final event should still be observed even though throttle interval wasn't met
        assert_eq!(state.get_total_calls(), 1);

        let event = state.get_latest_event();
        assert_eq!(
            event[0],
            (
                DURATION_KEY.into(),
                Stat::Duration(Duration::from_millis(1))
            )
        );
        assert_eq!(event[1], (ROWS_IN_KEY.into(), Stat::Count(100)));
        assert_eq!(event[2], (ROWS_OUT_KEY.into(), Stat::Count(50)));
    }

    /// Mock subscriber that tracks on_process_stats calls.
    #[derive(Debug)]
    struct ProcessStatsMockSubscriber {
        process_stats_calls: AtomicU64,
    }

    impl ProcessStatsMockSubscriber {
        fn new() -> Self {
            Self {
                process_stats_calls: AtomicU64::new(0),
            }
        }

        fn get_process_stats_calls(&self) -> u64 {
            self.process_stats_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl Subscriber for ProcessStatsMockSubscriber {
        fn on_event(&self, event: Event) -> DaftResult<()> {
            match event {
                Event::ProcessStats(_) => {
                    self.process_stats_calls
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_process_stats_delivered_to_subscriber_when_enabled() {
        let subscriber = Arc::new(ProcessStatsMockSubscriber::new());
        let throttle_interval = Duration::from_millis(50);
        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_process_stats_enabled"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;

        let stats_manager = make_stats_manager(
            vec![subscriber.clone()],
            node_stat.clone(),
            throttle_interval,
            "test_ps_enabled",
            true,
        );

        // Let a few ticks fire
        sleep(Duration::from_millis(150)).await;

        assert!(
            subscriber.get_process_stats_calls() >= 1,
            "on_process_stats should be called at least once, got {}",
            subscriber.get_process_stats_calls()
        );

        stats_manager.finish(QueryEndState::Finished).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_process_stats_not_delivered_when_disabled() {
        let subscriber = Arc::new(ProcessStatsMockSubscriber::new());
        let throttle_interval = Duration::from_millis(50);
        let node_stat = Arc::new(DefaultRuntimeStats::new(
            &Meter::test_scope("test_process_stats_disabled"),
            &node_info_from_id(0),
        )) as Arc<dyn RuntimeStats>;

        let stats_manager = make_stats_manager(
            vec![subscriber.clone()],
            node_stat.clone(),
            throttle_interval,
            "test_ps_disabled",
            false,
        );
        // Let a few ticks fire
        sleep(Duration::from_millis(150)).await;

        assert_eq!(
            subscriber.get_process_stats_calls(),
            0,
            "on_process_stats should not be called when monitor is disabled"
        );

        stats_manager.finish(QueryEndState::Finished).await;
    }
}
