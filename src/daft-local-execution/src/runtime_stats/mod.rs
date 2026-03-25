mod process_stats;
mod progress_bar;
mod values;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common_error::DaftResult;
use common_metrics::{NodeID, QueryEndState, QueryID, ops::NodeInfo, snapshot::StatSnapshotImpl};
use common_runtime::RuntimeTask;
use daft_context::{
    Subscriber,
    subscribers::events::{
        EventHeader, OperatorEndEvent, OperatorMeta, OperatorStartEvent, StatsEvent,
    },
};
use daft_dsl::common_treenode::{TreeNode, TreeNodeRecursion};
use daft_local_plan::ExecutionStats;
use futures::future;
use progress_bar::{ProgressBar, make_progress_bar_manager};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    time::interval,
};
pub use values::{DefaultRuntimeStats, RuntimeStats};

use crate::pipeline::PipelineNode;

fn now_epoch_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_secs_f64()
}

fn should_enable_process_monitor() -> bool {
    if let Ok(val) = std::env::var("DAFT_PROCESS_MONITOR_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_PROCESS_MONITOR_ENABLED=true
    }
}

fn should_enable_progress_bar() -> bool {
    if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
        return false;
    }
    if let Ok(val) = std::env::var("DAFT_PROGRESS_BAR") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        true // Return true when env var is not set
    }
}

#[derive(Clone)]
pub struct RuntimeStatsManagerHandle(Arc<mpsc::UnboundedSender<(usize, bool)>>);

impl RuntimeStatsManagerHandle {
    pub fn activate_node(&self, node_id: usize) {
        if let Err(e) = self.0.send((node_id, true)) {
            log::warn!(
                "Unable to activate node: {node_id} because RuntimeStatsManager was already finished: {e}"
            );
        }
    }

    pub fn finalize_node(&self, node_id: usize) {
        if let Err(e) = self.0.send((node_id, false)) {
            log::warn!(
                "Unable to finalize node: {node_id} because RuntimeStatsManager was already finished: {e}"
            );
        }
    }
}

/// Event handler for RuntimeStats
/// The event handler contains a vector of subscribers
/// When a new event is broadcast, `RuntimeStatsEventHandler` manages notifying the subscribers.
///
/// For a given event, the event handler ensures that the subscribers only get the latest event at a frequency of once every 500ms
/// This prevents the subscribers from being overwhelmed by too many events.
pub struct RuntimeStatsManager {
    node_tx: Arc<mpsc::UnboundedSender<(usize, bool)>>,
    finish_tx: oneshot::Sender<QueryEndState>,
    stats_manager_task: RuntimeTask<ExecutionStats>,
}

impl std::fmt::Debug for RuntimeStatsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeStatsEventHandler")
    }
}

impl RuntimeStatsManager {
    async fn flush_and_finalize_node(
        query_id: &QueryID,
        node_id: NodeID,
        node_map: &HashMap<NodeID, (Arc<NodeInfo>, Arc<dyn RuntimeStats>)>,
        progress_bar: Option<&dyn ProgressBar>,
        subscribers: &[Arc<dyn Subscriber>],
        err_context: &'static str,
        operators: &HashMap<NodeID, Arc<OperatorMeta>>,
    ) {
        let runtime_stats = &node_map[&node_id].1;
        let snapshot = runtime_stats.flush();

        if let Some(progress_bar) = progress_bar {
            progress_bar.finalize_node(node_id, &snapshot);
        }

        let Some(operator_meta) = operators.get(&node_id) else {
            log::warn!("Unknown node_id {node_id} in operators during {err_context}, skipping");
            return;
        };

        let stats_event = Arc::new(StatsEvent {
            header: EventHeader {
                query_id: query_id.clone(),
                timestamp_epoch_secs: now_epoch_secs(),
            },
            stats: Arc::new(vec![(node_id, snapshot.to_stats())]),
        });

        let end_event = Arc::new(OperatorEndEvent {
            header: EventHeader {
                query_id: query_id.clone(),
                timestamp_epoch_secs: now_epoch_secs(),
            },
            operator: operator_meta.clone(),
        });
        for res in future::join_all(subscribers.iter().map(|subscriber| {
            let stats_event = stats_event.clone();
            let end_event = end_event.clone();
            async move {
                subscriber.on_stats(stats_event).await?;
                subscriber.on_operator_end(end_event).await
            }
        }))
        .await
        {
            if let Err(e) = res {
                log::error!("Failed to {}: {}", err_context, e);
            }
        }
    }

    #[allow(clippy::borrowed_box)]
    pub fn try_new(
        handle: &Handle,
        pipeline: &Box<dyn PipelineNode>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        query_id: QueryID,
    ) -> DaftResult<Self> {
        // Construct mapping between node id and their node info and runtime stats
        let mut node_map = HashMap::new();
        let mut node_info_map = HashMap::new();
        let mut operator_meta_map: HashMap<NodeID, Arc<OperatorMeta>> = HashMap::new();
        let _ = pipeline.apply(|node| {
            let node_info = node.node_info();
            let runtime_stats = node.runtime_stats();
            node_info_map.insert(node_info.id, node_info.clone());
            node_map.insert(node_info.id, (node_info.clone(), runtime_stats));
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
        for subscriber in &subscribers {
            subscriber.on_exec_start(query_id.clone(), serialized_plan.clone())?;
        }

        let progress_bar = if should_enable_progress_bar() {
            Some(make_progress_bar_manager(&node_info_map))
        } else {
            None
        };

        let throttle_interval = Duration::from_millis(200);
        let enable_process_monitor = should_enable_process_monitor();
        Ok(Self::new_impl(
            handle,
            query_id,
            query_plan,
            subscribers,
            progress_bar,
            node_map,
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
        node_map: HashMap<NodeID, (Arc<NodeInfo>, Arc<dyn RuntimeStats>)>,
        throttle_interval: Duration,
        enable_process_monitor: bool,
        operators: HashMap<NodeID, Arc<OperatorMeta>>,
    ) -> Self {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<(usize, bool)>();
        let node_tx = Arc::new(node_tx);
        let (finish_tx, mut finish_rx) = oneshot::channel::<QueryEndState>();

        let mut process_stats = if enable_process_monitor {
            let meter = common_metrics::Meter::global_scope("daft-process-monitor");
            process_stats::ProcessStatsCollector::new(&meter)
        } else {
            None
        };

        let event_loop = async move {
            let mut interval = interval(throttle_interval);
            let mut active_nodes = HashSet::with_capacity(node_map.len());
            // Reuse container for ticks
            let mut snapshot_container = Vec::with_capacity(node_map.len());

            loop {
                tokio::select! {
                    biased;
                    Some((node_id, is_initialize)) = node_rx.recv() => {
                        if is_initialize && active_nodes.insert(node_id) {
                            if let Some(progress_bar) = &progress_bar {
                                progress_bar.initialize_node(node_id);
                            }

                            let Some(operator_meta) = operators.get(&node_id) else {
                                log::warn!("Unknown node_id {node_id} in operators during on_start, skipping subscriber notification");
                                continue;
                            };

                            let event = Arc::new(OperatorStartEvent {
                                header: EventHeader {
                                    query_id: query_id.clone(),
                                    timestamp_epoch_secs: now_epoch_secs(),
                                },
                                operator: operator_meta.clone(),
                            });

                            for res in future::join_all(subscribers.iter().map(|subscriber| {
                                subscriber.on_operator_start(event.clone())
                            })).await {
                                if let Err(e) = res {
                                    log::error!("Failed to initialize node: {}", e);
                                }
                            }
                        } else if !is_initialize && active_nodes.remove(&node_id) {
                            Self::flush_and_finalize_node(
                                &query_id,
                                node_id,
                                &node_map,
                                progress_bar.as_deref(),
                                &subscribers,
                                "finalize node",
                                &operators,
                            )
                            .await;
                        }
                    }

                    _ = &mut finish_rx => {
                        // Queries that terminate early (e.g. LIMIT) may still have active upstream nodes.
                        // Flush and finalize those nodes so subscribers and progress bars end in a consistent state.
                        for node_id in active_nodes.drain() {
                            Self::flush_and_finalize_node(
                                &query_id,
                                node_id,
                                &node_map,
                                progress_bar.as_deref(),
                                &subscribers,
                                "finalize node during shutdown",
                                &operators,
                            )
                            .await;
                        }
                        break;
                    }

                    _ = interval.tick() => {
                        if let Some(ps) = &mut process_stats {
                            let ps_stats = ps.sample();
                            for res in future::join_all(subscribers.iter().map(|subscriber| {
                                subscriber.on_process_stats(query_id.clone(), ps_stats.clone())
                            })).await {
                                if let Err(e) = res {
                                    log::error!("Failed to emit process stats: {}", e);
                                }
                            }
                        }

                        if active_nodes.is_empty() {
                            continue;
                        }

                        for node_id in &active_nodes {
                            let runtime_stats = &node_map[node_id].1;
                            let snapshot = runtime_stats.snapshot();
                            if let Some(progress_bar) = &progress_bar {
                                progress_bar.handle_event(*node_id, &snapshot);
                            }
                            snapshot_container.push((*node_id, snapshot.to_stats()));
                        }

                        let snapshot_container = Arc::new(std::mem::take(&mut snapshot_container));
                        let event = Arc::new(StatsEvent {
                            header: EventHeader {
                                query_id: query_id.clone(),
                                timestamp_epoch_secs: now_epoch_secs(),
                            },
                            stats: snapshot_container.clone(),
                        });
                        for res in future::join_all(subscribers.iter().map(|subscriber| {
                            subscriber.on_stats(event.clone())
                        })).await {
                            if let Err(e) = res {
                                log::error!("Failed to handle event: {}", e);
                            }
                        }
                    }
                }
            }

            if let Some(progress_bar) = progress_bar
                && let Err(e) = progress_bar.finish()
            {
                log::warn!("Failed to finish progress bar: {}", e);
            }

            for subscriber in subscribers {
                if let Err(e) = subscriber.on_exec_end(query_id.clone()).await {
                    log::error!("Failed to flush subscriber: {}", e);
                }
            }

            // Return the final stat snapshot for all nodes
            let mut final_snapshot = Vec::new();
            for (node_info, runtime_stats) in node_map.values() {
                let event = runtime_stats.flush();
                final_snapshot.push((node_info.clone(), event));
            }

            ExecutionStats::new(query_id, final_snapshot).with_query_plan(query_plan)
        };

        let task_handle = RuntimeTask::new(handle, event_loop);
        Self {
            node_tx,
            finish_tx,
            stats_manager_task: task_handle,
        }
    }

    pub fn handle(&self) -> RuntimeStatsManagerHandle {
        RuntimeStatsManagerHandle(self.node_tx.clone())
    }

    pub async fn finish(self, status: QueryEndState) -> ExecutionStats {
        self.finish_tx
            .send(status)
            .expect("The finish_tx channel was closed");
        self.stats_manager_task
            .await
            .expect("The finish_tx channel was closed")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, atomic::AtomicU64},
    };

    use async_trait::async_trait;
    use common_error::DaftResult;
    use common_metrics::{
        DURATION_KEY, Meter, QueryPlan, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, Stats,
    };
    use daft_context::{QueryMetadata, QueryResult, Subscriber};
    use daft_micropartition::MicroPartitionRef;
    use tokio::time::{Duration, sleep};

    use super::*;

    fn make_stats_manager(
        subscribers: Vec<Arc<dyn Subscriber>>,
        node_stat: Arc<dyn RuntimeStats>,
        throttle_interval: Duration,
        query_id: &str,
        enable_process_monitor: bool,
    ) -> RuntimeStatsManager {
        RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            query_id.into(),
            serde_json::Value::Null,
            subscribers,
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat))]),
            throttle_interval,
            enable_process_monitor,
            HashMap::from([(0, Arc::new(OperatorMeta::from(&NodeInfo::default())))]),
        )
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

    #[async_trait]
    impl Subscriber for MockSubscriber {
        fn on_query_start(&self, _: QueryID, _: Arc<QueryMetadata>) -> DaftResult<()> {
            Ok(())
        }
        fn on_query_end(&self, _: QueryID, _: QueryResult) -> DaftResult<()> {
            Ok(())
        }
        fn on_result_out(&self, _: QueryID, _: MicroPartitionRef) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_start(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_end(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
            Ok(())
        }
        fn on_exec_start(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
            Ok(())
        }

        async fn on_exec_end(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        async fn on_operator_start(&self, _: Arc<OperatorStartEvent>) -> DaftResult<()> {
            Ok(())
        }
        async fn on_operator_end(&self, _: Arc<OperatorEndEvent>) -> DaftResult<()> {
            Ok(())
        }

        async fn on_stats(&self, event: Arc<StatsEvent>) -> DaftResult<()> {
            self.state
                .total_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            for (_, snapshot) in event.stats.iter() {
                *self.state.event.lock().unwrap() = Some(snapshot.clone());
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

        #[async_trait]
        impl Subscriber for FailingSubscriber {
            fn on_query_start(&self, _: QueryID, _: Arc<QueryMetadata>) -> DaftResult<()> {
                Ok(())
            }
            fn on_query_end(&self, _: QueryID, _: QueryResult) -> DaftResult<()> {
                Ok(())
            }
            fn on_result_out(&self, _: QueryID, _: MicroPartitionRef) -> DaftResult<()> {
                Ok(())
            }
            fn on_optimization_start(&self, _: QueryID) -> DaftResult<()> {
                Ok(())
            }
            fn on_optimization_end(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
                Ok(())
            }
            fn on_exec_start(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
                Ok(())
            }

            async fn on_exec_end(&self, _: QueryID) -> DaftResult<()> {
                Ok(())
            }
            async fn on_operator_start(&self, _: Arc<OperatorStartEvent>) -> DaftResult<()> {
                Ok(())
            }
            async fn on_operator_end(&self, _: Arc<OperatorEndEvent>) -> DaftResult<()> {
                Ok(())
            }

            async fn on_stats(&self, _: Arc<StatsEvent>) -> DaftResult<()> {
                Err(common_error::DaftError::InternalError(
                    "Test error".to_string(),
                ))
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

    #[async_trait]
    impl Subscriber for ProcessStatsMockSubscriber {
        fn on_query_start(&self, _: QueryID, _: Arc<QueryMetadata>) -> DaftResult<()> {
            Ok(())
        }
        fn on_query_end(&self, _: QueryID, _: QueryResult) -> DaftResult<()> {
            Ok(())
        }
        fn on_result_out(&self, _: QueryID, _: MicroPartitionRef) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_start(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_end(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
            Ok(())
        }
        fn on_exec_start(&self, _: QueryID, _: QueryPlan) -> DaftResult<()> {
            Ok(())
        }
        async fn on_exec_end(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        async fn on_operator_start(&self, _: Arc<OperatorStartEvent>) -> DaftResult<()> {
            Ok(())
        }
        async fn on_operator_end(&self, _: Arc<OperatorEndEvent>) -> DaftResult<()> {
            Ok(())
        }
        async fn on_stats(&self, _: Arc<StatsEvent>) -> DaftResult<()> {
            Ok(())
        }
        async fn on_process_stats(&self, _: QueryID, _: Stats) -> DaftResult<()> {
            self.process_stats_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
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
