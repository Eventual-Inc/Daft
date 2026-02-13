mod progress_bar;
mod values;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common_error::DaftResult;
use common_metrics::{NodeID, QueryID, ops::NodeInfo, snapshot::StatSnapshotImpl};
use common_runtime::RuntimeTask;
use daft_context::Subscriber;
use daft_dsl::common_treenode::{TreeNode, TreeNodeRecursion};
use daft_local_plan::ExecutionEngineFinalResult;
use futures::future;
use itertools::Itertools;
use progress_bar::{ProgressBar, make_progress_bar_manager};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    time::interval,
};
use tracing::{Instrument, instrument::Instrumented};
pub use values::{DefaultRuntimeStats, RuntimeStats};

use crate::pipeline::PipelineNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEndState {
    Finished,
    Failed,
    Cancelled,
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
    stats_manager_task: RuntimeTask<ExecutionEngineFinalResult>,
}

impl std::fmt::Debug for RuntimeStatsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeStatsEventHandler")
    }
}

impl RuntimeStatsManager {
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
        let _ = pipeline.apply(|node| {
            let node_info = node.node_info();
            let runtime_stats = node.runtime_stats();
            node_info_map.insert(node_info.id, node_info.clone());
            node_map.insert(node_info.id, (node_info, runtime_stats));
            Ok(TreeNodeRecursion::Continue)
        });

        let serialized_plan: Arc<str> = serde_json::to_string(&pipeline.repr_json())
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
        Ok(Self::new_impl(
            handle,
            query_id,
            subscribers,
            progress_bar,
            node_map,
            throttle_interval,
        ))
    }

    // Mostly used for testing purposes so we can inject our own subscribers and throttling interval
    fn new_impl(
        handle: &Handle,
        query_id: QueryID,
        subscribers: Vec<Arc<dyn Subscriber>>,
        progress_bar: Option<Box<dyn ProgressBar>>,
        node_map: HashMap<NodeID, (Arc<NodeInfo>, Arc<dyn RuntimeStats>)>,
        throttle_interval: Duration,
    ) -> Self {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<(usize, bool)>();
        let node_tx = Arc::new(node_tx);
        let (finish_tx, mut finish_rx) = oneshot::channel::<QueryEndState>();

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

                            for res in future::join_all(subscribers.iter().map(|subscriber| subscriber.on_exec_operator_start(query_id.clone(), node_id))).await {
                                if let Err(e) = res {
                                    log::error!("Failed to initialize node: {}", e);
                                }
                            }
                        } else if !is_initialize && active_nodes.remove(&node_id) {
                            let runtime_stats = &node_map[&node_id].1;
                            let snapshot = runtime_stats.flush();

                            if let Some(progress_bar) = &progress_bar {
                                progress_bar.finalize_node(node_id, &snapshot);
                            }

                            let event = Arc::new(vec![(node_id, snapshot.to_stats())]);
                            for res in future::join_all(subscribers.iter().map(|subscriber| async {
                                subscriber.on_exec_emit_stats(query_id.clone(), event.clone()).await?;
                                subscriber.on_exec_operator_end(query_id.clone(), node_id).await
                            })).await {
                                if let Err(e) = res {
                                    log::error!("Failed to finalize node: {}", e);
                                }
                            }
                        }
                    }

                    finish_status = &mut finish_rx => {
                        if finish_status == Ok(QueryEndState::Finished) && !active_nodes.is_empty() {
                            log::error!(
                                "RuntimeStatsManager finished with active nodes {{{}}}",
                                active_nodes.iter().map(|id: &usize| id.to_string()).join(", ")
                            );
                        }
                        break;
                    }

                    _ = interval.tick() => {
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
                        for res in future::join_all(subscribers.iter().map(|subscriber| {
                            subscriber.on_exec_emit_stats(query_id.clone(), snapshot_container.clone())
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
            ExecutionEngineFinalResult::new(final_snapshot)
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

    pub async fn finish(self, status: QueryEndState) -> ExecutionEngineFinalResult {
        self.finish_tx
            .send(status)
            .expect("The finish_tx channel was closed");
        self.stats_manager_task
            .await
            .expect("The finish_tx channel was closed")
    }
}

#[pin_project::pin_project]
pub struct TimedFuture<F: Future> {
    start: Option<Instant>,
    #[pin]
    future: Instrumented<F>,
    runtime_stats: Arc<dyn RuntimeStats>,
}

impl<F: Future> TimedFuture<F> {
    pub fn new(future: F, runtime_stats: Arc<dyn RuntimeStats>, span: tracing::Span) -> Self {
        let instrumented = future.instrument(span);
        Self {
            start: None,
            future: instrumented,
            runtime_stats,
        }
    }
}

impl<F: Future> Future for TimedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.project();
        let start = this.start.get_or_insert_with(Instant::now);
        let inner_poll = this.future.as_mut().poll(cx);
        let elapsed = start.elapsed();
        this.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

        match inner_poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready(output),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::AtomicU64};

    use async_trait::async_trait;
    use common_error::DaftResult;
    use common_metrics::{
        CPU_US_KEY, NodeID, QueryPlan, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, Stats,
    };
    use daft_context::{QueryMetadata, QueryResult, Subscriber};
    use daft_micropartition::MicroPartitionRef;
    use opentelemetry::global;
    use tokio::time::{Duration, sleep};

    use super::*;

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

    #[async_trait]
    impl Subscriber for MockSubscriber {
        fn on_query_start(&self, _: QueryID, __: Arc<QueryMetadata>) -> DaftResult<()> {
            Ok(())
        }
        fn on_query_end(&self, _: QueryID, __: QueryResult) -> DaftResult<()> {
            Ok(())
        }
        fn on_result_out(&self, _: QueryID, __: MicroPartitionRef) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_start(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        fn on_optimization_end(&self, _: QueryID, __: QueryPlan) -> DaftResult<()> {
            Ok(())
        }
        fn on_exec_start(&self, _: QueryID, __: QueryPlan) -> DaftResult<()> {
            Ok(())
        }

        async fn on_exec_end(&self, _: QueryID) -> DaftResult<()> {
            Ok(())
        }
        async fn on_exec_operator_start(&self, _: QueryID, _: NodeID) -> DaftResult<()> {
            Ok(())
        }
        async fn on_exec_operator_end(&self, _: QueryID, __: NodeID) -> DaftResult<()> {
            Ok(())
        }

        async fn on_exec_emit_stats(
            &self,
            _query_id: QueryID,
            stats: std::sync::Arc<Vec<(NodeID, Stats)>>,
        ) -> DaftResult<()> {
            self.state
                .total_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            for (_, snapshot) in stats.iter() {
                *self.state.event.lock().unwrap() = Some(snapshot.clone());
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_interval_respected() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let mock_state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![mock_subscriber],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
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

        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![subscriber1, subscriber2],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
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
            fn on_query_start(&self, _: QueryID, __: Arc<QueryMetadata>) -> DaftResult<()> {
                Ok(())
            }
            fn on_query_end(&self, _: QueryID, __: QueryResult) -> DaftResult<()> {
                Ok(())
            }
            fn on_result_out(&self, _: QueryID, __: MicroPartitionRef) -> DaftResult<()> {
                Ok(())
            }
            fn on_optimization_start(&self, _: QueryID) -> DaftResult<()> {
                Ok(())
            }
            fn on_optimization_end(&self, _: QueryID, __: QueryPlan) -> DaftResult<()> {
                Ok(())
            }
            fn on_exec_start(&self, _: QueryID, __: QueryPlan) -> DaftResult<()> {
                Ok(())
            }

            async fn on_exec_end(&self, _: QueryID) -> DaftResult<()> {
                Ok(())
            }
            async fn on_exec_operator_start(&self, _: QueryID, _: NodeID) -> DaftResult<()> {
                Ok(())
            }
            async fn on_exec_operator_end(&self, _: QueryID, __: NodeID) -> DaftResult<()> {
                Ok(())
            }

            async fn on_exec_emit_stats(
                &self,
                _: QueryID,
                __: std::sync::Arc<Vec<(NodeID, Stats)>>,
            ) -> DaftResult<()> {
                Err(common_error::DaftError::InternalError(
                    "Test error".to_string(),
                ))
            }
        }

        let failing_subscriber = Arc::new(FailingSubscriber);
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![failing_subscriber, mock_subscriber],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
        );
        let handle = stats_manager.handle();

        handle.activate_node(0);
        node_stat.add_rows_in(100);
        sleep(Duration::from_millis(50)).await;

        // Mock subscriber should still receive event despite other failing
        assert_eq!(state.get_total_calls(), 1);
    }

    /// Reproduction test for https://github.com/Eventual-Inc/Daft/issues/6007
    /// Simulates a long-running operation (3+ hours) to test whether the
    /// tokio::time::Interval with default MissedTickBehavior::Burst overflows
    /// when computing next_deadline = start + (tick_count * interval).
    #[tokio::test(start_paused = true)]
    async fn test_long_running_operation_interval_overflow_issue_6007() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let mock_state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        // Use the same 200ms throttle interval as production
        let throttle_interval = Duration::from_millis(200);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![mock_subscriber],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
        );
        let handle = stats_manager.handle();

        // Activate a node (so the interval branch actually processes stats)
        handle.activate_node(0);
        node_stat.add_rows_in(1000);

        // Simulate a 3-hour operation by sleeping in the paused-time runtime.
        // With 200ms intervals, this means ~54,000 interval ticks.
        // Each tick computes: next_deadline = deadline + period
        // With MissedTickBehavior::Burst, the deadline accumulates.
        // This tests whether the accumulation causes an Instant overflow.
        let three_hours = Duration::from_secs(3 * 3600);
        sleep(three_hours).await;

        // If we get here without a panic, the interval didn't overflow.
        // Verify the stats manager is still working correctly.
        let total_calls = mock_state.get_total_calls();
        assert!(
            total_calls > 0,
            "Stats manager should have processed ticks during the 3-hour simulation"
        );

        // Finish the manager and verify it completes cleanly
        handle.finalize_node(0);
        sleep(Duration::from_millis(200)).await;

        let result = stats_manager.finish(QueryEndState::Finished).await;
        assert!(
            !result.nodes.is_empty(),
            "Should have final stats after long operation"
        );
    }

    /// Reproduction of the exact root cause of https://github.com/Eventual-Inc/Daft/issues/6007
    ///
    /// The "overflow when adding duration to instant" panic occurs in object_store's
    /// Azure credential code (used by delta-rs/DeltaStorageHandler) when:
    ///
    /// 1. A bearer token has a Unix timestamp expiry (e.g., 1739350000)
    /// 2. The token expires during a long-running operation (>1 hour)
    /// 3. The code computes: exp_in = expiry - current_timestamp (unsigned subtraction)
    /// 4. Since expiry < current_timestamp, this UNDERFLOWS in release mode → ~u64::MAX
    /// 5. Duration::from_secs(~u64::MAX) creates a ~584-billion-year Duration
    /// 6. Instant::now() + that Duration → OVERFLOW PANIC
    ///
    /// This exactly matches the user's scenario: writing 2840 files to Azure OneLake
    /// with use_fabric_endpoint=True, where the bearer token expires after ~1 hour
    /// but the operation takes 81 minutes.
    #[test]
    fn test_reproduce_issue_6007_token_expiry_overflow() {
        use std::time::{Duration, Instant};

        // Simulate the object_store FabricTokenOAuthProvider scenario:
        // The token was issued with an expiry timestamp 1 hour in the future.
        // But now we're 81 minutes later, so the token has expired.

        let token_expiry_unix: u64 = 1_739_350_000; // Token expiry (Unix timestamp)
        let current_time_unix: u64 = 1_739_354_860; // Current time (81 min later)

        // This is what object_store does in release mode:
        //   let exp_in = expiry - Self::get_current_timestamp();
        // When expiry < current_time, unsigned subtraction wraps around:
        let exp_in = token_expiry_unix.wrapping_sub(current_time_unix);

        // exp_in is now a HUGE number (close to u64::MAX)
        assert!(
            exp_in > u64::MAX / 2,
            "Unsigned underflow should produce a very large value, got {}",
            exp_in
        );

        // object_store then creates a Duration from this:
        let duration = Duration::from_secs(exp_in);

        // And adds it to Instant::now():
        //   expiry: Some(Instant::now() + Duration::from_secs(exp_in))
        // This PANICS with "overflow when adding duration to instant"
        let result = Instant::now().checked_add(duration);
        assert!(
            result.is_none(),
            "Adding ~u64::MAX seconds to Instant should overflow"
        );

        // Confirm the exact same panic message as in the issue
        let panic_result = std::panic::catch_unwind(|| {
            let _ = Instant::now() + duration;
        });
        assert!(panic_result.is_err(), "Should panic with overflow");

        // Verify the panic message matches the issue
        if let Err(panic) = panic_result {
            let msg = panic
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown");
            assert!(
                msg.contains("overflow when adding duration to instant"),
                "Expected 'overflow when adding duration to instant', got: {}",
                msg
            );
        }
    }

    #[tokio::test]
    async fn test_runtime_stats_context_operations() {
        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0));

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

        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![mock_subscriber],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
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
        let node_stat = Arc::new(DefaultRuntimeStats::new(&global::meter("test_stats"), 0))
            as Arc<dyn RuntimeStats>;
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            "test_query_id".into(),
            vec![mock_subscriber],
            None,
            HashMap::from([(0, (Arc::new(NodeInfo::default()), node_stat.clone()))]),
            throttle_interval,
        );
        let handle = stats_manager.handle();

        handle.activate_node(0);

        // Simulate a fast query that completes within the throttle interval (500ms)
        node_stat.add_rows_in(100);
        node_stat.add_rows_out(50);
        node_stat.add_cpu_us(1000);

        handle.finalize_node(0);

        // Wait less than throttle interval (500ms) but enough for processing (1ms)
        sleep(Duration::from_millis(10)).await;

        // The final event should still be observed even though throttle interval wasn't met
        assert_eq!(state.get_total_calls(), 1);

        let event = state.get_latest_event();
        assert_eq!(
            event[0],
            (CPU_US_KEY.into(), Stat::Duration(Duration::from_millis(1)))
        );
        assert_eq!(event[1], (ROWS_IN_KEY.into(), Stat::Count(100)));
        assert_eq!(event[2], (ROWS_OUT_KEY.into(), Stat::Count(50)));
    }
}
