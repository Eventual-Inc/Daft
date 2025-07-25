mod subscribers;
mod values;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use std::cell::RefCell;

pub use common_metrics::{Stat, StatSnapshot};
use common_runtime::{get_io_runtime, RuntimeTask};
use common_tracing::should_enable_opentelemetry;
use daft_dsl::common_treenode::{TreeNode, TreeNodeRecursion};
use daft_micropartition::MicroPartition;
use kanal::SendError;
use tokio::{
    sync::{mpsc, oneshot},
    time::interval,
};
use tracing::{instrument::Instrumented, Instrument};
pub use values::{
    DefaultRuntimeStats, RuntimeStats, CPU_US_KEY, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY,
};

#[cfg(debug_assertions)]
use crate::runtime_stats::subscribers::debug::DebugSubscriber;
use crate::{
    channel::{Receiver, Sender},
    pipeline::{NodeInfo, PipelineNode},
    runtime_stats::subscribers::{
        dashboard::DashboardSubscriber, opentelemetry::OpenTelemetrySubscriber,
        progress_bar::make_progress_bar_manager, rpc::RpcSubscriber, RuntimeStatsSubscriber,
    },
};

fn should_enable_progress_bar() -> bool {
    let progress_var_name = "DAFT_PROGRESS_BAR";
    if let Ok(val) = std::env::var(progress_var_name) {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        true // Return true when env var is not set
    }
}

/// Event handler for RuntimeStats
/// The event handler contains a vector of subscribers
/// When a new event is broadcast, `RuntimeStatsEventHandler` manages notifying the subscribers.
///
/// For a given event, the event handler ensures that the subscribers only get the latest event at a frequency of once every 500ms
/// This prevents the subscribers from being overwhelmed by too many events.
pub struct RuntimeStatsManager {
    flush_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
    node_tx: Arc<mpsc::UnboundedSender<(usize, bool)>>,
    finish_tx: RefCell<Option<tokio::sync::oneshot::Sender<()>>>,
    handle: Mutex<RuntimeTask<()>>,
}

impl std::fmt::Debug for RuntimeStatsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeStatsEventHandler")
    }
}

impl RuntimeStatsManager {
    #[allow(clippy::borrowed_box)]
    pub fn new(pipeline: &Box<dyn PipelineNode>) -> Self {
        // Construct mapping between node id and their node info and runtime stats
        let mut node_stats_map = HashMap::new();
        let _ = pipeline.apply(|node| {
            let node_info = node.node_info();
            let runtime_stats = node.runtime_stats();
            node_stats_map.insert(node_info.id, (node_info, runtime_stats));
            Ok(TreeNodeRecursion::Continue)
        });

        let total_nodes = node_stats_map.len();
        let node_stats = (0..total_nodes)
            .map(|id| {
                let (node_info, runtime_stats) = node_stats_map.remove(&id).unwrap();
                (node_info, runtime_stats)
            })
            .collect::<Vec<_>>();

        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();

        if should_enable_progress_bar() {
            subscribers.push(make_progress_bar_manager(&node_stats));
        }

        if let Ok(rpc_url) = std::env::var("DAFT_RPC_URL") {
            subscribers.push(Box::new(RpcSubscriber::new(rpc_url).unwrap()));
        }

        if should_enable_opentelemetry() {
            subscribers.push(Box::new(OpenTelemetrySubscriber::new()));
        }

        if DashboardSubscriber::is_enabled() {
            subscribers.push(Box::new(DashboardSubscriber::new()));
        }

        #[cfg(debug_assertions)]
        if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
            let s = s.to_lowercase();
            match s.as_ref() {
                "1" | "true" => {
                    subscribers.push(Box::new(DebugSubscriber));
                }
                _ => {}
            }
        }

        let throttle_interval = Duration::from_millis(200);
        Self::new_impl(subscribers, node_stats, throttle_interval)
    }

    // Mostly used for testing purposes so we can inject our own subscribers and throttling interval
    fn new_impl(
        subscribers: Vec<Box<dyn RuntimeStatsSubscriber>>,
        node_stats: Vec<(Arc<NodeInfo>, Arc<dyn RuntimeStats>)>,
        throttle_interval: Duration,
    ) -> Self {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<(usize, bool)>();
        let node_tx = Arc::new(node_tx);
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();
        let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel::<()>();

        let rt = get_io_runtime(true);
        let handle = rt.spawn(async move {
            let mut interval = interval(throttle_interval);
            let mut active_nodes = HashSet::with_capacity(node_stats.len());

            loop {
                tokio::select! {
                    biased;
                    Some((node_id, is_initialize)) = node_rx.recv() => {
                        if is_initialize && active_nodes.insert(node_id) {
                            for subscriber in &subscribers {
                                if let Err(e) = subscriber.initialize_node(&node_stats[node_id].0) {
                                    log::error!("Failed to initialize node: {}", e);
                                }
                            }
                        } else if !is_initialize && active_nodes.remove(&node_id) {
                            let (node_info, runtime_stats) = &node_stats[node_id];
                            let event = runtime_stats.flush();
                            for subscriber in &subscribers {
                                if let Err(e) = subscriber.handle_event(&event, node_info) {
                                    log::error!("Failed to handle event: {}", e);
                                }
                                if let Err(e) = subscriber.finalize_node(&node_stats[node_id].0) {
                                    log::error!("Failed to finalize node: {}", e);
                                }
                            }
                        }
                    }

                    _ = interval.tick() => {
                        for node_id in &active_nodes {
                            let (node_info, runtime_stats) = &node_stats[*node_id];
                            let event = runtime_stats.snapshot();
                            for subscriber in &subscribers {
                                if let Err(e) = subscriber.handle_event(&event, node_info) {
                                    log::error!("Failed to handle event: {}", e);
                                }
                            }
                        }
                    }

                    Some(flush_response) = flush_rx.recv() => {
                        if !active_nodes.is_empty() {
                            log::error!("Received flush event while nodes are active: {:?}", active_nodes);
                        }
                        for subscriber in &subscribers {
                            if let Err(e) = subscriber.flush().await {
                                log::error!("Failed to flush subscriber: {}", e);
                            }
                        }
                        let _ = flush_response.send(());
                    }

                    Ok(()) = &mut finish_rx => {
                        break;
                    }
                }
            }

            for subscriber in subscribers {
                if let Err(e) = subscriber.finish() {
                    log::error!("Failed to finish subscriber: {}", e);
                }
            }
        });

        Self {
            flush_tx,
            node_tx,
            finish_tx: RefCell::new(Some(finish_tx)),
            handle: Mutex::new(handle),
        }
    }

    pub fn activate_node(&self, node_id: usize) {
        self.node_tx
            .send((node_id, true))
            .expect("The node_tx channel was closed");
    }

    pub fn finalize_node(&self, node_id: usize) {
        self.node_tx
            .send((node_id, false))
            .expect("The node_tx channel was closed");
    }

    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx)?;
        rx.await?;

        Ok(())
    }

    pub fn finish(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let finish_tx = self.finish_tx.take().expect("finish_tx was already taken");
        finish_tx
            .send(())
            .map_err(|()| {
                common_error::DaftError::MiscTransient(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to send finish signal",
                )))
            })?;

        Ok(())
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

/// Sender that wraps an internal sender and counts the number of rows passed through
pub struct CountingSender {
    sender: Sender<Arc<MicroPartition>>,
    rt: Arc<dyn RuntimeStats>,
}

impl CountingSender {
    pub(crate) fn new(sender: Sender<Arc<MicroPartition>>, rt: Arc<dyn RuntimeStats>) -> Self {
        Self { sender, rt }
    }
    #[inline]
    pub(crate) async fn send(&self, v: Arc<MicroPartition>) -> Result<(), SendError> {
        self.rt.add_rows_emitted(v.len() as u64);
        self.sender.send(v).await?;
        Ok(())
    }
}

/// Receiver that wraps an internal received and
/// - Counts the number of rows passed through
/// - Activates the associated node on first receive
pub struct InitializingCountingReceiver {
    receiver: Receiver<Arc<MicroPartition>>,
    rt: Arc<dyn RuntimeStats>,

    first_receive: AtomicBool,
    node_id: usize,
    stats_manager: Arc<RuntimeStatsManager>,
}

impl InitializingCountingReceiver {
    pub(crate) fn new(
        receiver: Receiver<Arc<MicroPartition>>,
        node_id: usize,
        rt: Arc<dyn RuntimeStats>,
        stats_manager: Arc<RuntimeStatsManager>,
    ) -> Self {
        Self {
            receiver,
            node_id,
            rt,
            stats_manager,
            first_receive: AtomicBool::new(true),
        }
    }
    #[inline]
    pub(crate) async fn recv(&self) -> Option<Arc<MicroPartition>> {
        let v = self.receiver.recv().await;
        if let Some(ref v) = v {
            if self
                .first_receive
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.stats_manager.activate_node(self.node_id);
            }
            self.rt.add_rows_received(v.len() as u64);
        }
        v
    }
}
#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU64, Arc, Mutex};

    use common_error::DaftResult;
    use common_metrics::{Stat, StatSnapshot};
    use tokio::time::{sleep, Duration};

    use super::*;
    use crate::pipeline::NodeCategory;

    #[derive(Debug)]
    struct MockState {
        total_calls: AtomicU64,
        events: Mutex<Vec<StatSnapshot>>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                total_calls: AtomicU64::new(0),
                events: Mutex::new(Vec::new()),
            }
        }

        fn get_total_calls(&self) -> u64 {
            self.total_calls.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn get_events(&self) -> Vec<StatSnapshot> {
            self.events.lock().unwrap().clone()
        }
    }

    #[derive(Debug)]
    struct MockSubscriber {
        state: Arc<MockState>,
    }

    impl MockSubscriber {
        fn new() -> (Self, Arc<MockState>) {
            let state = Arc::new(MockState::new());
            (
                Self {
                    state: state.clone(),
                },
                state,
            )
        }
    }

    #[async_trait::async_trait]
    impl RuntimeStatsSubscriber for MockSubscriber {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
            Ok(())
        }

        fn finalize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
            Ok(())
        }

        fn handle_event(&self, event: &StatSnapshot, _node_info: &NodeInfo) -> DaftResult<()> {
            self.state
                .total_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.state.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn flush(&self) -> DaftResult<()> {
            Ok(())
        }
        fn finish(self: Box<Self>) -> DaftResult<()> {
            Ok(())
        }
    }

    fn create_node_info(name: &str, id: usize) -> Arc<NodeInfo> {
        let node_info = NodeInfo {
            name: Arc::from(name.to_string()),
            id,
            node_type: Arc::from(""),
            node_category: NodeCategory::Intermediate,
            context: std::collections::HashMap::new(),
        };
        Arc::new(node_info)
    }

    fn create_mock_subscriber() -> (Box<dyn RuntimeStatsSubscriber>, Arc<MockState>) {
        let (subscriber, state) = MockSubscriber::new();
        (Box::new(subscriber), state)
    }

    #[tokio::test]
    async fn test_throttling_per_node_info() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info = create_node_info("test_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        // Send multiple events rapidly
        node_stat.add_rows_received(100);
        node_stat.add_rows_received(200);
        node_stat.add_rows_received(300);

        // Wait for processing
        sleep(Duration::from_millis(100)).await;

        // Should only get 1 call due to throttling
        assert_eq!(state.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_different_node_infos_not_throttled_together() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info1 = create_node_info("node1", 1);
        let node_info2 = create_node_info("node2", 2);
        let node_stat1 = Arc::new(DefaultRuntimeStats::default());
        let node_stat2 = Arc::new(DefaultRuntimeStats::default());

        let throttle_interval = Duration::from_millis(50);
        let _handler = Arc::new(RuntimeStatsManager::new_impl(
            subscribers,
            vec![
                (node_info1, node_stat1.clone()),
                (node_info2, node_stat2.clone()),
            ],
            throttle_interval,
        ));

        // Send events for different nodes
        node_stat1.add_rows_received(100);
        node_stat2.add_rows_received(200);

        // Wait for processing - use longer interval for Windows compatibility
        sleep(Duration::from_millis(100)).await;

        // Should get 2 calls (one for each node)
        assert_eq!(state.get_total_calls(), 2);
    }

    #[tokio::test]
    async fn test_throttling_interval_respected() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info = create_node_info("test_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        // Send first event
        node_stat.add_rows_received(100);
        assert_eq!(
            state.get_total_calls(),
            0,
            "No materialized events should be sent yet"
        );

        // Send second event rapidly (within throttle interval)
        node_stat.add_rows_received(200);
        sleep(Duration::from_millis(100)).await;

        // Should only get 1 call due to throttling
        assert_eq!(
            state.get_total_calls(),
            1,
            "Rapid events should be throttled to a single call"
        );

        // Wait for throttle interval to pass, then send another event
        sleep(throttle_interval).await;
        node_stat.add_rows_received(300);
        sleep(Duration::from_millis(100)).await;

        // Should now get a second call
        assert_eq!(
            state.get_total_calls(),
            2,
            "Event after throttle interval should trigger a new call"
        );
    }

    #[tokio::test]
    #[ignore = "flake; TODO(cory): investigate flaky test"]
    async fn test_event_contains_cumulative_stats() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info = create_node_info("test_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        node_stat.add_rows_received(100);
        node_stat.add_cpu_us(1000);
        node_stat.add_rows_emitted(50);

        sleep(Duration::from_millis(100)).await;

        // Only 1 call since all operations are on same NodeInfo within throttle window
        assert_eq!(state.get_total_calls(), 1);

        let events = state.get_events();
        assert_eq!(events.len(), 1);

        // Event should contain cumulative stats
        let event = &events[0];
        assert_eq!(event[0], (CPU_US_KEY.to_string(), Stat::Count(1000)));
        assert_eq!(event[1], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(100)));
        assert_eq!(event[2], (ROWS_EMITTED_KEY.to_string(), Stat::Count(50)));
    }

    #[tokio::test]
    async fn test_multiple_subscribers_all_receive_events() {
        let (subscriber1, state1) = create_mock_subscriber();
        let (subscriber2, state2) = create_mock_subscriber();
        let subscribers = vec![
            subscriber1 as Box<dyn RuntimeStatsSubscriber>,
            subscriber2 as Box<dyn RuntimeStatsSubscriber>,
        ];

        let node_info = create_node_info("test_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        node_stat.add_rows_received(100);
        sleep(Duration::from_millis(100)).await;

        // Both subscribers should receive the event
        assert_eq!(state1.get_total_calls(), 1);
        assert_eq!(state2.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_subscriber_error_doesnt_affect_others() {
        #[derive(Debug)]
        struct FailingSubscriber;

        #[async_trait::async_trait]
        impl RuntimeStatsSubscriber for FailingSubscriber {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn initialize_node(&self, _: &NodeInfo) -> DaftResult<()> {
                Ok(())
            }
            fn finalize_node(&self, _: &NodeInfo) -> DaftResult<()> {
                Ok(())
            }

            fn handle_event(&self, _: &StatSnapshot, _: &NodeInfo) -> DaftResult<()> {
                Err(common_error::DaftError::InternalError(
                    "Test error".to_string(),
                ))
            }
            async fn flush(&self) -> DaftResult<()> {
                Ok(())
            }
            fn finish(self: Box<Self>) -> DaftResult<()> {
                Ok(())
            }
        }

        let failing_subscriber = Box::new(FailingSubscriber);
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![
            failing_subscriber as Box<dyn RuntimeStatsSubscriber>,
            mock_subscriber as Box<dyn RuntimeStatsSubscriber>,
        ];

        let node_info = create_node_info("test_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        node_stat.add_rows_received(100);
        sleep(Duration::from_millis(100)).await;

        // Mock subscriber should still receive event despite other failing
        assert_eq!(state.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_runtime_stats_context_operations() {
        let node_stat = Arc::new(DefaultRuntimeStats::default());

        // Test initial state
        let stats = node_stat.snapshot();
        assert_eq!(stats[0], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(0)));
        assert_eq!(stats[1], (ROWS_EMITTED_KEY.to_string(), Stat::Count(0)));

        // Test incremental updates
        node_stat.add_rows_received(100);
        node_stat.add_rows_received(50);
        let stats = node_stat.snapshot();
        assert_eq!(stats[0], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(150)));

        node_stat.add_rows_emitted(75);
        let stats = node_stat.snapshot();
        assert_eq!(stats[1], (ROWS_EMITTED_KEY.to_string(), Stat::Count(75)));
    }

    #[tokio::test]
    async fn test_rapid_event_updates_latest_wins() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info = create_node_info("rapid_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = Arc::new(RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        ));

        // Send many rapid updates
        for i in 1..=20 {
            node_stat.add_rows_received(i * 10);
        }

        sleep(Duration::from_millis(100)).await;

        // Should only get 1 event due to throttling
        assert_eq!(state.get_total_calls(), 1);

        let events = state.get_events();
        let event = &events[0];

        // Should contain cumulative rows: 10+20+30+...+200 = 2100
        assert_eq!(event[0], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(2100)));
    }

    #[tokio::test]
    async fn test_mixed_event_types_cumulative() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        let node_info = create_node_info("mixed_node", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let throttle_interval = Duration::from_millis(50);
        let _handler = Arc::new(RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        ));

        // Interleave different event types
        node_stat.add_rows_received(100);
        node_stat.add_cpu_us(500);
        node_stat.add_rows_emitted(50);
        node_stat.add_rows_received(200); // Additional increment
        node_stat.add_cpu_us(1500);

        sleep(Duration::from_millis(100)).await;

        assert_eq!(state.get_total_calls(), 1);

        let events = state.get_events();
        let event = &events[0];

        // Should contain cumulative values
        assert_eq!(event[0], (CPU_US_KEY.to_string(), Stat::Count(2000))); // 500 + 1500
        assert_eq!(event[1], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(300))); // 100 + 200
        assert_eq!(event[2], (ROWS_EMITTED_KEY.to_string(), Stat::Count(50)));
    }

    #[tokio::test]
    #[ignore]
    async fn test_final_event_observed_under_throttle_threshold() {
        let (mock_subscriber, state) = create_mock_subscriber();
        let subscribers = vec![mock_subscriber as Box<dyn RuntimeStatsSubscriber>];

        // Use 500ms for the throttle interval.
        let throttle_interval = Duration::from_millis(500);
        let node_info = create_node_info("fast_query", 1);
        let node_stat = Arc::new(DefaultRuntimeStats::default());
        let _handler = RuntimeStatsManager::new_impl(
            subscribers,
            vec![(node_info, node_stat.clone())],
            throttle_interval,
        );

        // Simulate a fast query that completes within the throttle interval (500ms)
        node_stat.add_rows_received(100);
        node_stat.add_rows_emitted(50);
        node_stat.add_cpu_us(1000);

        // Wait less than throttle interval (500ms) but enough for processing (1ms)
        sleep(Duration::from_millis(10)).await;

        // The final event should still be observed even though throttle interval wasn't met
        assert_eq!(state.get_total_calls(), 1);

        let events = state.get_events();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event[0], (CPU_US_KEY.to_string(), Stat::Count(1000)));
        assert_eq!(event[1], (ROWS_RECEIVED_KEY.to_string(), Stat::Count(100)));
        assert_eq!(event[2], (ROWS_EMITTED_KEY.to_string(), Stat::Count(50)));
    }
}
