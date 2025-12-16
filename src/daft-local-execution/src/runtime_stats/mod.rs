mod subscribers;
mod values;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common_error::DaftResult;
use common_metrics::{NodeID, QueryID, StatSnapshot};
use common_runtime::RuntimeTask;
use daft_context::Subscriber;
use daft_dsl::common_treenode::{TreeNode, TreeNodeRecursion};
use daft_micropartition::MicroPartition;
use futures::future;
use itertools::Itertools;
use kanal::SendError;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    time::interval,
};
use tracing::{Instrument, instrument::Instrumented};
pub use values::{Counter, DefaultRuntimeStats, Gauge, RuntimeStats};

use crate::{
    channel::{Receiver, Sender},
    pipeline::PipelineNode,
    runtime_stats::subscribers::{
        RuntimeStatsSubscriber, progress_bar::make_progress_bar_manager, query::SubscriberWrapper,
    },
};

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
    stats_manager_task: RuntimeTask<Vec<(usize, StatSnapshot)>>,
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
        query_subscribers: Vec<Arc<dyn Subscriber>>,
        query_id: QueryID,
    ) -> DaftResult<Self> {
        // Construct mapping between node id and their node info and runtime stats
        let mut node_stats_map = HashMap::new();
        let mut node_info_map = HashMap::new();
        let _ = pipeline.apply(|node| {
            let node_info = node.node_info();
            let runtime_stats = node.runtime_stats();
            node_stats_map.insert(node_info.id, runtime_stats);
            node_info_map.insert(node_info.id, node_info);
            Ok(TreeNodeRecursion::Continue)
        });

        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();
        for subscriber in query_subscribers {
            subscribers.push(Box::new(SubscriberWrapper::try_new(
                subscriber,
                query_id.clone(),
                serde_json::to_string(&pipeline.repr_json())
                    .expect("Failed to serialize physical plan")
                    .into(),
            )?));
        }

        if should_enable_progress_bar() {
            subscribers.push(make_progress_bar_manager(&node_info_map));
        }

        let throttle_interval = Duration::from_millis(200);
        Ok(Self::new_impl(
            handle,
            subscribers,
            node_stats_map,
            throttle_interval,
        ))
    }

    // Mostly used for testing purposes so we can inject our own subscribers and throttling interval
    fn new_impl(
        handle: &Handle,
        subscribers: Vec<Box<dyn RuntimeStatsSubscriber>>,
        node_stats_map: HashMap<NodeID, Arc<dyn RuntimeStats>>,
        throttle_interval: Duration,
    ) -> Self {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel::<(usize, bool)>();
        let node_tx = Arc::new(node_tx);
        let (finish_tx, mut finish_rx) = oneshot::channel::<QueryEndState>();

        let event_loop = async move {
            let mut interval = interval(throttle_interval);
            let mut active_nodes = HashSet::with_capacity(node_stats_map.len());
            // Reuse container for ticks
            let mut snapshot_container = Vec::with_capacity(node_stats_map.len());

            loop {
                tokio::select! {
                    biased;
                    Some((node_id, is_initialize)) = node_rx.recv() => {
                        if is_initialize && active_nodes.insert(node_id) {
                            for res in future::join_all(subscribers.iter().map(|subscriber| subscriber.initialize_node(node_id))).await {
                                if let Err(e) = res {
                                    log::error!("Failed to initialize node: {}", e);
                                }
                            }
                        } else if !is_initialize && active_nodes.remove(&node_id) {
                            let runtime_stats = &node_stats_map[&node_id];
                            let event = runtime_stats.flush();
                            let event = [(node_id, event)];

                            for res in future::join_all(subscribers.iter().map(|subscriber| async {
                                subscriber.handle_event(&event).await?;
                                subscriber.finalize_node(node_id).await
                            })).await {
                                if let Err(e) = res {
                                    log::error!("Failed to finalize node: {}", e);
                                }
                            }
                        }
                    }

                    finish_status = &mut finish_rx => {
                        if let Ok(status) = finish_status && status == QueryEndState::Finished && !active_nodes.is_empty() {
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
                            let runtime_stats = &node_stats_map[node_id];
                            let event = runtime_stats.snapshot();
                            snapshot_container.push((*node_id, event));
                        }

                        for res in future::join_all(subscribers.iter().map(|subscriber| {
                            subscriber.handle_event(snapshot_container.as_slice())
                        })).await {
                            if let Err(e) = res {
                                log::error!("Failed to handle event: {}", e);
                            }
                        }
                        snapshot_container.clear();
                    }
                }
            }

            for subscriber in subscribers {
                if let Err(e) = subscriber.finish().await {
                    log::error!("Failed to flush subscriber: {}", e);
                }
            }

            // Return the final stat snapshot for all nodes
            let mut final_snapshot = Vec::new();
            for (node_id, runtime_stats) in &node_stats_map {
                let event = runtime_stats.flush();
                final_snapshot.push((*node_id, event));
            }
            final_snapshot
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

    pub async fn finish(self, status: QueryEndState) -> Vec<(usize, StatSnapshot)> {
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
        self.rt.add_rows_out(v.len() as u64);
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
    stats_manager: RuntimeStatsManagerHandle,
}

impl InitializingCountingReceiver {
    pub(crate) fn new(
        receiver: Receiver<Arc<MicroPartition>>,
        node_id: usize,
        rt: Arc<dyn RuntimeStats>,
        stats_manager: RuntimeStatsManagerHandle,
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
            self.rt.add_rows_in(v.len() as u64);
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::AtomicU64};

    use async_trait::async_trait;
    use common_error::DaftResult;
    use common_metrics::{CPU_US_KEY, NodeID, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot};
    use tokio::time::{Duration, sleep};

    use super::*;

    #[derive(Debug)]
    struct MockState {
        total_calls: AtomicU64,
        event: Mutex<Option<StatSnapshot>>,
    }

    impl MockState {
        fn get_total_calls(&self) -> u64 {
            self.total_calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn get_latest_event(&self) -> StatSnapshot {
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
    impl RuntimeStatsSubscriber for MockSubscriber {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn initialize_node(&self, _node_id: NodeID) -> DaftResult<()> {
            Ok(())
        }

        async fn finalize_node(&self, _node_id: NodeID) -> DaftResult<()> {
            Ok(())
        }

        async fn handle_event(&self, events: &[(NodeID, StatSnapshot)]) -> DaftResult<()> {
            self.state
                .total_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            for (_, snapshot) in events {
                *self.state.event.lock().unwrap() = Some(snapshot.clone());
            }
            Ok(())
        }

        async fn finish(self: Box<Self>) -> DaftResult<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_interval_respected() {
        let mock_subscriber = Box::new(MockSubscriber::new());
        let mock_state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(0)) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            vec![mock_subscriber],
            HashMap::from([(0, node_stat.clone())]),
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
        let subscriber1 = Box::new(MockSubscriber::new());
        let subscriber2 = Box::new(MockSubscriber::new());
        let state1 = subscriber1.state.clone();
        let state2 = subscriber2.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(0)) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            vec![subscriber1, subscriber2],
            HashMap::from([(0, node_stat.clone())]),
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
        impl RuntimeStatsSubscriber for FailingSubscriber {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            async fn initialize_node(&self, _: NodeID) -> DaftResult<()> {
                Ok(())
            }
            async fn finalize_node(&self, _: NodeID) -> DaftResult<()> {
                Ok(())
            }
            async fn handle_event(&self, _: &[(NodeID, StatSnapshot)]) -> DaftResult<()> {
                Err(common_error::DaftError::InternalError(
                    "Test error".to_string(),
                ))
            }
            async fn finish(self: Box<Self>) -> DaftResult<()> {
                Ok(())
            }
        }

        let failing_subscriber = Box::new(FailingSubscriber);
        let mock_subscriber = Box::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(0)) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            vec![failing_subscriber, mock_subscriber],
            HashMap::from([(0, node_stat.clone())]),
            throttle_interval,
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
        let node_stat = Arc::new(DefaultRuntimeStats::new(0));

        // Test initial state
        let stats = node_stat.snapshot();
        assert_eq!(stats[1], (ROWS_IN_KEY.into(), Stat::Count(0)));
        assert_eq!(stats[2], (ROWS_OUT_KEY.into(), Stat::Count(0)));

        // Test incremental updates
        node_stat.add_rows_in(100);
        node_stat.add_rows_in(50);
        let stats = node_stat.snapshot();
        assert_eq!(stats[1], (ROWS_IN_KEY.into(), Stat::Count(150)));

        node_stat.add_rows_out(75);
        let stats = node_stat.snapshot();
        assert_eq!(stats[2], (ROWS_OUT_KEY.into(), Stat::Count(75)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_events_without_init() {
        let mock_subscriber = Box::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        let node_stat = Arc::new(DefaultRuntimeStats::new(0)) as Arc<dyn RuntimeStats>;
        let throttle_interval = Duration::from_millis(50);
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            vec![mock_subscriber],
            HashMap::from([(0, node_stat.clone())]),
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
        let mock_subscriber = Box::new(MockSubscriber::new());
        let state = mock_subscriber.state.clone();

        // Use 500ms for the throttle interval.
        let throttle_interval = Duration::from_millis(500);
        let node_stat = Arc::new(DefaultRuntimeStats::new(0)) as Arc<dyn RuntimeStats>;
        let stats_manager = RuntimeStatsManager::new_impl(
            &tokio::runtime::Handle::current(),
            vec![mock_subscriber],
            HashMap::from([(0, node_stat.clone())]),
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
