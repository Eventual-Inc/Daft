mod subscribers;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common_runtime::get_io_runtime;
use common_tracing::should_enable_opentelemetry;
use daft_micropartition::MicroPartition;
use indexmap::IndexMap;
use indicatif::{HumanCount, HumanDuration};
use kanal::SendError;
use parking_lot::Mutex;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::interval,
};
use tracing::{instrument::Instrumented, Instrument};

#[cfg(debug_assertions)]
use crate::runtime_stats::subscribers::debug::DebugSubscriber;
use crate::{
    channel::{Receiver, Sender},
    pipeline::NodeInfo,
    progress_bar::OperatorProgressBar,
    runtime_stats::subscribers::{
        dashboard::DashboardSubscriber, opentelemetry::OpenTelemetrySubscriber,
        RuntimeStatsSubscriber,
    },
};

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //
pub trait RuntimeStatsBuilder: Send + Sync + std::any::Any {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;

    fn build(
        &self,
        stats: &mut IndexMap<&'static str, String>,
        rows_received: u64,
        rows_emitted: u64,
    );
}

pub const ROWS_RECEIVED_KEY: &str = "rows received";
pub const ROWS_EMITTED_KEY: &str = "rows emitted";

pub struct BaseStatsBuilder {}

impl RuntimeStatsBuilder for BaseStatsBuilder {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build(
        &self,
        stats: &mut IndexMap<&'static str, String>,
        rows_received: u64,
        rows_emitted: u64,
    ) {
        stats.insert(ROWS_RECEIVED_KEY, HumanCount(rows_received).to_string());
        stats.insert(ROWS_EMITTED_KEY, HumanCount(rows_emitted).to_string());
    }
}

/// Event handler for RuntimeStats
/// The event handler contains a vector of subscribers
/// When a new event is broadcast, `RuntimeStatsEventHandler` manages notifying the subscribers.
///
/// For a given event, the event handler ensures that the subscribers only get the latest event at a frequency of once every 500ms
/// This prevents the subscribers from being overwhelmed by too many events.
pub struct RuntimeStatsEventHandler {
    senders: Arc<Mutex<HashMap<String, watch::Sender<Option<RuntimeStatsEvent>>>>>,
    new_receiver_tx: mpsc::UnboundedSender<(String, watch::Receiver<Option<RuntimeStatsEvent>>)>,
    flush_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
    _handle: JoinHandle<()>,
}

impl std::fmt::Debug for RuntimeStatsEventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeStatsEventHandler")
    }
}

impl RuntimeStatsEventHandler {
    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx)?;
        rx.await?;

        Ok(())
    }

    pub fn new() -> Self {
        let mut subscribers: Vec<Arc<dyn RuntimeStatsSubscriber>> = Vec::new();

        if should_enable_opentelemetry() {
            subscribers.push(Arc::new(OpenTelemetrySubscriber::new()));
        }

        if DashboardSubscriber::is_enabled() {
            subscribers.push(DashboardSubscriber::new());
        }

        #[cfg(debug_assertions)]
        if let Ok(s) = std::env::var("DAFT_DEV_ENABLE_RUNTIME_STATS_DBG") {
            let s = s.to_lowercase();
            match s.as_ref() {
                "1" | "true" => {
                    subscribers.push(Arc::new(DebugSubscriber));
                }
                _ => {}
            }
        }

        let subscribers = Arc::new(subscribers);
        let throttle_interval = Duration::from_millis(100);
        Self::new_impl(subscribers, throttle_interval)
    }

    // Mostly used for testing purposes so we can inject our own subscribers and throttling interval
    fn new_impl(
        subscribers: Arc<Vec<Arc<dyn RuntimeStatsSubscriber>>>,
        throttle_interval: Duration,
    ) -> Self {
        let senders = Arc::new(Mutex::new(HashMap::new()));
        let (new_receiver_tx, mut new_receiver_rx) = mpsc::unbounded_channel();
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();

        let rt = get_io_runtime(true);
        let handle = rt.runtime.spawn(async move {
            let mut last_sent: HashMap<String, Instant> = HashMap::new();
            let mut receivers: HashMap<String, watch::Receiver<Option<RuntimeStatsEvent>>> = HashMap::new();

            let mut interval = interval(throttle_interval);

            loop {
                tokio::select! {
                    Some((key, rx)) = new_receiver_rx.recv() => {
                        receivers.insert(key, rx);
                    }
                    Some(flush_response) = flush_rx.recv() => {
                         // Process all events immediately
                         for rx in receivers.values_mut() {
                             if rx.has_changed().unwrap_or(false) {
                                 let event = rx.borrow_and_update().clone();
                                 if let Some(event) = event {
                                    for subscriber in subscribers.iter() {
                                        if let Err(e) = subscriber.handle_event(&event) {
                                            log::error!("Failed to handle event: {}", e);
                                        }
                                        if let Err(e) = subscriber.flush().await {
                                            log::error!("Failed to flush subscriber: {}", e);
                                        }
                                    }
                                 }
                             }
                         }
                         let _ = flush_response.send(());
                     }

                    _ = interval.tick() => {
                        // Process all receivers
                        for (key, rx) in &mut receivers {
                            if rx.has_changed().unwrap_or(false) {
                                let event = rx.borrow_and_update().clone();
                                if let Some(event) = event {
                                    let now = Instant::now();
                                    let should_send = last_sent.get(key).is_none_or(|last| {
                                        // Handle potential time overflow
                                        now.checked_duration_since(*last)
                                            .map(|duration| duration >= throttle_interval)
                                            .unwrap_or(true)
                                    });

                                    if should_send {
                                        for subscriber in subscribers.iter() {
                                            if let Err(e) = subscriber.handle_event(&event) {
                                                log::error!("Failed to handle event: {}", e);
                                            }
                                        }
                                        last_sent.insert(key.clone(), now);
                                    }
                                }
                            }
                        }
                        // Cleanup - remove old events to free up memory
                        let cutoff = Instant::now().checked_sub(Duration::from_secs(300)).unwrap_or_else(Instant::now);
                        last_sent.retain(|_, &mut last_time| last_time > cutoff);
                    }
                }
            }
        });

        Self {
            senders,
            new_receiver_tx,
            flush_tx,
            _handle: handle,
        }
    }

    fn get_or_create_sender(&self, key: String) -> watch::Sender<Option<RuntimeStatsEvent>> {
        let mut senders = self.senders.lock();
        if let Some(sender) = senders.get(&key) {
            sender.clone()
        } else {
            let (tx, rx) = watch::channel(None);
            senders.insert(key.clone(), tx.clone());
            let _ = self.new_receiver_tx.send((key, rx));
            tx
        }
    }
}

pub struct RuntimeStatsContext {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
    node_info: NodeInfo,
    pub builder: Arc<dyn RuntimeStatsBuilder>,
}

#[derive(Clone, Debug)]
struct RuntimeStatsEvent {
    rows_received: u64,
    rows_emitted: u64,
    cpu_us: u64,
    node_info: NodeInfo,
}

impl From<&RuntimeStatsContext> for RuntimeStatsEvent {
    fn from(value: &RuntimeStatsContext) -> Self {
        Self {
            rows_received: value.rows_received.load(Ordering::Relaxed),
            rows_emitted: value.rows_emitted.load(Ordering::Relaxed),
            cpu_us: value.cpu_us.load(Ordering::Relaxed),
            node_info: value.node_info.clone(),
        }
    }
}

pub(crate) struct RuntimeEventsProducer {
    sender: watch::Sender<Option<RuntimeStatsEvent>>,
    rt: Arc<RuntimeStatsContext>,
}

impl RuntimeEventsProducer {
    pub fn new(
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
        rt: Arc<RuntimeStatsContext>,
    ) -> Self {
        let key = format!("{}:{}", rt.node_info.name, rt.node_info.id);
        let sender = rt_stats_handler.get_or_create_sender(key);

        Self { sender, rt }
    }

    fn record_elapsed_cpu_time(&self, elapsed: std::time::Duration) {
        self.rt.record_elapsed_cpu_time(elapsed);
        self.emit_event();
    }

    pub(crate) fn mark_rows_received(&self, rows: u64) {
        self.rt.mark_rows_received(rows);
        self.emit_event();
    }

    pub(crate) fn mark_rows_emitted(&self, rows: u64) {
        self.rt.mark_rows_emitted(rows);
        self.emit_event();
    }

    fn emit_event(&self) {
        // Use send to avoid blocking, but log if we can't send
        if let Err(e) = self.sender.send(Some(self.rt.as_ref().into())) {
            // Log send failures (channel might be closed during shutdown)
            eprintln!("Failed to send runtime stats event: {}", e);
        }
    }
}

impl RuntimeStatsContext {
    pub(crate) fn new(node_info: NodeInfo) -> Arc<Self> {
        Self::new_with_builder(node_info, Arc::new(BaseStatsBuilder {}))
    }
    pub(crate) fn new_with_builder(
        node_info: NodeInfo,
        builder: Arc<dyn RuntimeStatsBuilder>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
            node_info,
            builder,
        })
    }

    fn record_elapsed_cpu_time(&self, elapsed: std::time::Duration) {
        self.cpu_us.fetch_add(
            elapsed.as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn mark_rows_received(&self, rows: u64) {
        self.rows_received
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    fn mark_rows_emitted(&self, rows: u64) {
        self.rows_emitted
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    #[allow(unused)]
    pub(crate) fn reset(&self) {
        self.rows_received
            .store(0, std::sync::atomic::Ordering::Release);
        self.rows_emitted
            .store(0, std::sync::atomic::Ordering::Release);
        self.cpu_us.store(0, std::sync::atomic::Ordering::Release);
    }

    /// Export the runtime stats out as a pair of (name, value)
    /// Name is a small description. Can be rendered with different casing
    /// Value can be a string. Function is responsible for formatting the value
    /// This is used to update in-progress stats and final stats in UI clients
    pub(crate) fn render(&self) -> IndexMap<&'static str, String> {
        let rows_received = self
            .rows_received
            .load(std::sync::atomic::Ordering::Relaxed);
        let rows_emitted = self.rows_emitted.load(std::sync::atomic::Ordering::Relaxed);
        let cpu_us = self.cpu_us.load(std::sync::atomic::Ordering::Relaxed);

        let mut stats = IndexMap::new();
        stats.insert(
            "cpu time",
            HumanDuration(Duration::from_micros(cpu_us)).to_string(),
        );

        self.builder.build(&mut stats, rows_received, rows_emitted);
        stats
    }
}

#[pin_project::pin_project]
pub struct TimedFuture<F: Future> {
    start: Option<Instant>,
    #[pin]
    future: Instrumented<F>,
    rt_stats_producer: RuntimeEventsProducer,
}

impl<F: Future> TimedFuture<F> {
    pub fn new(
        future: F,
        runtime_context: Arc<RuntimeStatsContext>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
        span: tracing::Span,
    ) -> Self {
        let instrumented = future.instrument(span);
        let rt_stats_producer = RuntimeEventsProducer::new(rt_stats_handler, runtime_context);
        Self {
            start: None,
            future: instrumented,
            rt_stats_producer,
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
        this.rt_stats_producer.record_elapsed_cpu_time(elapsed);

        match inner_poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready(output),
        }
    }
}

pub struct CountingSender {
    sender: Sender<Arc<MicroPartition>>,
    progress_bar: Option<Arc<OperatorProgressBar>>,
    rt_stats_producer: RuntimeEventsProducer,
}

impl CountingSender {
    pub(crate) fn new(
        sender: Sender<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
    ) -> Self {
        let rt_stats_producer = RuntimeEventsProducer::new(rt_stats_handler, rt);

        Self {
            sender,
            progress_bar,
            rt_stats_producer,
        }
    }
    #[inline]
    pub(crate) async fn send(&self, v: Arc<MicroPartition>) -> Result<(), SendError> {
        self.rt_stats_producer.mark_rows_emitted(v.len() as u64);
        if let Some(ref pb) = self.progress_bar {
            pb.render();
        }
        self.sender.send(v).await?;
        Ok(())
    }
}

pub struct CountingReceiver {
    receiver: Receiver<Arc<MicroPartition>>,
    progress_bar: Option<Arc<OperatorProgressBar>>,
    rt_stats_producer: RuntimeEventsProducer,
}

impl CountingReceiver {
    pub(crate) fn new(
        receiver: Receiver<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
    ) -> Self {
        let rt_stats_producer = RuntimeEventsProducer::new(rt_stats_handler, rt);

        Self {
            receiver,
            progress_bar,
            rt_stats_producer,
        }
    }
    #[inline]
    pub(crate) async fn recv(&self) -> Option<Arc<MicroPartition>> {
        let v = self.receiver.recv().await;
        if let Some(ref v) = v {
            self.rt_stats_producer.mark_rows_received(v.len() as u64);
            if let Some(ref pb) = self.progress_bar {
                pb.render();
            }
        }
        v
    }
}
#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU64, Arc, Mutex};

    use common_error::DaftResult;
    use tokio::time::{sleep, Duration};

    use super::*;

    #[derive(Debug)]
    struct MockSubscriber {
        total_calls: AtomicU64,
        events: Mutex<Vec<RuntimeStatsEvent>>,
    }

    impl MockSubscriber {
        fn new() -> Self {
            Self {
                total_calls: AtomicU64::new(0),
                events: Mutex::new(Vec::new()),
            }
        }

        fn get_total_calls(&self) -> u64 {
            self.total_calls.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn get_events(&self) -> Vec<RuntimeStatsEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl RuntimeStatsSubscriber for MockSubscriber {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
            self.total_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn flush(&self) -> DaftResult<()> {
            Ok(())
        }
    }

    fn create_node_info(name: &str, id: usize) -> NodeInfo {
        NodeInfo {
            name: Arc::from(name.to_string()),
            id,
            context: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_throttling_per_node_info() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler.clone(), rt_context);

        // Send multiple events rapidly
        producer.mark_rows_received(100);
        producer.mark_rows_received(200);
        producer.mark_rows_received(300);

        // Wait for processing
        sleep(Duration::from_millis(100)).await;

        // Should only get 1 call due to throttling
        assert_eq!(mock_subscriber.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_different_node_infos_not_throttled_together() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));

        let node_info1 = create_node_info("node1", 1);
        let node_info2 = create_node_info("node2", 2);
        let rt_context1 = RuntimeStatsContext::new(node_info1);
        let producer1 = RuntimeEventsProducer::new(handler.clone(), rt_context1);

        let rt_context2 = RuntimeStatsContext::new(node_info2);
        let producer2 = RuntimeEventsProducer::new(handler, rt_context2);

        // Send events for different nodes
        producer1.mark_rows_received(100);
        producer2.mark_rows_received(200);

        // Wait for processing - use longer interval for Windows compatibility
        sleep(Duration::from_millis(100)).await;

        // Should get 2 calls (one for each node)
        assert_eq!(mock_subscriber.get_total_calls(), 2);
    }

    #[tokio::test]
    async fn test_throttling_interval_respected() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler.clone(), rt_context);

        // Send first event
        producer.mark_rows_received(100);
        assert_eq!(
            mock_subscriber.get_total_calls(),
            0,
            "No materialized events should be sent yet"
        );

        // Send second event rapidly (within throttle interval)
        producer.mark_rows_received(200);
        sleep(Duration::from_millis(100)).await;

        // Should only get 1 call due to throttling
        assert_eq!(
            mock_subscriber.get_total_calls(),
            1,
            "Rapid events should be throttled to a single call"
        );

        // Wait for throttle interval to pass, then send another event
        sleep(throttle_interval).await;
        producer.mark_rows_received(300);
        sleep(Duration::from_millis(100)).await;

        // Should now get a second call
        assert_eq!(
            mock_subscriber.get_total_calls(),
            2,
            "Event after throttle interval should trigger a new call"
        );
    }

    #[tokio::test]
    #[ignore = "flake; TODO(cory): investigate flaky test"]
    async fn test_event_contains_cumulative_stats() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        producer.mark_rows_received(100);
        producer.record_elapsed_cpu_time(Duration::from_micros(1000));
        producer.mark_rows_emitted(50);

        sleep(Duration::from_millis(100)).await;

        // Only 1 call since all operations are on same NodeInfo within throttle window
        assert_eq!(mock_subscriber.get_total_calls(), 1);

        let events = mock_subscriber.get_events();
        assert_eq!(events.len(), 1);

        // Event should contain cumulative stats
        let event = &events[0];
        assert_eq!(event.rows_received, 100);
        assert_eq!(event.rows_emitted, 50);
        assert_eq!(event.cpu_us, 1000);
    }

    #[tokio::test]
    async fn test_multiple_subscribers_all_receive_events() {
        let subscriber1 = Arc::new(MockSubscriber::new());
        let subscriber2 = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            subscriber1.clone() as Arc<dyn RuntimeStatsSubscriber>,
            subscriber2.clone() as Arc<dyn RuntimeStatsSubscriber>,
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        producer.mark_rows_received(100);
        sleep(Duration::from_millis(100)).await;

        // Both subscribers should receive the event
        assert_eq!(subscriber1.get_total_calls(), 1);
        assert_eq!(subscriber2.get_total_calls(), 1);
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
            fn handle_event(&self, _: &RuntimeStatsEvent) -> DaftResult<()> {
                Err(common_error::DaftError::InternalError(
                    "Test error".to_string(),
                ))
            }
            async fn flush(&self) -> DaftResult<()> {
                Ok(())
            }
        }

        let failing_subscriber = Arc::new(FailingSubscriber);
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            failing_subscriber as Arc<dyn RuntimeStatsSubscriber>,
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>,
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        producer.mark_rows_received(100);
        sleep(Duration::from_millis(100)).await;

        // Mock subscriber should still receive event despite other failing
        assert_eq!(mock_subscriber.get_total_calls(), 1);
    }

    #[tokio::test]
    async fn test_runtime_stats_context_operations() {
        let node_info = create_node_info("test_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);

        // Test initial state
        let stats = rt_context.render();
        assert_eq!(stats.get(ROWS_RECEIVED_KEY).unwrap(), "0");
        assert_eq!(stats.get(ROWS_EMITTED_KEY).unwrap(), "0");

        // Test incremental updates
        rt_context.mark_rows_received(100);
        rt_context.mark_rows_received(50);
        let stats = rt_context.render();
        assert_eq!(stats.get(ROWS_RECEIVED_KEY).unwrap(), "150");

        rt_context.mark_rows_emitted(75);
        let stats = rt_context.render();
        assert_eq!(stats.get(ROWS_EMITTED_KEY).unwrap(), "75");

        // Test reset
        rt_context.reset();
        let stats = rt_context.render();
        assert_eq!(stats.get(ROWS_RECEIVED_KEY).unwrap(), "0");
        assert_eq!(stats.get(ROWS_EMITTED_KEY).unwrap(), "0");
    }

    #[tokio::test]
    async fn test_rapid_event_updates_latest_wins() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("rapid_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        // Send many rapid updates
        for i in 1..=20 {
            producer.mark_rows_received(i * 10);
        }

        sleep(Duration::from_millis(100)).await;

        // Should only get 1 event due to throttling
        assert_eq!(mock_subscriber.get_total_calls(), 1);

        let events = mock_subscriber.get_events();
        let event = &events[0];

        // Should contain cumulative rows: 10+20+30+...+200 = 2100
        assert_eq!(event.rows_received, 2100);
    }

    #[tokio::test]
    async fn test_mixed_event_types_cumulative() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);
        let throttle_interval = Duration::from_millis(50);
        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("mixed_node", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        // Interleave different event types
        producer.mark_rows_received(100);
        producer.record_elapsed_cpu_time(Duration::from_micros(500));
        producer.mark_rows_emitted(50);
        producer.mark_rows_received(200); // Additional increment
        producer.record_elapsed_cpu_time(Duration::from_micros(1500));

        sleep(Duration::from_millis(100)).await;

        assert_eq!(mock_subscriber.get_total_calls(), 1);

        let events = mock_subscriber.get_events();
        let event = &events[0];

        // Should contain cumulative values
        assert_eq!(event.rows_received, 300); // 100 + 200
        assert_eq!(event.rows_emitted, 50);
        assert_eq!(event.cpu_us, 2000); // 500 + 1500
    }

    #[tokio::test]
    #[ignore]
    async fn test_final_event_observed_under_throttle_threshold() {
        let mock_subscriber = Arc::new(MockSubscriber::new());
        let subscribers = Arc::new(vec![
            mock_subscriber.clone() as Arc<dyn RuntimeStatsSubscriber>
        ]);

        // Use 500ms for the throttle interval.
        let throttle_interval = Duration::from_millis(500);

        let handler = Arc::new(RuntimeStatsEventHandler::new_impl(
            subscribers,
            throttle_interval,
        ));
        let node_info = create_node_info("fast_query", 1);
        let rt_context = RuntimeStatsContext::new(node_info);
        let producer = RuntimeEventsProducer::new(handler, rt_context);

        // Simulate a fast query that completes within the throttle interval (500ms)
        producer.mark_rows_received(100);
        producer.mark_rows_emitted(50);
        producer.record_elapsed_cpu_time(Duration::from_micros(1000));

        // Wait less than throttle interval (500ms) but enough for processing (1ms)
        sleep(Duration::from_millis(10)).await;

        // The final event should still be observed even though throttle interval wasn't met
        assert_eq!(mock_subscriber.get_total_calls(), 1);

        let events = mock_subscriber.get_events();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event.rows_received, 100);
        assert_eq!(event.rows_emitted, 50);
        assert_eq!(event.cpu_us, 1000);
    }
}
