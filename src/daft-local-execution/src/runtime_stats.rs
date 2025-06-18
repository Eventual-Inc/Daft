use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common_tracing::should_enable_opentelemetry;
use daft_micropartition::MicroPartition;
use indexmap::IndexMap;
use indicatif::{HumanCount, HumanDuration};
use kanal::SendError;
use opentelemetry::{global, metrics::Counter, KeyValue};
use tracing::{instrument::Instrumented, Instrument};

use crate::{
    channel::{Receiver, Sender},
    pipeline::NodeInfo,
    progress_bar::OperatorProgressBar,
};

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //
pub trait RuntimeStatsBuilder: Send + Sync + std::any::Any {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;

    fn render(&self, stats: &mut IndexMap<Arc<str>, String>, rows_received: u64, rows_emitted: u64);
}

pub struct BaseStatsBuilder {}

impl BaseStatsBuilder {
    pub fn render_helper(
        &self,
        stats: &mut IndexMap<Arc<str>, String>,
        rows_received: u64,
        rows_emitted: u64,
    ) {
        stats.insert(
            Arc::from("rows received"),
            HumanCount(rows_received).to_string(),
        );
        stats.insert(
            Arc::from("rows emitted"),
            HumanCount(rows_emitted).to_string(),
        );
    }
}

impl RuntimeStatsBuilder for BaseStatsBuilder {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn render(
        &self,
        stats: &mut IndexMap<Arc<str>, String>,
        rows_received: u64,
        rows_emitted: u64,
    ) {
        // Default is render the stats as is
        self.render_helper(stats, rows_received, rows_emitted);
    }
}

pub struct RuntimeStatsContext {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
    pub additional: Arc<dyn RuntimeStatsBuilder>,
    node_info: NodeInfo,
    subscribers: Arc<parking_lot::RwLock<Vec<Box<dyn RuntimeStatsSubscriber>>>>,
}

pub trait RuntimeStatsSubscriber: Send + Sync {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;

    fn on_rows_received(&self, context: &NodeInfo, count: u64);
    fn on_rows_emitted(&self, context: &NodeInfo, count: u64);
    fn on_cpu_time_elapsed(&self, context: &NodeInfo, microseconds: u64);
}

pub struct OpenTelemetrySubscriber {
    rows_received: Counter<u64>,
    rows_emitted: Counter<u64>,
    cpu_us: Counter<u64>,
}

impl OpenTelemetrySubscriber {
    pub fn new() -> Self {
        let meter = global::meter("runtime_stats");
        Self {
            rows_received: meter
                .u64_counter("daft.runtime_stats.rows_received")
                .build(),
            rows_emitted: meter.u64_counter("daft.runtime_stats.rows_emitted").build(),
            cpu_us: meter.u64_counter("daft.runtime_stats.cpu_us").build(),
        }
    }
}

impl RuntimeStatsSubscriber for OpenTelemetrySubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn on_rows_received(&self, context: &NodeInfo, count: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }

        self.rows_received.add(count, &attributes);
    }
    fn on_rows_emitted(&self, context: &NodeInfo, count: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }
        self.rows_emitted.add(count, &attributes);
    }

    fn on_cpu_time_elapsed(&self, context: &NodeInfo, microseconds: u64) {
        let mut attributes = vec![
            KeyValue::new("name", context.name.to_string()),
            KeyValue::new("id", context.id.to_string()),
        ];

        for (k, v) in &context.context {
            attributes.push(KeyValue::new(k.clone(), v.clone()));
        }
        self.cpu_us.add(microseconds, &attributes);
    }
}

impl RuntimeStatsContext {
    pub(crate) fn new_with_subscribers(
        node_info: NodeInfo,
        builder: Arc<dyn RuntimeStatsBuilder>,
        subscribers: Vec<Box<dyn RuntimeStatsSubscriber>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
            additional: builder,
            node_info,
            subscribers: Arc::new(parking_lot::RwLock::new(subscribers)),
        })
    }

    pub(crate) fn new_with_builder(
        node_info: NodeInfo,
        builder: Arc<dyn RuntimeStatsBuilder>,
    ) -> Arc<Self> {
        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();
        if should_enable_opentelemetry() {
            subscribers.push(Box::new(OpenTelemetrySubscriber::new()));
        }

        Self::new_with_subscribers(node_info, builder, subscribers)
    }

    pub(crate) fn new(node_info: NodeInfo) -> Arc<Self> {
        Self::new_with_builder(node_info, Arc::new(BaseStatsBuilder {}))
    }

    // pub(crate) fn record_update(&self, )

    pub(crate) fn record_elapsed_cpu_time(&self, elapsed: std::time::Duration) {
        self.cpu_us.fetch_add(
            elapsed.as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_cpu_time_elapsed(&self.node_info, elapsed.as_micros() as u64);
        }
    }

    pub(crate) fn mark_rows_received(&self, rows: u64) {
        self.rows_received
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_rows_received(&self.node_info, rows);
        }
    }

    pub(crate) fn mark_rows_emitted(&self, rows: u64) {
        self.rows_emitted
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_rows_emitted(&self.node_info, rows);
        }
    }

    /// Export the runtime stats out as a pair of (name, value)
    /// Name is a small description. Can be rendered with different casing
    /// Value can be a string. Function is responsible for formatting the value
    /// This is used to update in-progress stats and final stats in UI clients
    pub(crate) fn render(&self) -> IndexMap<Arc<str>, String> {
        let rows_received = self
            .rows_received
            .load(std::sync::atomic::Ordering::Relaxed);
        let rows_emitted = self.rows_emitted.load(std::sync::atomic::Ordering::Relaxed);
        let cpu_us = self.cpu_us.load(std::sync::atomic::Ordering::Relaxed);

        let mut stats = IndexMap::new();
        stats.insert(
            Arc::from("cpu time"),
            HumanDuration(Duration::from_micros(cpu_us)).to_string(),
        );

        self.additional
            .render(&mut stats, rows_received, rows_emitted);
        stats
    }
}

#[pin_project::pin_project]
pub struct TimedFuture<F: Future> {
    start: Option<Instant>,
    #[pin]
    future: Instrumented<F>,
    runtime_context: Arc<RuntimeStatsContext>,
}

impl<F: Future> TimedFuture<F> {
    pub fn new(future: F, runtime_context: Arc<RuntimeStatsContext>, span: tracing::Span) -> Self {
        let instrumented = future.instrument(span);
        Self {
            start: None,
            future: instrumented,
            runtime_context,
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
        this.runtime_context.record_elapsed_cpu_time(elapsed);

        match inner_poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready(output),
        }
    }
}

pub struct CountingSender {
    sender: Sender<Arc<MicroPartition>>,
    rt: Arc<RuntimeStatsContext>,
    progress_bar: Option<Arc<OperatorProgressBar>>,
}

impl CountingSender {
    pub(crate) fn new(
        sender: Sender<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> Self {
        Self {
            sender,
            rt,
            progress_bar,
        }
    }
    #[inline]
    pub(crate) async fn send(&self, v: Arc<MicroPartition>) -> Result<(), SendError> {
        self.rt.mark_rows_emitted(v.len() as u64);
        if let Some(ref pb) = self.progress_bar {
            pb.render();
        }
        self.sender.send(v).await?;
        Ok(())
    }
}

pub struct CountingReceiver {
    receiver: Receiver<Arc<MicroPartition>>,
    rt: Arc<RuntimeStatsContext>,
    progress_bar: Option<Arc<OperatorProgressBar>>,
}

impl CountingReceiver {
    pub(crate) fn new(
        receiver: Receiver<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> Self {
        Self {
            receiver,
            rt,
            progress_bar,
        }
    }
    #[inline]
    pub(crate) async fn recv(&self) -> Option<Arc<MicroPartition>> {
        let v = self.receiver.recv().await;
        if let Some(ref v) = v {
            self.rt.mark_rows_received(v.len() as u64);
            if let Some(ref pb) = self.progress_bar {
                pb.render();
            }
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, collections::HashMap, sync::atomic::Ordering, time::Duration};

    use parking_lot::Mutex;

    use super::*;
    use crate::runtime_stats::RuntimeStatsContext;
    struct MockSubscriber {
        rows_received: AtomicU64,
        rows_emitted: AtomicU64,
        cpu_us: AtomicU64,
        messages: Arc<Mutex<Vec<NodeInfo>>>,
    }

    impl MockSubscriber {
        fn new() -> Self {
            Self {
                rows_received: AtomicU64::new(0),
                rows_emitted: AtomicU64::new(0),
                cpu_us: AtomicU64::new(0),
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl RuntimeStatsSubscriber for MockSubscriber {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn on_rows_received(&self, context: &NodeInfo, count: u64) {
            self.rows_received.fetch_add(count, Ordering::Relaxed);
            self.messages.lock().push(context.clone());
        }

        fn on_rows_emitted(&self, context: &NodeInfo, count: u64) {
            self.rows_emitted.fetch_add(count, Ordering::Relaxed);
            self.messages.lock().push(context.clone());
        }

        fn on_cpu_time_elapsed(&self, context: &NodeInfo, microseconds: u64) {
            self.cpu_us.fetch_add(microseconds, Ordering::Relaxed);
            self.messages.lock().push(context.clone());
        }
    }

    #[test]
    fn test_rt_stats_subscriber() {
        let node_info = NodeInfo {
            name: Arc::from("test"),
            id: 1,
            context: HashMap::new(),
        };
        let subscriber = Box::new(MockSubscriber::new());
        let ctx = RuntimeStatsContext::new_with_subscribers(
            node_info,
            Arc::new(BaseStatsBuilder {}),
            vec![subscriber as _],
        );

        ctx.mark_rows_emitted(100);
        ctx.record_elapsed_cpu_time(Duration::from_secs(5));
        ctx.mark_rows_received(200);

        let subscribers = ctx.subscribers.read();
        let subscriber = &subscribers[0];
        let subscriber_any = subscriber.as_any();
        let mock_subscriber = subscriber_any.downcast_ref::<MockSubscriber>().unwrap();
        let rows_emitted = mock_subscriber.rows_emitted.load(Ordering::Relaxed);
        let rows_received = mock_subscriber.rows_received.load(Ordering::Relaxed);
        let cpu_us = mock_subscriber.cpu_us.load(Ordering::Relaxed);
        let messages = mock_subscriber.messages.lock();
        assert_eq!(rows_emitted, 100);
        assert_eq!(rows_received, 200);
        assert_eq!(cpu_us, 5000000);
        assert_eq!(messages.len(), 3);
        for message in messages.iter() {
            assert_eq!(message.name, Arc::from("test"));
            assert_eq!(message.id, 1);
        }
    }
}
