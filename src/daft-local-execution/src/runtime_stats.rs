use core::fmt;
use std::{
    fmt::Write,
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::Instant,
};

use common_tracing::should_enable_opentelemetry;
use daft_micropartition::MicroPartition;
use kanal::SendError;
use opentelemetry::{global, metrics::Counter, KeyValue};
use tracing::{instrument::Instrumented, Instrument};

use crate::{
    channel::{Receiver, Sender},
    pipeline::NodeInfo,
    progress_bar::OperatorProgressBar,
};

pub struct RuntimeStatsContext {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
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

#[derive(Debug)]
pub struct RuntimeStats {
    pub rows_received: u64,
    pub rows_emitted: u64,
    pub cpu_us: u64,
}

impl RuntimeStats {
    pub(crate) fn display<W: Write>(
        &self,
        w: &mut W,
        received: bool,
        emitted: bool,
        cpu_time: bool,
    ) -> Result<(), fmt::Error> {
        use num_format::{Locale, ToFormattedString};
        if received {
            writeln!(
                w,
                "Rows received =  {}",
                self.rows_received.to_formatted_string(&Locale::en)
            )?;
        }

        if emitted {
            writeln!(
                w,
                "Rows emitted =  {}",
                self.rows_emitted.to_formatted_string(&Locale::en)
            )?;
        }

        if cpu_time {
            let tms = (self.cpu_us as f32) / 1000f32;
            writeln!(w, "CPU Time = {tms:.2}ms")?;
        }

        Ok(())
    }
}

impl RuntimeStatsContext {
    pub(crate) fn new_with_subscribers(
        node_info: NodeInfo,
        subscribers: Vec<Box<dyn RuntimeStatsSubscriber>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
            node_info,
            subscribers: Arc::new(parking_lot::RwLock::new(subscribers)),
        })
    }

    pub(crate) fn new(node_info: NodeInfo) -> Arc<Self> {
        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();
        if should_enable_opentelemetry() {
            subscribers.push(Box::new(OpenTelemetrySubscriber::new()));
        }

        Self::new_with_subscribers(node_info, subscribers)
    }
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

    pub(crate) fn get_rows_received(&self) -> u64 {
        self.rows_received
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn get_rows_emitted(&self) -> u64 {
        self.rows_emitted.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[allow(unused)]
    pub(crate) fn reset(&self) {
        self.rows_received
            .store(0, std::sync::atomic::Ordering::Release);
        self.rows_emitted
            .store(0, std::sync::atomic::Ordering::Release);
        self.cpu_us.store(0, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn result(&self) -> RuntimeStats {
        RuntimeStats {
            rows_received: self
                .rows_received
                .load(std::sync::atomic::Ordering::Relaxed),
            rows_emitted: self.rows_emitted.load(std::sync::atomic::Ordering::Relaxed),
            cpu_us: self.cpu_us.load(std::sync::atomic::Ordering::Relaxed),
        }
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
        let ctx = RuntimeStatsContext::new_with_subscribers(node_info, vec![subscriber as _]);

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
