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
    pub(crate) fn new(node_info: NodeInfo) -> Arc<Self> {
        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();
        if should_enable_opentelemetry() {
            subscribers.push(Box::new(OpenTelemetrySubscriber::new()));
        }

        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
            node_info,
            subscribers: Arc::new(parking_lot::RwLock::new(subscribers)),
        })
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
