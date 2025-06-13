mod subscribers;
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
use tracing::{instrument::Instrumented, Instrument};

use crate::{
    channel::{Receiver, Sender},
    pipeline::NodeInfo,
    progress_bar::OperatorProgressBar,
    runtime_stats::subscribers::{
        dashboard::DashboardSubscriber, opentelemetry::OpenTelemetrySubscriber,
        RuntimeStatsSubscriber,
    },
};

#[derive(Debug)]
pub struct RuntimeStatsEventHandler {
    subscribers: Arc<parking_lot::RwLock<Vec<Box<dyn RuntimeStatsSubscriber>>>>,
}

impl RuntimeStatsEventHandler {
    pub fn new() -> Self {
        let mut subscribers: Vec<Box<dyn RuntimeStatsSubscriber>> = Vec::new();

        if should_enable_opentelemetry() {
            subscribers.push(Box::new(OpenTelemetrySubscriber::new()));
        }

        if DashboardSubscriber::is_enabled() {
            subscribers.push(Box::new(DashboardSubscriber::new()));
        }

        Self {
            subscribers: Arc::new(parking_lot::RwLock::new(subscribers)),
        }
    }
    pub(crate) fn record_elapsed_cpu_time(
        &self,
        elapsed: std::time::Duration,
        rt: &RuntimeStatsContext,
    ) {
        rt.record_elapsed_cpu_time(elapsed);
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_cpu_time_elapsed(&rt.node_info, elapsed.as_micros() as _);
        }
    }
    pub(crate) fn mark_rows_received(&self, rows: u64, rt: &RuntimeStatsContext) {
        rt.mark_rows_received(rows);
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_rows_received(&rt.node_info, rows);
        }
    }

    pub(crate) fn mark_rows_emitted(&self, rows: u64, rt: &RuntimeStatsContext) {
        rt.mark_rows_emitted(rows);
        for subscriber in self.subscribers.read().iter() {
            subscriber.on_rows_emitted(&rt.node_info, rows);
        }
    }
}

pub struct RuntimeStatsContext {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
    node_info: NodeInfo,
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
        // let subscribers = get_subscriber_context();
        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
            node_info,
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
    rt_stats_handler: Arc<RuntimeStatsEventHandler>,
}

impl<F: Future> TimedFuture<F> {
    pub fn new(
        future: F,
        runtime_context: Arc<RuntimeStatsContext>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
        span: tracing::Span,
    ) -> Self {
        let instrumented = future.instrument(span);
        Self {
            start: None,
            future: instrumented,
            runtime_context,
            rt_stats_handler,
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
        this.rt_stats_handler
            .record_elapsed_cpu_time(elapsed, this.runtime_context);

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
    rt_stats_handler: Arc<RuntimeStatsEventHandler>,
}

impl CountingSender {
    pub(crate) fn new(
        sender: Sender<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
        rt_stats_manager: Arc<RuntimeStatsEventHandler>,
    ) -> Self {
        Self {
            sender,
            rt,
            progress_bar,
            rt_stats_handler: rt_stats_manager,
        }
    }
    #[inline]
    pub(crate) async fn send(&self, v: Arc<MicroPartition>) -> Result<(), SendError> {
        self.rt_stats_handler
            .mark_rows_emitted(v.len() as u64, &self.rt);
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
    rt_stats_handler: Arc<RuntimeStatsEventHandler>,
}

impl CountingReceiver {
    pub(crate) fn new(
        receiver: Receiver<Arc<MicroPartition>>,
        rt: Arc<RuntimeStatsContext>,
        progress_bar: Option<Arc<OperatorProgressBar>>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
    ) -> Self {
        Self {
            receiver,
            rt,
            progress_bar,
            rt_stats_handler,
        }
    }
    #[inline]
    pub(crate) async fn recv(&self) -> Option<Arc<MicroPartition>> {
        let v = self.receiver.recv().await;
        if let Some(ref v) = v {
            self.rt_stats_handler
                .mark_rows_received(v.len() as u64, &self.rt);
            if let Some(ref pb) = self.progress_bar {
                pb.render();
            }
        }
        v
    }
}
