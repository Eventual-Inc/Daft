use core::fmt;
use std::{
    fmt::Write,
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

use daft_micropartition::MicroPartition;
use tokio::sync::mpsc::error::SendError;

use crate::channel::SingleSender;

#[derive(Default)]
pub(crate) struct RuntimeStatsContext {
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct RuntimeStats {
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
        use num_format::Locale;
        use num_format::ToFormattedString;
        if received {
            writeln!(
                w,
                "rows received =  {}",
                self.rows_received.to_formatted_string(&Locale::en)
            )?;
        }

        if emitted {
            writeln!(
                w,
                "rows emitted =  {}",
                self.rows_emitted.to_formatted_string(&Locale::en)
            )?;
        }

        if cpu_time {
            let tms = (self.cpu_us as f32) / 1000f32;
            writeln!(w, "CPU Time = {:.2}ms", tms)?;
        }

        Ok(())
    }
}

impl RuntimeStatsContext {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            rows_received: AtomicU64::new(0),
            rows_emitted: AtomicU64::new(0),
            cpu_us: AtomicU64::new(0),
        })
    }
    pub(crate) fn in_span<F: FnOnce() -> T, T>(&self, span: &tracing::Span, f: F) -> T {
        let _enter = span.enter();
        let start = Instant::now();
        let result = f();
        let total = start.elapsed();
        let micros = total.as_micros() as u64;
        self.cpu_us
            .fetch_add(micros, std::sync::atomic::Ordering::Relaxed);
        result
    }

    pub(crate) fn mark_rows_received(&self, rows: u64) {
        self.rows_received
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn mark_rows_emitted(&self, rows: u64) {
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

pub(crate) struct CountingSender {
    sender: SingleSender,
    rt: Arc<RuntimeStatsContext>,
}

impl CountingSender {
    pub(crate) fn new(sender: SingleSender, rt: Arc<RuntimeStatsContext>) -> Self {
        Self { sender, rt }
    }
    #[inline]
    pub(crate) async fn send(
        &self,
        v: Arc<MicroPartition>,
    ) -> Result<(), SendError<Arc<MicroPartition>>> {
        let len = v.len();
        self.sender.send(v).await?;
        self.rt.mark_rows_emitted(len as u64);
        Ok(())
    }
}
