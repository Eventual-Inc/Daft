use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use common_error::DaftResult;
use indicatif::{HumanCount, ProgressStyle};

use crate::runtime_stats::RuntimeStatsContext;

pub trait ProgressBar: Send + Sync {
    fn set_message(&self, message: String) -> DaftResult<()>;
    fn close(&self) -> DaftResult<()>;
}

pub trait ProgressBarManager: std::fmt::Debug + Send + Sync {
    fn make_new_bar(
        &self,
        color: ProgressBarColor,
        prefix: &str,
    ) -> DaftResult<Box<dyn ProgressBar>>;

    fn close_all(&self) -> DaftResult<()>;
}

pub enum ProgressBarColor {
    Blue,
    Magenta,
    Cyan,
}

impl ProgressBarColor {
    fn to_str(&self) -> &'static str {
        match self {
            Self::Blue => "blue",
            Self::Magenta => "magenta",
            Self::Cyan => "cyan",
        }
    }
}

pub struct OperatorProgressBar {
    inner_progress_bar: Box<dyn ProgressBar>,
    runtime_stats: Arc<RuntimeStatsContext>,
    show_received: bool,
    start_time: Instant,
    last_update: AtomicU64,
}

impl OperatorProgressBar {
    // 500ms = 500_000_000ns
    const UPDATE_INTERVAL: u64 = 500_000_000;

    pub fn new(
        progress_bar: Box<dyn ProgressBar>,
        runtime_stats: Arc<RuntimeStatsContext>,
        show_received: bool,
    ) -> Self {
        Self {
            inner_progress_bar: progress_bar,
            runtime_stats,
            show_received,
            start_time: Instant::now(),
            last_update: AtomicU64::new(0),
        }
    }

    fn should_update_progress_bar(&self, now: Instant) -> bool {
        if now < self.start_time {
            return false;
        }

        let prev = self.last_update.load(Ordering::Acquire);
        let elapsed = (now - self.start_time).as_nanos() as u64;
        let diff = elapsed.saturating_sub(prev);

        // Fast path - check if enough time has passed
        if diff < Self::UPDATE_INTERVAL {
            return false;
        }

        // Only calculate remainder if we're actually going to update
        let remainder = diff % Self::UPDATE_INTERVAL;
        self.last_update
            .store(elapsed - remainder, Ordering::Release);
        true
    }

    pub fn render(&self) {
        let now = std::time::Instant::now();
        if self.should_update_progress_bar(now) {
            let rows_received = self.runtime_stats.get_rows_received();
            let rows_emitted = self.runtime_stats.get_rows_emitted();
            let msg = if self.show_received {
                format!(
                    "{} rows received, {} rows emitted",
                    HumanCount(rows_received),
                    HumanCount(rows_emitted)
                )
            } else {
                format!("{} rows emitted", HumanCount(rows_emitted))
            };
            let _ = self.inner_progress_bar.set_message(msg);
        }
    }
}

impl Drop for OperatorProgressBar {
    fn drop(&mut self) {
        let _ = self.inner_progress_bar.close();
    }
}

struct IndicatifProgressBar(indicatif::ProgressBar);

impl ProgressBar for IndicatifProgressBar {
    fn set_message(&self, message: String) -> DaftResult<()> {
        self.0.set_message(message);
        Ok(())
    }

    fn close(&self) -> DaftResult<()> {
        self.0.finish_and_clear();
        Ok(())
    }
}

#[derive(Debug)]
struct IndicatifProgressBarManager {
    multi_progress: indicatif::MultiProgress,
}

impl IndicatifProgressBarManager {
    fn new() -> Self {
        Self {
            multi_progress: indicatif::MultiProgress::new(),
        }
    }
}

impl ProgressBarManager for IndicatifProgressBarManager {
    fn make_new_bar(
        &self,
        color: ProgressBarColor,
        prefix: &str,
    ) -> DaftResult<Box<dyn ProgressBar>> {
        #[allow(clippy::literal_string_with_formatting_args)]
        let template_str = format!(
            "🗡️ 🐟 {{spinner:.green}} {{prefix:.{color}/bold}} | [{{elapsed_precise}}] {{msg}}",
            color = color.to_str(),
        );

        let pb = indicatif::ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .template(template_str.as_str())
                    .unwrap(),
            )
            .with_prefix(prefix.to_string());

        self.multi_progress.add(pb.clone());
        DaftResult::Ok(Box::new(IndicatifProgressBar(pb)))
    }

    fn close_all(&self) -> DaftResult<()> {
        Ok(self.multi_progress.clear()?)
    }
}

pub fn make_progress_bar_manager() -> Arc<dyn ProgressBarManager> {
    #[cfg(feature = "python")]
    {
        if python::in_notebook() {
            Arc::new(python::TqdmProgressBarManager::new())
        } else {
            Arc::new(IndicatifProgressBarManager::new())
        }
    }

    #[cfg(not(feature = "python"))]
    {
        Arc::new(IndicatifProgressBarManager::new())
    }
}

#[cfg(feature = "python")]
mod python {
    use pyo3::{types::PyAnyMethods, PyObject, Python};

    use super::*;

    pub fn in_notebook() -> bool {
        pyo3::Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "daft.utils"))
                .and_then(|m| m.getattr(pyo3::intern!(py, "in_notebook")))
                .and_then(|m| m.call0())
                .and_then(|m| m.extract())
                .expect("Failed to determine if running in notebook")
        })
    }

    struct TqdmProgressBar {
        pb_id: usize,
        manager: TqdmProgressBarManager,
    }

    impl ProgressBar for TqdmProgressBar {
        fn set_message(&self, message: String) -> DaftResult<()> {
            self.manager.update_bar(self.pb_id, message.as_str())
        }

        fn close(&self) -> DaftResult<()> {
            self.manager.close_bar(self.pb_id)
        }
    }

    #[derive(Clone, Debug)]
    pub struct TqdmProgressBarManager {
        inner: Arc<PyObject>,
    }

    impl TqdmProgressBarManager {
        pub fn new() -> Self {
            Python::with_gil(|py| {
                let module = py.import("daft.runners.progress_bar")?;
                let progress_bar_class = module.getattr("SwordfishProgressBar")?;
                let pb_object = progress_bar_class.call0()?;
                DaftResult::Ok(Self {
                    inner: Arc::new(pb_object.into()),
                })
            })
            .expect("Failed to create progress bar")
        }

        fn update_bar(&self, pb_id: usize, message: &str) -> DaftResult<()> {
            Python::with_gil(|py| {
                self.inner
                    .call_method1(py, "update_bar", (pb_id, message))?;
                DaftResult::Ok(())
            })
        }

        fn close_bar(&self, pb_id: usize) -> DaftResult<()> {
            Python::with_gil(|py| {
                self.inner.call_method1(py, "close_bar", (pb_id,))?;
                DaftResult::Ok(())
            })
        }
    }

    impl ProgressBarManager for TqdmProgressBarManager {
        fn make_new_bar(
            &self,
            _color: ProgressBarColor,
            prefix: &str,
        ) -> DaftResult<Box<dyn ProgressBar>> {
            let bar_format = format!("🗡️ 🐟 {prefix}: {{elapsed}} {{desc}}", prefix = prefix);
            let pb_id = Python::with_gil(|py| {
                let pb_id = self.inner.call_method1(py, "make_new_bar", (bar_format,))?;
                let pb_id = pb_id.extract::<usize>(py)?;
                DaftResult::Ok(pb_id)
            })?;

            DaftResult::Ok(Box::new(TqdmProgressBar {
                pb_id,
                manager: self.clone(),
            }))
        }

        fn close_all(&self) -> DaftResult<()> {
            Python::with_gil(|py| {
                self.inner.call_method0(py, "close")?;
                DaftResult::Ok(())
            })
        }
    }
}
