use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use common_error::DaftResult;

mod indicatif;
#[cfg(feature = "python")]
mod tqdm;

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

/// A progress bar wrapper that throttles updates to avoid overwhelming the display.
/// Only updates the underlying progress bar at most once per update interval.
pub struct ThrottledProgressBar {
    inner_progress_bar: Box<dyn ProgressBar>,
    update_interval: u64, // in nanoseconds
    start_time: Instant,
    last_update: AtomicU64,
}

impl ThrottledProgressBar {
    /// Create a new throttled progress bar with the specified update interval.
    ///
    /// # Arguments
    /// * `progress_bar` - The underlying progress bar to wrap
    /// * `update_interval_ms` - Update interval in milliseconds
    pub fn new(progress_bar: Box<dyn ProgressBar>, update_interval_ms: u64) -> Self {
        Self {
            inner_progress_bar: progress_bar,
            update_interval: update_interval_ms * 1_000_000, // convert to nanoseconds
            start_time: Instant::now(),
            last_update: AtomicU64::new(0),
        }
    }

    /// Create a new throttled progress bar with a default 500ms update interval.
    pub fn new_default(progress_bar: Box<dyn ProgressBar>) -> Self {
        Self::new(progress_bar, 500)
    }

    fn should_update(&self, now: Instant) -> bool {
        if now < self.start_time {
            return false;
        }

        let prev = self.last_update.load(Ordering::Acquire);
        let elapsed = (now - self.start_time).as_nanos() as u64;
        let diff = elapsed.saturating_sub(prev);

        // Fast path - check if enough time has passed
        if diff < self.update_interval {
            return false;
        }

        // Only calculate remainder if we're actually going to update
        let remainder = diff % self.update_interval;
        self.last_update
            .store(elapsed - remainder, Ordering::Release);
        true
    }
}

impl Drop for ThrottledProgressBar {
    fn drop(&mut self) {
        let _ = self.inner_progress_bar.close();
    }
}

impl ProgressBar for ThrottledProgressBar {
    fn set_message(&self, message: String) -> DaftResult<()> {
        let now = Instant::now();
        if self.should_update(now) {
            self.inner_progress_bar.set_message(message)
        } else {
            Ok(())
        }
    }

    fn close(&self) -> DaftResult<()> {
        self.inner_progress_bar.close()
    }
}

pub fn make_progress_bar_manager() -> Arc<dyn ProgressBarManager> {
    #[cfg(feature = "python")]
    {
        if tqdm::in_notebook() {
            Arc::new(tqdm::TqdmProgressBarManager::new())
        } else {
            Arc::new(indicatif::IndicatifProgressBarManager::new())
        }
    }

    #[cfg(not(feature = "python"))]
    {
        Arc::new(indicatif::IndicatifProgressBarManager::new())
    }
}
