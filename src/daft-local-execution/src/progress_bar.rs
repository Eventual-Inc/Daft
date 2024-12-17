use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use indicatif::{HumanCount, ProgressBar, ProgressStyle};

use crate::runtime_stats::RuntimeStatsContext;

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
    inner_progress_bar: ProgressBar,
    runtime_stats: Arc<RuntimeStatsContext>,
    show_received: bool,
    start_time: Instant,
    last_update: AtomicU64,
}

impl OperatorProgressBar {
    // 100ms = 100_000_000ns
    const UPDATE_INTERVAL: u64 = 100_000_000;

    pub fn new(
        prefix: impl Into<Cow<'static, str>>,
        color: ProgressBarColor,
        show_received: bool,
        runtime_stats: Arc<RuntimeStatsContext>,
    ) -> Self {
        let template_str = format!(
            "🗡️ 🐟 {{spinner:.green}} {{prefix:.{color}/bold}} | [{{elapsed_precise}}] {{msg}}",
            color = color.to_str(),
        );

        let initial_message = if show_received {
            "0 rows received, 0 rows emitted".to_string()
        } else {
            "0 rows emitted".to_string()
        };

        let pb = ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .template(template_str.as_str())
                    .unwrap(),
            )
            .with_prefix(prefix)
            .with_message(initial_message);

        Self {
            inner_progress_bar: pb,
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
            let stats = self.runtime_stats.clone();
            let rows_received = stats.get_rows_received();
            let rows_emitted = stats.get_rows_emitted();
            let msg = if self.show_received {
                format!(
                    "{} rows received, {} rows emitted",
                    HumanCount(rows_received),
                    HumanCount(rows_emitted)
                )
            } else {
                format!("{} rows emitted", HumanCount(rows_emitted))
            };
            self.inner_progress_bar.set_message(msg);
        }
    }

    pub fn inner(&self) -> &ProgressBar {
        &self.inner_progress_bar
    }
}

impl Drop for OperatorProgressBar {
    fn drop(&mut self) {
        self.inner_progress_bar.finish_and_clear();
    }
}
