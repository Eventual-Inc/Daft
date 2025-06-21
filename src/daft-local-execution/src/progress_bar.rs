// Progress bar code has been moved to the common progress-bar crate.
use std::sync::Arc;

pub use common_progress_bar::{
    make_progress_bar_manager, ProgressBar, ProgressBarColor, ProgressBarManager,
    ThrottledProgressBar,
};
use indicatif::HumanCount;

use crate::runtime_stats::RuntimeStatsContext;

pub struct OperatorProgressBar {
    inner_progress_bar: ThrottledProgressBar,
    runtime_stats: Arc<RuntimeStatsContext>,
    show_received: bool,
}

impl OperatorProgressBar {
    pub fn new(
        progress_bar: Box<dyn ProgressBar>,
        runtime_stats: Arc<RuntimeStatsContext>,
        show_received: bool,
    ) -> Self {
        Self {
            inner_progress_bar: ThrottledProgressBar::new_default(progress_bar),
            runtime_stats,
            show_received,
        }
    }

    pub fn render(&self) {
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
