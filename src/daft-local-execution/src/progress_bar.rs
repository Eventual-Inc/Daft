use std::{borrow::Cow, sync::atomic::AtomicU64};

use indicatif::{HumanCount, ProgressBar, ProgressStyle};

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

pub struct ProgressBarWrapper {
    inner_progress_bar: ProgressBar,
    emitted: AtomicU64,
    show_received: bool,
}

impl ProgressBarWrapper {
    pub fn new(
        prefix: impl Into<Cow<'static, str>>,
        color: ProgressBarColor,
        show_received: bool,
    ) -> Self {
        let template_str = if show_received {
            format!(
                "🗡️ 🐟 {{spinner:.green}} {{prefix:.{color}/bold}} | [{{elapsed_precise}}] {{human_pos}} rows received, {{msg}} rows emitted",
                color = color.to_str(),
            )
        } else {
            format!(
                "🗡️ 🐟 {{spinner:.green}} {{prefix:.{color}/bold}} | [{{elapsed_precise}}] {{msg}} rows emitted",
                color = color.to_str(),
            )
        };

        let pb = ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::default_spinner()
                    .template(template_str.as_str())
                    .unwrap(),
            )
            .with_prefix(prefix)
            .with_message("0".to_string());
        Self {
            inner_progress_bar: pb,
            emitted: AtomicU64::new(0),
            show_received,
        }
    }

    pub fn increment_received(&self, amount: u64) {
        if self.show_received {
            self.inner_progress_bar.inc(amount);
        }
    }

    pub fn increment_emitted(&self, amount: u64) {
        let prev = self
            .emitted
            .fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
        let count = prev + amount;
        self.inner_progress_bar
            .set_message(HumanCount(count).to_string());
    }

    pub fn inner(&self) -> &ProgressBar {
        &self.inner_progress_bar
    }
}

impl Drop for ProgressBarWrapper {
    fn drop(&mut self) {
        self.inner_progress_bar.finish_and_clear();
    }
}
