use common_error::DaftResult;

use crate::{ProgressBar, ProgressBarColor, ProgressBarManager};

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
pub(crate) struct IndicatifProgressBarManager {
    multi_progress: indicatif::MultiProgress,
}

impl IndicatifProgressBarManager {
    pub fn new() -> Self {
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
                indicatif::ProgressStyle::default_spinner()
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
