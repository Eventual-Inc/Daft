use std::{sync::Arc, time::Duration};

use common_error::DaftResult;
use common_logging::GLOBAL_LOGGER;
use common_metrics::StatSnapshotSend;
use indicatif::{ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use log::Log;

use crate::{
    ops::{NodeCategory, NodeInfo},
    runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStats, CPU_US_KEY},
    PythonPrintTarget, STDOUT,
};

/// Convert statistics to a message for progress bars
fn event_to_message(event: &StatSnapshotSend) -> String {
    event
        .iter()
        .filter(|(name, _)| *name != CPU_US_KEY)
        .map(|(name, value)| format!("{} {}", value, name.to_lowercase()))
        .join(", ")
}

pub enum ProgressBarColor {
    Blue,
    Magenta,
    Cyan,
    Yellow,
}

impl ProgressBarColor {
    fn to_str(&self) -> &'static str {
        match self {
            Self::Blue => "blue",
            Self::Magenta => "magenta",
            Self::Cyan => "cyan",
            Self::Yellow => "yellow",
        }
    }
}

const TICK_INTERVAL: Duration = Duration::from_millis(100);

struct IndicatifLogger<L: Log> {
    pbar: indicatif::MultiProgress,
    inner: L,
}

impl<L: Log> IndicatifLogger<L> {
    fn new(pbar: indicatif::MultiProgress, inner: L) -> Self {
        Self { pbar, inner }
    }
}

impl<L: Log> Log for IndicatifLogger<L> {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if self.inner.enabled(record.metadata()) {
            self.pbar.suspend(|| self.inner.log(record));
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

struct IndicatifPrintTarget {
    pbar: indicatif::MultiProgress,
}

impl IndicatifPrintTarget {
    fn new(pbar: indicatif::MultiProgress) -> Self {
        Self { pbar }
    }
}

impl PythonPrintTarget for IndicatifPrintTarget {
    fn println(&self, message: &str) {
        self.pbar.println(message).unwrap();
    }
}

#[derive(Debug)]
struct IndicatifProgressBarManager {
    multi_progress: indicatif::MultiProgress,
    pbars: Vec<indicatif::ProgressBar>,
    total: usize,
}

impl IndicatifProgressBarManager {
    fn new(node_stats: &[(Arc<NodeInfo>, Arc<dyn RuntimeStats>)]) -> Self {
        let multi_progress = indicatif::MultiProgress::new();

        if cfg!(feature = "python") {
            // Register the IndicatifLogger to redirect Rust logs correctly
            GLOBAL_LOGGER.set_temp_logger(Box::new(IndicatifLogger::new(
                multi_progress.clone(),
                GLOBAL_LOGGER.get_base_logger(),
            )));

            STDOUT.set_target(Box::new(IndicatifPrintTarget::new(multi_progress.clone())));
        }

        multi_progress.set_move_cursor(true);
        multi_progress.set_draw_target(ProgressDrawTarget::stderr_with_hz(10));

        let total = node_stats.len();

        let mut manager = Self {
            multi_progress,
            pbars: Vec::new(),
            total,
        };

        for (node_info, _) in node_stats {
            manager.make_new_bar(node_info.as_ref());
        }

        manager
    }

    fn make_new_bar(&mut self, node_info: &NodeInfo) {
        let color = match node_info.node_category {
            NodeCategory::Source => ProgressBarColor::Blue,
            NodeCategory::Intermediate => ProgressBarColor::Magenta,
            NodeCategory::BlockingSink => ProgressBarColor::Cyan,
            NodeCategory::StreamingSink => ProgressBarColor::Yellow,
        };

        #[allow(clippy::literal_string_with_formatting_args)]
        let template_str = format!(
            "🗡️ 🐟[{node_id:>total_len$}/{total}] {{spinner:.green}} {{prefix:.{color}/bold}} | [{{elapsed_precise}}] {{msg}}",
            color = color.to_str(),
            node_id = (node_info.id + 1),
            total = self.total,
            total_len = self.total.to_string().len(),
        );

        let formatted_prefix = format!("{:>1$}", node_info.name, MAX_PIPELINE_NAME_LEN);

        let pb = indicatif::ProgressBar::new_spinner()
            .with_style(
                ProgressStyle::with_template(template_str.as_str())
                    .unwrap()
                    .tick_chars("⠁⠁⠉⠙⠚⠒⠂⠂⠒⠲⠴⠤⠄⠄⠤⠠⠠⠤⠦⠖⠒⠐⠐⠒⠓⠋⠉⠈⠈✓"),
            )
            .with_prefix(formatted_prefix);
        self.multi_progress.add(pb.clone());
        // Additional reference for updating bar directly
        self.pbars.push(pb);
    }
}

impl Drop for IndicatifProgressBarManager {
    fn drop(&mut self) {
        if cfg!(feature = "python") {
            GLOBAL_LOGGER.reset_temp_logger();
            STDOUT.reset_target();
        }
    }
}

impl RuntimeStatsSubscriber for IndicatifProgressBarManager {
    #[cfg(test)]
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize_node(&self, node_info: &NodeInfo) -> DaftResult<()> {
        let pb = self.pbars.get(node_info.id).unwrap();
        pb.enable_steady_tick(TICK_INTERVAL);
        Ok(())
    }

    fn finalize_node(&self, node_info: &NodeInfo) -> DaftResult<()> {
        let pb = self.pbars.get(node_info.id).unwrap();
        pb.finish();
        Ok(())
    }

    fn handle_event(&self, events: &[(&NodeInfo, StatSnapshotSend)]) -> DaftResult<()> {
        for (node_info, event) in events {
            let pb = self.pbars.get(node_info.id).unwrap();
            pb.set_message(event_to_message(event));
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> DaftResult<()> {
        self.pbars.clear();
        self.multi_progress.clear()?;
        Ok(())
    }
}

pub const MAX_PIPELINE_NAME_LEN: usize = 18;

pub fn make_progress_bar_manager(
    node_stats: &[(Arc<NodeInfo>, Arc<dyn RuntimeStats>)],
) -> Box<dyn RuntimeStatsSubscriber> {
    #[cfg(feature = "python")]
    {
        if python::in_notebook() {
            Box::new(python::TqdmProgressBarManager::new(node_stats))
        } else {
            Box::new(IndicatifProgressBarManager::new(node_stats))
        }
    }

    #[cfg(not(feature = "python"))]
    {
        Box::new(IndicatifProgressBarManager::new(node_stats))
    }
}

#[cfg(feature = "python")]
mod python {
    use std::collections::HashMap;

    use pyo3::{types::PyAnyMethods, PyObject, Python};

    use super::*;
    use crate::ops::NodeInfo;

    pub fn in_notebook() -> bool {
        pyo3::Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "daft.utils"))
                .and_then(|m| m.getattr(pyo3::intern!(py, "in_notebook")))
                .and_then(|m| m.call0())
                .and_then(|m| m.extract())
                .expect("Failed to determine if running in notebook")
        })
    }

    #[derive(Clone, Debug)]
    pub struct TqdmProgressBarManager {
        inner: Arc<PyObject>,
        node_id_to_pb_id: HashMap<usize, usize>,
    }

    impl TqdmProgressBarManager {
        pub fn new(node_stats: &[(Arc<NodeInfo>, Arc<dyn RuntimeStats>)]) -> Self {
            let mut node_id_to_pb_id = HashMap::new();

            Python::with_gil(|py| {
                let module = py.import("daft.runners.progress_bar")?;
                let progress_bar_class = module.getattr("SwordfishProgressBar")?;
                let pb_object = progress_bar_class.call0()?;

                for (node_info, _) in node_stats {
                    let bar_format = format!(
                        "🗡️ 🐟 {prefix}: {{elapsed}} {{desc}}",
                        prefix = node_info.name
                    );

                    let pb_id = pb_object.call_method1("make_new_bar", (bar_format,))?;
                    let pb_id = pb_id.extract::<usize>()?;

                    node_id_to_pb_id.insert(node_info.id, pb_id);
                }

                DaftResult::Ok(Self {
                    inner: Arc::new(pb_object.into()),
                    node_id_to_pb_id,
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

    impl RuntimeStatsSubscriber for TqdmProgressBarManager {
        #[cfg(test)]
        #[allow(dead_code)]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialize_node(&self, _: &NodeInfo) -> DaftResult<()> {
            Ok(())
        }

        fn finalize_node(&self, node_info: &NodeInfo) -> DaftResult<()> {
            let pb_id = self.node_id_to_pb_id.get(&node_info.id).unwrap();
            self.close_bar(*pb_id)?;
            Ok(())
        }

        fn handle_event(&self, events: &[(&NodeInfo, StatSnapshotSend)]) -> DaftResult<()> {
            for (node_info, event) in events {
                let pb_id = self.node_id_to_pb_id.get(&node_info.id).unwrap();
                self.update_bar(*pb_id, &event_to_message(event))?;
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> DaftResult<()> {
            Ok(())
        }
    }
}
