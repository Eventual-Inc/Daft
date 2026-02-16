use std::{collections::HashMap, sync::Arc, time::Duration};

use common_error::DaftResult;
use common_logging::GLOBAL_LOGGER;
use common_metrics::{
    NodeID, StatSnapshot,
    ops::{NodeCategory, NodeInfo},
    snapshot::StatSnapshotImpl,
};
use indicatif::{ProgressDrawTarget, ProgressStyle};
use log::Log;

use crate::{PythonPrintTarget, STDOUT};

pub(crate) trait ProgressBar: Send + Sync {
    fn initialize_node(&self, node_id: NodeID);
    fn finalize_node(&self, node_id: NodeID, last_snapshot: &StatSnapshot);
    fn handle_event(&self, node_id: NodeID, event: &StatSnapshot);
    fn finish(self: Box<Self>) -> DaftResult<()>;
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

pub const MAX_PIPELINE_NAME_LEN: usize = 18;

struct IndicatifProgressBarManager {
    multi_progress: indicatif::MultiProgress,
    pbars: Vec<indicatif::ProgressBar>,
    total: usize,
}

impl IndicatifProgressBarManager {
    fn new(node_info_map: &HashMap<NodeID, Arc<NodeInfo>>) -> Self {
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

        let total = node_info_map.len();

        let mut manager = Self {
            multi_progress,
            pbars: Vec::new(),
            total,
        };

        // Determine max name for alignment and minimizing whitespace
        // Use char count (not byte count) since this controls terminal column width
        let max_name_len = (node_info_map
            .values()
            .map(|v| v.name.chars().count())
            .max()
            .unwrap_or(0))
        .min(MAX_PIPELINE_NAME_LEN);

        // For Swordfish only, so node ids should be consecutive
        for node_id in 0..total {
            let node_info = node_info_map
                .get(&node_id)
                .expect("Expected node info for all node ids in range 0..total");
            manager.make_new_bar(node_info.as_ref(), max_name_len);
        }

        manager
    }

    fn make_new_bar(&mut self, node_info: &NodeInfo, max_name_len: usize) {
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

        let formatted_prefix = if node_info.name.chars().count() > MAX_PIPELINE_NAME_LEN {
            let truncated: String = node_info
                .name
                .chars()
                .take(MAX_PIPELINE_NAME_LEN - 3)
                .collect();
            format!("{truncated}...")
        } else {
            format!("{:>1$}", node_info.name, max_name_len)
        };

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

impl ProgressBar for IndicatifProgressBarManager {
    fn initialize_node(&self, node_id: NodeID) {
        let pb = self.pbars.get(node_id).unwrap();
        pb.enable_steady_tick(TICK_INTERVAL);
    }

    fn finalize_node(&self, node_id: NodeID, last_snapshot: &StatSnapshot) {
        let pb = self.pbars.get(node_id).unwrap();
        pb.set_message(last_snapshot.to_message());
        pb.finish();
    }

    fn handle_event(&self, node_id: NodeID, event: &StatSnapshot) {
        let pb = self.pbars.get(node_id).unwrap();
        pb.set_message(event.to_message());
    }

    fn finish(mut self: Box<Self>) -> DaftResult<()> {
        self.pbars.clear();
        self.multi_progress.clear()?;
        Ok(())
    }
}

pub fn make_progress_bar_manager(
    node_info_map: &HashMap<NodeID, Arc<NodeInfo>>,
) -> Box<dyn ProgressBar> {
    #[cfg(feature = "python")]
    {
        if python::in_notebook() {
            Box::new(python::TqdmProgressBarManager::new(node_info_map))
        } else {
            Box::new(IndicatifProgressBarManager::new(node_info_map))
        }
    }

    #[cfg(not(feature = "python"))]
    {
        Box::new(IndicatifProgressBarManager::new(node_info_map))
    }
}

#[cfg(feature = "python")]
mod python {
    use std::collections::HashMap;

    use common_metrics::ops::NodeInfo;
    use pyo3::{Python, types::PyAnyMethods};

    use super::*;

    pub fn in_notebook() -> bool {
        pyo3::Python::attach(|py| {
            py.import(pyo3::intern!(py, "daft.utils"))
                .and_then(|m| m.getattr(pyo3::intern!(py, "in_notebook")))
                .and_then(|m| m.call0())
                .and_then(|m| m.extract())
                .expect("Failed to determine if running in notebook")
        })
    }

    #[derive(Clone)]
    pub struct TqdmProgressBarManager {
        inner: Arc<pyo3::Py<pyo3::PyAny>>,
        node_id_to_pb_id: HashMap<usize, usize>,
    }

    impl TqdmProgressBarManager {
        pub fn new(node_info_map: &HashMap<NodeID, Arc<NodeInfo>>) -> Self {
            let mut node_id_to_pb_id = HashMap::new();

            Python::attach(|py| {
                let module = py.import("daft.runners.progress_bar")?;
                let progress_bar_class = module.getattr("SwordfishProgressBar")?;
                let pb_object = progress_bar_class.call0()?;

                // For Swordfish only, so node ids should be consecutive
                for node_id in 0..node_info_map.len() {
                    let node_info = node_info_map
                        .get(&node_id)
                        .expect("Expected node info for all node ids in range 0..total");
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
            Python::attach(|py| {
                self.inner
                    .call_method1(py, "update_bar", (pb_id, message))?;
                DaftResult::Ok(())
            })
        }

        fn close_bar(&self, pb_id: usize) {
            Python::attach(|py| {
                // Should never fail, since it's empty anyways
                self.inner
                    .call_method1(py, "close_bar", (pb_id,))
                    .expect("Failed to close bar");
            });
        }
    }

    impl ProgressBar for TqdmProgressBarManager {
        fn initialize_node(&self, _: NodeID) {}

        fn finalize_node(&self, node_id: NodeID, last_snapshot: &StatSnapshot) {
            let pb_id = self.node_id_to_pb_id.get(&node_id).unwrap();
            self.update_bar(*pb_id, &last_snapshot.to_message())
                .expect("Failed to update TQDM progress bar");

            self.close_bar(*pb_id);
        }

        fn handle_event(&self, node_id: NodeID, event: &StatSnapshot) {
            let pb_id = self.node_id_to_pb_id.get(&node_id).unwrap();
            self.update_bar(*pb_id, &event.to_message())
                .expect("Failed to update TQDM progress bar");
        }

        fn finish(self: Box<Self>) -> DaftResult<()> {
            Python::attach(|py| {
                self.inner.call_method0(py, "close")?;
                DaftResult::Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_bar_truncation_on_multibyte_utf8() {
        // Regression test: IndicatifProgressBarManager::new() panics when a node name
        // contains multi-byte UTF-8 characters and exceeds MAX_PIPELINE_NAME_LEN (18 bytes).
        // See: https://github.com/Eventual-Inc/Daft/actions/runs/21921434809
        let node_info = Arc::new(NodeInfo {
            name: "ññññññññññ".into(), // 10 × ñ = 20 bytes > 18
            id: 0,
            ..Default::default()
        });
        let mut node_info_map = HashMap::new();
        node_info_map.insert(0, node_info);

        // This panics in make_new_bar due to byte-level string slicing on multi-byte UTF-8
        let _manager = IndicatifProgressBarManager::new(&node_info_map);
    }
}
