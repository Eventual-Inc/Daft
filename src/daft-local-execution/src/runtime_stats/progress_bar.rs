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
    pbars: HashMap<NodeID, indicatif::ProgressBar>,
    total: usize,
    persist_on_finish: bool,
}

impl IndicatifProgressBarManager {
    fn new(node_info_map: &HashMap<NodeID, Arc<NodeInfo>>, persist_on_finish: bool) -> Self {
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
            pbars: HashMap::with_capacity(total),
            total,
            persist_on_finish,
        };

        // Determine max name for alignment and minimizing whitespace
        // Use char count (not byte count) since this controls terminal column width
        let max_name_len = (node_info_map
            .values()
            .map(|v| v.name.chars().count())
            .max()
            .unwrap_or(0))
        .min(MAX_PIPELINE_NAME_LEN);

        // Iterate over actual NodeIDs (sorted) rather than assuming `0..total` is dense.
        // This keeps display order deterministic across HashMap iterations and avoids
        // panics if node IDs are sparse or non-consecutive (e.g. distributed tasks
        // where only a subset of plan nodes are dispatched to a worker).
        let mut sorted_ids: Vec<NodeID> = node_info_map.keys().copied().collect();
        sorted_ids.sort_unstable();
        for (display_idx, node_id) in sorted_ids.into_iter().enumerate() {
            if let Some(node_info) = node_info_map.get(&node_id) {
                manager.make_new_bar(node_id, display_idx + 1, node_info.as_ref(), max_name_len);
            }
        }

        manager
    }

    fn make_new_bar(
        &mut self,
        node_id: NodeID,
        display_idx: usize,
        node_info: &NodeInfo,
        max_name_len: usize,
    ) {
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
            node_id = display_idx,
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
        // Additional reference for updating bar directly, keyed by the actual NodeID.
        self.pbars.insert(node_id, pb);
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
        // A missing node_id means the node was not present in the node_info_map at
        // construction (e.g. a registration/dispatch mismatch). A missing telemetry
        // bar is not a fatal condition, so warn and skip rather than panic.
        if let Some(pb) = self.pbars.get(&node_id) {
            pb.enable_steady_tick(TICK_INTERVAL);
        } else {
            log::warn!(
                "Indicatif progress bar not registered for node_id {node_id}; skipping initialize"
            );
        }
    }

    fn finalize_node(&self, node_id: NodeID, last_snapshot: &StatSnapshot) {
        if let Some(pb) = self.pbars.get(&node_id) {
            pb.set_message(last_snapshot.to_message());
            pb.finish();
        } else {
            log::warn!(
                "Indicatif progress bar not registered for node_id {node_id}; skipping finalize"
            );
        }
    }

    fn handle_event(&self, node_id: NodeID, event: &StatSnapshot) {
        if let Some(pb) = self.pbars.get(&node_id) {
            pb.set_message(event.to_message());
        } else {
            log::warn!(
                "Indicatif progress bar not registered for node_id {node_id}; skipping event"
            );
        }
    }

    fn finish(mut self: Box<Self>) -> DaftResult<()> {
        if !self.persist_on_finish {
            self.pbars.clear();
            self.multi_progress.clear()?;
        }
        Ok(())
    }
}

pub fn make_progress_bar_manager(
    node_info_map: &HashMap<NodeID, Arc<NodeInfo>>,
    persist_on_finish: bool,
) -> Box<dyn ProgressBar> {
    #[cfg(feature = "python")]
    {
        if python::in_notebook() {
            Box::new(python::TqdmProgressBarManager::new(node_info_map))
        } else {
            Box::new(IndicatifProgressBarManager::new(
                node_info_map,
                persist_on_finish,
            ))
        }
    }

    #[cfg(not(feature = "python"))]
    {
        Box::new(IndicatifProgressBarManager::new(
            node_info_map,
            persist_on_finish,
        ))
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

                // Iterate over actual NodeIDs (sorted) rather than assuming `0..len`
                // is dense. This avoids panicking when node IDs are sparse or
                // non-consecutive (e.g. distributed tasks dispatch a subset of nodes).
                let mut sorted_ids: Vec<NodeID> = node_info_map.keys().copied().collect();
                sorted_ids.sort_unstable();
                for node_id in sorted_ids {
                    if let Some(node_info) = node_info_map.get(&node_id) {
                        let bar_format = format!(
                            "🗡️ 🐟 {prefix}: {{elapsed}} {{desc}}",
                            prefix = node_info.name
                        );

                        let pb_id = pb_object.call_method1("make_new_bar", (bar_format,))?;
                        let pb_id = pb_id.extract::<usize>()?;

                        node_id_to_pb_id.insert(node_info.id, pb_id);
                    }
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

        fn close_bar(&self, pb_id: usize) -> DaftResult<()> {
            Python::attach(|py| {
                self.inner.call_method1(py, "close_bar", (pb_id,))?;
                DaftResult::Ok(())
            })
        }
    }

    impl ProgressBar for TqdmProgressBarManager {
        fn initialize_node(&self, _: NodeID) {}

        fn finalize_node(&self, node_id: NodeID, last_snapshot: &StatSnapshot) {
            // A missing node_id means the node was not registered at construction.
            // Progress telemetry is non-fatal; warn and skip rather than panic.
            let Some(pb_id) = self.node_id_to_pb_id.get(&node_id).copied() else {
                log::warn!(
                    "Tqdm progress bar not registered for node_id {node_id}; skipping finalize"
                );
                return;
            };
            if let Err(e) = self.update_bar(pb_id, &last_snapshot.to_message()) {
                log::warn!("Failed to update TQDM progress bar for node_id {node_id}: {e}");
            }
            if let Err(e) = self.close_bar(pb_id) {
                log::warn!("Failed to close TQDM progress bar for node_id {node_id}: {e}");
            }
        }

        fn handle_event(&self, node_id: NodeID, event: &StatSnapshot) {
            let Some(pb_id) = self.node_id_to_pb_id.get(&node_id).copied() else {
                log::warn!(
                    "Tqdm progress bar not registered for node_id {node_id}; skipping event"
                );
                return;
            };
            if let Err(e) = self.update_bar(pb_id, &event.to_message()) {
                log::warn!("Failed to update TQDM progress bar for node_id {node_id}: {e}");
            }
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
    use common_metrics::snapshot::DefaultSnapshot;

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
        let _manager = IndicatifProgressBarManager::new(&node_info_map, false);
    }

    #[test]
    fn test_progress_bar_sparse_node_ids_no_panic() {
        // Regression test: prior implementation iterated `0..node_info_map.len()`
        // and unwrapped per-node lookups, so any sparse or non-consecutive NodeID
        // crashed both at construction and at dispatch. Sparse IDs occur in
        // distributed runs where a worker only owns a subset of plan nodes, and
        // can also occur if registration and dispatch maps drift apart.
        let mut node_info_map: HashMap<NodeID, Arc<NodeInfo>> = HashMap::new();
        for &id in &[0_usize, 5, 9] {
            node_info_map.insert(
                id,
                Arc::new(NodeInfo {
                    name: format!("node_{id}").into(),
                    id,
                    ..Default::default()
                }),
            );
        }

        // Construction must not panic on sparse IDs.
        let manager = IndicatifProgressBarManager::new(&node_info_map, false);

        let snapshot = StatSnapshot::Default(DefaultSnapshot {
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            num_tasks: 0,
        });

        // Dispatching to registered IDs must not panic.
        manager.initialize_node(0);
        manager.handle_event(5, &snapshot);
        manager.finalize_node(9, &snapshot);

        // Dispatching to unregistered IDs must not panic; the manager should
        // log a warning and skip the missing telemetry update.
        manager.initialize_node(42);
        manager.handle_event(99, &snapshot);
        manager.finalize_node(123, &snapshot);

        // `finish` must not panic either.
        Box::new(manager).finish().expect("finish should succeed");
    }
}
