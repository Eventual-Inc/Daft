use std::sync::{Arc, atomic::Ordering};

use common_display::{DisplayAs, DisplayLevel};
use common_metrics::{
    BYTES_READ_KEY, Counter, Meter, StatSnapshot, UNIT_BYTES,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::SourceSnapshot,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_scan::{Pushdowns, ScanTaskRef, SourceConfig};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream};
use opentelemetry::KeyValue;

use super::{PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID, metrics::key_values_from_context},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
};

pub struct SourceStats {
    duration_us: Counter,
    rows_out: Counter,
    bytes_read: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    node_kv: Vec<KeyValue>,
}

impl SourceStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_read: meter.u64_counter_with_desc_and_unit(
                BYTES_READ_KEY,
                None,
                Some(UNIT_BYTES.into()),
            ),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            node_kv,
        }
    }
}

impl RuntimeStats for SourceStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Source(snapshot) = snapshot else {
            return;
        };
        self.duration_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());
        self.bytes_read
            .add(snapshot.bytes_read, self.node_kv.as_slice());
        self.bytes_out
            .add(snapshot.bytes_out, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Source(SourceSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            num_tasks: self.num_tasks.load(Ordering::Relaxed),
        })
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }
}

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskRef>>,
}

impl ScanSourceNode {
    const NODE_NAME: &'static str = "ScanTaskSource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskRef>>,
        schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::ScanTask,
            NodeCategory::Source,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(
                scan_tasks.len(),
            )),
        );
        Self {
            config,
            context,
            pushdowns,
            scan_tasks,
        }
    }

    fn make_source_task(self: &Arc<Self>, scan_task: ScanTaskRef) -> SwordfishTaskBuilder {
        let physical_scan = LocalPhysicalPlan::physical_scan(
            self.node_id(),
            Some(scan_task.source_config.clone()),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
        );

        SwordfishTaskBuilder::new(physical_scan, self.as_ref(), self.node_id())
            .with_scan_tasks(self.node_id(), vec![scan_task])
    }
}

impl PipelineNodeImpl for ScanSourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![]
    }

    fn multiline_display(&self, verbose: bool) -> Vec<String> {
        let mut res = vec!["ScanTaskSource:".to_string()];

        let num_scan_tasks = self.scan_tasks.len();
        let total_bytes: usize = self
            .scan_tasks
            .iter()
            .map(|st| {
                st.estimate_in_memory_size_bytes(Some(self.config.execution_config.as_ref()))
                    .or_else(|| st.size_bytes_on_disk())
                    .unwrap_or(0)
            })
            .sum();
        res.push(format!("Num Scan Tasks = {num_scan_tasks}"));
        res.push(format!("Estimated Scan Bytes = {total_bytes}"));

        if let Some(sc) = self
            .scan_tasks
            .first()
            .map(|s| s.source_config.clone())
            .as_deref()
        {
            match sc {
                #[cfg(feature = "python")]
                SourceConfig::Database(config) => {
                    if num_scan_tasks == 1 {
                        res.push(format!("SQL Query = {}", &config.sql));
                    } else {
                        res.push(format!("SQL Queries = [{},..]", &config.sql));
                    }
                }
                #[cfg(feature = "python")]
                SourceConfig::PythonFunction { source_name, .. } => {
                    res.push(format!(
                        "Source = {}",
                        source_name.clone().unwrap_or_else(|| "None".to_string())
                    ));
                }
                _ => {}
            }
        }

        if verbose {
            res.push("Scan Tasks: [".to_string());
            for st in self.scan_tasks.iter() {
                res.push(st.as_ref().display_as(DisplayLevel::Verbose));
            }
        } else {
            let pushdown = &self.pushdowns;
            if !pushdown.is_empty() {
                res.push(pushdown.display_as(DisplayLevel::Compact));
            }

            let schema = &self.config.schema;
            res.push(format!(
                "Schema: {{{}}}",
                schema.display_as(DisplayLevel::Compact)
            ));

            res.push("Scan Tasks: [".to_string());
            let tasks = self.scan_tasks.iter();
            for (i, st) in tasks.enumerate() {
                if i < 3 || i >= self.scan_tasks.len() - 3 {
                    res.push(st.as_ref().display_as(DisplayLevel::Compact));
                } else if i == 3 {
                    res.push("...".to_string());
                }
            }
        }

        res.push("]".to_string());
        res
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SourceStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        _plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        if self.scan_tasks.is_empty() {
            let physical_scan = LocalPhysicalPlan::physical_scan(
                self.node_id(),
                None,
                self.pushdowns.clone(),
                self.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)),
            );

            let empty_scan_task =
                SwordfishTaskBuilder::new(physical_scan, self.as_ref(), self.node_id())
                    .with_scan_tasks(self.node_id(), vec![]);
            TaskBuilderStream::new(stream::iter(std::iter::once(empty_scan_task)).boxed())
        } else {
            let slf = self.clone();
            let builders_iter = (0..self.scan_tasks.len())
                .map(move |i| slf.make_source_task(slf.scan_tasks[i].clone()));
            TaskBuilderStream::new(stream::iter(builders_iter).boxed())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_common_error::DaftResult;
    use common_metrics::{Meter, StatSnapshot};
    use daft_scan::{
        CsvSourceConfig, FileFormatConfig, Pushdowns, ScanSource, ScanSourceKind, ScanTask,
        ScanTaskRef, SourceConfig, storage_config::StorageConfig,
    };

    use super::*;
    use crate::pipeline_node::test_helpers::{
        run_pipeline_and_get_stats, test_plan_config, test_schema,
    };

    fn write_csv_file(path: &std::path::Path, values: &[i64]) -> u64 {
        use std::io::Write;
        let mut file = std::fs::File::create(path).expect("create csv file");
        writeln!(file, "x").expect("write header");
        for v in values {
            writeln!(file, "{v}").expect("write row");
        }
        file.sync_all().expect("sync");
        drop(file);
        std::fs::metadata(path).expect("metadata").len()
    }

    fn build_scan_pipeline(
        file_paths: &[std::path::PathBuf],
        meter: &Meter,
    ) -> DistributedPipelineNode {
        let schema = test_schema();
        let csv_cfg = CsvSourceConfig {
            delimiter: None,
            has_headers: true,
            double_quote: true,
            quote: None,
            escape_char: None,
            comment: None,
            allow_variable_columns: false,
            buffer_size: None,
            chunk_size: None,
        };
        let source_config = Arc::new(SourceConfig::File(FileFormatConfig::Csv(csv_cfg)));
        let storage_config = Arc::new(StorageConfig::new_internal(false, None));
        let pushdowns = Pushdowns::default();

        let scan_tasks: Vec<ScanTaskRef> = file_paths
            .iter()
            .map(|path| {
                let size = std::fs::metadata(path).expect("file metadata").len();
                Arc::new(ScanTask::new(
                    vec![ScanSource {
                        size_bytes: Some(size),
                        metadata: None,
                        statistics: None,
                        partition_spec: None,
                        kind: ScanSourceKind::File {
                            path: path.to_string_lossy().into_owned(),
                            chunk_spec: None,
                            iceberg_delete_files: None,
                            parquet_metadata: None,
                        },
                    }],
                    source_config.clone(),
                    schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                ))
            })
            .collect();

        let plan_config = test_plan_config();
        let scan_source_node =
            ScanSourceNode::new(0, &plan_config, pushdowns, Arc::new(scan_tasks), schema);

        DistributedPipelineNode::new(Arc::new(scan_source_node), meter)
    }

    /// Regression test: Flotilla correctly reports `bytes_read` on multi-file scans.
    ///
    /// Previously (issue #6761), `NativeExecutor`'s plan-sharing caused an N×
    /// overcount: all scan tasks from one `ScanSourceNode` shared a single
    /// `IOStatsRef`, so each `take_input_snapshot` read the cumulative counter.
    /// This test guards against that regression.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bytes_read_multiple_files() -> DaftResult<()> {
        let meter = Meter::test_scope("test_scan_bytes_read_multi_file");

        let tmpdir = std::env::temp_dir().join(format!(
            "daft_flotilla_bytes_read_repro_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&tmpdir).expect("create tmpdir");

        let num_files: usize = 5;
        let rows_per_file: usize = 1_000;
        let mut file_paths = Vec::with_capacity(num_files);
        let mut total_bytes_on_disk: u64 = 0;
        for i in 0..num_files {
            let path = tmpdir.join(format!("file_{i}.csv"));
            let values: Vec<i64> = (0..rows_per_file)
                .map(|j| (i * rows_per_file + j) as i64)
                .collect();
            total_bytes_on_disk += write_csv_file(&path, &values);
            file_paths.push(path);
        }

        let pipeline = build_scan_pipeline(&file_paths, &meter);
        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 0)
            .expect("scan source stats");

        let bytes_read = match snapshot {
            StatSnapshot::Source(s) => s.bytes_read,
            other => panic!("expected Source snapshot, got: {other:?}"),
        };

        let _ = std::fs::remove_dir_all(&tmpdir);

        assert!(
            bytes_read <= total_bytes_on_disk * 3,
            "bytes_read ({bytes_read}) > 3x file size ({total_bytes_on_disk}) — overreporting"
        );

        Ok(())
    }
}
