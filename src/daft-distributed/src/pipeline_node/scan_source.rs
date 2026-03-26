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
    }

    fn export_snapshot(&self) -> StatSnapshot {
        let rows_out = self.rows_out.load(Ordering::SeqCst);
        StatSnapshot::Source(SourceSnapshot {
            cpu_us: self.duration_us.load(Ordering::SeqCst),
            rows_out,
            bytes_read: self.bytes_read.load(Ordering::SeqCst),
            estimated_total_rows: rows_out,
        })
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
