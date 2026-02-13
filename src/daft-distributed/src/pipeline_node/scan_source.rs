use std::sync::{Arc, atomic::Ordering};

use common_display::{DisplayAs, DisplayLevel};
#[cfg(feature = "python")]
use common_file_formats::FileFormatConfig;
use common_metrics::{
    CPU_US_KEY, Counter, ROWS_OUT_KEY, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::SourceSnapshot,
};
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream};
use opentelemetry::{KeyValue, metrics::Meter};

use super::{
    NodeName, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
};
use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
};

pub struct SourceStats {
    cpu_us: Counter,
    rows_out: Counter,
    bytes_read: Counter,
    node_kv: Vec<KeyValue>,
}

impl SourceStats {
    pub fn new(meter: &Meter, node_id: NodeID) -> Self {
        let node_kv = vec![KeyValue::new("node_id", node_id.to_string())];
        Self {
            cpu_us: Counter::new(meter, CPU_US_KEY, None),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None),
            bytes_read: Counter::new(meter, "bytes read", None),
            node_kv,
        }
    }
}

impl RuntimeStats for SourceStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Source(snapshot) = snapshot else {
            return;
        };
        self.cpu_us.add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());
        self.bytes_read
            .add(snapshot.bytes_read, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Source(SourceSnapshot {
            cpu_us: self.cpu_us.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        })
    }
}

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
    const NODE_NAME: NodeName = "ScanSource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn make_source_task(self: &Arc<Self>, scan_task: ScanTaskLikeRef) -> SwordfishTaskBuilder {
        let physical_scan = LocalPhysicalPlan::physical_scan(
            self.node_id(),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        SwordfishTaskBuilder::new(physical_scan, self.as_ref())
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
        fn base_display(scan: &ScanSourceNode) -> Vec<String> {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| {
                    st.estimate_in_memory_size_bytes(Some(scan.config.execution_config.as_ref()))
                        .or_else(|| st.size_bytes_on_disk())
                        .unwrap_or(0)
                })
                .sum();

            #[allow(unused_mut)]
            let mut s = vec![
                "ScanTaskSource:".to_string(),
                format!("Num Scan Tasks = {num_scan_tasks}"),
                format!("Estimated Scan Bytes = {total_bytes}"),
            ];

            if num_scan_tasks == 0 {
                return s;
            }

            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config().as_ref()
            {
                if num_scan_tasks == 1 {
                    s.push(format!("SQL Query = {}", &config.sql));
                } else {
                    s.push(format!("SQL Queries = [{},..]", &config.sql));
                }
            }
            s
        }

        let mut s = base_display(self);
        if !verbose {
            let pushdown = &self.pushdowns;
            if !pushdown.is_empty() {
                s.push(pushdown.display_as(DisplayLevel::Compact));
            }

            let schema = &self.config.schema;
            s.push(format!(
                "Schema: {{{}}}",
                schema.display_as(DisplayLevel::Compact)
            ));

            s.push("Scan Tasks: [".to_string());
            let tasks = self.scan_tasks.iter();
            for (i, st) in tasks.enumerate() {
                if i < 3 || i >= self.scan_tasks.len() - 3 {
                    s.push(st.as_ref().display_as(DisplayLevel::Compact));
                } else if i == 3 {
                    s.push("...".to_string());
                }
            }
        } else {
            s.push("Scan Tasks: [".to_string());

            for st in self.scan_tasks.iter() {
                s.push(st.as_ref().display_as(DisplayLevel::Verbose));
            }
        }
        s.push("]".to_string());
        s
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SourceStats::new(meter, self.node_id()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        _plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        if self.scan_tasks.is_empty() {
            let transformed_plan = LocalPhysicalPlan::empty_scan(
                self.config.schema.clone(),
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let empty_scan_task = SwordfishTaskBuilder::new(transformed_plan, self.as_ref());
            TaskBuilderStream::new(stream::iter(std::iter::once(empty_scan_task)).boxed())
        } else {
            let slf = self.clone();
            let builders_iter = (0..self.scan_tasks.len())
                .map(move |i| slf.make_source_task(slf.scan_tasks[i].clone()));
            TaskBuilderStream::new(stream::iter(builders_iter).boxed())
        }
    }
}
