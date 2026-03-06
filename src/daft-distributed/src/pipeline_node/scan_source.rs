use std::sync::{Arc, atomic::Ordering};

use common_display::{DisplayAs, DisplayLevel};
use common_metrics::{
    BYTES_READ_KEY, Counter, DURATION_KEY, ROWS_OUT_KEY, StatSnapshot, UNIT_BYTES,
    UNIT_MICROSECONDS, UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::SourceSnapshot,
};
use common_scan_info::{LazyTaskProducer, Pushdowns};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream};
use opentelemetry::{KeyValue, metrics::Meter};
use tokio_stream::wrappers::ReceiverStream;

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
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
            bytes_read: Counter::new(meter, BYTES_READ_KEY, None, Some(UNIT_BYTES.into())),
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
        StatSnapshot::Source(SourceSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
        })
    }
}

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    lazy_producer: Arc<LazyTaskProducer>,
}

impl ScanSourceNode {
    const NODE_NAME: &'static str = "ScanSource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pushdowns: Pushdowns,
        lazy_producer: Arc<LazyTaskProducer>,
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
                lazy_producer.estimated_stats.estimated_num_tasks,
            )),
        );
        Self {
            config,
            context,
            pushdowns,
            lazy_producer,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn make_source_task(
        self: &Arc<Self>,
        scan_task: common_scan_info::ScanTaskLikeRef,
    ) -> SwordfishTaskBuilder {
        let physical_scan = LocalPhysicalPlan::physical_scan(
            self.node_id(),
            Some(scan_task.file_format_config()),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
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
        let mut res = vec!["ScanTaskSource:".to_string()];

        let stats = &self.lazy_producer.estimated_stats;
        res.push(format!(
            "Num Scan Tasks = ~{} (estimated)",
            stats.estimated_num_tasks
        ));
        res.push(format!(
            "Estimated Scan Bytes = {}",
            stats.estimated_total_bytes
        ));

        if !verbose {
            let pushdown = &self.pushdowns;
            if !pushdown.is_empty() {
                res.push(pushdown.display_as(DisplayLevel::Compact));
            }

            let schema = &self.config.schema;
            res.push(format!(
                "Schema: {{{}}}",
                schema.display_as(DisplayLevel::Compact)
            ));
        }

        res
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SourceStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        _plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        if self.lazy_producer.estimated_stats.estimated_num_tasks == 0 {
            let transformed_plan = LocalPhysicalPlan::empty_scan(
                self.config.schema.clone(),
                LocalNodeContext::new(Some(self.node_id() as usize)),
            );
            let empty_scan_task = SwordfishTaskBuilder::new(transformed_plan, self.as_ref());
            TaskBuilderStream::new(stream::iter(std::iter::once(empty_scan_task)).boxed())
        } else {
            let slf = self;
            let (tx, rx) = tokio::sync::mpsc::channel(64);

            // Spawn a blocking thread to iterate the lazy producer and send tasks
            // through the channel. When the pipeline shuts down (e.g. limit satisfied),
            // the receiver drops, tx.blocking_send() returns Err, and iteration stops.
            std::thread::spawn(move || {
                let iter = match slf.lazy_producer.produce() {
                    Ok(iter) => iter,
                    Err(e) => {
                        tracing::error!("Error producing lazy scan tasks: {e}");
                        return;
                    }
                };
                for task_result in iter {
                    match task_result {
                        Ok(task) => {
                            let builder = slf.make_source_task(task);
                            if tx.blocking_send(builder).is_err() {
                                break; // Pipeline closed
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error in lazy scan task iteration: {e}");
                            break;
                        }
                    }
                }
            });

            TaskBuilderStream::new(ReceiverStream::new(rx).boxed())
        }
    }
}
