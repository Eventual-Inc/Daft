use std::sync::{Arc, atomic::Ordering};

use common_error::DaftResult;
use common_metrics::{
    BYTES_WRITTEN_KEY, Counter, ROWS_IN_KEY, ROWS_WRITTEN_KEY, StatSnapshot, TASK_DURATION_KEY,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::WriteSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{OutputFileInfo, SinkInfo, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;
use opentelemetry::{KeyValue, metrics::Meter};

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

pub struct WriteStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_written: Counter,
    bytes_written: Counter,
    node_kv: Vec<KeyValue>,
}

impl WriteStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            cpu_us: Counter::new(meter, TASK_DURATION_KEY, None),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None),
            rows_written: Counter::new(meter, ROWS_WRITTEN_KEY, None),
            bytes_written: Counter::new(meter, BYTES_WRITTEN_KEY, None),
            node_kv,
        }
    }
}

impl RuntimeStats for WriteStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Write(snapshot) = snapshot else {
            return;
        };
        self.cpu_us.add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_written
            .add(snapshot.rows_written, self.node_kv.as_slice());
        self.bytes_written
            .add(snapshot.bytes_written, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Write(WriteSnapshot {
            cpu_us: self.cpu_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        })
    }
}

pub(crate) struct SinkNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    sink_info: Arc<SinkInfo<BoundExpr>>,
    data_schema: SchemaRef,
    child: DistributedPipelineNode,
}

impl SinkNode {
    const NODE_NAME: NodeName = "Sink";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        sink_info: Arc<SinkInfo<BoundExpr>>,
        file_schema: SchemaRef,
        data_schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::Write,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            file_schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            sink_info,
            data_schema,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn create_sink_plan(
        &self,
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
    ) -> LocalPhysicalPlanRef {
        let file_schema = self.config.schema.clone();
        let node_id = self.node_id();
        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(info) => LocalPhysicalPlan::physical_write(
                input,
                data_schema,
                file_schema,
                info.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            ),
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(info) => match &info.catalog {
                daft_logical_plan::CatalogType::DeltaLake(..)
                | daft_logical_plan::CatalogType::Iceberg(..) => LocalPhysicalPlan::catalog_write(
                    input,
                    info.catalog.clone(),
                    data_schema,
                    file_schema,
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(node_id as usize),
                        additional: None,
                    },
                ),
                daft_logical_plan::CatalogType::Lance(info) => LocalPhysicalPlan::lance_write(
                    input,
                    info.clone(),
                    data_schema,
                    file_schema,
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(node_id as usize),
                        additional: None,
                    },
                ),
            },
            #[cfg(feature = "python")]
            SinkInfo::DataSinkInfo(data_sink_info) => LocalPhysicalPlan::data_sink(
                input,
                data_sink_info.clone(),
                file_schema,
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            ),
        }
    }

    async fn finish_writes_and_commit(
        self: Arc<Self>,
        info: OutputFileInfo<BoundExpr>,
        input: TaskBuilderStream,
        scheduler: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
        sender: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let file_schema = self.config.schema.clone();
        let materialized_stream =
            input.materialize(scheduler, self.context.query_idx, task_id_counter);
        let materialized = materialized_stream.try_collect::<Vec<_>>().await?;

        let (in_memory_source_plan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
            materialized,
            self.data_schema.clone(),
            self.node_id(),
        );
        let plan = LocalPhysicalPlan::commit_write(
            in_memory_source_plan,
            self.data_schema.clone(),
            file_schema,
            info,
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );
        let builder = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);
        let _ = sender.send(builder).await;
        Ok(())
    }
}

impl PipelineNodeImpl for SinkNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![];

        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                res.push(format!("Sink: {:?}", output_file_info.file_format));
                res.extend(output_file_info.multiline_display());
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(catalog_info) => match &catalog_info.catalog {
                daft_logical_plan::CatalogType::Iceberg(iceberg_info) => {
                    res.push(format!("Sink: Iceberg({})", iceberg_info.table_name));
                    res.extend(iceberg_info.multiline_display());
                }
                daft_logical_plan::CatalogType::DeltaLake(deltalake_info) => {
                    res.push(format!("Sink: DeltaLake({})", deltalake_info.path));
                    res.extend(deltalake_info.multiline_display());
                }
                daft_logical_plan::CatalogType::Lance(lance_info) => {
                    res.push(format!("Sink: Lance({})", lance_info.path));
                    res.extend(lance_info.multiline_display());
                }
            },
            #[cfg(feature = "python")]
            SinkInfo::DataSinkInfo(data_sink_info) => {
                res.push(format!("Sink: DataSink({})", data_sink_info.name));
            }
        }
        res.push(format!(
            "Output schema = {}",
            self.config.schema.short_string()
        ));
        res
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(WriteStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let sink_node = self.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            sink_node.create_sink_plan(input, sink_node.data_schema.clone())
        };

        let pipelined_node_with_writes =
            input_node.pipeline_instruction(self.clone(), plan_builder);
        if let SinkInfo::OutputFileInfo(info) = self.sink_info.as_ref() {
            let (sender, receiver) = create_channel(1);
            plan_context.spawn(self.clone().finish_writes_and_commit(
                info.clone(),
                pipelined_node_with_writes,
                plan_context.scheduler_handle(),
                plan_context.task_id_counter(),
                sender,
            ));
            TaskBuilderStream::from(receiver)
        } else {
            pipelined_node_with_writes
        }
    }
}
