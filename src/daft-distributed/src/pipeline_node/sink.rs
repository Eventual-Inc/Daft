use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{OutputFileInfo, SinkInfo, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use super::{PipelineNodeImpl, SubmittableTaskStream, make_new_task_from_materialized_outputs};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    utils::channel::{Sender, create_channel},
};

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
        data_schema: SchemaRef,
        info: OutputFileInfo<BoundExpr>,
        input: SubmittableTaskStream,
        scheduler: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
        sender: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        let file_schema = self.config.schema.clone();
        let materialized_stream = input.materialize(scheduler);
        let materialized = materialized_stream.try_collect::<Vec<_>>().await?;
        let node_id = self.node_id();
        let task = make_new_task_from_materialized_outputs(
            TaskContext::from((&self.context, task_id_counter.next())),
            materialized,
            self.config.schema.clone(),
            &(self as Arc<dyn PipelineNodeImpl>),
            move |input| {
                LocalPhysicalPlan::commit_write(
                    input,
                    data_schema,
                    file_schema,
                    info,
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(node_id as usize),
                        additional: None,
                    },
                )
            },
            None,
        );
        let _ = sender.send(task).await;
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

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let sink_node = self.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            sink_node.create_sink_plan(input, sink_node.data_schema.clone())
        };

        let pipelined_node_with_writes =
            input_node.pipeline_instruction(self.clone(), plan_builder);
        if let SinkInfo::OutputFileInfo(info) = self.sink_info.as_ref() {
            let sink_node = self.clone();
            let scheduler = plan_context.scheduler_handle();
            let task_id_counter = plan_context.task_id_counter();
            let data_schema = sink_node.data_schema.clone();
            let (sender, receiver) = create_channel(1);
            plan_context.spawn(Self::finish_writes_and_commit(
                sink_node,
                data_schema,
                info.clone(),
                pipelined_node_with_writes,
                scheduler,
                task_id_counter,
                sender,
            ));
            SubmittableTaskStream::from(receiver)
        } else {
            pipelined_node_with_writes
        }
    }
}
