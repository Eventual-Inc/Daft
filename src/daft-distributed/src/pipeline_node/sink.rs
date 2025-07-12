use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_file_formats::WriteMode;
use daft_dsl::ExprRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, OutputFileInfo, SinkInfo};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use super::{
    make_new_task_from_materialized_outputs, DistributedPipelineNode, PipelineOutput,
    RunningPipelineNode,
};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct SinkNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    sink_info: Arc<SinkInfo>,
    data_schema: SchemaRef,
    child: Arc<dyn DistributedPipelineNode>,
}

impl SinkNode {
    const NODE_NAME: NodeName = "Sink";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        sink_info: Arc<SinkInfo>,
        file_schema: SchemaRef,
        data_schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            file_schema,
            stage_config.config.clone(),
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

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn create_sink_plan(
        &self,
        input: LocalPhysicalPlanRef,
        data_schema: SchemaRef,
    ) -> DaftResult<LocalPhysicalPlanRef> {
        let file_schema = self.config.schema.clone();
        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(info) => {
                let info = info.clone().bind(&data_schema)?;
                Ok(LocalPhysicalPlan::physical_write(
                    input,
                    data_schema,
                    file_schema,
                    info,
                    StatsState::NotMaterialized,
                ))
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(info) => match &info.catalog {
                daft_logical_plan::CatalogType::DeltaLake(..)
                | daft_logical_plan::CatalogType::Iceberg(..) => {
                    let catalog = info.catalog.clone().bind(&data_schema)?;
                    Ok(LocalPhysicalPlan::catalog_write(
                        input,
                        catalog,
                        data_schema,
                        file_schema,
                        StatsState::NotMaterialized,
                    ))
                }
                daft_logical_plan::CatalogType::Lance(info) => Ok(LocalPhysicalPlan::lance_write(
                    input,
                    info.clone(),
                    data_schema,
                    file_schema,
                    StatsState::NotMaterialized,
                )),
            },
            #[cfg(feature = "python")]
            SinkInfo::DataSinkInfo(data_sink_info) => Ok(LocalPhysicalPlan::data_sink(
                input,
                data_sink_info.clone(),
                file_schema,
                StatsState::NotMaterialized,
            )),
        }
    }

    async fn finish_writes_and_commit(
        self: Arc<Self>,
        info: OutputFileInfo<ExprRef>,
        input: RunningPipelineNode,
        scheduler: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
        sender: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        let file_schema = self.config.schema.clone();
        let data_schema = self.data_schema.clone();
        let materialized_stream = input.materialize(scheduler);
        let materialized = materialized_stream.try_collect::<Vec<_>>().await?;
        let task = make_new_task_from_materialized_outputs(
            TaskContext::from((&self.context, task_id_counter.next())),
            materialized,
            &(self as Arc<dyn DistributedPipelineNode>),
            &move |input| {
                Ok(LocalPhysicalPlan::commit_write(
                    input,
                    file_schema.clone(),
                    info.clone().bind(&data_schema)?,
                    StatsState::NotMaterialized,
                ))
            },
        )?;
        let _ = sender.send(PipelineOutput::Task(task)).await;
        Ok(())
    }

    fn multiline_display(&self) -> Vec<String> {
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
}

impl TreeDisplay for SinkNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for SinkNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        let sink_node = self.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> DaftResult<LocalPhysicalPlanRef> {
            sink_node.create_sink_plan(input, sink_node.data_schema.clone())
        };

        let pipelined_node_with_writes =
            input_node.pipeline_instruction(stage_context, self.clone(), plan_builder);
        if let SinkInfo::OutputFileInfo(info) = self.sink_info.as_ref()
            && matches!(
                info.write_mode,
                WriteMode::Overwrite | WriteMode::OverwritePartitions
            )
        {
            let sink_node = self.clone();
            let scheduler = stage_context.scheduler_handle();
            let task_id_counter = stage_context.task_id_counter();
            let (sender, receiver) = create_channel(1);
            stage_context.spawn(Self::finish_writes_and_commit(
                sink_node,
                info.clone(),
                pipelined_node_with_writes,
                scheduler,
                task_id_counter,
                sender,
            ));
            RunningPipelineNode::new(receiver)
        } else {
            pipelined_node_with_writes
        }
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
