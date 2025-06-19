use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_file_formats::WriteMode;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, SinkInfo};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct SinkNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    sink_info: Arc<SinkInfo>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl SinkNode {
    const NODE_NAME: NodeName = "Sink";

    pub fn new(
        stage_config: &StageConfig,
        node_id: NodeID,
        sink_info: Arc<SinkInfo>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![*child.node_id()],
            vec![child.name()],
        );
        let config = PipelineNodeConfig::new(schema, stage_config.config.clone());
        Self {
            config,
            context,
            sink_info,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn create_sink_plan(&self, input: LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
        let data_schema = input.schema().clone();
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
            sink_node.create_sink_plan(input)
        };

        let pipelined_node_with_writes =
            input_node.pipeline_instruction(stage_context, self.clone(), plan_builder);
        if let SinkInfo::OutputFileInfo(info) = self.sink_info.as_ref()
            && matches!(
                info.write_mode,
                WriteMode::Overwrite | WriteMode::OverwritePartitions
            )
        {
            pipelined_node_with_writes.pipeline_instruction(
                stage_context,
                self,
                move |input: LocalPhysicalPlanRef| -> DaftResult<LocalPhysicalPlanRef> {
                    Ok(LocalPhysicalPlan::commit_write(
                        input,
                        StatsState::NotMaterialized,
                    ))
                },
            )
        } else {
            pipelined_node_with_writes
        }
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
