use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, SinkInfo};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    stage::{StageContext, StageID},
};

#[derive(Clone)]
pub(crate) struct SinkNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    sink_info: Arc<SinkInfo>,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl SinkNode {
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        sink_info: Arc<SinkInfo>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            sink_info,
            schema,
            config,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn create_sink_plan(&self, input: LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
        let schema = input.schema().clone();
        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(info) => {
                let info = info.clone().bind(&schema)?;
                Ok(LocalPhysicalPlan::physical_write(
                    input,
                    schema,
                    self.schema.clone(),
                    info,
                    StatsState::NotMaterialized,
                ))
            }
            #[cfg(feature = "python")]
            SinkInfo::CatalogInfo(info) => match &info.catalog {
                daft_logical_plan::CatalogType::DeltaLake(..)
                | daft_logical_plan::CatalogType::Iceberg(..) => {
                    Ok(LocalPhysicalPlan::catalog_write(
                        input,
                        info.catalog.clone().bind(&schema)?,
                        schema,
                        self.schema.clone(),
                        StatsState::NotMaterialized,
                    ))
                }
                daft_logical_plan::CatalogType::Lance(info) => Ok(LocalPhysicalPlan::lance_write(
                    input,
                    info.clone(),
                    schema,
                    self.schema.clone(),
                    StatsState::NotMaterialized,
                )),
            },
            #[cfg(feature = "python")]
            SinkInfo::DataSinkInfo(data_sink_info) => Ok(LocalPhysicalPlan::data_sink(
                input,
                data_sink_info.clone(),
                self.schema.clone(),
                StatsState::NotMaterialized,
            )),
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
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
        res.push(format!("Output schema = {}", self.schema.short_string()));
        res
    }
}

impl TreeDisplay for SinkNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.name()).unwrap();
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
        self.name().to_string()
    }
}

impl DistributedPipelineNode for SinkNode {
    fn name(&self) -> &'static str {
        "DistributedSinkNode"
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode {
        let context = {
            let child_name = self.child.name();
            let child_id = self.child.node_id();

            HashMap::from([
                ("plan_id".to_string(), self.plan_id.to_string()),
                ("stage_id".to_string(), format!("{}", self.stage_id)),
                ("node_id".to_string(), format!("{}", self.node_id)),
                ("node_name".to_string(), self.name().to_string()),
                ("child_id".to_string(), format!("{}", child_id)),
                ("child_name".to_string(), child_name.to_string()),
            ])
        };

        let input_node = self.child.clone().start(stage_context);

        // Create the plan builder closure that uses the create_sink_plan method
        let sink_node = self.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> DaftResult<LocalPhysicalPlanRef> {
            sink_node.create_sink_plan(input)
        };

        input_node.pipeline_instruction(
            stage_context,
            self.config.clone(),
            self.node_id,
            self.schema.clone(),
            context,
            plan_builder,
        )
    }

    fn plan_id(&self) -> &PlanID {
        &self.plan_id
    }

    fn stage_id(&self) -> &StageID {
        &self.stage_id
    }

    fn node_id(&self) -> &NodeID {
        &self.node_id
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
