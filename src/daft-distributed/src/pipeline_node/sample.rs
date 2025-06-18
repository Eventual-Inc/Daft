use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    stage::{StageContext, StageID},
};

pub(crate) struct SampleNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    fraction: f64,
    with_replacement: bool,
    seed: Option<u64>,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl SampleNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            fraction,
            with_replacement,
            seed,
            schema,
            config,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Sample: {}", self.fraction));
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        res
    }
}

impl TreeDisplay for SampleNode {
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

impl DistributedPipelineNode for SampleNode {
    fn name(&self) -> &'static str {
        "Sample"
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

        // Create the plan builder closure
        let fraction = self.fraction;
        let with_replacement = self.with_replacement;
        let seed = self.seed;
        let plan_builder = move |input: LocalPhysicalPlanRef| -> DaftResult<LocalPhysicalPlanRef> {
            Ok(LocalPhysicalPlan::sample(
                input,
                fraction,
                with_replacement,
                seed,
                StatsState::NotMaterialized,
            ))
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
