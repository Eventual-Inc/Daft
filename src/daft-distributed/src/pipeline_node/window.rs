use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::{DaftError, DaftResult};
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    WindowExpr, WindowFrame,
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

fn window_to_agg_exprs(window_exprs: Vec<BoundWindowExpr>) -> DaftResult<Vec<BoundAggExpr>> {
    window_exprs
    .into_iter()
    .map(|w| {
        if let WindowExpr::Agg(agg_expr) = w.as_ref() {
            Ok(BoundAggExpr::new_unchecked(agg_expr.clone()))
        } else {
            Err(DaftError::TypeError(format!(
                "Window function {:?} not implemented in partition-only windows, only aggregation functions are supported",
                w
            )))
        }
    })
    .collect::<DaftResult<Vec<_>>>()
}

pub(crate) struct WindowNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // All window-related parameters
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    frame: Option<WindowFrame>,
    min_periods: usize,
    window_exprs: Vec<BoundWindowExpr>,
    aliases: Vec<String>,

    child: Arc<dyn DistributedPipelineNode>,
}

impl WindowNode {
    const NODE_NAME: NodeName = "Window";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        partition_by: Vec<BoundExpr>,
        order_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        frame: Option<WindowFrame>,
        min_periods: usize,
        window_exprs: Vec<BoundWindowExpr>,
        aliases: Vec<String>,
        schema: SchemaRef,
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
            schema,
            stage_config.config.clone(),
            Arc::new(
                HashClusteringConfig::new(
                    child.config().clustering_spec.num_partitions(),
                    partition_by.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );
        Self {
            config,
            context,
            partition_by,
            order_by,
            descending,
            nulls_first,
            frame,
            min_periods,
            window_exprs,
            aliases,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec!["Window:".to_string()];
        res.push(format!(
            "Partition by: {}",
            self.partition_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Order by: {}",
            self.order_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Descending: {}",
            self.descending.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Nulls first: {}",
            self.nulls_first.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Frame: {:?}", self.frame));
        res.push(format!("Min periods: {}", self.min_periods));
        res
    }
}

impl TreeDisplay for WindowNode {
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

impl DistributedPipelineNode for WindowNode {
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

        // Pipeline the window op
        let self_clone = self.clone();
        input_node.pipeline_instruction(stage_context, self.clone(), move |input| {
            match (
                !self_clone.partition_by.is_empty(),
                !self_clone.order_by.is_empty(),
                self_clone.frame.is_some(),
            ) {
                (true, false, false) => Ok(LocalPhysicalPlan::window_partition_only(
                    input,
                    self_clone.partition_by.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    window_to_agg_exprs(self_clone.window_exprs.clone())?,
                    self_clone.aliases.clone(),
                )),
                (true, true, false) => Ok(LocalPhysicalPlan::window_partition_and_order_by(
                    input,
                    self_clone.partition_by.clone(),
                    self_clone.order_by.clone(),
                    self_clone.descending.clone(),
                    self_clone.nulls_first.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    self_clone.window_exprs.clone(),
                    self_clone.aliases.clone(),
                )),
                (true, true, true) => Ok(LocalPhysicalPlan::window_partition_and_dynamic_frame(
                    input,
                    self_clone.partition_by.clone(),
                    self_clone.order_by.clone(),
                    self_clone.descending.clone(),
                    self_clone.nulls_first.clone(),
                    self_clone.frame.as_ref().unwrap().clone(),
                    self_clone.min_periods,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    window_to_agg_exprs(self_clone.window_exprs.clone())?,
                    self_clone.aliases.clone(),
                )),
                (false, true, false) => Ok(LocalPhysicalPlan::window_order_by_only(
                    input,
                    self_clone.order_by.clone(),
                    self_clone.descending.clone(),
                    self_clone.nulls_first.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    self_clone.window_exprs.clone(),
                    self_clone.aliases.clone(),
                )),
                (false, true, true) => Err(DaftError::not_implemented(
                    "Window with order by and frame not yet implemented",
                )),
                _ => Err(DaftError::ValueError(
                    "Window requires either partition by or order by".to_string(),
                )),
            }
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
