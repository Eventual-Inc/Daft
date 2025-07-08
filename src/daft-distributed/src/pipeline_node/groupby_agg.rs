use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::expr::{
    bound_col,
    bound_expr::{BoundAggExpr, BoundExpr},
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub struct GroupbyAggSplit {
    pub first_stage_aggs: Vec<BoundAggExpr>,
    pub first_stage_schema: SchemaRef,
    pub first_stage_group_by: Vec<BoundExpr>,

    pub second_stage_aggs: Vec<BoundAggExpr>,
    pub second_stage_schema: SchemaRef,
    pub second_stage_group_by: Vec<BoundExpr>,

    pub final_exprs: Option<Vec<BoundExpr>>,
}

pub(crate) fn split_groupby_aggs(
    group_by: Vec<BoundExpr>,
    aggs: &[BoundAggExpr],
    input_schema: &SchemaRef,
) -> DaftResult<GroupbyAggSplit> {
    // Split the aggs into two stages and final projection
    let (
        (first_stage_aggs, first_stage_schema),
        (second_stage_aggs, second_stage_schema),
        final_exprs,
    ) = daft_physical_plan::populate_aggregation_stages_bound_with_schema(
        aggs,
        input_schema,
        &group_by,
    )?;

    // Generate the expression for the second stage group_by
    let second_stage_group_by = if !first_stage_aggs.is_empty() {
        group_by
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let field = e.as_ref().to_field(input_schema)?;
                Ok(BoundExpr::new_unchecked(bound_col(i, field)))
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        group_by.clone()
    };

    // If the final expr is a pointless projection, remove it
    let final_exprs = if final_exprs.len() == second_stage_aggs.len() {
        None
    } else {
        Some(final_exprs)
    };

    Ok(GroupbyAggSplit {
        first_stage_aggs,
        first_stage_schema: Arc::new(first_stage_schema),
        first_stage_group_by: group_by,

        second_stage_aggs,
        second_stage_schema: Arc::new(second_stage_schema),
        second_stage_group_by,

        final_exprs,
    })
}

pub(crate) struct GroupbyAggNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    groupby: Vec<BoundExpr>,
    aggs: Vec<BoundAggExpr>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl GroupbyAggNode {
    const NODE_NAME: NodeName = "GroupbyAgg";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stage_config: &StageConfig,
        node_id: NodeID,
        groupby: Vec<BoundExpr>,
        aggs: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
        );
        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
            Arc::new(
                HashClusteringConfig::new(
                    child.config().clustering_spec.num_partitions(),
                    groupby.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );
        Self {
            config,
            context,
            groupby,
            aggs,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!(
            "Groupby Agg: {}",
            self.groupby.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Aggregations = {}",
            self.aggs.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

impl TreeDisplay for GroupbyAggNode {
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

impl DistributedPipelineNode for GroupbyAggNode {
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

        // Pipeline the groupby
        let self_clone = self.clone();

        input_node.pipeline_instruction(stage_context, self.clone(), move |input| {
            Ok(LocalPhysicalPlan::hash_aggregate(
                input,
                self_clone.aggs.clone(),
                self_clone.groupby.clone(),
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            ))
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
