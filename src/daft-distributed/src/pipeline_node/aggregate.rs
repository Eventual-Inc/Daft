use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::{
    expr::{
        bound_col,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
    AggExpr,
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::{Schema, SchemaRef};

use super::DistributedPipelineNode;
use crate::{
    pipeline_node::{
        project::ProjectNode, translate::LogicalPlanToPipelineNodeTranslator, NodeID, NodeName,
        PipelineNodeConfig, PipelineNodeContext, RunningPipelineNode,
    },
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct AggregateNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    group_by: Vec<BoundExpr>,
    aggs: Vec<BoundAggExpr>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl AggregateNode {
    const GROUPED_NAME: NodeName = "GroupBy Aggregate";
    const UNGROUPED_NAME: NodeName = "Aggregate";
    fn node_name(group_by: &[BoundExpr]) -> NodeName {
        if group_by.is_empty() {
            Self::UNGROUPED_NAME
        } else {
            Self::GROUPED_NAME
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        group_by: Vec<BoundExpr>,
        aggs: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::node_name(&group_by),
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
            // Often child is a repartition node
            // TODO: Be more specific if group_by columns overlap with partitioning columns
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            group_by,
            aggs,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let agg_str = self.aggs.iter().map(|e| e.to_string()).join(", ");
        if self.group_by.is_empty() {
            vec![
                format!("Ungrouped Aggregate: {}", agg_str),
                format!("Output Schema = {}", self.config.schema.short_string()),
            ]
        } else {
            vec![
                format!(
                    "Group-By Aggregate: {}",
                    self.group_by.iter().map(|e| e.to_string()).join(", ")
                ),
                format!("Aggregations = {}", agg_str),
            ]
        }
    }
}

impl TreeDisplay for AggregateNode {
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

impl DistributedPipelineNode for AggregateNode {
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

        // Pipeline the aggregation
        let self_clone = self.clone();

        input_node.pipeline_instruction(stage_context, self.clone(), move |input| {
            if self_clone.group_by.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.group_by.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                ))
            }
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

struct GroupByAggSplit {
    pub first_stage_aggs: Vec<BoundAggExpr>,
    pub first_stage_schema: SchemaRef,
    pub first_stage_group_by: Vec<BoundExpr>,

    pub second_stage_aggs: Vec<BoundAggExpr>,
    pub second_stage_schema: SchemaRef,
    pub second_stage_group_by: Vec<BoundExpr>,

    pub final_exprs: Vec<BoundExpr>,
}

fn split_groupby_aggs(
    group_by: &[BoundExpr],
    aggs: &[BoundAggExpr],
    input_schema: &Schema,
) -> DaftResult<GroupByAggSplit> {
    // Split the aggs into two stages and final projection
    let (
        (first_stage_aggs, first_stage_schema),
        (second_stage_aggs, second_stage_schema),
        final_exprs,
    ) = daft_physical_plan::populate_aggregation_stages_bound_with_schema(
        aggs,
        input_schema,
        group_by,
    )?;
    let second_stage_schema = Arc::new(second_stage_schema);

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
        group_by.to_vec()
    };

    Ok(GroupByAggSplit {
        first_stage_aggs,
        first_stage_schema: Arc::new(first_stage_schema),
        first_stage_group_by: group_by.to_vec(),

        second_stage_aggs,
        second_stage_schema,
        second_stage_group_by,

        final_exprs,
    })
}

impl LogicalPlanToPipelineNodeTranslator {
    /// Generate PipelineNodes for aggregates with no pre-aggregation.
    /// This is only necessary if the pre-aggregation (first stage aggregation) is empty.
    /// That is currently only applicable for MapGroup aggregations
    fn gen_without_pre_agg(
        &mut self,
        input_node: Arc<dyn DistributedPipelineNode>,
        logical_node_id: Option<NodeID>,
        group_by: Vec<BoundExpr>,
        aggregations: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
    ) -> Arc<dyn DistributedPipelineNode> {
        let shuffle = self.gen_shuffle_node(logical_node_id, input_node, group_by.clone());

        AggregateNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            group_by,
            aggregations,
            output_schema,
            shuffle,
        )
        .arced()
    }

    /// Generate PipelineNodes for aggregates with some pre-aggregation.
    /// This is used by most other aggregations
    fn gen_with_pre_agg(
        &mut self,
        input_node: Arc<dyn DistributedPipelineNode>,
        logical_node_id: Option<NodeID>,
        split_details: GroupByAggSplit,
        output_schema: SchemaRef,
    ) -> Arc<dyn DistributedPipelineNode> {
        let initial_agg = AggregateNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            split_details.first_stage_group_by,
            split_details.first_stage_aggs,
            split_details.first_stage_schema.clone(),
            input_node,
        )
        .arced();

        // Second stage: Shuffle to distribute the dataset
        let shuffle = self.gen_shuffle_node(
            logical_node_id,
            initial_agg,
            split_details.second_stage_group_by.clone(),
        );

        // Third stage re-agg to compute the final result
        let final_aggregation = AggregateNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            split_details.second_stage_group_by,
            split_details.second_stage_aggs,
            split_details.second_stage_schema.clone(),
            shuffle,
        )
        .arced();

        // Last stage project to get the final result
        ProjectNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            split_details.final_exprs,
            output_schema,
            final_aggregation,
        )
        .arced()
    }

    /// Generate PipelineNodes for aggregates
    pub fn gen_agg_nodes(
        &mut self,
        input_node: Arc<dyn DistributedPipelineNode>,
        logical_node_id: Option<NodeID>,
        group_by: Vec<BoundExpr>,
        aggregations: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        let split_details =
            split_groupby_aggs(&group_by, &aggregations, &input_node.config().schema)?;

        // Special case for ApproxCountDistinct
        // Right now, we can't do a pre-aggregation because we can't recursively merge HLL sketches
        // TODO: Look for alternative approaches for this
        if split_details.first_stage_aggs.is_empty()
            || aggregations
                .iter()
                .any(|agg| matches!(agg.as_ref(), AggExpr::ApproxCountDistinct(_)))
        {
            Ok(self.gen_without_pre_agg(
                input_node,
                logical_node_id,
                group_by,
                aggregations,
                output_schema,
            ))
        } else {
            Ok(self.gen_with_pre_agg(input_node, logical_node_id, split_details, output_schema))
        }
    }
}
