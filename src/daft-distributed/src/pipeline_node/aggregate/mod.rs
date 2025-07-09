mod grouped;
mod ungrouped;

use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::{
    bound_col,
    bound_expr::{BoundAggExpr, BoundExpr},
};
use daft_schema::schema::{Schema, SchemaRef};
use grouped::GroupByAggNode;
use ungrouped::AggregateNode;

use crate::pipeline_node::{
    project::ProjectNode, translate::LogicalPlanToPipelineNodeTranslator, DistributedPipelineNode,
    NodeID,
};

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
    /// Helper function to generate the aggregate PipelineNode
    /// Whether it should be a group-by aggregate or ungrouped aggregate
    fn agg_node_helper(
        &mut self,
        input_node: Arc<dyn DistributedPipelineNode>,
        logical_node_id: Option<NodeID>,
        group_by: Vec<BoundExpr>,
        aggregations: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
    ) -> Arc<dyn DistributedPipelineNode> {
        let node_id = self.get_next_pipeline_node_id();

        if group_by.is_empty() {
            AggregateNode::new(
                &self.stage_config,
                node_id,
                logical_node_id,
                aggregations,
                output_schema,
                input_node,
            )
            .arced()
        } else {
            GroupByAggNode::new(
                &self.stage_config,
                node_id,
                logical_node_id,
                group_by,
                aggregations,
                output_schema,
                input_node,
            )
            .arced()
        }
    }

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

        self.agg_node_helper(
            shuffle,
            logical_node_id,
            group_by,
            aggregations,
            output_schema,
        )
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
        let initial_agg = self.agg_node_helper(
            input_node,
            logical_node_id,
            split_details.first_stage_group_by,
            split_details.first_stage_aggs,
            split_details.first_stage_schema.clone(),
        );

        // Second stage: Shuffle to distribute the dataset
        let shuffle = self.gen_shuffle_node(
            logical_node_id,
            initial_agg,
            split_details.second_stage_group_by.clone(),
        );

        // Third stage re-agg to compute the final result
        let final_group_by = self.agg_node_helper(
            shuffle,
            logical_node_id,
            split_details.second_stage_group_by,
            split_details.second_stage_aggs,
            split_details.second_stage_schema.clone(),
        );

        // Last stage project to get the final result
        let node_id = self.get_next_pipeline_node_id();
        ProjectNode::new(
            &self.stage_config,
            node_id,
            logical_node_id,
            split_details.final_exprs,
            output_schema,
            final_group_by,
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

        if split_details.first_stage_aggs.is_empty() {
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
