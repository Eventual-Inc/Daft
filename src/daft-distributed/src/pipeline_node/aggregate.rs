use std::{cmp::min, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{
    AggExpr,
    expr::{
        bound_col,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    stats::StatsState,
};
use daft_schema::{
    dtype::DataType,
    schema::{Schema, SchemaRef},
};

use super::PipelineNodeImpl;
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream, project::ProjectNode,
        translate::LogicalPlanToPipelineNodeTranslator,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct AggregateNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    group_by: Vec<BoundExpr>,
    aggs: Vec<BoundAggExpr>,
    child: DistributedPipelineNode,
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
        plan_config: &PlanConfig,
        group_by: Vec<BoundExpr>,
        aggs: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::node_name(&group_by),
        );
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for AggregateNode {
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
                format!("Output Schema = {}", self.config.schema.short_string()),
            ]
        }
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // Pipeline the aggregation
        let self_clone = self.clone();

        input_node.pipeline_instruction(self, move |input| {
            if self_clone.group_by.is_empty() {
                LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(self_clone.node_id() as usize),
                        additional: None,
                    },
                )
            } else {
                LocalPhysicalPlan::hash_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.group_by.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(self_clone.node_id() as usize),
                        additional: None,
                    },
                )
            }
        })
    }
}

struct GroupByAggSplit {
    pub first_stage_aggs: Vec<BoundAggExpr>,
    pub first_stage_schema: SchemaRef,
    pub first_stage_group_by: Vec<BoundExpr>,

    pub partition_by: Vec<BoundExpr>,

    pub second_stage_aggs: Vec<BoundAggExpr>,
    pub second_stage_schema: SchemaRef,
    pub second_stage_group_by: Vec<BoundExpr>,

    pub final_exprs: Vec<BoundExpr>,
}

fn split_groupby_aggs(
    group_by: &[BoundExpr],
    aggs: &[BoundAggExpr],
    partition_by: &[BoundExpr],
    input_schema: &Schema,
) -> DaftResult<GroupByAggSplit> {
    // Split the aggs into two stages and final projection
    let (
        (first_stage_aggs, first_stage_schema),
        (second_stage_aggs, second_stage_schema),
        final_exprs,
    ) = daft_local_plan::agg::populate_aggregation_stages_bound_with_schema(
        aggs,
        input_schema,
        group_by,
    )?;
    let first_stage_schema = Arc::new(first_stage_schema);
    let second_stage_schema = Arc::new(second_stage_schema);

    // If there is a pre-agg, the group_by / partition_by columns are moved to the front
    // Thus, we need to remap the BoundExprs to the new index
    let partition_by = if !first_stage_aggs.is_empty() {
        partition_by
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let field = e.as_ref().to_field(input_schema)?;
                Ok(BoundExpr::new_unchecked(bound_col(i, field)))
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        partition_by.to_vec()
    };

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
        first_stage_schema,
        first_stage_group_by: group_by.to_vec(),

        partition_by,

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
        input_node: DistributedPipelineNode,
        group_by: Vec<BoundExpr>,
        aggregations: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
        partition_by: Vec<BoundExpr>,
    ) -> DaftResult<DistributedPipelineNode> {
        let shuffle = if partition_by.is_empty() {
            self.gen_gather_node(input_node)
        } else {
            self.gen_shuffle_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    None,
                    partition_by.into_iter().map(|e| e.into()).collect(),
                )),
                input_node.config().schema.clone(),
                input_node,
            )?
        };

        Ok(AggregateNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            group_by,
            aggregations,
            output_schema,
            shuffle,
        )
        .into_node())
    }

    /// Generate PipelineNodes for aggregates with some pre-aggregation.
    /// This is used by most other aggregations
    fn gen_with_pre_agg(
        &mut self,
        input_node: DistributedPipelineNode,
        split_details: GroupByAggSplit,
        output_schema: SchemaRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let num_partitions = input_node.config().clustering_spec.num_partitions();
        let initial_agg = AggregateNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            split_details.first_stage_group_by,
            split_details.first_stage_aggs,
            split_details.first_stage_schema.clone(),
            input_node,
        )
        .into_node();

        // Second stage: Shuffle to distribute the dataset
        let num_partitions = min(
            num_partitions,
            self.plan_config
                .config
                .shuffle_aggregation_default_partitions,
        );
        let shuffle = if split_details.partition_by.is_empty() {
            self.gen_gather_node(initial_agg)
        } else {
            self.gen_shuffle_node(
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    split_details
                        .partition_by
                        .into_iter()
                        .map(|e| e.into())
                        .collect(),
                )),
                split_details.second_stage_schema.clone(),
                initial_agg,
            )?
        };

        // Third stage re-agg to compute the final result
        let final_aggregation = AggregateNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            split_details.second_stage_group_by,
            split_details.second_stage_aggs,
            split_details.second_stage_schema.clone(),
            shuffle,
        )
        .into_node();

        // Last stage project to get the final result
        Ok(ProjectNode::new(
            self.get_next_pipeline_node_id(),
            &self.plan_config,
            split_details.final_exprs,
            output_schema,
            final_aggregation,
        )
        .into_node())
    }

    /// Generate PipelineNodes for aggregates
    ///
    /// ### Arguments
    ///
    /// * `input_node` The input node to the aggregation.
    /// * `logical_node_id` The logical node id generating this physical plan node(s).
    /// * `group_by` The columns to group by.
    /// * `aggregations` The aggregations to perform.
    /// * `output_schema` The output schema after the aggregation.
    /// * `partition_by` The columns to partition by. Most of the time, this will be the same as the group_by columns.
    pub fn gen_agg_nodes(
        &mut self,
        input_node: DistributedPipelineNode,
        group_by: Vec<BoundExpr>,
        aggregations: Vec<BoundAggExpr>,
        output_schema: SchemaRef,
        partition_by: Vec<BoundExpr>,
    ) -> DaftResult<DistributedPipelineNode> {
        if Self::needs_hash_repartition(&input_node, &group_by)? {
            return Ok(AggregateNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                group_by,
                aggregations,
                output_schema,
                input_node,
            )
            .into_node());
        }

        let split_details = split_groupby_aggs(
            &group_by,
            &aggregations,
            &partition_by,
            &input_node.config().schema,
        )?;

        if split_details.first_stage_aggs.is_empty()
        // Special case for ApproxCountDistinct
        // Right now, we can't do a pre-aggregation because we can't recursively merge HLL sketches
        // TODO: Look for alternative approaches for this
            || aggregations
                .iter()
                .any(|agg| matches!(agg.as_ref(), AggExpr::ApproxCountDistinct(_)))
            // Special case for Decimal128
            // Right now, we can't do a pre-aggregation because decimal dtype will change in swordfish's own two stage aggregation
            || split_details
                .first_stage_schema
                .fields()
                .iter()
                .any(|f| matches!(f.dtype, DataType::Decimal128(_, _)))
        {
            self.gen_without_pre_agg(
                input_node,
                group_by,
                aggregations,
                output_schema,
                partition_by,
            )
        } else {
            self.gen_with_pre_agg(input_node, split_details, output_schema)
        }
    }
}
