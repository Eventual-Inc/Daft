use std::{cmp::min, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::{
    expr::{
        bound_col,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
    is_partition_compatible, AggExpr,
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    stats::StatsState,
    ClusteringSpec,
};
use daft_schema::{
    dtype::DataType,
    schema::{Schema, SchemaRef},
};

use super::DistributedPipelineNode;
use crate::{
    pipeline_node::{
        project::ProjectNode, translate::LogicalPlanToPipelineNodeTranslator, NodeID, NodeName,
        PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
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

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(stage_context);

        // Pipeline the aggregation
        let self_clone = self.clone();

        input_node.pipeline_instruction(self.clone(), move |input| {
            if self_clone.group_by.is_empty() {
                LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                )
            } else {
                LocalPhysicalPlan::hash_aggregate(
                    input,
                    self_clone.aggs.clone(),
                    self_clone.group_by.clone(),
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                )
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
    let first_stage_schema = Arc::new(first_stage_schema);
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
        first_stage_schema,
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
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        let shuffle = if group_by.is_empty() {
            self.gen_gather_node(logical_node_id, input_node)
        } else {
            self.gen_shuffle_node(
                logical_node_id,
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    None,
                    group_by.clone().into_iter().map(|e| e.into()).collect(),
                )),
                input_node.config().schema.clone(),
                input_node,
            )?
        };

        Ok(AggregateNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            group_by,
            aggregations,
            output_schema,
            shuffle,
        )
        .arced())
    }

    /// Generate PipelineNodes for aggregates with some pre-aggregation.
    /// This is used by most other aggregations
    fn gen_with_pre_agg(
        &mut self,
        input_node: Arc<dyn DistributedPipelineNode>,
        logical_node_id: Option<NodeID>,
        split_details: GroupByAggSplit,
        output_schema: SchemaRef,
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        let num_partitions = input_node.config().clustering_spec.num_partitions();
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
        let num_partitions = min(
            num_partitions,
            self.stage_config
                .config
                .shuffle_aggregation_default_partitions,
        );
        let shuffle = if split_details.second_stage_group_by.is_empty() {
            self.gen_gather_node(logical_node_id, initial_agg)
        } else {
            self.gen_shuffle_node(
                logical_node_id,
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(num_partitions),
                    split_details
                        .second_stage_group_by
                        .clone()
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
            logical_node_id,
            &self.stage_config,
            split_details.second_stage_group_by,
            split_details.second_stage_aggs,
            split_details.second_stage_schema.clone(),
            shuffle,
        )
        .arced();

        // Last stage project to get the final result
        Ok(ProjectNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            split_details.final_exprs,
            output_schema,
            final_aggregation,
        )
        .arced())
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
        let input_clustering_spec = &input_node.config().clustering_spec;
        // If there is only one partition, or the input is already partitioned by the group_by columns,
        // then we can just do a single stage aggregation and skip the shuffle.
        let is_hash_partitioned_by_group_by =
            matches!(input_clustering_spec.as_ref(), ClusteringSpec::Hash(_))
                && !group_by.is_empty()
                && is_partition_compatible(
                    BoundExpr::bind_all(
                        &input_clustering_spec.partition_by(),
                        &input_node.config().schema,
                    )?
                    .iter()
                    .map(|e| e.inner()),
                    group_by.iter().map(|e| e.inner()),
                );
        if input_clustering_spec.num_partitions() == 1 || is_hash_partitioned_by_group_by {
            return Ok(AggregateNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                group_by,
                aggregations,
                output_schema,
                input_node,
            )
            .arced());
        }

        let split_details =
            split_groupby_aggs(&group_by, &aggregations, &input_node.config().schema)?;

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
                logical_node_id,
                group_by,
                aggregations,
                output_schema,
            )
        } else {
            self.gen_with_pre_agg(input_node, logical_node_id, split_details, output_schema)
        }
    }
}
