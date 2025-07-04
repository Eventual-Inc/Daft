use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_dsl::{
    expr::{
        bound_col,
        bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    },
    functions::python::{get_resource_request, try_get_batch_size_from_udf},
};
use daft_logical_plan::{
    partitioning::RepartitionSpec, JoinStrategy, LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_physical_plan::extract_agg_expr;

use crate::{
    pipeline_node::{
        cross_join::CrossJoinNode, distinct::DistinctNode, explode::ExplodeNode,
        filter::FilterNode, gather::GatherNode, groupby_agg::GroupbyAggNode,
        hash_join::HashJoinNode, in_memory_source::InMemorySourceNode, limit::LimitNode,
        project::ProjectNode, repartition::RepartitionNode, sample::SampleNode,
        scan_source::ScanSourceNode, sink::SinkNode, sort::SortNode, top_n::TopNNode,
        unpivot::UnpivotNode, window::WindowNode, DistributedPipelineNode, NodeID,
    },
    stage::StageConfig,
};

pub(crate) fn logical_plan_to_pipeline_node(
    stage_config: StageConfig,
    plan: LogicalPlanRef,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
    let mut translator = LogicalPlanToPipelineNodeTranslator::new(stage_config, psets);
    let _ = plan.visit(&mut translator)?;
    Ok(translator.curr_node.pop().unwrap())
}

struct LogicalPlanToPipelineNodeTranslator {
    stage_config: StageConfig,
    node_id_counter: NodeID,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<Arc<dyn DistributedPipelineNode>>,
}

impl LogicalPlanToPipelineNodeTranslator {
    fn new(stage_config: StageConfig, psets: Arc<HashMap<String, Vec<PartitionRef>>>) -> Self {
        Self {
            stage_config,
            node_id_counter: 0,
            psets,
            curr_node: Vec::new(),
        }
    }

    fn get_next_node_id(&mut self) -> NodeID {
        self.node_id_counter += 1;
        self.node_id_counter
    }
}

impl TreeNodeVisitor for LogicalPlanToPipelineNodeTranslator {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        let node_id = self.get_next_node_id();
        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => InMemorySourceNode::new(
                        &self.stage_config,
                        node_id,
                        info.clone(),
                        self.psets.clone(),
                    ).arced(),
                    SourceInfo::Physical(info) => {
                        // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!("ScanOperator should not be present in the optimized logical plan for pipeline node translation"),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };
                        ScanSourceNode::new(
                            &self.stage_config,
                            node_id,
                            info.pushdowns.clone(),
                            scan_tasks,
                            source.output_schema.clone(),
                        ).arced()
                    }
                    SourceInfo::PlaceHolder(_) => unreachable!("PlaceHolder should not be present in the logical plan for pipeline node translation"),
                }
            }
            LogicalPlan::ActorPoolProject(actor_pool_project) => {
                #[cfg(feature = "python")]
                {
                    let batch_size = try_get_batch_size_from_udf(&actor_pool_project.projection)?;
                    let memory_request = get_resource_request(&actor_pool_project.projection)
                        .and_then(|req| req.memory_bytes())
                        .map(|m| m as u64)
                        .unwrap_or(0);
                    let projection = BoundExpr::bind_all(
                        &actor_pool_project.projection,
                        &actor_pool_project.input.schema(),
                    )?;
                    crate::pipeline_node::actor_udf::ActorUDF::new(
                        &self.stage_config,
                        node_id,
                        projection,
                        batch_size,
                        memory_request,
                        actor_pool_project.projected_schema.clone(),
                        self.curr_node.pop().unwrap(),
                    )?
                    .arced()
                }
                #[cfg(not(feature = "python"))]
                {
                    panic!("ActorUDF is not supported without Python feature")
                }
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                FilterNode::new(
                    &self.stage_config,
                    node_id,
                    predicate,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Limit(limit) => Arc::new(LimitNode::new(
                &self.stage_config,
                node_id,
                limit.limit as usize,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                ProjectNode::new(
                    &self.stage_config,
                    node_id,
                    projection,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                ExplodeNode::new(
                    &self.stage_config,
                    node_id,
                    to_explode,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Unpivot(unpivot) => {
                let ids = BoundExpr::bind_all(&unpivot.ids, &unpivot.input.schema())?;
                let values = BoundExpr::bind_all(&unpivot.values, &unpivot.input.schema())?;
                UnpivotNode::new(
                    &self.stage_config,
                    node_id,
                    ids,
                    values,
                    unpivot.variable_name.clone(),
                    unpivot.value_name.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Sample(sample) => SampleNode::new(
                &self.stage_config,
                node_id,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::Sink(sink) => SinkNode::new(
                &self.stage_config,
                node_id,
                sink.sink_info.clone(),
                sink.schema.clone(),
                sink.input.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::MonotonicallyIncreasingId(_) => {
                todo!("FLOTILLA_MS1: Implement MonotonicallyIncreasingId")
            }
            LogicalPlan::Concat(_) => {
                todo!("FLOTILLA_MS1: Implement Concat")
            }
            LogicalPlan::Repartition(repartition) => {
                let RepartitionSpec::Hash(repart_spec) = &repartition.repartition_spec else {
                    todo!("FLOTILLA_MS3: Support other types of repartition");
                };
                let Some(num_partitions) = repart_spec.num_partitions else {
                    todo!("FLOTILLA_MS2: Support repartitioning into unknown number of partitions");
                };

                let columns = BoundExpr::bind_all(&repart_spec.by, &repartition.input.schema())?;

                assert!(!columns.is_empty());
                RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    columns,
                    num_partitions,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Aggregate(aggregate) => {
                let input_schema = aggregate.input.schema();
                let group_by = BoundExpr::bind_all(&aggregate.groupby, &input_schema)?;
                let aggregations = aggregate
                    .aggregations
                    .iter()
                    .map(|expr| {
                        let agg_expr = extract_agg_expr(expr)?;
                        BoundAggExpr::try_new(agg_expr, &input_schema)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let (
                    (first_stage_aggs, first_stage_schema),
                    (second_stage_aggs, second_stage_schema),
                    final_exprs,
                ) = daft_physical_plan::populate_aggregation_stages_bound_with_schema(
                    &aggregations,
                    &input_schema,
                    &group_by,
                )?;
                let final_group_by = if !first_stage_aggs.is_empty() {
                    group_by
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            let field = e.as_ref().to_field(&input_schema)?;
                            Ok(BoundExpr::new_unchecked(bound_col(i, field)))
                        })
                        .collect::<DaftResult<Vec<_>>>()?
                } else {
                    group_by.clone()
                };
                let first_stage_schema = Arc::new(first_stage_schema);
                let second_stage_schema = Arc::new(second_stage_schema);

                // First stage groupby-agg to reduce the dataset
                let initial_groupby = GroupbyAggNode::new(
                    &self.stage_config,
                    node_id,
                    group_by,
                    first_stage_aggs,
                    first_stage_schema.clone(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage:
                // If 0 groupby columns, gather all data to a single node
                // Else, repartition to distribute the dataset
                let transfer = if final_group_by.is_empty() {
                    GatherNode::new(
                        &self.stage_config,
                        node_id,
                        first_stage_schema,
                        initial_groupby,
                    )
                    .arced()
                } else {
                    assert!(!final_group_by.is_empty());
                    RepartitionNode::new(
                        &self.stage_config,
                        node_id,
                        final_group_by.clone(),
                        4, // TODO(colin): How do we determine this?
                        first_stage_schema,
                        initial_groupby,
                    )
                    .arced()
                };

                // Third stage re-groupby-agg to compute the final result
                let final_groupby = GroupbyAggNode::new(
                    &self.stage_config,
                    node_id,
                    final_group_by,
                    second_stage_aggs,
                    second_stage_schema,
                    transfer,
                )
                .arced();

                // Last stage project to get the final result
                ProjectNode::new(
                    &self.stage_config,
                    node_id,
                    final_exprs,
                    aggregate.output_schema.clone(),
                    final_groupby,
                )
                .arced()
            }
            LogicalPlan::Distinct(distinct) => {
                let columns = distinct.columns.clone().unwrap_or_else(|| {
                    distinct
                        .input
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| bound_col(idx, field.clone()))
                        .collect::<Vec<_>>()
                });
                let columns = BoundExpr::bind_all(&columns, &distinct.input.schema())?;

                // First stage: Initial local distinct to reduce the dataset
                let initial_distinct = DistinctNode::new(
                    &self.stage_config,
                    node_id,
                    columns.clone(),
                    distinct.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Repartition to distribute the dataset
                assert!(!columns.is_empty());
                let repartition = RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    columns.clone(),
                    20, // TODO(colin): How do we determine this?
                    distinct.input.schema(),
                    initial_distinct,
                )
                .arced();

                // Last stage: Redo the distinct to get the final result
                DistinctNode::new(
                    &self.stage_config,
                    node_id,
                    columns,
                    distinct.input.schema(),
                    repartition,
                )
                .arced()
            }
            LogicalPlan::Window(window) => {
                // Not sure if this is possible right now, just in case
                if window.window_spec.partition_by.is_empty() {
                    todo!("FLOTILLA_MS2: Implement Window without partition by")
                }

                let partition_by =
                    BoundExpr::bind_all(&window.window_spec.partition_by, &window.input.schema())?;
                let order_by =
                    BoundExpr::bind_all(&window.window_spec.order_by, &window.input.schema())?;
                let window_functions =
                    BoundWindowExpr::bind_all(&window.window_functions, &window.input.schema())?;

                // First stage: Repartition by the partition_by columns to colocate rows
                assert!(!partition_by.is_empty());
                let repartition = RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    partition_by.clone(),
                    20, // TODO(colin): How do we determine this?
                    window.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Final stage: The actual window op
                WindowNode::new(
                    &self.stage_config,
                    node_id,
                    partition_by,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.window_spec.frame.clone(),
                    window.window_spec.min_periods,
                    window_functions,
                    window.aliases.clone(),
                    window.input.schema(),
                    repartition,
                )
                .arced()
            }
            LogicalPlan::Join(join) => {
                if join.join_strategy.is_some_and(|x| x != JoinStrategy::Hash) {
                    return Err(DaftError::not_implemented(
                        "Only hash join is supported in flotilla for now",
                    ));
                }
                let right = self.curr_node.pop().unwrap();
                let left = self.curr_node.pop().unwrap();

                if join.on.is_empty() {
                    CrossJoinNode::new(
                        &self.stage_config,
                        node_id,
                        left,
                        right,
                        join.output_schema.clone(),
                    )
                    .arced()
                } else {
                    let (remaining_on, left_on, right_on, null_equals_nulls) =
                        join.on.split_eq_preds();
                    assert!(!left_on.is_empty());
                    assert!(!right_on.is_empty());

                    if !remaining_on.is_empty() {
                        return Err(DaftError::not_implemented("Execution of non-equality join"));
                    }

                    let left_on = BoundExpr::bind_all(&left_on, &join.left.schema())?;
                    let right_on = BoundExpr::bind_all(&right_on, &join.right.schema())?;

                    HashJoinNode::new(
                        &self.stage_config,
                        node_id,
                        20, // TODO(colin): How do we determine this?
                        left_on,
                        right_on,
                        Some(null_equals_nulls),
                        join.join_type,
                        left,
                        right,
                        join.output_schema.clone(),
                    )
                    .arced()
                }
            }
            LogicalPlan::Sort(sort) => {
                let sort_by = BoundExpr::bind_all(&sort.sort_by, &sort.input.schema())?;

                // First stage: Gather all data to a single node
                let gather = GatherNode::new(
                    &self.stage_config,
                    node_id,
                    sort.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Perform a local sort
                SortNode::new(
                    &self.stage_config,
                    node_id,
                    sort_by,
                    sort.descending.clone(),
                    sort.nulls_first.clone(),
                    sort.input.schema(),
                    gather,
                )
                .arced()
            }
            LogicalPlan::TopN(top_n) => {
                let sort_by = BoundExpr::bind_all(&top_n.sort_by, &top_n.input.schema())?;

                // First stage: Perform a local topN
                let local_topn = TopNNode::new(
                    &self.stage_config,
                    node_id,
                    sort_by.clone(),
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit,
                    top_n.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Gather all data to a single node
                let gather = GatherNode::new(
                    &self.stage_config,
                    node_id,
                    top_n.input.schema(),
                    local_topn,
                )
                .arced();

                // Final stage: Do another topN to get the final result
                TopNNode::new(
                    &self.stage_config,
                    node_id,
                    sort_by,
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit,
                    top_n.input.schema(),
                    gather,
                )
                .arced()
            }
            LogicalPlan::Pivot(_) => {
                todo!("FLOTILLA_MS3: Implement Pivot")
            }
            LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Intersect(_)
            | LogicalPlan::Shard(_) => {
                panic!("Logical plan operators SubqueryAlias, Union, Intersect, and Shard should be handled by the optimizer")
            }
        };
        self.curr_node.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}
