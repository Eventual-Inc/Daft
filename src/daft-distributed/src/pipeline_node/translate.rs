use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    resolved_col,
};
use daft_logical_plan::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_physical_plan::extract_agg_expr;

use crate::{
    pipeline_node::{
        concat::ConcatNode, distinct::DistinctNode, explode::ExplodeNode, filter::FilterNode,
        in_memory_source::InMemorySourceNode, into_batches::IntoBatchesNode,
        into_partitions::IntoPartitionsNode, limit::LimitNode,
        monotonically_increasing_id::MonotonicallyIncreasingIdNode, project::ProjectNode,
        sample::SampleNode, scan_source::ScanSourceNode, sink::SinkNode, sort::SortNode,
        top_n::TopNNode, udf::UDFNode, unpivot::UnpivotNode, window::WindowNode,
        DistributedPipelineNode, NodeID,
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

pub(crate) struct LogicalPlanToPipelineNodeTranslator {
    pub stage_config: StageConfig,
    pipeline_node_id_counter: NodeID,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<Arc<dyn DistributedPipelineNode>>,
}

impl LogicalPlanToPipelineNodeTranslator {
    fn new(stage_config: StageConfig, psets: Arc<HashMap<String, Vec<PartitionRef>>>) -> Self {
        Self {
            stage_config,
            pipeline_node_id_counter: 0,
            psets,
            curr_node: Vec::new(),
        }
    }

    pub fn get_next_pipeline_node_id(&mut self) -> NodeID {
        self.pipeline_node_id_counter += 1;
        self.pipeline_node_id_counter
    }
}

impl TreeNodeVisitor for LogicalPlanToPipelineNodeTranslator {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &LogicalPlanRef) -> DaftResult<TreeNodeRecursion> {
        let logical_node_id = node.node_id().map(|id| id as NodeID);
        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => InMemorySourceNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.stage_config,
                        info.clone(),
                        self.psets.clone(),
                        logical_node_id,
                    ).arced(),
                    SourceInfo::Physical(info) => {
                        // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!("ScanOperator should not be present in the optimized logical plan for pipeline node translation"),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };
                        ScanSourceNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.stage_config,
                            info.pushdowns.clone(),
                            scan_tasks,
                            source.output_schema.clone(),
                            logical_node_id,
                        ).arced()
                    }
                    SourceInfo::PlaceHolder(_) => unreachable!("PlaceHolder should not be present in the logical plan for pipeline node translation"),
                }
            }
            LogicalPlan::UDFProject(udf) if udf.is_actor_pool_udf() => {
                #[cfg(feature = "python")]
                {
                    let projection = udf
                        .passthrough_columns
                        .iter()
                        .chain(std::iter::once(&udf.project.clone()))
                        .cloned()
                        .collect::<Vec<_>>();
                    let projection =
                        BoundExpr::bind_all(projection.as_slice(), &udf.input.schema())?;
                    crate::pipeline_node::actor_udf::ActorUDF::new(
                        self.get_next_pipeline_node_id(),
                        logical_node_id,
                        &self.stage_config,
                        projection,
                        udf.udf_properties.clone(),
                        udf.projected_schema.clone(),
                        self.curr_node.pop().unwrap(),
                    )?
                    .arced()
                }
                #[cfg(not(feature = "python"))]
                {
                    panic!("ActorUDF is not supported without Python feature")
                }
            }
            LogicalPlan::UDFProject(udf) => {
                let project = BoundExpr::try_new(udf.project.clone(), &udf.input.schema())?;
                let passthrough_columns =
                    BoundExpr::bind_all(&udf.passthrough_columns, &udf.input.schema())?;

                UDFNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    project,
                    passthrough_columns,
                    udf.udf_properties.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                FilterNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    predicate,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::IntoBatches(into_batches) => IntoBatchesNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                into_batches.batch_size,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::Limit(limit) => Arc::new(LimitNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                limit.limit as usize,
                limit.offset.map(|x| x as usize),
                node.schema(),
                self.curr_node.pop().unwrap(),
            )),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                ProjectNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    projection,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                ExplodeNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
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
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
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
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::Sink(sink) => {
                let sink_info = sink.sink_info.bind(&sink.input.schema())?;
                SinkNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    sink_info.into(),
                    sink.schema.clone(),
                    sink.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                MonotonicallyIncreasingIdNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    monotonically_increasing_id.column_name.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Concat(_) => ConcatNode::new(
                self.get_next_pipeline_node_id(),
                logical_node_id,
                &self.stage_config,
                node.schema(),
                self.curr_node.pop().unwrap(), // Other
                self.curr_node.pop().unwrap(), // Child
            )
            .arced(),
            LogicalPlan::Repartition(repartition) => match &repartition.repartition_spec {
                RepartitionSpec::Hash(_)
                | RepartitionSpec::Random(_)
                | RepartitionSpec::Range(_) => {
                    let child = self.curr_node.pop().unwrap();
                    self.gen_shuffle_node(
                        logical_node_id,
                        repartition.repartition_spec.clone(),
                        node.schema(),
                        child,
                    )?
                }
                RepartitionSpec::IntoPartitions(into_partitions_spec) => IntoPartitionsNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    into_partitions_spec.num_partitions,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced(),
            },
            LogicalPlan::Aggregate(aggregate) => {
                let input_schema = aggregate.input.schema();
                let group_by = BoundExpr::bind_all(&aggregate.groupby, &input_schema)?;
                let aggregations = aggregate
                    .aggregations
                    .iter()
                    .map(|expr| {
                        let agg_expr = extract_agg_expr(expr)?;
                        BoundAggExpr::try_new(agg_expr, &aggregate.input.schema())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let input_node = self.curr_node.pop().unwrap();
                self.gen_agg_nodes(
                    input_node,
                    logical_node_id,
                    group_by,
                    aggregations,
                    aggregate.output_schema.clone(),
                )?
            }
            LogicalPlan::Distinct(distinct) => {
                let columns = distinct.columns.clone().unwrap_or_else(|| {
                    distinct
                        .input
                        .schema()
                        .field_names()
                        .map(resolved_col)
                        .collect::<Vec<_>>()
                });
                let columns = BoundExpr::bind_all(&columns, &distinct.input.schema())?;

                // First stage: Initial local distinct to reduce the dataset
                let initial_distinct = DistinctNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    columns.clone(),
                    distinct.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Repartition to distribute the dataset
                let repartition = self.gen_shuffle_node(
                    logical_node_id,
                    RepartitionSpec::Hash(HashRepartitionConfig::new(
                        None,
                        columns.clone().into_iter().map(|e| e.into()).collect(),
                    )),
                    distinct.input.schema(),
                    initial_distinct,
                )?;

                // Last stage: Redo the distinct to get the final result
                DistinctNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    columns,
                    distinct.input.schema(),
                    repartition,
                )
                .arced()
            }
            LogicalPlan::Window(window) => {
                let partition_by =
                    BoundExpr::bind_all(&window.window_spec.partition_by, &window.input.schema())?;
                let order_by =
                    BoundExpr::bind_all(&window.window_spec.order_by, &window.input.schema())?;
                let window_functions =
                    BoundWindowExpr::bind_all(&window.window_functions, &window.input.schema())?;

                // First stage: Shuffle by the partition_by columns to colocate rows
                let input_node = self.curr_node.pop().unwrap();
                let repartition = if partition_by.is_empty() {
                    self.gen_gather_node(logical_node_id, input_node)
                } else {
                    self.gen_shuffle_node(
                        logical_node_id,
                        RepartitionSpec::Hash(HashRepartitionConfig::new(
                            None,
                            partition_by.clone().into_iter().map(|e| e.into()).collect(),
                        )),
                        window.input.schema(),
                        input_node,
                    )?
                };

                // Final stage: The actual window op
                WindowNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    partition_by,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.window_spec.frame.clone(),
                    window.window_spec.min_periods,
                    window_functions,
                    window.aliases.clone(),
                    window.schema.clone(),
                    repartition,
                )?
                .arced()
            }
            LogicalPlan::Join(join) => {
                // Visitor appends in in-order
                // TODO: Just use regular recursion?
                let right_node = self.curr_node.pop().unwrap();
                let left_node = self.curr_node.pop().unwrap();

                self.translate_join(logical_node_id, join, left_node, right_node)?
            }
            LogicalPlan::Sort(sort) => {
                let sort_by = BoundExpr::bind_all(&sort.sort_by, &sort.input.schema())?;

                SortNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    sort_by,
                    sort.descending.clone(),
                    sort.nulls_first.clone(),
                    sort.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::TopN(top_n) => {
                let sort_by = BoundExpr::bind_all(&top_n.sort_by, &top_n.input.schema())?;

                // First stage: Perform a local topN
                let local_topn = TopNNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    sort_by.clone(),
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit + top_n.offset.unwrap_or(0),
                    Some(0),
                    top_n.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Gather all data to a single node
                let gather = self.gen_gather_node(logical_node_id, local_topn);

                // Final stage: Do another topN to get the final result
                TopNNode::new(
                    self.get_next_pipeline_node_id(),
                    logical_node_id,
                    &self.stage_config,
                    sort_by,
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit,
                    top_n.offset,
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
            | LogicalPlan::Shard(_)
            | LogicalPlan::Offset(_) => {
                panic!(
                    "Logical plan operator {} should be handled by the optimizer",
                    node.name()
                )
            }
        };
        self.curr_node.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}
