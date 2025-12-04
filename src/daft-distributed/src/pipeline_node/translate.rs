use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::{SPLIT_AND_MERGE_PASS, ScanState};
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_dsl::{
    expr::{
        agg::extract_agg_expr,
        bound_expr::{BoundAggExpr, BoundExpr, BoundVLLMExpr, BoundWindowExpr},
    },
    is_partition_compatible, resolved_col,
};
use daft_logical_plan::{
    LogicalPlan, LogicalPlanRef, SourceInfo,
    partitioning::{ClusteringSpec, HashRepartitionConfig, RepartitionSpec},
};
use daft_schema::schema::Schema;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, concat::ConcatNode, distinct::DistinctNode,
        explode::ExplodeNode, filter::FilterNode, glob_scan_source::GlobScanSourceNode,
        in_memory_source::InMemorySourceNode, into_batches::IntoBatchesNode,
        into_partitions::IntoPartitionsNode, limit::LimitNode,
        monotonically_increasing_id::MonotonicallyIncreasingIdNode, pivot::PivotNode,
        project::ProjectNode, sample::SampleNode, scan_source::ScanSourceNode, sink::SinkNode,
        sort::SortNode, top_n::TopNNode, udf::UDFNode, unpivot::UnpivotNode, vllm::VLLMNode,
        window::WindowNode,
    },
    plan::PlanConfig,
};

pub(crate) fn logical_plan_to_pipeline_node(
    plan_config: PlanConfig,
    plan: LogicalPlanRef,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
) -> DaftResult<DistributedPipelineNode> {
    let mut translator = LogicalPlanToPipelineNodeTranslator::new(plan_config, psets);
    let _ = plan.visit(&mut translator)?;
    Ok(translator.curr_node.pop().unwrap())
}

pub(crate) struct LogicalPlanToPipelineNodeTranslator {
    pub plan_config: PlanConfig,
    pipeline_node_id_counter: NodeID,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<DistributedPipelineNode>,
}

impl LogicalPlanToPipelineNodeTranslator {
    fn new(plan_config: PlanConfig, psets: Arc<HashMap<String, Vec<PartitionRef>>>) -> Self {
        Self {
            plan_config,
            pipeline_node_id_counter: 0,
            psets,
            curr_node: Vec::new(),
        }
    }

    pub fn get_next_pipeline_node_id(&mut self) -> NodeID {
        self.pipeline_node_id_counter += 1;
        self.pipeline_node_id_counter
    }

    pub(crate) fn needs_hash_repartition(
        input_node: &DistributedPipelineNode,
        partition_columns: &[BoundExpr],
    ) -> DaftResult<bool> {
        let input_clustering_spec = &input_node.config().clustering_spec;
        // If there is only one partition, we can skip the shuffle
        if input_clustering_spec.num_partitions() == 1 {
            return Ok(true);
        }

        // Check if input is hash partitioned
        if !matches!(input_clustering_spec.as_ref(), ClusteringSpec::Hash(_)) {
            return Ok(false);
        }

        // Check if the partition columns are compatible
        let is_compatible = is_partition_compatible(
            BoundExpr::bind_all(
                &input_clustering_spec.partition_by(),
                &input_node.config().schema,
            )?
            .iter()
            .map(|e| e.inner()),
            partition_columns.iter().map(|e| e.inner()),
        );

        Ok(is_compatible)
    }
}

impl TreeNodeVisitor for LogicalPlanToPipelineNodeTranslator {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &LogicalPlanRef) -> DaftResult<TreeNodeRecursion> {
        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => InMemorySourceNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        info.clone(),
                        self.psets.clone(),
                    )
                    .into_node(),
                    SourceInfo::Physical(info) => {
                        // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!(
                                "ScanOperator should not be present in the optimized logical plan for pipeline node translation"
                            ),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };
                        // Perform scan task splitting and merging.
                        let scan_tasks = if self.plan_config.config.enable_scan_task_split_and_merge
                            && let Some(split_and_merge_pass) = SPLIT_AND_MERGE_PASS.get()
                        {
                            split_and_merge_pass(
                                scan_tasks,
                                &info.pushdowns,
                                &self.plan_config.config,
                            )?
                        } else {
                            scan_tasks
                        };
                        ScanSourceNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            info.pushdowns.clone(),
                            scan_tasks,
                            source.output_schema.clone(),
                        )
                        .into_node()
                    }
                    SourceInfo::GlobScan(info) => GlobScanSourceNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        info.glob_paths.clone(),
                        info.pushdowns.clone(),
                        source.output_schema.clone(),
                        info.io_config.clone().map(|c| *c),
                    )
                    .into_node(),
                    SourceInfo::PlaceHolder(_) => unreachable!(
                        "PlaceHolder should not be present in the logical plan for pipeline node translation"
                    ),
                }
            }
            LogicalPlan::UDFProject(udf) if udf.is_actor_pool_udf() => {
                #[cfg(feature = "python")]
                {
                    let projection = udf
                        .passthrough_columns
                        .iter()
                        .chain(std::iter::once(&udf.expr))
                        .cloned()
                        .collect::<Vec<_>>();
                    let projection =
                        BoundExpr::bind_all(projection.as_slice(), &udf.input.schema())?;
                    crate::pipeline_node::actor_udf::ActorUDF::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        projection,
                        udf.udf_properties.clone(),
                        udf.projected_schema.clone(),
                        self.curr_node.pop().unwrap(),
                    )?
                    .into_node()
                }
                #[cfg(not(feature = "python"))]
                {
                    panic!("ActorUDF is not supported without Python feature")
                }
            }
            LogicalPlan::UDFProject(udf) => {
                let expr = BoundExpr::try_new(udf.expr.clone(), &udf.input.schema())?;
                let passthrough_columns =
                    BoundExpr::bind_all(&udf.passthrough_columns, &udf.input.schema())?;

                UDFNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    expr,
                    udf.udf_properties.clone(),
                    passthrough_columns,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                FilterNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    predicate,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::IntoBatches(into_batches) => IntoBatchesNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                into_batches.batch_size,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .into_node(),
            LogicalPlan::Limit(limit) => LimitNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                limit.limit as usize,
                limit.offset.map(|x| x as usize),
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .into_node(),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                ProjectNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    projection,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                ExplodeNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    to_explode,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::Unpivot(unpivot) => {
                let ids = BoundExpr::bind_all(&unpivot.ids, &unpivot.input.schema())?;
                let values = BoundExpr::bind_all(&unpivot.values, &unpivot.input.schema())?;
                UnpivotNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    ids,
                    values,
                    unpivot.variable_name.clone(),
                    unpivot.value_name.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::Sample(sample) => SampleNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                sample.fraction,
                sample.size,
                sample.with_replacement,
                sample.seed,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .into_node(),
            LogicalPlan::Sink(sink) => {
                let sink_info = sink.sink_info.bind(&sink.input.schema())?;
                SinkNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sink_info.into(),
                    sink.schema.clone(),
                    sink.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                MonotonicallyIncreasingIdNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    monotonically_increasing_id.column_name.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::Concat(_) => ConcatNode::new(
                self.get_next_pipeline_node_id(),
                &self.plan_config,
                node.schema(),
                self.curr_node.pop().unwrap(), // Other
                self.curr_node.pop().unwrap(), // Child
            )
            .into_node(),
            LogicalPlan::Repartition(repartition) => match &repartition.repartition_spec {
                RepartitionSpec::Hash(_)
                | RepartitionSpec::Random(_)
                | RepartitionSpec::Range(_) => {
                    let child = self.curr_node.pop().unwrap();
                    self.gen_shuffle_node(
                        repartition.repartition_spec.clone(),
                        node.schema(),
                        child,
                    )?
                }
                RepartitionSpec::IntoPartitions(into_partitions_spec) => IntoPartitionsNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    into_partitions_spec.num_partitions,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node(),
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
                    group_by.clone(),
                    aggregations,
                    aggregate.output_schema.clone(),
                    group_by,
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
                let input_node = self.curr_node.pop().unwrap();

                // Check if we can elide the repartition
                if Self::needs_hash_repartition(&input_node, &columns)? {
                    DistinctNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        columns,
                        distinct.input.schema(),
                        input_node,
                    )
                    .into_node()
                } else {
                    // Need full 2-stage distinct with shuffle
                    // First stage: Initial local distinct to reduce the dataset
                    let initial_distinct = DistinctNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        columns.clone(),
                        distinct.input.schema(),
                        input_node,
                    )
                    .into_node();

                    // Second stage: Repartition to distribute the dataset
                    let repartition = self.gen_shuffle_node(
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
                        &self.plan_config,
                        columns,
                        distinct.input.schema(),
                        repartition,
                    )
                    .into_node()
                }
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
                    self.gen_gather_node(input_node)
                } else if Self::needs_hash_repartition(&input_node, &partition_by)? {
                    input_node
                } else {
                    self.gen_shuffle_node(
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
                    &self.plan_config,
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
                .into_node()
            }
            LogicalPlan::Join(join) => {
                // Visitor appends in in-order
                // TODO: Just use regular recursion?
                let right_node = self.curr_node.pop().unwrap();
                let left_node = self.curr_node.pop().unwrap();

                self.translate_join(join, left_node, right_node)?
            }
            LogicalPlan::Sort(sort) => {
                let sort_by = BoundExpr::bind_all(&sort.sort_by, &sort.input.schema())?;

                SortNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sort_by,
                    sort.descending.clone(),
                    sort.nulls_first.clone(),
                    sort.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
            }
            LogicalPlan::TopN(top_n) => {
                let sort_by = BoundExpr::bind_all(&top_n.sort_by, &top_n.input.schema())?;

                // First stage: Perform a local topN
                let local_topn = TopNNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sort_by.clone(),
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit + top_n.offset.unwrap_or(0),
                    Some(0),
                    top_n.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node();

                // Second stage: Gather all data to a single node
                let gather = self.gen_gather_node(local_topn);

                // Final stage: Do another topN to get the final result
                TopNNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sort_by,
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit,
                    top_n.offset,
                    top_n.input.schema(),
                    gather,
                )
                .into_node()
            }
            LogicalPlan::Pivot(pivot) => {
                let input_schema = pivot.input.schema();
                let group_by = BoundExpr::bind_all(&pivot.group_by, &input_schema)?;
                let pivot_column = BoundExpr::try_new(pivot.pivot_column.clone(), &input_schema)?;
                let value_column = BoundExpr::try_new(pivot.value_column.clone(), &input_schema)?;
                let aggregation = BoundAggExpr::try_new(pivot.aggregation.clone(), &input_schema)?;

                let input_node = self.curr_node.pop().unwrap();

                let group_by_with_pivot = {
                    let mut gb = group_by.clone();
                    gb.push(pivot_column.clone());
                    gb
                };
                // Generate the output schema for the aggregation
                let output_fields = group_by_with_pivot
                    .iter()
                    .map(|expr| expr.inner().to_field(&pivot.input.schema()))
                    .chain(std::iter::once(
                        aggregation.inner().to_field(&pivot.input.schema()),
                    ))
                    .collect::<DaftResult<Vec<_>>>()?;
                let output_schema = Arc::new(Schema::new(output_fields));

                // First stage: Local aggregation with group_by + pivot_column
                let agg = self.gen_agg_nodes(
                    input_node,
                    group_by_with_pivot,
                    vec![aggregation.clone()],
                    output_schema,
                    group_by.clone(),
                )?;

                // Final stage: Pivot transformation
                PivotNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    group_by,
                    pivot_column,
                    value_column,
                    aggregation,
                    pivot.names.clone(),
                    pivot.output_schema.clone(),
                    agg,
                )
                .into_node()
            }
            LogicalPlan::VLLMProject(vllm_project) => {
                let input_schema = vllm_project.input.schema();
                let expr = BoundVLLMExpr::try_new(vllm_project.expr.clone(), &input_schema)?;

                VLLMNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    expr,
                    vllm_project.output_column_name.clone(),
                    vllm_project.output_schema.clone(),
                    self.curr_node.pop().unwrap(),
                )
                .into_node()
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
