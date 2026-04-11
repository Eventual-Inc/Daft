use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_metrics::Meter;
use common_partitioning::PartitionRef;
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
use daft_scan::{ScanState, scan_task_iters};
use daft_schema::schema::Schema;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, concat::ConcatNode, distinct::DistinctNode,
        explode::ExplodeNode, filter::FilterNode, glob_scan_source::GlobScanSourceNode,
        in_memory_source::InMemorySourceNode, into_batches::IntoBatchesNode,
        into_partitions::IntoPartitionsNode, limit::LimitNode,
        monotonically_increasing_id::MonotonicallyIncreasingIdNode, pivot::PivotNode,
        project::ProjectNode, random_shuffle::RandomShuffleNode, sample::SampleNode,
        scan_source::ScanSourceNode, sink::SinkNode, sort::SortNode, top_n::TopNNode, udf::UDFNode,
        unpivot::UnpivotNode, vllm::VLLMNode, window::WindowNode,
    },
    plan::PlanConfig,
};

pub(crate) fn logical_plan_to_pipeline_node(
    plan_config: PlanConfig,
    plan: LogicalPlanRef,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    meter: &Meter,
) -> DaftResult<DistributedPipelineNode> {
    let mut translator =
        LogicalPlanToPipelineNodeTranslator::new(plan_config, psets, meter.clone());
    let _ = plan.visit(&mut translator)?;
    Ok(translator.curr_node.pop().unwrap())
}

pub(crate) struct LogicalPlanToPipelineNodeTranslator {
    pub plan_config: PlanConfig,
    pub meter: Meter,
    pipeline_node_id_counter: NodeID,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<DistributedPipelineNode>,
}

impl LogicalPlanToPipelineNodeTranslator {
    fn new(
        plan_config: PlanConfig,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        meter: Meter,
    ) -> Self {
        Self {
            plan_config,
            meter,
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
                    SourceInfo::InMemory(info) => DistributedPipelineNode::new(
                        Arc::new(InMemorySourceNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            info.clone(),
                            self.psets.clone(),
                        )),
                        &self.meter,
                    ),
                    SourceInfo::Physical(info) => {
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!(
                                "ScanOperator should not be present in the optimized logical plan for pipeline node translation"
                            ),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };

                        // Perform scan task splitting and merging.
                        let scan_tasks = if self.plan_config.config.enable_scan_task_split_and_merge
                        {
                            scan_task_iters::split_and_merge_pass(
                                scan_tasks,
                                &info.pushdowns,
                                &self.plan_config.config,
                            )?
                        } else {
                            scan_tasks
                        };
                        DistributedPipelineNode::new(
                            Arc::new(ScanSourceNode::new(
                                self.get_next_pipeline_node_id(),
                                &self.plan_config,
                                info.pushdowns.clone(),
                                scan_tasks,
                                source.output_schema.clone(),
                            )),
                            &self.meter,
                        )
                    }
                    SourceInfo::GlobScan(info) => DistributedPipelineNode::new(
                        Arc::new(GlobScanSourceNode::try_new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            info.glob_paths.clone(),
                            info.pushdowns.clone(),
                            source.output_schema.clone(),
                            info.io_config.clone().map(|c| *c),
                        )?),
                        &self.meter,
                    ),
                    SourceInfo::PlaceHolder(_) => unreachable!(
                        "PlaceHolder should not be present in the logical plan for pipeline node translation"
                    ),
                }
            }
            LogicalPlan::UDFProject(udf) if udf.is_actor_pool_udf() => {
                #[cfg(feature = "python")]
                {
                    let udf_expr = BoundExpr::try_new(udf.expr.clone(), &udf.input.schema())?;
                    let passthrough_columns =
                        BoundExpr::bind_all(&udf.passthrough_columns, &udf.input.schema())?;
                    DistributedPipelineNode::new(
                        Arc::new(crate::pipeline_node::actor_udf::ActorUDF::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            udf_expr,
                            passthrough_columns,
                            udf.udf_properties.clone(),
                            udf.projected_schema.clone(),
                            self.curr_node.pop().unwrap(),
                        )?),
                        &self.meter,
                    )
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

                DistributedPipelineNode::new(
                    Arc::new(UDFNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        expr,
                        udf.udf_properties.clone(),
                        passthrough_columns,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(FilterNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        predicate,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::IntoBatches(into_batches) => DistributedPipelineNode::new(
                Arc::new(IntoBatchesNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    into_batches.batch_size,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Limit(limit) => DistributedPipelineNode::new(
                Arc::new(LimitNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    limit.limit as usize,
                    limit.offset.map(|x| x as usize),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(ProjectNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        projection,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(ExplodeNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        to_explode,
                        explode.ignore_empty_and_null,
                        explode.index_column.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Unpivot(unpivot) => {
                let ids = BoundExpr::bind_all(&unpivot.ids, &unpivot.input.schema())?;
                let values = BoundExpr::bind_all(&unpivot.values, &unpivot.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(UnpivotNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        ids,
                        values,
                        unpivot.variable_name.clone(),
                        unpivot.value_name.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Sample(sample) => DistributedPipelineNode::new(
                Arc::new(SampleNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sample.fraction,
                    sample.size,
                    sample.with_replacement,
                    sample.seed,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Sink(sink) => {
                let sink_info = sink.sink_info.bind(&sink.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(SinkNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sink_info.into(),
                        sink.schema.clone(),
                        sink.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                DistributedPipelineNode::new(
                    Arc::new(MonotonicallyIncreasingIdNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        monotonically_increasing_id.column_name.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Concat(_) => DistributedPipelineNode::new(
                Arc::new(ConcatNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    node.schema(),
                    self.curr_node.pop().unwrap(), // Other
                    self.curr_node.pop().unwrap(), // Child
                )),
                &self.meter,
            ),
            LogicalPlan::Repartition(repartition) => match &repartition.repartition_spec {
                RepartitionSpec::Hash(_)
                | RepartitionSpec::Random(_)
                | RepartitionSpec::Range(_) => {
                    let child = self.curr_node.pop().unwrap();
                    self.gen_repartition_node(
                        repartition.repartition_spec.clone(),
                        node.schema(),
                        child,
                    )?
                }
            },
            LogicalPlan::IntoPartitions(into_partitions) => DistributedPipelineNode::new(
                Arc::new(IntoPartitionsNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    into_partitions.num_partitions,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Aggregate(aggregate) => {
                let input_schema = aggregate.input.schema();
                let group_by = BoundExpr::bind_all(&aggregate.groupby, &input_schema)?;
                let (aggregations, aliases) = aggregate
                    .aggregations
                    .iter()
                    .map(|expr| {
                        let (agg_expr, alias) = extract_agg_expr(expr)?;
                        Ok((
                            BoundAggExpr::try_new(agg_expr, &aggregate.input.schema())?,
                            alias,
                        ))
                    })
                    .collect::<DaftResult<(Vec<_>, Vec<_>)>>()?;

                let input_node = self.curr_node.pop().unwrap();
                self.gen_agg_nodes(
                    input_node,
                    group_by.clone(),
                    aggregations,
                    aliases,
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
                    DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns,
                            distinct.input.schema(),
                            input_node,
                        )),
                        &self.meter,
                    )
                } else {
                    // Need full 2-stage distinct with shuffle
                    // First stage: Initial local distinct to reduce the dataset
                    let initial_distinct = DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns.clone(),
                            distinct.input.schema(),
                            input_node,
                        )),
                        &self.meter,
                    );

                    // Second stage: Repartition to distribute the dataset
                    let repartition = self.gen_repartition_node(
                        RepartitionSpec::Hash(HashRepartitionConfig::new(
                            None,
                            columns.clone().into_iter().map(|e| e.into()).collect(),
                        )),
                        distinct.input.schema(),
                        initial_distinct,
                    )?;

                    // Last stage: Redo the distinct to get the final result
                    DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns,
                            distinct.input.schema(),
                            repartition,
                        )),
                        &self.meter,
                    )
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
                    self.gen_repartition_node(
                        RepartitionSpec::Hash(HashRepartitionConfig::new(
                            None,
                            partition_by.clone().into_iter().map(|e| e.into()).collect(),
                        )),
                        window.input.schema(),
                        input_node,
                    )?
                };

                // Final stage: The actual window op
                DistributedPipelineNode::new(
                    Arc::new(WindowNode::new(
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
                    )?),
                    &self.meter,
                )
            }
            LogicalPlan::Join(join) => {
                // Visitor appends in in-order
                // TODO: Just use regular recursion?
                let right_node = self.curr_node.pop().unwrap();
                let left_node = self.curr_node.pop().unwrap();

                self.translate_join(join, left_node, right_node)?
            }
            LogicalPlan::AsofJoin(_) => {
                return Err(DaftError::not_implemented(
                    "ASOF join is not yet supported on the Ray runner; use the native runner (DAFT_RUNNER=native).",
                ));
            }
            LogicalPlan::Sort(sort) => {
                let sort_by = BoundExpr::bind_all(&sort.sort_by, &sort.input.schema())?;

                DistributedPipelineNode::new(
                    Arc::new(SortNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by,
                        sort.descending.clone(),
                        sort.nulls_first.clone(),
                        sort.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::TopN(top_n) => {
                let sort_by = BoundExpr::bind_all(&top_n.sort_by, &top_n.input.schema())?;

                // First stage: Perform a local topN
                let local_topn = DistributedPipelineNode::new(
                    Arc::new(TopNNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by.clone(),
                        top_n.descending.clone(),
                        top_n.nulls_first.clone(),
                        top_n.limit + top_n.offset.unwrap_or(0),
                        Some(0),
                        top_n.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                );

                // Second stage: Gather all data to a single node
                let gather = self.gen_gather_node(local_topn);

                // Final stage: Do another topN to get the final result
                DistributedPipelineNode::new(
                    Arc::new(TopNNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by,
                        top_n.descending.clone(),
                        top_n.nulls_first.clone(),
                        top_n.limit,
                        top_n.offset,
                        top_n.input.schema(),
                        gather,
                    )),
                    &self.meter,
                )
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
                    vec![None],
                    output_schema,
                    group_by.clone(),
                )?;

                // Final stage: Pivot transformation
                DistributedPipelineNode::new(
                    Arc::new(PivotNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        group_by,
                        pivot_column,
                        value_column,
                        aggregation,
                        pivot.names.clone(),
                        pivot.output_schema.clone(),
                        agg,
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::VLLMProject(vllm_project) => {
                let input_schema = vllm_project.input.schema();
                let expr = BoundVLLMExpr::try_new(vllm_project.expr.clone(), &input_schema)?;

                DistributedPipelineNode::new(
                    Arc::new(VLLMNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        expr,
                        vllm_project.output_column_name.clone(),
                        vllm_project.output_schema.clone(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Shuffle(shuffle) => DistributedPipelineNode::new(
                Arc::new(RandomShuffleNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    shuffle.seed,
                    shuffle.input.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
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
