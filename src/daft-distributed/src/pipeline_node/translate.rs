use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    functions::python::{get_resource_request, try_get_batch_size_from_udf},
    resolved_col,
};
use daft_logical_plan::{partitioning::RepartitionSpec, LogicalPlan, LogicalPlanRef, SourceInfo};
use daft_physical_plan::extract_agg_expr;

use crate::{
    pipeline_node::{
        distinct::DistinctNode, explode::ExplodeNode, filter::FilterNode,
        groupby_agg::gen_agg_nodes, in_memory_source::InMemorySourceNode, limit::LimitNode,
        project::ProjectNode, repartition::RepartitionNode, sample::SampleNode,
        scan_source::ScanSourceNode, sink::SinkNode, unpivot::UnpivotNode, window::WindowNode,
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

struct LogicalPlanToPipelineNodeTranslator {
    stage_config: StageConfig,
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

    fn get_next_pipeline_node_id(&mut self) -> NodeID {
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
        let node_id = self.get_next_pipeline_node_id();
        let logical_node_id = node.node_id();
        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => InMemorySourceNode::new(
                        &self.stage_config,
                        node_id,
                        info.clone(),
                        self.psets.clone(),
                        logical_node_id.map(|id| id as NodeID),
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
                            logical_node_id.map(|id| id as NodeID),
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
                        logical_node_id.map(|id| id as NodeID),
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
                    logical_node_id.map(|id| id as NodeID),
                )
                .arced()
            }
            LogicalPlan::Limit(limit) => Arc::new(LimitNode::new(
                &self.stage_config,
                node_id,
                limit.limit as usize,
                node.schema(),
                self.curr_node.pop().unwrap(),
                logical_node_id.map(|id| id as NodeID),
            )),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                ProjectNode::new(
                    &self.stage_config,
                    node_id,
                    projection,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                    logical_node_id.map(|id| id as NodeID),
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
                    logical_node_id.map(|id| id as NodeID),
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
                    logical_node_id.map(|id| id as NodeID),
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
                logical_node_id.map(|id| id as NodeID),
            )
            .arced(),
            LogicalPlan::Sink(sink) => SinkNode::new(
                &self.stage_config,
                node_id,
                sink.sink_info.clone(),
                sink.schema.clone(),
                sink.input.schema(),
                self.curr_node.pop().unwrap(),
                logical_node_id.map(|id| id as NodeID),
            )
            .arced(),
            LogicalPlan::MonotonicallyIncreasingId(_) => {
                todo!("FLOTILLA_MS1: Implement MonotonicallyIncreasingId")
            }
            LogicalPlan::Concat(_) => {
                todo!("FLOTILLA_MS1: Implement Concat")
            }
            LogicalPlan::Aggregate(aggregate) => {
                if aggregate.groupby.is_empty() {
                    todo!("FLOTILLA_MS2: Implement Aggregate without groupby")
                }

                let groupby = BoundExpr::bind_all(&aggregate.groupby, &aggregate.input.schema())?;
                let aggregations = aggregate
                    .aggregations
                    .iter()
                    .map(|expr| {
                        let agg_expr = extract_agg_expr(expr)?;
                        BoundAggExpr::try_new(agg_expr, &aggregate.input.schema())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                gen_agg_nodes(
                    self.curr_node.pop().unwrap(),
                    &self.stage_config,
                    node_id,
                    groupby,
                    aggregations,
                    aggregate.output_schema.clone(),
                )?
            }
            LogicalPlan::Repartition(repartition) => {
                let RepartitionSpec::Hash(repart_spec) = &repartition.repartition_spec else {
                    todo!("FLOTILLA_MS2: Support other types of repartition");
                };

                let columns = BoundExpr::bind_all(&repart_spec.by, &repartition.input.schema())?;
                RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    columns,
                    repart_spec.num_partitions,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                    logical_node_id.map(|id| id as NodeID),
                )
                .arced()
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
                    &self.stage_config,
                    node_id,
                    columns.clone(),
                    distinct.input.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced();

                // Second stage: Repartition to distribute the dataset
                let repartition = RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    columns.clone(),
                    None,
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
                let repartition = RepartitionNode::new(
                    &self.stage_config,
                    node_id,
                    partition_by.clone(),
                    None,
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
            LogicalPlan::Sort(_) => {
                todo!("FLOTILLA_MS2: Implement Sort")
            }
            LogicalPlan::TopN(_) => {
                todo!("FLOTILLA_MS2: Implement TopN")
            }
            LogicalPlan::Pivot(_) => {
                todo!("FLOTILLA_MS3: Implement Pivot")
            }
            LogicalPlan::Join(_) => {
                todo!("FLOTILLA_MS2: Implement Join")
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
