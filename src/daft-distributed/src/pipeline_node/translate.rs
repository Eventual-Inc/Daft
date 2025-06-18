use core::panic;
use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::{LogicalPlan, LogicalPlanRef, SourceInfo};

use crate::{
    pipeline_node::{
        explode::ExplodeNode, filter::FilterNode, in_memory_source::InMemorySourceNode,
        limit::LimitNode, project::ProjectNode, sample::SampleNode, scan_source::ScanSourceNode,
        sink::SinkNode, unpivot::UnpivotNode, DistributedPipelineNode,
    },
    plan::PlanID,
    stage::StageID,
};

pub(crate) fn logical_plan_to_pipeline_node(
    plan_id: PlanID,
    stage_id: StageID,
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
    let mut translator = LogicalPlanToPipelineNodeTranslator::new(plan_id, stage_id, config, psets);
    let _ = plan.visit(&mut translator)?;
    Ok(translator.curr_node.pop().unwrap())
}

struct LogicalPlanToPipelineNodeTranslator {
    plan_id: PlanID,
    stage_id: StageID,
    node_id_counter: usize,
    config: Arc<DaftExecutionConfig>,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<Arc<dyn DistributedPipelineNode>>,
}

impl LogicalPlanToPipelineNodeTranslator {
    fn new(
        plan_id: PlanID,
        stage_id: StageID,
        config: Arc<DaftExecutionConfig>,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id_counter: 0,
            config,
            psets,
            curr_node: Vec::new(),
        }
    }

    fn get_next_node_id(&mut self) -> usize {
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
        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => InMemorySourceNode::new(
                        self.plan_id.clone(),
                        self.stage_id.clone(),
                        self.get_next_node_id(),
                        self.config.clone(),
                        info.clone(),
                        self.psets.clone(),
                    ).arced(),
                    SourceInfo::Physical(info) => {
                        // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!("ScanOperator should not be present in the optimized logical plan for pipeline node translation"),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };
                        ScanSourceNode::new(self.plan_id.clone(), self.stage_id.clone(), self.get_next_node_id(), self.config.clone(), info.pushdowns.clone(), scan_tasks, source.output_schema.clone()).arced()
                    }
                    SourceInfo::PlaceHolder(_) => unreachable!("PlaceHolder should not be present in the logical plan for pipeline node translation"),
                }
            }
            LogicalPlan::ActorPoolProject(actor_pool_project) => {
                #[cfg(feature = "python")]
                {
                    Arc::new(crate::pipeline_node::actor_udf::ActorUDF::new(
                        self.plan_id.clone(),
                        self.stage_id.clone(),
                        self.get_next_node_id(),
                        self.config.clone(),
                        actor_pool_project.projection.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )?)
                }
                #[cfg(not(feature = "python"))]
                {
                    unreachable!("ActorUDF is not supported without Python feature")
                }
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                FilterNode::new(
                    self.plan_id.clone(),
                    self.stage_id.clone(),
                    self.get_next_node_id(),
                    self.config.clone(),
                    predicate,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Limit(limit) => Arc::new(LimitNode::new(
                self.plan_id.clone(),
                self.stage_id.clone(),
                self.get_next_node_id(),
                limit.limit as usize,
                node.schema(),
                self.config.clone(),
                self.curr_node.pop().unwrap(),
            )),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                ProjectNode::new(
                    self.plan_id.clone(),
                    self.stage_id.clone(),
                    self.get_next_node_id(),
                    self.config.clone(),
                    projection,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )
                .arced()
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                ExplodeNode::new(
                    self.plan_id.clone(),
                    self.stage_id.clone(),
                    self.get_next_node_id(),
                    self.config.clone(),
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
                    self.plan_id.clone(),
                    self.stage_id.clone(),
                    self.get_next_node_id(),
                    self.config.clone(),
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
                self.plan_id.clone(),
                self.stage_id.clone(),
                self.get_next_node_id(),
                self.config.clone(),
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::Sink(sink) => SinkNode::new(
                self.plan_id.clone(),
                self.stage_id.clone(),
                self.get_next_node_id(),
                self.config.clone(),
                sink.sink_info.clone(),
                node.schema(),
                self.curr_node.pop().unwrap(),
            )
            .arced(),
            LogicalPlan::MonotonicallyIncreasingId(_) => {
                todo!("FLOTILLA_MS1: Implement MonotonicallyIncreasingId")
            }
            LogicalPlan::Concat(_) => {
                todo!("FLOTILLA_MS1: Implement Concat")
            }
            LogicalPlan::Aggregate(_) => {
                todo!("FLOTILLA_MS2: Implement Aggregate")
            }
            LogicalPlan::Repartition(_) => {
                todo!("FLOTILLA_MS2: Implement Repartition")
            }
            LogicalPlan::Distinct(_) => {
                todo!("FLOTILLA_MS2: Implement Distinct")
            }
            LogicalPlan::Sort(_) => {
                todo!("FLOTILLA_MS2: Implement Sort")
            }
            LogicalPlan::TopN(_) => {
                todo!("FLOTILLA_MS2: Implement TopN")
            }
            LogicalPlan::Pivot(_) => {
                todo!("FLOTILLA_MS2: Implement Pivot")
            }
            LogicalPlan::Join(_) => {
                todo!("FLOTILLA_MS2: Implement Join")
            }
            LogicalPlan::Window(_) => {
                todo!("FLOTILLA_MS2: Implement Window")
            }
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) | LogicalPlan::Intersect(_) => {
                panic!("LogicalPlan::SubqueryAlias, LogicalPlan::Union, and LogicalPlan::Intersect should be handled by the optimizer")
            }
        };
        self.curr_node.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}
