use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanRef,
    SourceInfo,
};

use super::{Stage, StageID, StagePlan, StageType};
use crate::pipeline_node::logical_plan_to_pipeline_node;

pub(crate) struct StagePlanBuilder {
    stages: HashMap<StageID, Stage>,
    stage_id_counter: usize,
}

impl StagePlanBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            stage_id_counter: 0,
        }
    }

    fn next_stage_id(&mut self) -> StageID {
        let curr = self.stage_id_counter;
        self.stage_id_counter += 1;
        StageID(curr)
    }

    fn build_stages_from_plan(
        &mut self,
        plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<StageID> {
        // Match on the type of the logical plan node
        match plan.as_ref() {
            _ => {
                struct MapPipelineBuilder {
                    remaining: Option<LogicalPlanRef>,
                }
                impl TreeNodeRewriter for MapPipelineBuilder {
                    type Node = Arc<LogicalPlan>;

                    fn f_down(
                        &mut self,
                        node: Self::Node,
                    ) -> DaftResult<common_treenode::Transformed<Self::Node>> {
                        // For simple operations, we can pipeline them together
                        // until we hit a stage boundary (e.g., a HashJoin)
                        if matches!(
                            node.as_ref(),
                            LogicalPlan::Join(_)
                                | LogicalPlan::Aggregate(_)
                                | LogicalPlan::Repartition(_)
                        ) {
                            let ph = PlaceHolderInfo::new(
                                node.schema(),
                                Arc::new(ClusteringSpec::unknown()),
                            );
                            let new_scan = LogicalPlan::Source(Source::new(
                                node.schema(),
                                SourceInfo::PlaceHolder(ph).into(),
                            ));
                            self.remaining = Some(node);
                            Ok(Transformed::yes(new_scan.into()))
                        } else {
                            Ok(Transformed::no(node))
                        }
                    }

                    fn f_up(
                        &mut self,
                        node: Self::Node,
                    ) -> DaftResult<common_treenode::Transformed<Self::Node>> {
                        Ok(Transformed::no(node))
                    }
                }

                let mut rewriter = MapPipelineBuilder { remaining: None };

                let output = plan.rewrite(&mut rewriter)?;
                let new_plan = output.data;
                // Create a MapPipeline stage
                let stage_id = self.next_stage_id();
                let pipeline_node = logical_plan_to_pipeline_node(new_plan, config)?;
                let stage = Stage {
                    id: stage_id.clone(),
                    type_: StageType::MapPipeline { pipeline_node },
                };

                // TODO: Add upstream stage to output channel stages
                self.stages.insert(stage_id.clone(), stage);
                Ok(stage_id)
            }
        }
    }

    pub fn build_stage_plan(
        mut self,
        plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<StagePlan> {
        let root_stage_id = self.build_stages_from_plan(plan, config)?;
        Ok(StagePlan {
            stages: self.stages,
            root_stage: root_stage_id,
        })
    }
}
