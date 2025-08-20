use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_logical_plan::{
    partitioning::ClusteringSpecRef, JoinStrategy, LogicalPlan, LogicalPlanRef,
};
use daft_schema::schema::SchemaRef;

use super::{DataChannel, OutputChannel, Stage, StageID, StagePlan, StageType};

pub(crate) struct StagePlanBuilder {
    stages: HashMap<StageID, Stage>,
    stage_id_counter: StageID,
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
        curr
    }

    fn can_translate_logical_plan(plan: &LogicalPlanRef) -> bool {
        let mut can_translate = true;
        let _ = plan.apply(|node| match node.as_ref() {
            LogicalPlan::Source(_)
            | LogicalPlan::Project(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::IntoBatches(_)
            | LogicalPlan::Sink(_)
            | LogicalPlan::Sample(_)
            | LogicalPlan::Explode(_)
            | LogicalPlan::UDFProject(_)
            | LogicalPlan::Unpivot(_)
            | LogicalPlan::MonotonicallyIncreasingId(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Concat(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::TopN(_) => Ok(TreeNodeRecursion::Continue),
            LogicalPlan::Join(join) => {
                if join
                    .join_strategy
                    .is_some_and(|x| !matches!(x, JoinStrategy::Hash | JoinStrategy::Broadcast))
                {
                    can_translate = false;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    let (remaining_on, left_on, right_on, _) = join.on.split_eq_preds();
                    if !remaining_on.is_empty() || left_on.is_empty() || right_on.is_empty() {
                        can_translate = false;
                        Ok(TreeNodeRecursion::Stop)
                    } else {
                        Ok(TreeNodeRecursion::Continue)
                    }
                }
            }
            LogicalPlan::Pivot(_) => {
                can_translate = false;
                Ok(TreeNodeRecursion::Stop)
            }
            LogicalPlan::Intersect(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Shard(_)
            | LogicalPlan::Offset(_) => panic!(
                "Logical plan operator {} should be optimized away before planning stages",
                node.name()
            ),
        });
        can_translate
    }

    fn build_stages_from_plan(&mut self, plan: LogicalPlanRef) -> DaftResult<StageID> {
        if !Self::can_translate_logical_plan(&plan) {
            return Err(DaftError::ValueError(format!(
                "Cannot translate logical plan: {} into stages",
                plan
            )));
        }

        let schema = plan.schema();
        // Create a MapPipeline stage
        let stage_id = self.next_stage_id();
        let stage = Stage {
            id: stage_id,
            type_: StageType::MapPipeline { plan },
            input_channels: vec![],
            output_channels: vec![self.create_output_channel(schema, None)?],
        };

        // TODO: Add upstream stage to output channel stages
        self.stages.insert(stage_id, stage);
        Ok(stage_id)
    }

    pub fn build_stage_plan(
        mut self,
        plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<StagePlan> {
        let root_stage_id = self.build_stages_from_plan(plan)?;
        Ok(StagePlan {
            stages: self.stages,
            root_stage: root_stage_id,
            config,
        })
    }

    fn create_output_channel(
        &self,
        schema: SchemaRef,
        clustering_spec: Option<ClusteringSpecRef>,
    ) -> DaftResult<OutputChannel> {
        Ok(OutputChannel {
            to_stages: vec![], // Will be populated later when connections are established
            data_channel: DataChannel {
                schema,
                clustering_spec,
                stats: None, // Stats will be computed during execution
            },
        })
    }
}
