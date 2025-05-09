use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{
    ops::Source, partitioning::ClusteringSpecRef, source_info::PlaceHolderInfo, ClusteringSpec,
    LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

use super::{
    ChannelID, DataChannel, InputChannel, OutputChannel, Stage, StageID, StagePlan, StageType,
};

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

    fn build_stages_from_plan(&mut self, plan: LogicalPlanRef) -> DaftResult<StageID> {
        // Match on the type of the logical plan node
        match plan.as_ref() {
            // For a HashJoin, create a separate stage
            LogicalPlan::Join(join) => {
                // Recursively build stages for left and right inputs
                let left_stage_id = self.build_stages_from_plan(join.left.clone())?;
                let right_stage_id = self.build_stages_from_plan(join.right.clone())?;

                // Create a new HashJoin stage
                let stage_id = self.next_stage_id();

                let left_child = {
                    let ph = PlaceHolderInfo::new(
                        join.left.schema(),
                        Arc::new(ClusteringSpec::unknown()),
                    );
                    LogicalPlan::Source(Source::new(
                        join.left.schema(),
                        SourceInfo::PlaceHolder(ph).into(),
                    ))
                    .arced()
                };

                let right_child = {
                    let ph = PlaceHolderInfo::new(
                        join.right.schema(),
                        Arc::new(ClusteringSpec::unknown()),
                    );
                    LogicalPlan::Source(Source::new(
                        join.right.schema(),
                        SourceInfo::PlaceHolder(ph).into(),
                    ))
                    .arced()
                };

                let plan = plan.with_new_children(&[left_child, right_child]).arced();

                let (remaining_on, left_on, right_on, null_equals_null) = join.on.split_eq_preds();

                if !remaining_on.is_empty() {
                    return Err(DaftError::not_implemented("Execution of non-equality join"));
                }
                let schema = plan.schema();
                let stage = Stage {
                    id: stage_id.clone(),
                    type_: StageType::HashJoin {
                        plan,
                        left_on,
                        right_on,
                        null_equals_null: Some(null_equals_null),
                        join_type: join.join_type,
                    },
                    input_channels: vec![
                        self.create_input_channel(left_stage_id, 0)?,
                        self.create_input_channel(right_stage_id, 0)?,
                    ],
                    output_channels: vec![self.create_output_channel(schema, None)?],
                };
                self.stages.insert(stage_id.clone(), stage);
                Ok(stage_id)
            }
            // For other operations, group into a MapPipeline stage
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
                let input_channels = if output.transformed {
                    let remaining = rewriter
                        .remaining
                        .expect("We should have remaining plan if plan was transformed");
                    let child_stage = self.build_stages_from_plan(remaining)?;
                    vec![self.create_input_channel(child_stage, 0)?]
                } else {
                    vec![]
                };
                let schema = new_plan.schema();
                // Create a MapPipeline stage
                let stage_id = self.next_stage_id();
                let stage = Stage {
                    id: stage_id.clone(),
                    type_: StageType::MapPipeline { plan: new_plan },
                    input_channels,
                    output_channels: vec![self.create_output_channel(schema, None)?],
                };

                // TODO: Add upstream stage to output channel stages
                self.stages.insert(stage_id.clone(), stage);
                Ok(stage_id)
            }
        }
    }

    pub fn build_stage_plan(mut self, plan: LogicalPlanRef) -> DaftResult<StagePlan> {
        let root_stage_id = self.build_stages_from_plan(plan)?;
        Ok(StagePlan {
            stages: self.stages,
            root_stage: root_stage_id,
        })
    }

    fn create_input_channel(
        &self,
        from_stage: StageID,
        channel_idx: usize,
    ) -> DaftResult<InputChannel> {
        let stage = self.stages.get(&from_stage).ok_or_else(|| {
            common_error::DaftError::InternalError(format!("Stage {} not found", from_stage.0))
        })?;

        let output_channel = &stage.output_channels[channel_idx];
        Ok(InputChannel {
            from_stage,
            channel_id: ChannelID(channel_idx),
            data_channel: output_channel.data_channel.clone(),
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
