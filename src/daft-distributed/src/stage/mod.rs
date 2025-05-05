use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_dsl::ExprRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source,
    partitioning::ClusteringSpecRef,
    source_info::PlaceHolderInfo,
    stats::{ApproxStats, StatsState},
    ClusteringSpec, JoinType, LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;
use futures::Stream;

use crate::{
    pipeline_node::PipelineOutput,
    runtime::JoinSet,
    scheduling::{
        dispatcher::{TaskDispatcher, TaskDispatcherHandle},
        worker::WorkerManagerFactory,
    },
};

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
struct StageID(usize);

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
struct ChannelID(usize);

#[derive(Clone, Debug)]
struct DataChannel {
    schema: SchemaRef,
    clustering_spec: Option<ClusteringSpecRef>,
    stats: Option<ApproxStats>,
    // ordering: Option<ExprRef>,
}
#[derive(Debug)]

struct InputChannel {
    from_stage: StageID,
    channel_id: ChannelID,
    data_channel: DataChannel,
}
#[derive(Debug)]

struct OutputChannel {
    to_stages: Vec<StageID>,
    data_channel: DataChannel,
}
#[derive(Debug)]

struct Stage {
    stage_id: StageID,
    stage_type: StageType,
    input_channels: Vec<InputChannel>,
    output_channels: Vec<OutputChannel>,
}

#[derive(Debug)]
enum StageType {
    MapPipeline {
        plan: LocalPhysicalPlanRef,
    },
    HashJoin {
        plan: LocalPhysicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_null: Option<Vec<bool>>,
        join_type: JoinType,
    },
    // SortMergeJoin {
    //     plan: LocalPhysicalPlanRef,
    // },
    HashAggregate {
        plan: LocalPhysicalPlanRef,
        aggregations: Vec<ExprRef>,
        group_by: Vec<ExprRef>,
    },
    Broadcast,
    Exchange {
        clustering_spec: ClusteringSpecRef,
    },
}
#[derive(Debug)]
pub(crate) struct StagePlan {
    stages: HashMap<StageID, Stage>,
    root_stage: StageID,
}

impl StagePlan {
    pub(crate) fn from_logical_plan(plan: LogicalPlanRef) -> DaftResult<Self> {
        let mut builder = StagePlanBuilder::new();
        let root_stage_id = builder.build_stages_from_plan(plan)?;

        Ok(StagePlan {
            stages: builder.stages,
            root_stage: root_stage_id,
        })
    }
}

struct StagePlanBuilder {
    stages: HashMap<StageID, Stage>,
    stage_id_counter: usize,
}

impl StagePlanBuilder {
    fn new() -> Self {
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

    fn build(self) -> StagePlan {
        StagePlan {
            stages: self.stages,
            root_stage: StageID(self.stage_id_counter),
        }
    }

    fn build_stages_from_plan(&mut self, plan: LogicalPlanRef) -> DaftResult<StageID> {
        // Match on the type of the logical plan node
        match plan.as_ref() {
            //     // For a HashJoin, create a separate stage
            //     LogicalPlan::Join(join) => {
            //         // Recursively build stages for left and right inputs
            //         let left_stage_id = self.build_stages_from_plan(join.left.clone())?;
            //         let right_stage_id = self.build_stages_from_plan(join.right.clone())?;

            //         // Create a new HashJoin stage
            //         let stage_id = self.next_id();
            //         let physical_plan = op.to_local_physical_plan()?;

            //         let (remaining_on, left_on, right_on, null_equals_null) = join.on.split_eq_preds();

            //         if !remaining_on.is_empty() {
            //             return Err(DaftError::not_implemented("Execution of non-equality join"));
            //         }

            //         let stage = Stage {
            //             stage_id: stage_id.clone(),
            //             stage_type: StageType::HashJoin {
            //                 plan: Arc::new(physical_plan),
            //                 left_on,
            //                 right_on,
            //                 null_equals_null: Some(null_equals_null),
            //                 join_type: join.join_type
            //             },
            //             input_channels: vec![
            //                 self.create_input_channel(left_stage_id, 0)?,
            //                 self.create_input_channel(right_stage_id, 0)?,
            //             ],
            //             output_channels: vec![self.create_output_channel(op.schema(), None)?],
            //         };

            //         self.stages.insert(stage_id.clone(), stage);
            //         Ok(stage_id)
            //     },
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

                // Create a MapPipeline stage
                let stage_id = self.next_stage_id();
                let physical_plan = daft_local_plan::translate(&new_plan)?;
                let stage = Stage {
                    stage_id: stage_id.clone(),
                    stage_type: StageType::MapPipeline {
                        plan: physical_plan,
                    },
                    input_channels,
                    output_channels: vec![self.create_output_channel(new_plan.schema(), None)?],
                };

                // TODO: Add upstream stage to output channel stages
                self.stages.insert(stage_id.clone(), stage);
                Ok(stage_id)
            }
        }
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

pub(crate) struct StageContext {
    pub task_dispatcher_handle: TaskDispatcherHandle,
    pub joinset: JoinSet<DaftResult<()>>,
}

impl StageContext {
    fn try_new(worker_manager_factory: Box<dyn WorkerManagerFactory>) -> DaftResult<Self> {
        let worker_manager = worker_manager_factory.create_worker_manager()?;
        let task_dispatcher = TaskDispatcher::new(worker_manager);
        let mut joinset = JoinSet::new();
        let task_dispatcher_handle =
            TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);
        Ok(Self {
            task_dispatcher_handle,
            joinset,
        })
    }
}
