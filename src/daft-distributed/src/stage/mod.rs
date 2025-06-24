use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use common_daft_config::DaftExecutionConfig;
use common_display::DisplayLevel;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_dsl::ExprRef;
use daft_logical_plan::{
    partitioning::ClusteringSpecRef, stats::ApproxStats, JoinType, LogicalPlanRef,
};
use daft_schema::schema::SchemaRef;
use futures::Stream;
use serde::{Deserialize, Serialize};
use stage_builder::StagePlanBuilder;

use crate::{
    pipeline_node::{
        logical_plan_to_pipeline_node, materialize::materialize_all_pipeline_outputs,
        viz_distributed_pipeline_ascii, viz_distributed_pipeline_mermaid, MaterializedOutput,
        PipelineOutput, RunningPipelineNode,
    },
    plan::PlanID,
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, TaskID},
    },
    utils::{joinset::JoinSet, stream::JoinableForwardingStream},
};

mod stage_builder;

pub(crate) type StageID = u16;

#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
struct ChannelID(usize);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct DataChannel {
    schema: SchemaRef,
    clustering_spec: Option<ClusteringSpecRef>,
    stats: Option<ApproxStats>,
    // ordering: Option<ExprRef>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct InputChannel {
    from_stage: StageID,
    channel_id: ChannelID,
    data_channel: DataChannel,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct OutputChannel {
    to_stages: Vec<StageID>,
    data_channel: DataChannel,
}

// THIS CODE IS SUBJECT TO CHANGE
// Tentatively: A stage represents a fragment of a logical plan that can be run from start to finish
// The boundaries of stages are determined based on whether or not data has to be moved between workers
// For example, a grouped aggregate will be split up into a map stage, an exchange stage, and a hash aggregate stage.
// Stages cannot produce unmaterialized results. The results of stages must be materialized before they can be used as input to other stages.
//
// KEY POINTS FOR CONSIDERATION (Whatever we end up doing here, we need to make sure that):
// - Our design must be evolvable, meaning we should be able to make modifications as and when new requirements / problems arise.
// - Reliable and scalable is the first priority.
// - It should be easy to understand the stages and what they do.
// - We must be able to do re-planning based on new statistics.
// - We must allow for potential concurrent execution of stages.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Stage {
    id: StageID,
    type_: StageType,
    input_channels: Vec<InputChannel>,
    output_channels: Vec<OutputChannel>,
}

impl Stage {
    pub(crate) fn run_stage(
        &self,
        plan_id: PlanID,
        psets: HashMap<String, Vec<PartitionRef>>,
        config: Arc<DaftExecutionConfig>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<RunningStage> {
        let stage_config = StageConfig::new(plan_id, self.id, config);
        let mut stage_context = StageExecutionContext::new(scheduler_handle);
        match &self.type_ {
            StageType::MapPipeline { plan } => {
                let pipeline_node =
                    logical_plan_to_pipeline_node(stage_config, plan.clone(), Arc::new(psets))?;
                let running_node = pipeline_node.start(&mut stage_context);
                Ok(RunningStage::new(running_node, stage_context))
            }
            _ => todo!("FLOTILLA_MS2: Implement run_stage for other stage types"),
        }
    }

    /// Get the logical plan for visualization purposes (only for MapPipeline stages)
    pub fn repr_mermaid(
        &self,
        plan_id: PlanID,
        simple: bool,
        bottom_up: bool,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<String> {
        let stage_config = StageConfig::new(plan_id, self.id, config);
        match &self.type_ {
            StageType::MapPipeline { plan } => {
                let pipeline_node =
                    logical_plan_to_pipeline_node(stage_config, plan.clone(), Default::default())?;
                let display_level = if simple {
                    DisplayLevel::Compact
                } else {
                    DisplayLevel::Default
                };
                Ok(viz_distributed_pipeline_mermaid(
                    pipeline_node.as_ref(),
                    display_level,
                    bottom_up,
                    None,
                ))
            }
            _ => todo!("FLOTILLA_MS2: Implement repr_mermaid for other stage types"),
        }
    }

    pub fn repr_ascii(
        &self,
        plan_id: PlanID,
        simple: bool,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<String> {
        let stage_config = StageConfig::new(plan_id, self.id, config);
        match &self.type_ {
            StageType::MapPipeline { plan } => {
                let pipeline_node =
                    logical_plan_to_pipeline_node(stage_config, plan.clone(), Default::default())?;
                Ok(viz_distributed_pipeline_ascii(
                    pipeline_node.as_ref(),
                    simple,
                ))
            }
            _ => todo!("FLOTILLA_MS2: Implement repr_ascii for other stage types"),
        }
    }

    /// Get the stage type name for visualization
    #[allow(dead_code)]
    pub fn stage_type_name(&self) -> &str {
        self.type_.name()
    }
}

pub(crate) struct RunningStage {
    running_pipeline_node: RunningPipelineNode,
    stage_context: StageExecutionContext,
}

impl RunningStage {
    fn new(
        running_pipeline_node: RunningPipelineNode,
        stage_context: StageExecutionContext,
    ) -> Self {
        Self {
            running_pipeline_node,
            stage_context,
        }
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let stream = self.into_stream();
        materialize_all_pipeline_outputs(stream, scheduler_handle)
    }

    pub fn into_stream(
        self,
    ) -> impl Stream<Item = DaftResult<PipelineOutput<SwordfishTask>>> + Send + Unpin + 'static
    {
        JoinableForwardingStream::new(
            self.running_pipeline_node.into_stream(),
            self.stage_context.joinset,
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
enum StageType {
    MapPipeline {
        plan: LogicalPlanRef,
    },
    HashJoin {
        plan: LogicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_null: Option<Vec<bool>>,
        join_type: JoinType,
    },
    // SortMergeJoin {
    //     plan: LocalPhysicalPlanRef,
    // },
    HashAggregate {
        plan: LogicalPlanRef,
        aggregations: Vec<ExprRef>,
        group_by: Vec<ExprRef>,
    },
    Broadcast,
    Exchange {
        clustering_spec: ClusteringSpecRef,
    },
}

impl StageType {
    #[allow(dead_code)]
    fn name(&self) -> &str {
        match self {
            Self::MapPipeline { .. } => "MapPipeline",
            Self::HashJoin { .. } => "HashJoin",
            Self::HashAggregate { .. } => "HashAggregate",
            Self::Broadcast => "Broadcast",
            Self::Exchange { .. } => "Exchange",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StagePlan {
    stages: HashMap<StageID, Stage>,
    root_stage: StageID,
    config: Arc<DaftExecutionConfig>,
}

impl StagePlan {
    pub(crate) fn from_logical_plan(
        plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let builder = StagePlanBuilder::new();
        let stage_plan = builder.build_stage_plan(plan, config.clone())?;

        Ok(Self {
            stages: stage_plan.stages,
            root_stage: stage_plan.root_stage,
            config,
        })
    }

    pub fn repr_mermaid(
        &self,
        plan_id: PlanID,
        simple: bool,
        bottom_up: bool,
    ) -> DaftResult<String> {
        let root_stage = self.get_root_stage();
        let config = self.config.clone();
        root_stage.repr_mermaid(plan_id, simple, bottom_up, config)
    }

    pub fn repr_ascii(&self, plan_id: PlanID, simple: bool) -> DaftResult<String> {
        let root_stage = self.get_root_stage();
        let config = self.config.clone();
        root_stage.repr_ascii(plan_id, simple, config)
    }

    pub(crate) fn num_stages(&self) -> usize {
        self.stages.len()
    }

    pub fn get_root_stage(&self) -> &Stage {
        self.stages
            .get(&self.root_stage)
            .expect("expect root stage to be in stages")
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StageConfig {
    pub plan_id: PlanID,
    pub stage_id: StageID,
    pub config: Arc<DaftExecutionConfig>,
}

impl StageConfig {
    pub fn new(plan_id: PlanID, stage_id: StageID, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            plan_id,
            stage_id,
            config,
        }
    }
}

pub(crate) struct StageExecutionContext {
    scheduler_handle: SchedulerHandle<SwordfishTask>,
    joinset: JoinSet<DaftResult<()>>,
    task_id_counter: TaskIDCounter,
}

impl StageExecutionContext {
    fn new(scheduler_handle: SchedulerHandle<SwordfishTask>) -> Self {
        let joinset = JoinSet::new();
        Self {
            scheduler_handle,
            joinset,
            task_id_counter: TaskIDCounter::new(),
        }
    }

    pub fn scheduler_handle(&self) -> SchedulerHandle<SwordfishTask> {
        self.scheduler_handle.clone()
    }

    pub fn spawn(&mut self, task: impl Future<Output = DaftResult<()>> + Send + 'static) {
        self.joinset.spawn(task);
    }

    pub fn task_id_counter(&self) -> TaskIDCounter {
        self.task_id_counter.clone()
    }
}

#[derive(Clone)]
pub(crate) struct TaskIDCounter {
    counter: Arc<AtomicU32>,
}

impl TaskIDCounter {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn next(&self) -> TaskID {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}
