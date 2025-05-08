use std::{collections::HashMap, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_dsl::ExprRef;
use daft_logical_plan::{
    partitioning::ClusteringSpecRef, stats::ApproxStats, JoinType, LogicalPlanRef,
};
use daft_schema::schema::SchemaRef;
use running_stage::RunningStage;
use stage_builder::StagePlanBuilder;

use crate::{
    pipeline_node::{logical_plan_to_pipeline_node, materialize::materialize_pipeline_results},
    runtime::JoinSet,
    scheduling::{
        dispatcher::{TaskDispatcher, TaskDispatcherHandle},
        worker::WorkerManagerFactory,
    },
};

mod running_stage;
mod stage_builder;

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
struct StageID(usize);

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
struct ChannelID(usize);

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct DataChannel {
    schema: SchemaRef,
    clustering_spec: Option<ClusteringSpecRef>,
    stats: Option<ApproxStats>,
    // ordering: Option<ExprRef>,
}
#[derive(Debug)]
#[allow(dead_code)]
struct InputChannel {
    from_stage: StageID,
    channel_id: ChannelID,
    data_channel: DataChannel,
}
#[derive(Debug)]
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

#[derive(Debug)]
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
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<RunningStage> {
        let mut stage_context = StageContext::try_new(worker_manager_factory)?;
        match &self.type_ {
            StageType::MapPipeline { plan } => {
                let mut pipeline_node =
                    logical_plan_to_pipeline_node(plan.clone(), config, psets, &mut stage_context)?;
                let running_node = pipeline_node.start(&mut stage_context);
                let materialized_results_receiver =
                    materialize_pipeline_results(running_node, &mut stage_context);
                let running_stage = RunningStage::new(materialized_results_receiver, stage_context);
                Ok(running_stage)
            }
            _ => todo!("FLOTILLA_MS2: Implement run stage for other stage types"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) struct StagePlan {
    stages: HashMap<StageID, Stage>,
    root_stage: StageID,
}

impl StagePlan {
    pub(crate) fn from_logical_plan(plan: LogicalPlanRef) -> DaftResult<Self> {
        let builder = StagePlanBuilder::new();
        let stage_plan = builder.build_stage_plan(plan)?;

        Ok(stage_plan)
    }

    #[allow(dead_code)]
    pub(crate) fn print_plan(&self) {
        let mut stack = vec![(0, self.root_stage.clone())];
        while let Some((depth, curr)) = stack.pop() {
            let stage = self
                .stages
                .get(&curr)
                .expect("expect this stage id to be in stages");
            let name = stage.type_.name();
            for _ in 0..depth {
                print!("  ");
            }
            println!("Stage {}: {}", curr.0, name);
            stage.input_channels.iter().enumerate().for_each(|(i, c)| {
                stack.push((depth + ((i != 0) as usize), c.from_stage.clone()));
            });
        }
    }

    pub(crate) fn num_stages(&self) -> usize {
        self.stages.len()
    }

    pub fn get_root_stage(&self) -> &Stage {
        self.stages
            .get(&self.root_stage)
            .expect("expect root stage to be in stages")
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct StageContext {
    task_dispatcher_handle: TaskDispatcherHandle,
    joinset: JoinSet<DaftResult<()>>,
    node_id_counter: usize,
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
            node_id_counter: 0,
        })
    }

    pub fn get_task_dispatcher_handle(&self) -> TaskDispatcherHandle {
        self.task_dispatcher_handle.clone()
    }

    pub fn spawn_task_on_joinset(
        &mut self,
        f: impl Future<Output = DaftResult<()>> + Send + 'static,
    ) {
        self.joinset.spawn(f);
    }

    pub fn get_node_id(&mut self) -> usize {
        let node_id = self.node_id_counter;
        self.node_id_counter += 1;
        node_id
    }

    pub fn into_inner(self) -> (TaskDispatcherHandle, JoinSet<DaftResult<()>>) {
        (self.task_dispatcher_handle, self.joinset)
    }
}
