use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_dsl::ExprRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    partitioning::ClusteringSpecRef,
    stats::{ApproxStats, StatsState},
    JoinType, LogicalPlanRef,
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

struct StageID(usize);
struct ChannelID(usize);

struct DataChannel {
    schema: SchemaRef,
    clustering_spec: Option<ClusteringSpecRef>,
    stats: Option<ApproxStats>,
    // ordering: Option<ExprRef>,
}

struct InputChannel {
    from_stage: StageID,
    channel_id: ChannelID,
    data_channel: DataChannel,
}

struct OutputChannel {
    to_stages: Vec<StageID>,
    data_channel: DataChannel,
}

struct Stage {
    stage_id: StageID,
    stage_type: StageType,
    input_channels: Vec<InputChannel>,
    output_channels: Vec<OutputChannel>,
}

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

pub(crate) struct StagePlan {
    stages: HashMap<StageID, Stage>,
    root_stage: StageID,
}

impl StagePlan {
    pub(crate) fn from_logical_plan(plan: LogicalPlanRef) -> DaftResult<Self> {
        let builder = StagePlanBuilder::new();

        unimplemented!()
    }
}

struct StagePlanBuilder {
    stages: HashMap<StageID, Stage>,
    id_counter: usize,
}

impl StagePlanBuilder {
    fn new() -> Self {
        Self {
            stages: HashMap::new(),
            id_counter: 0,
        }
    }

    fn next_id(&mut self) -> StageID {
        let curr = self.id_counter;
        self.id_counter += 1;
        StageID(curr)
    }

    fn build(self) -> StagePlan {
        StagePlan {
            stages: self.stages,
            root_stage: StageID(self.id_counter),
        }
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
