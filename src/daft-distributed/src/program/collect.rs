use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::LogicalPlanRef;

use crate::{
    channel::{create_channel, Receiver, Sender},
    program::task_producer::TaskProducer,
    runtime::JoinSet,
    scheduling::dispatcher::TaskDispatcherHandle,
};

pub struct CollectProgram {
    #[allow(dead_code)]
    plan: LogicalPlanRef,
}

impl CollectProgram {
    pub fn new(plan: LogicalPlanRef) -> Self {
        Self { plan }
    }

    async fn run_program(
        _task_dispatcher_handle: TaskDispatcherHandle,
        _task_producer: TaskProducer,
        _result_tx: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        todo!()
    }

    #[allow(dead_code)]
    pub fn spawn_program(
        self,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        input_rx: Option<Receiver<PartitionRef>>,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> Receiver<PartitionRef> {
        let task_producer = TaskProducer::new(self.plan, input_rx, psets, config);
        let (result_tx, result_rx) = create_channel(1);
        joinset.spawn(Self::run_program(
            task_dispatcher_handle,
            task_producer,
            result_tx,
        ));
        result_rx
    }
}
