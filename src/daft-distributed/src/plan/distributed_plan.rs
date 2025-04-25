use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};

use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::{get_or_init_runtime, JoinHandle},
    scheduling::worker::WorkerManager,
};

pub struct DistributedPhysicalPlan {
    remaining_logical_plan: Option<LogicalPlanRef>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: &Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();
        if !can_translate_logical_plan(&plan) {
            return Err(DaftError::InternalError(
                "Cannot run this physical plan on distributed swordfish yet".to_string(),
            ));
        }

        Ok(Self {
            remaining_logical_plan: Some(plan),
            config: config.clone(),
        })
    }

    pub async fn plan_loop(
        _remaining_logical_plan: Option<LogicalPlanRef>,
        _config: Arc<DaftExecutionConfig>,
        _worker_manager_creator: Arc<dyn Fn() -> Box<dyn WorkerManager> + Send + Sync>,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _result_sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        todo!()
    }

    #[allow(dead_code)]
    pub fn update(
        _plan: LogicalPlanRef,
        _results: Vec<PartitionRef>,
    ) -> DaftResult<LogicalPlanRef> {
        // Update the logical plan with the results of the previous stage.
        // This is where the AQE magic happens.
        todo!()
    }

    pub fn run_plan(
        &mut self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_creator: Arc<dyn Fn() -> Box<dyn WorkerManager> + Send + Sync>,
    ) -> PlanResultProducer {
        let (result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let handle = runtime.spawn(Self::plan_loop(
            self.remaining_logical_plan.take(),
            self.config.clone(),
            worker_manager_creator,
            psets,
            result_sender,
        ));
        PlanResultProducer::new(handle, result_receiver)
    }
}

fn can_translate_logical_plan(_plan: &LogicalPlanRef) -> bool {
    todo!()
}

// This is the output of a plan, a receiver to receive the results of the plan.
// And the join handle to the task that runs the plan.
pub struct PlanResultProducer {
    handle: Option<JoinHandle<DaftResult<()>>>,
    rx: Receiver<PartitionRef>,
}

impl PlanResultProducer {
    pub fn new(handle: JoinHandle<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self {
            handle: Some(handle),
            rx,
        }
    }

    pub async fn get_next(&mut self) -> Option<DaftResult<PartitionRef>> {
        self.handle.as_ref()?;
        match self.rx.recv().await {
            Some(result) => Some(Ok(result)),
            None => {
                if let Some(handle) = self.handle.take() {
                    let res = handle
                        .await
                        .map_err(|e| DaftError::InternalError(e.to_string()));
                    match res {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(Err(e)),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            }
        }
    }
}
