use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use daft_plan::ResourceRequest;
use snafu::futures::TryFutureExt;
use snafu::ResultExt;

use crate::executor::{
    resource_manager::{ExecutionResources, ResourceManager},
    Executor,
};
use crate::partition::{virtual_partition::VirtualPartition, PartitionRef};
use crate::task::Task;

use super::local_partition_ref::LocalPartitionRef;

#[derive(Debug)]
pub struct LocalExecutor {
    resource_manager: Arc<Mutex<ResourceManager>>,
}

impl LocalExecutor {
    pub fn new(resource_capacity: ExecutionResources) -> Self {
        let resource_manager = Mutex::new(ResourceManager::new(resource_capacity)).into();
        Self { resource_manager }
    }
}

impl Executor<LocalPartitionRef> for LocalExecutor {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool {
        self.resource_manager
            .lock()
            .unwrap()
            .can_admit(resource_request)
    }

    async fn submit_task(
        &self,
        task: Task<LocalPartitionRef>,
    ) -> DaftResult<(usize, Vec<LocalPartitionRef>)> {
        let task_id = task.task_id();
        let resource_manager = self.resource_manager.clone();
        resource_manager
            .lock()
            .unwrap()
            .admit(task.resource_request());
        let result = tokio::spawn(async move {
            let (send, recv) = tokio::sync::oneshot::channel();
            rayon::spawn(move || {
                let resource_request = task.resource_request().clone();
                let result = task.execute();
                resource_manager.lock().unwrap().release(&resource_request);
                let result = result.and_then(|r| {
                    r.into_iter()
                        .map(LocalPartitionRef::try_new)
                        .collect::<DaftResult<Vec<_>>>()
                });
                let _ = send.send(result);
            });
            recv.await.context(crate::OneShotRecvSnafu {})?
        })
        .context(crate::JoinSnafu {})
        .await??;
        Ok((task_id, result))
    }

    fn current_capacity(&self) -> ExecutionResources {
        self.resource_manager
            .lock()
            .unwrap()
            .current_capacity()
            .clone()
    }

    fn current_utilization(&self) -> ExecutionResources {
        self.resource_manager
            .lock()
            .unwrap()
            .current_utilization()
            .clone()
    }
}

pub struct SerialExecutor {}

impl SerialExecutor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Executor<LocalPartitionRef> for SerialExecutor {
    fn can_admit(&self, _: &ResourceRequest) -> bool {
        true
    }

    async fn submit_task(
        &self,
        task: Task<LocalPartitionRef>,
    ) -> DaftResult<(usize, Vec<LocalPartitionRef>)> {
        let task_id = task.task_id();
        let result = task.execute()?;
        Ok((
            task_id,
            result
                .into_iter()
                .map(LocalPartitionRef::try_new)
                .collect::<DaftResult<Vec<_>>>()?,
        ))
    }

    fn current_capacity(&self) -> ExecutionResources {
        Default::default()
    }

    fn current_utilization(&self) -> ExecutionResources {
        Default::default()
    }
}
