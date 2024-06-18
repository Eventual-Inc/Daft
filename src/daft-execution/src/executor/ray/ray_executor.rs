use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use daft_plan::ResourceRequest;

use crate::{
    executor::{
        resource_manager::{ExecutionResources, ResourceManager},
        Executor,
    },
    task::Task,
};

use super::ray_partition_ref::RayPartitionRef;

#[derive(Debug)]
pub struct RayExecutor {
    resource_manager: Arc<Mutex<ResourceManager>>,
}

impl RayExecutor {
    pub fn new(resource_capacity: ExecutionResources) -> Self {
        let resource_manager = Mutex::new(ResourceManager::new(resource_capacity)).into();
        Self { resource_manager }
    }
}

impl Executor<RayPartitionRef> for RayExecutor {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool {
        self.resource_manager
            .lock()
            .unwrap()
            .can_admit(resource_request)
    }

    async fn submit_task(
        &self,
        task: Task<RayPartitionRef>,
    ) -> DaftResult<(usize, Vec<RayPartitionRef>)> {
        self.resource_manager
            .lock()
            .unwrap()
            .admit(task.resource_request());
        // TODO(Clark): Submit task to Ray cluster.
        todo!()
    }

    fn current_capacity(&self) -> ExecutionResources {
        todo!()
    }

    fn current_utilization(&self) -> ExecutionResources {
        todo!()
    }
}
