pub mod local;
#[cfg(feature = "python")]
pub mod ray;
pub mod resource_manager;

use common_error::DaftResult;
use daft_plan::ResourceRequest;

use crate::partition::PartitionRef;
use crate::task::Task;

use resource_manager::ExecutionResources;

pub trait Executor<T: PartitionRef>: Send + Sync {
    fn can_admit(&self, resource_request: &ResourceRequest) -> bool;
    async fn submit_task(&self, task: Task<T>) -> DaftResult<(usize, Vec<T>)>;
    fn current_capacity(&self) -> ExecutionResources;
    fn current_utilization(&self) -> ExecutionResources;
}
