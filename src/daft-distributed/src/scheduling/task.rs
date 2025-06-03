use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_resource_request::ResourceRequest;
use daft_local_plan::LocalPhysicalPlanRef;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::worker::WorkerId;
use crate::{pipeline_node::MaterializedOutput, utils::channel::OneshotSender};

#[derive(Debug, Clone)]
pub(crate) struct TaskResourceRequest {
    resource_request: ResourceRequest,
}

impl TaskResourceRequest {
    pub fn new(resource_request: ResourceRequest) -> Self {
        Self { resource_request }
    }

    pub fn num_cpus(&self) -> usize {
        self.resource_request.num_cpus().unwrap_or(1.0) as usize
    }

    #[allow(dead_code)]
    pub fn num_gpus(&self) -> usize {
        self.resource_request.num_gpus().unwrap_or(0.0) as usize
    }

    #[allow(dead_code)]
    pub fn memory_bytes(&self) -> usize {
        self.resource_request.memory_bytes().unwrap_or(0)
    }
}

pub(crate) type TaskId = Arc<str>;
pub(crate) type TaskPriority = u32;
#[allow(dead_code)]
pub(crate) trait Task: Send + Sync + Debug + 'static {
    fn priority(&self) -> TaskPriority;
    fn task_id(&self) -> &TaskId;
    fn resource_request(&self) -> &TaskResourceRequest;
    fn strategy(&self) -> &SchedulingStrategy;
}

#[derive(Debug, Clone)]
pub(crate) struct TaskDetails {
    #[allow(dead_code)]
    pub id: TaskId,
    pub resource_request: TaskResourceRequest,
}

impl TaskDetails {
    pub fn num_cpus(&self) -> usize {
        self.resource_request.num_cpus()
    }
}

impl<T: Task> From<&T> for TaskDetails {
    fn from(task: &T) -> Self {
        Self {
            id: task.task_id().clone(),
            resource_request: task.resource_request().clone(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum SchedulingStrategy {
    Spread,
    // TODO: In the future if we run multiple workers on the same node, we can have a NodeAffinity strategy or a multi-worker affinity strategy
    WorkerAffinity { worker_id: WorkerId, soft: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct SwordfishTask {
    id: TaskId,
    plan: LocalPhysicalPlanRef,
    resource_request: TaskResourceRequest,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
}

#[allow(dead_code)]
impl SwordfishTask {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        strategy: SchedulingStrategy,
    ) -> Self {
        let task_id = Uuid::new_v4().to_string();
        let resource_request = TaskResourceRequest::new(plan.resource_request());
        Self {
            id: Arc::from(task_id),
            plan,
            resource_request,
            config,
            psets,
            strategy,
        }
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }

    pub fn psets(&self) -> &HashMap<String, Vec<PartitionRef>> {
        &self.psets
    }
}

impl Task for SwordfishTask {
    fn task_id(&self) -> &TaskId {
        &self.id
    }

    fn resource_request(&self) -> &TaskResourceRequest {
        &self.resource_request
    }

    fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    fn priority(&self) -> TaskPriority {
        // Default priority for now, could be enhanced later
        0
    }
}

pub(crate) trait TaskResultHandle: Send + Sync {
    #[allow(dead_code)]
    fn get_result(
        &mut self,
    ) -> impl Future<Output = DaftResult<Vec<MaterializedOutput>>> + Send + 'static;
    fn cancel_callback(&mut self) -> DaftResult<()>;
}

pub(crate) struct TaskResultHandleAwaiter<H: TaskResultHandle> {
    task_id: TaskId,
    worker_id: WorkerId,
    handle: H,
    result_sender: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: CancellationToken,
}

impl<H: TaskResultHandle> TaskResultHandleAwaiter<H> {
    pub fn new(
        task_id: TaskId,
        worker_id: WorkerId,
        handle: H,
        result_sender: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task_id,
            worker_id,
            handle,
            result_sender,
            cancel_token,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub async fn await_result(mut self) {
        tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
                if let Err(e) = self.handle.cancel_callback() {
                    tracing::debug!("Failed to cancel task: {}", e);
                }
            }
            result = self.handle.get_result() => {
                if self.result_sender.send(result).is_err() {
                    tracing::debug!("Task result receiver was dropped before task result could be sent");
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{any::Any, time::Duration};

    use common_error::DaftError;
    use common_partitioning::Partition;

    use super::*;
    use crate::utils::channel::OneshotSender;

    #[derive(Debug)]
    pub struct MockPartition {
        num_rows: usize,
        size_bytes: usize,
    }

    impl MockPartition {
        pub fn new(num_rows: usize, size_bytes: usize) -> Self {
            Self {
                num_rows,
                size_bytes,
            }
        }
    }

    impl Partition for MockPartition {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn size_bytes(&self) -> DaftResult<Option<usize>> {
            Ok(Some(self.size_bytes))
        }

        fn num_rows(&self) -> DaftResult<usize> {
            Ok(self.num_rows)
        }
    }

    pub fn create_mock_partition_ref(num_rows: usize, size_bytes: usize) -> PartitionRef {
        Arc::new(MockPartition::new(num_rows, size_bytes))
    }

    #[derive(Debug)]
    pub struct MockTask {
        task_id: TaskId,
        priority: u32,
        scheduling_strategy: SchedulingStrategy,
        resource_request: TaskResourceRequest,
        task_result: Vec<MaterializedOutput>,
        cancel_notifier: Option<OneshotSender<()>>,
        sleep_duration: Option<std::time::Duration>,
        failure: Option<MockTaskFailure>,
    }

    #[derive(Debug, Clone)]
    pub enum MockTaskFailure {
        Error(String),
        Panic(String),
    }

    /// A builder pattern implementation for creating MockTask instances
    pub struct MockTaskBuilder {
        task_id: TaskId,
        priority: u32,
        scheduling_strategy: SchedulingStrategy,
        task_result: Vec<MaterializedOutput>,
        resource_request: TaskResourceRequest,
        cancel_notifier: Option<OneshotSender<()>>,
        sleep_duration: Option<Duration>,
        failure: Option<MockTaskFailure>,
    }

    impl Default for MockTaskBuilder {
        fn default() -> Self {
            Self::new(create_mock_partition_ref(100, 100))
        }
    }

    impl MockTaskBuilder {
        /// Create a new MockTaskBuilder with required parameters
        pub fn new(partition_ref: PartitionRef) -> Self {
            Self {
                task_id: Arc::from(Uuid::new_v4().to_string()),
                priority: 0,
                scheduling_strategy: SchedulingStrategy::Spread,
                resource_request: TaskResourceRequest::new(ResourceRequest::default()),
                task_result: vec![MaterializedOutput::new(partition_ref, "".into())],
                cancel_notifier: None,
                sleep_duration: None,
                failure: None,
            }
        }

        pub fn with_priority(mut self, priority: u32) -> Self {
            self.priority = priority;
            self
        }

        pub fn with_resource_request(mut self, resource_request: ResourceRequest) -> Self {
            self.resource_request = TaskResourceRequest::new(resource_request);
            self
        }

        pub fn with_scheduling_strategy(mut self, scheduling_strategy: SchedulingStrategy) -> Self {
            self.scheduling_strategy = scheduling_strategy;
            self
        }

        /// Set a custom task ID (defaults to a UUID if not specified)
        pub fn with_task_id(mut self, task_id: TaskId) -> Self {
            self.task_id = task_id;
            self
        }

        /// Set a cancel marker
        pub fn with_cancel_notifier(mut self, cancel_notifier: OneshotSender<()>) -> Self {
            self.cancel_notifier = Some(cancel_notifier);
            self
        }

        /// Set a sleep duration
        pub fn with_sleep_duration(mut self, sleep_duration: Duration) -> Self {
            self.sleep_duration = Some(sleep_duration);
            self
        }

        pub fn with_failure(mut self, failure: MockTaskFailure) -> Self {
            self.failure = Some(failure);
            self
        }

        /// Build the MockTask
        pub fn build(self) -> MockTask {
            MockTask {
                task_id: self.task_id,
                priority: self.priority,
                scheduling_strategy: self.scheduling_strategy,
                resource_request: self.resource_request,
                task_result: self.task_result,
                cancel_notifier: self.cancel_notifier,
                sleep_duration: self.sleep_duration,
                failure: self.failure,
            }
        }
    }

    impl Task for MockTask {
        fn priority(&self) -> u32 {
            self.priority
        }

        fn task_id(&self) -> &TaskId {
            &self.task_id
        }

        fn resource_request(&self) -> &TaskResourceRequest {
            &self.resource_request
        }

        fn strategy(&self) -> &SchedulingStrategy {
            &self.scheduling_strategy
        }
    }

    /// A mock implementation of the SwordfishTaskResultHandle trait for testing
    pub struct MockTaskResultHandle {
        result: Vec<MaterializedOutput>,
        sleep_duration: Option<Duration>,
        cancel_notifier: Option<OneshotSender<()>>,
        failure: Option<MockTaskFailure>,
    }

    impl MockTaskResultHandle {
        pub fn new(task: MockTask) -> Self {
            Self {
                result: task.task_result,
                sleep_duration: task.sleep_duration,
                cancel_notifier: task.cancel_notifier,
                failure: task.failure,
            }
        }
    }

    impl TaskResultHandle for MockTaskResultHandle {
        fn get_result(
            &mut self,
        ) -> impl Future<Output = DaftResult<Vec<MaterializedOutput>>> + Send + 'static {
            let sleep_duration = self.sleep_duration.clone();
            let failure = self.failure.clone();
            let result = self.result.clone();

            async move {
                if let Some(sleep_duration) = sleep_duration {
                    tokio::time::sleep(sleep_duration).await;
                }
                if let Some(failure) = failure {
                    match failure {
                        MockTaskFailure::Error(error_message) => {
                            return Err(DaftError::InternalError(error_message.clone()));
                        }
                        MockTaskFailure::Panic(error_message) => {
                            panic!("{}", error_message);
                        }
                    }
                }
                Ok(result)
            }
        }

        fn cancel_callback(&mut self) -> DaftResult<()> {
            if let Some(cancel_notifier) = self.cancel_notifier.take() {
                cancel_notifier.send(()).unwrap();
            }
            Ok(())
        }
    }
}
