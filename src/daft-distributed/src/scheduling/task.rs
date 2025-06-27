use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftError;
use common_partitioning::PartitionRef;
use common_resource_request::ResourceRequest;
use daft_local_plan::LocalPhysicalPlanRef;
use tokio_util::sync::CancellationToken;

use super::worker::WorkerId;
use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext},
    plan::PlanID,
    stage::StageID,
};

#[derive(Debug, Clone)]
pub(crate) struct TaskResourceRequest {
    resource_request: ResourceRequest,
}

impl TaskResourceRequest {
    pub fn new(resource_request: ResourceRequest) -> Self {
        Self { resource_request }
    }

    pub fn num_cpus(&self) -> f64 {
        self.resource_request.num_cpus().unwrap_or(1.0)
    }

    pub fn num_gpus(&self) -> f64 {
        self.resource_request.num_gpus().unwrap_or(0.0)
    }

    #[allow(dead_code)]
    pub fn memory_bytes(&self) -> usize {
        self.resource_request.memory_bytes().unwrap_or(0)
    }
}

pub(crate) type TaskID = u32;
pub(crate) type TaskName = String;
pub(crate) type TaskPriority = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(clippy::struct_field_names)]
pub(crate) struct TaskContext {
    pub plan_id: PlanID,
    pub stage_id: StageID,
    pub node_id: NodeID,
    pub task_id: TaskID,
}

impl TaskContext {
    pub fn new(plan_id: PlanID, stage_id: StageID, node_id: NodeID, task_id: TaskID) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            task_id,
        }
    }
}

impl From<(&PipelineNodeContext, TaskID)> for TaskContext {
    fn from((node_context, task_id): (&PipelineNodeContext, TaskID)) -> Self {
        Self::new(
            node_context.plan_id,
            node_context.stage_id,
            node_context.node_id,
            task_id,
        )
    }
}

pub(crate) trait Task: Send + Sync + Clone + Debug + 'static {
    fn priority(&self) -> TaskPriority;
    fn task_context(&self) -> TaskContext;
    fn resource_request(&self) -> &TaskResourceRequest;
    fn strategy(&self) -> &SchedulingStrategy;
    fn task_id(&self) -> TaskID {
        self.task_context().task_id
    }

    #[allow(dead_code)]
    fn plan_id(&self) -> PlanID {
        self.task_context().plan_id
    }

    #[allow(dead_code)]
    fn stage_id(&self) -> StageID {
        self.task_context().stage_id
    }

    #[allow(dead_code)]
    fn node_id(&self) -> NodeID {
        self.task_context().node_id
    }

    fn task_name(&self) -> TaskName;
}

#[derive(Debug, Clone)]
pub(crate) struct TaskDetails {
    #[allow(dead_code)]
    pub id: TaskID,
    pub resource_request: TaskResourceRequest,
}

impl TaskDetails {
    pub fn num_cpus(&self) -> f64 {
        self.resource_request.num_cpus()
    }

    pub fn num_gpus(&self) -> f64 {
        self.resource_request.num_gpus()
    }
}

impl<T: Task> From<&T> for TaskDetails {
    fn from(task: &T) -> Self {
        Self {
            id: task.task_id(),
            resource_request: task.resource_request().clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SchedulingStrategy {
    Spread,
    // TODO: In the future if we run multiple workers on the same node, we can have a NodeAffinity strategy or a multi-worker affinity strategy
    WorkerAffinity { worker_id: WorkerId, soft: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct SwordfishTask {
    task_context: TaskContext,
    plan: LocalPhysicalPlanRef,
    resource_request: TaskResourceRequest,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
    context: HashMap<String, String>,
    task_priority: TaskPriority,
}

impl SwordfishTask {
    pub fn new(
        task_context: TaskContext,
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        strategy: SchedulingStrategy,
        mut context: HashMap<String, String>,
        task_priority: TaskPriority,
    ) -> Self {
        let resource_request = TaskResourceRequest::new(plan.resource_request());
        context.insert("task_id".to_string(), task_context.task_id.to_string());

        Self {
            task_context,
            plan,
            resource_request,
            config,
            psets,
            strategy,
            context,
            task_priority,
        }
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

    pub fn context(&self) -> &HashMap<String, String> {
        &self.context
    }

    pub fn name(&self) -> String {
        self.plan.single_line_display()
    }

    pub fn task_priority(&self) -> TaskPriority {
        self.task_priority
    }
}

impl Task for SwordfishTask {
    fn task_context(&self) -> TaskContext {
        self.task_context
    }

    fn task_name(&self) -> TaskName {
        self.name().into()
    }

    fn resource_request(&self) -> &TaskResourceRequest {
        &self.resource_request
    }

    fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    fn priority(&self) -> TaskPriority {
        self.task_priority
    }
}

#[derive(Debug)]
pub(crate) enum TaskStatus {
    Success { result: Vec<MaterializedOutput> },
    Failed { error: DaftError },
    Cancelled,
    WorkerDied,
    WorkerUnavailable,
}

pub(crate) trait TaskResultHandle: Send + Sync {
    fn task_context(&self) -> TaskContext;
    fn get_result(&mut self) -> impl Future<Output = TaskStatus> + Send + 'static;
    fn cancel_callback(&mut self);
}

pub(crate) struct TaskResultAwaiter<H: TaskResultHandle> {
    handle: H,
    cancel_token: CancellationToken,
}

impl<H: TaskResultHandle> TaskResultAwaiter<H> {
    pub fn new(handle: H, cancel_token: CancellationToken) -> Self {
        Self {
            handle,
            cancel_token,
        }
    }

    pub async fn await_result(mut self) -> TaskStatus {
        tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
                self.handle.cancel_callback();
                TaskStatus::Cancelled
            }
            result = self.handle.get_result() => {
                result
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{any::Any, time::Duration};

    use common_error::{DaftError, DaftResult};
    use common_partitioning::Partition;
    use parking_lot::Mutex;

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

    #[derive(Debug, Clone)]
    pub struct MockTask {
        task_context: TaskContext,
        task_name: TaskName,
        priority: TaskPriority,
        scheduling_strategy: SchedulingStrategy,
        resource_request: TaskResourceRequest,
        task_result: Vec<MaterializedOutput>,
        cancel_notifier: Arc<Mutex<Option<OneshotSender<()>>>>,
        sleep_duration: Option<std::time::Duration>,
        failure: Option<MockTaskFailure>,
    }

    #[derive(Debug, Clone)]
    pub enum MockTaskFailure {
        Error(String),
        Panic(String),
        WorkerDied,
        WorkerUnavailable,
    }

    /// A builder pattern implementation for creating MockTask instances
    pub struct MockTaskBuilder {
        task_context: TaskContext,
        task_name: TaskName,
        priority: TaskPriority,
        scheduling_strategy: SchedulingStrategy,
        task_result: Vec<MaterializedOutput>,
        resource_request: TaskResourceRequest,
        cancel_notifier: Arc<Mutex<Option<OneshotSender<()>>>>,
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
                task_context: TaskContext::default(),
                task_name: "".into(),
                priority: 0,
                scheduling_strategy: SchedulingStrategy::Spread,
                resource_request: TaskResourceRequest::new(ResourceRequest::default()),
                task_result: vec![MaterializedOutput::new(partition_ref, "".into())],
                cancel_notifier: Arc::new(Mutex::new(None)),
                sleep_duration: None,
                failure: None,
            }
        }

        pub fn with_priority(mut self, priority: TaskPriority) -> Self {
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
        pub fn with_task_id(mut self, task_id: TaskID) -> Self {
            self.task_context.task_id = task_id;
            self
        }

        /// Set a cancel marker
        pub fn with_cancel_notifier(mut self, cancel_notifier: OneshotSender<()>) -> Self {
            self.cancel_notifier = Arc::new(Mutex::new(Some(cancel_notifier)));
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
                task_context: self.task_context,
                task_name: self.task_name,
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
        fn task_context(&self) -> TaskContext {
            self.task_context
        }

        fn priority(&self) -> TaskPriority {
            self.priority
        }

        fn resource_request(&self) -> &TaskResourceRequest {
            &self.resource_request
        }

        fn strategy(&self) -> &SchedulingStrategy {
            &self.scheduling_strategy
        }

        fn task_name(&self) -> TaskName {
            self.task_name.clone()
        }
    }

    /// A mock implementation of the SwordfishTaskResultHandle trait for testing
    pub struct MockTaskResultHandle {
        task: MockTask,
    }

    impl MockTaskResultHandle {
        pub fn new(task: MockTask) -> Self {
            Self { task }
        }
    }

    impl TaskResultHandle for MockTaskResultHandle {
        fn task_context(&self) -> TaskContext {
            self.task.task_context
        }

        fn get_result(&mut self) -> impl Future<Output = TaskStatus> + Send + 'static {
            let task = self.task.clone();

            async move {
                if let Some(sleep_duration) = task.sleep_duration {
                    tokio::time::sleep(sleep_duration).await;
                }
                if let Some(failure) = task.failure {
                    match failure {
                        MockTaskFailure::Error(error_message) => {
                            return TaskStatus::Failed {
                                error: DaftError::InternalError(error_message.clone()),
                            };
                        }
                        MockTaskFailure::Panic(error_message) => {
                            panic!("{}", error_message);
                        }
                        MockTaskFailure::WorkerDied => {
                            return TaskStatus::WorkerDied;
                        }
                        MockTaskFailure::WorkerUnavailable => {
                            return TaskStatus::WorkerUnavailable;
                        }
                    }
                }
                TaskStatus::Success {
                    result: task.task_result,
                }
            }
        }

        fn cancel_callback(&mut self) {
            let mut cancel_notifier = self.task.cancel_notifier.lock();
            if let Some(cancel_notifier) = cancel_notifier.take() {
                let _ = cancel_notifier.send(());
            }
        }
    }
}
