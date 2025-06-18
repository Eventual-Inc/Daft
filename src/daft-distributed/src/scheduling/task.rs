use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_resource_request::ResourceRequest;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::worker::WorkerId;
use crate::{
    pipeline_node::{DistributedPipelineNodeContext, MaterializedOutput, NodeID, NodeName},
    plan::PlanID,
    stage::StageID,
    utils::channel::OneshotSender,
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

    pub fn memory_bytes(&self) -> usize {
        self.resource_request.memory_bytes().unwrap_or(0)
    }
}

pub(crate) type TaskID = Arc<str>;
pub(crate) type TaskPriority = usize;

pub(crate) trait Task: Send + Sync + Debug + 'static {
    fn priority(&self) -> TaskPriority;
    fn task_id(&self) -> &TaskID;
    fn resource_request(&self) -> &TaskResourceRequest;
    fn strategy(&self) -> &SchedulingStrategy;
}

#[derive(Debug, Clone)]
pub(crate) struct TaskDetails {
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
            id: task.task_id().clone(),
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
pub(crate) enum SwordfishTaskInput {
    InMemory(Vec<PartitionRef>),
    ScanTasks(Arc<Vec<ScanTaskLikeRef>>, Pushdowns),
}

impl SwordfishTaskInput {
    fn merge_in_memory_inputs_with_plan(
        partitions: Vec<PartitionRef>,
        plan: LocalPhysicalPlanRef,
        node_id: NodeID,
    ) -> LocalPhysicalPlanRef {
        plan.clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(placeholder_scan) => {
                    let info = InMemoryInfo::new(
                        placeholder_scan.schema.clone(),
                        node_id.to_string(),
                        None,
                        partitions.len(),
                        partitions
                            .iter()
                            .filter_map(|p| p.size_bytes().unwrap())
                            .sum::<usize>(),
                        partitions.iter().filter_map(|p| p.num_rows().ok()).sum(),
                        None,
                        None,
                    );
                    let in_memory_source =
                        LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
                    Ok(Transformed::yes(in_memory_source.clone()))
                }
                _ => Ok(Transformed::no(p)),
            })
            .unwrap()
            .data
    }

    fn merge_scan_tasks_with_plan(
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        pushdowns: Pushdowns,
        plan: LocalPhysicalPlanRef,
    ) -> LocalPhysicalPlanRef {
        plan.clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(placeholder_scan) => {
                    let physical_source = LocalPhysicalPlan::physical_scan(
                        scan_tasks.clone(),
                        pushdowns.clone(),
                        placeholder_scan.schema.clone(),
                        StatsState::NotMaterialized,
                    );
                    Ok(Transformed::yes(physical_source.clone()))
                }
                _ => Ok(Transformed::no(p)),
            })
            .unwrap()
            .data
    }

    pub fn merge_plan_with_input(
        self,
        plan: LocalPhysicalPlanRef,
        node_id: NodeID,
    ) -> (LocalPhysicalPlanRef, HashMap<String, Vec<PartitionRef>>) {
        match self {
            SwordfishTaskInput::InMemory(partitions) => {
                let psets = partitions
                    .iter()
                    .map(|p| (node_id.to_string(), vec![p.clone()]))
                    .collect();
                (
                    Self::merge_in_memory_inputs_with_plan(partitions, plan, node_id),
                    psets,
                )
            }
            SwordfishTaskInput::ScanTasks(scan_tasks, pushdowns) => {
                let transformed_plan =
                    Self::merge_scan_tasks_with_plan(scan_tasks.clone(), pushdowns.clone(), plan);
                (transformed_plan, HashMap::new())
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SwordfishTaskContext {
    pub plan_id: PlanID,
    pub stage_id: StageID,
    pub node_id: NodeID,
    pub task_id: TaskID,
    pub node_name: NodeName,
    pub additional_context: HashMap<String, String>,
}

impl SwordfishTaskContext {
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        node_name: NodeName,
        additional_context: HashMap<String, String>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            task_id: Arc::from(Uuid::new_v4().to_string()),
            node_name,
            additional_context,
        }
    }
}

impl From<SwordfishTaskContext> for HashMap<String, String> {
    fn from(context: SwordfishTaskContext) -> Self {
        let mut context_map = HashMap::new();
        context_map.insert("plan_id".to_string(), context.plan_id.to_string());
        context_map.insert("stage_id".to_string(), context.stage_id.to_string());
        context_map.insert("node_id".to_string(), context.node_id.to_string());
        context_map.insert("node_name".to_string(), context.node_name.to_string());
        context_map.extend(context.additional_context);
        context_map
    }
}

impl From<DistributedPipelineNodeContext> for SwordfishTaskContext {
    fn from(context: DistributedPipelineNodeContext) -> Self {
        Self::new(
            context.plan_id,
            context.stage_id,
            context.node_id,
            context.node_name,
            context.additional_context,
        )
    }
}

#[derive(Debug)]
pub(crate) struct SwordfishTask {
    pub plan: LocalPhysicalPlanRef,
    pub resource_request: TaskResourceRequest,
    pub config: Arc<DaftExecutionConfig>,
    pub inputs: SwordfishTaskInput,
    pub strategy: SchedulingStrategy,
    pub context: SwordfishTaskContext,
    pub notify_token: Option<CancellationToken>,
    pub task_priority: TaskPriority,
}

impl SwordfishTask {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        inputs: SwordfishTaskInput,
        strategy: SchedulingStrategy,
        context: SwordfishTaskContext,
    ) -> Self {
        let resource_request = TaskResourceRequest::new(plan.resource_request());
        let priority = context.node_id;
        Self {
            plan,
            resource_request,
            config,
            inputs,
            strategy,
            context,
            notify_token: None,
            task_priority: priority,
        }
    }
}

impl Task for SwordfishTask {
    fn task_id(&self) -> &TaskID {
        &self.context.task_id
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

// TaskResultHandle is a trait for handling the result of a task.
pub(crate) trait TaskResultHandle: Send + Sync {
    // Produce a future that will await the result of the task.
    fn get_result(
        &mut self,
    ) -> impl Future<Output = DaftResult<Vec<MaterializedOutput>>> + Send + 'static;

    // The callback to be called when the task is cancelled.
    fn cancel_callback(&mut self) -> DaftResult<()>;
}

pub(crate) struct TaskResultHandleAwaiter<H: TaskResultHandle> {
    task_id: TaskID,
    worker_id: WorkerId,
    handle: H,
    result_sender: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: CancellationToken,
}

impl<H: TaskResultHandle> TaskResultHandleAwaiter<H> {
    pub fn new(
        task_id: TaskID,
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

    pub fn task_id(&self) -> &TaskID {
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
        task_id: TaskID,
        priority: TaskPriority,
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
        task_id: TaskID,
        priority: TaskPriority,
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
        fn priority(&self) -> TaskPriority {
            self.priority
        }

        fn task_id(&self) -> &TaskID {
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
