use std::{cmp::Ordering, collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftError;
use common_metrics::StatSnapshot;
use common_partitioning::PartitionRef;
use common_resource_request::ResourceRequest;
use daft_local_plan::LocalPhysicalPlanRef;
use tokio_util::sync::CancellationToken;

use super::worker::WorkerId;
use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext},
    plan::QueryIdx,
};

#[derive(Debug, Clone)]
pub(crate) struct TaskResourceRequest {
    pub resource_request: ResourceRequest,
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

pub(crate) type TaskID = u32;
pub(crate) type TaskName = String;

#[derive(Clone, PartialEq, Eq, Hash, Default)]
#[allow(clippy::struct_field_names)]
pub(crate) struct TaskContext {
    /// The query index that the task belongs to.
    pub query_idx: QueryIdx,
    /// The ID of the last operator / node in the task's pipeline.
    /// This gives us a general indication of what portion of the query this task is related to.
    pub last_node_id: NodeID,
    /// The task ID
    pub task_id: TaskID,
    pub node_ids: Vec<NodeID>,
}

impl TaskContext {
    pub fn new(
        query_idx: QueryIdx,
        node_id: NodeID,
        task_id: TaskID,
        node_ids: Vec<NodeID>,
    ) -> Self {
        Self {
            query_idx,
            last_node_id: node_id,
            task_id,
            node_ids,
        }
    }

    pub fn add_node_id(&mut self, node_id: NodeID) {
        self.node_ids.push(node_id);
    }
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskContext(query_idx = {}, last_node_id = {}, task_id = {})",
            self.query_idx, self.last_node_id, self.task_id
        )
    }
}

impl From<(&PipelineNodeContext, TaskID)> for TaskContext {
    fn from((node_context, task_id): (&PipelineNodeContext, TaskID)) -> Self {
        Self::new(
            node_context.query_idx,
            node_context.node_id,
            task_id,
            vec![node_context.node_id],
        )
    }
}

pub(crate) trait TaskPriority: PartialOrd + PartialEq + Ord + Eq + Copy + Clone {}

pub(crate) trait Task: Send + Sync + Clone + Debug + 'static {
    fn priority(&self) -> impl TaskPriority;
    fn task_context(&self) -> TaskContext;
    fn resource_request(&self) -> &TaskResourceRequest;
    fn strategy(&self) -> &SchedulingStrategy;
    fn task_id(&self) -> TaskID {
        self.task_context().task_id
    }

    #[allow(dead_code)]
    fn query_idx(&self) -> QueryIdx {
        self.task_context().query_idx
    }

    #[allow(dead_code)]
    fn last_node_id(&self) -> NodeID {
        self.task_context().last_node_id
    }

    fn task_name(&self) -> TaskName;
}

#[derive(Clone)]
pub(crate) struct TaskDetails {
    pub name: TaskName,
    pub resource_request: TaskResourceRequest,
}

impl TaskDetails {
    pub fn num_cpus(&self) -> f64 {
        self.resource_request.num_cpus()
    }

    pub fn num_gpus(&self) -> f64 {
        self.resource_request.num_gpus()
    }

    pub fn memory_bytes(&self) -> usize {
        self.resource_request.memory_bytes()
    }
}

impl<T: Task> From<&T> for TaskDetails {
    fn from(task: &T) -> Self {
        Self {
            name: task.task_name(),
            resource_request: task.resource_request().clone(),
        }
    }
}

impl std::fmt::Debug for TaskDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskDetails(name = {}, resource_request = (num_cpus = {}, num_gpus = {}, memory_bytes = {}))",
            self.name,
            self.num_cpus(),
            self.num_gpus(),
            self.memory_bytes()
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SchedulingStrategy {
    Spread,
    // TODO: In the future if we run multiple workers on the same node, we can have a NodeAffinity strategy or a multi-worker affinity strategy
    WorkerAffinity { worker_id: WorkerId, soft: bool },
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy)]
struct SwordfishTaskPriority {
    query_idx: QueryIdx,
    node_id: NodeID,
    task_id: TaskID,
}

impl PartialEq for SwordfishTaskPriority {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
            && self.query_idx == other.query_idx
            && self.node_id == other.node_id
    }
}

impl Eq for SwordfishTaskPriority {}

impl PartialOrd for SwordfishTaskPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SwordfishTaskPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        // Rules for swordfish task priority:
        // 1. Query Idx: Lower query_idx, higher priority
        // 2. Node ID:   Higher node_id, higher priority
        // 3. Task ID:   Lower task_id, higher priority
        other
            .query_idx
            .cmp(&self.query_idx)
            .then_with(|| self.node_id.cmp(&other.node_id))
            .then_with(|| other.task_id.cmp(&self.task_id))
    }
}

impl TaskPriority for SwordfishTaskPriority {}

#[derive(Debug, Clone)]
pub(crate) struct SwordfishTask {
    task_context: TaskContext,
    plan: LocalPhysicalPlanRef,
    resource_request: TaskResourceRequest,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
    context: HashMap<String, String>,
}

impl SwordfishTask {
    pub fn new(
        task_context: TaskContext,
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        strategy: SchedulingStrategy,
        mut context: HashMap<String, String>,
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
}

impl Task for SwordfishTask {
    fn task_context(&self) -> TaskContext {
        self.task_context.clone()
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

    fn priority(&self) -> impl TaskPriority {
        SwordfishTaskPriority {
            query_idx: self.task_context.query_idx,
            node_id: self.task_context.last_node_id,
            task_id: self.task_context.task_id,
        }
    }
}

#[derive(Debug)]
pub(crate) enum TaskStatus {
    Success {
        result: MaterializedOutput,
        stats: Vec<(usize, StatSnapshot)>,
    },
    Failed {
        error: DaftError,
    },
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
    use std::{any::Any, sync::Mutex, time::Duration};

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

        fn size_bytes(&self) -> usize {
            self.size_bytes
        }

        fn num_rows(&self) -> usize {
            self.num_rows
        }
    }

    pub fn create_mock_partition_ref(num_rows: usize, size_bytes: usize) -> PartitionRef {
        Arc::new(MockPartition::new(num_rows, size_bytes))
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MockTaskPriority {
        priority: usize,
    }

    impl TaskPriority for MockTaskPriority {}

    #[derive(Debug, Clone)]
    pub struct MockTask {
        task_context: TaskContext,
        task_name: TaskName,
        priority: MockTaskPriority,
        scheduling_strategy: SchedulingStrategy,
        resource_request: TaskResourceRequest,
        task_result: MaterializedOutput,
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
        priority: MockTaskPriority,
        scheduling_strategy: SchedulingStrategy,
        task_result: MaterializedOutput,
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
                priority: MockTaskPriority { priority: 0 },
                scheduling_strategy: SchedulingStrategy::Spread,
                resource_request: TaskResourceRequest::new(ResourceRequest::default()),
                task_result: MaterializedOutput::new(vec![partition_ref], "".into()),
                cancel_notifier: Arc::new(Mutex::new(None)),
                sleep_duration: None,
                failure: None,
            }
        }

        pub fn with_priority(mut self, priority: usize) -> Self {
            self.priority = MockTaskPriority { priority };
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
            self.task_context.clone()
        }

        fn priority(&self) -> impl TaskPriority {
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
            self.task.task_context.clone()
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
                    stats: vec![],
                }
            }
        }

        fn cancel_callback(&mut self) {
            let mut cancel_notifier = self
                .task
                .cancel_notifier
                .lock()
                .expect("Failed to lock cancel_notifier");
            if let Some(cancel_notifier) = cancel_notifier.take() {
                let _ = cancel_notifier.send(());
            }
        }
    }

    #[test]
    fn test_swordfish_task_priority_ordering() {
        // Test cases for priority ordering:
        // Lower query_idx, higher stage_id, higher node_id, lower task_id should have higher priority

        // Test 1: Different query_idxs (lower query_idx should have higher priority)
        let task1 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        };
        let task2 = SwordfishTaskPriority {
            query_idx: 2,
            node_id: 1,
            task_id: 1,
        };
        assert!(task1 > task2); // query_idx 1 < query_idx 2, so task1 has higher priority (larger in ordering)

        // Test 2: Same query_idx, different task_ids (higher task_id should have higher priority)
        let task1 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 2,
            task_id: 1,
        };
        let task2 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        };
        assert!(task1 > task2); // node_id 2 > node_id 1, so task1 has higher priority (larger in ordering)

        // Test 3: Same query_idx and node_id, different task_ids (lower task_id should have higher priority)
        let task1 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        };
        let task2 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 2,
        };
        assert!(task1 > task2); // task_id 1 < task_id 2, so task1 has higher priority (larger in ordering)

        // Test 4: Complex case with multiple differences
        let task1 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 2,
            task_id: 1,
        };
        let task2 = SwordfishTaskPriority {
            query_idx: 2,
            node_id: 1,
            task_id: 1,
        };
        assert!(task1 > task2); // task1 has lower query_idx, so it has higher priority (larger in ordering)

        // Test 5: Equality
        let task1 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        };
        let task2 = SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        };
        assert_eq!(task1, task2);
    }

    #[test]
    fn test_swordfish_task_priority_binary_heap() {
        use std::collections::BinaryHeap;

        // Test that tasks are correctly ordered in a binary heap
        // Higher priority tasks are now "larger" and come out first
        let mut heap = BinaryHeap::new();

        // Add tasks in random order - ensuring unique task_ids within each stage
        heap.push(SwordfishTaskPriority {
            query_idx: 2,
            node_id: 1,
            task_id: 1,
        });
        heap.push(SwordfishTaskPriority {
            query_idx: 1,
            node_id: 2,
            task_id: 3,
        });
        heap.push(SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 2,
        });
        heap.push(SwordfishTaskPriority {
            query_idx: 1,
            node_id: 1,
            task_id: 1,
        });

        // Pop tasks in order (BinaryHeap is a max heap, so highest priority comes out first)
        // Expected order (highest priority to lowest priority):
        // 1. query_idx=1, stage_id=1, node_id=2, task_id=3 (higher node_id = higher priority = larger in heap)
        // 2. query_idx=1, stage_id=1, node_id=1, task_id=1 (lower task_id = higher priority = larger in heap)
        // 3. query_idx=1, stage_id=1, node_id=1, task_id=2 (higher task_id = lower priority = smaller in heap)
        // 4. query_idx=2, stage_id=1, node_id=1, task_id=1 (higher query_idx = lowest priority = smallest in heap)

        assert_eq!(
            heap.pop().unwrap(),
            SwordfishTaskPriority {
                query_idx: 1,
                node_id: 2,
                task_id: 3
            }
        );
        assert_eq!(
            heap.pop().unwrap(),
            SwordfishTaskPriority {
                query_idx: 1,
                node_id: 1,
                task_id: 1
            }
        );
        assert_eq!(
            heap.pop().unwrap(),
            SwordfishTaskPriority {
                query_idx: 1,
                node_id: 1,
                task_id: 2
            }
        );
        assert_eq!(
            heap.pop().unwrap(),
            SwordfishTaskPriority {
                query_idx: 2,
                node_id: 1,
                task_id: 1
            }
        );
        assert!(heap.pop().is_none()); // Heap should be empty
    }
}
