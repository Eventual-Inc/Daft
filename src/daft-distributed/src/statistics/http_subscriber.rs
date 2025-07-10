use std::{collections::HashMap, env, sync::Arc};

use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeTask};
use common_treenode::TreeNode;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::task::TaskContext,
    statistics::{
        PlanState, StatisticsEvent, StatisticsSubscriber, TaskExecutionStatus, TaskState,
    },
};

const HTTP_LOG_TARGET: &str = "DaftHttpSubscriber";

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraph {
    pub version: String,
    pub plan_id: PlanID,
    pub nodes: Vec<QueryGraphNode>,
    pub adjacency_list: HashMap<NodeID, Vec<NodeID>>,
    pub metrics: Option<Vec<MetricDisplayInformation>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraphNode {
    pub id: NodeID,
    pub label: String,
    pub description: String,
    pub metadata: HashMap<String, String>,
    pub status: NodeStatus,
    pub pending: u32,
    pub completed: u32,
    pub canceled: u32,
    pub failed: u32,
    pub total: u32,
    pub metrics: Option<Vec<MetricDisplayInformation>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Created,
    Running,
    Completed,
    Failed,
    Canceled,
}

type QueryID = String;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryPayload {
    pub id: QueryID,
    pub optimized_plan: String,
    pub run_id: Option<String>,
    pub logs: String,
    #[serde(skip)]
    #[allow(dead_code)]
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDisplayInformation {
    pub name: String,
    pub description: String,
    pub value: f64,
    pub unit: String,
}

#[derive(Debug)]
pub struct PlanData {
    pub plan_state: PlanState,
    pub tasks: HashMap<TaskContext, TaskState>,
    pub adjacency_list: HashMap<NodeID, Vec<NodeID>>,
}

impl PlanData {
    pub fn new(plan_state: PlanState) -> Self {
        // Build adjacency list once from the logical plan
        let adjacency_list = Self::build_adjacency_list_from_logical_plan(&plan_state.logical_plan);

        Self {
            plan_state,
            tasks: HashMap::new(),
            adjacency_list,
        }
    }

    fn build_adjacency_list_from_logical_plan(
        logical_plan: &daft_logical_plan::LogicalPlanRef,
    ) -> HashMap<NodeID, Vec<NodeID>> {
        let mut adjacency_list: HashMap<NodeID, Vec<NodeID>> = HashMap::new();

        let _ = logical_plan.apply(|node| {
            let node_id = node
                .node_id()
                .expect("Node ID must be set for optimized logical plan");
            let parent_id = node_id as NodeID;
            let children = node.children();

            let mut child_ids: Vec<NodeID> = Vec::new();
            for child in children {
                let child_node_id = child
                    .node_id()
                    .expect("Node ID must be set for optimized logical plan");
                child_ids.push(child_node_id as NodeID);
            }

            adjacency_list.insert(parent_id, child_ids);

            Ok(common_treenode::TreeNodeRecursion::Continue)
        });

        adjacency_list
    }
}

pub struct HttpSubscriber {
    plan_data: HashMap<PlanID, PlanData>,
    sender: watch::Sender<Arc<QueryPayload>>,
    _task_handle: RuntimeTask<()>,
    flush_sender: mpsc::UnboundedSender<oneshot::Sender<()>>,
    sequence_counter: std::sync::atomic::AtomicU64,
}

impl HttpSubscriber {
    const DEFAULT_DASHBOARD_URL: &str = "http://localhost:3238/api/queries";

    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(Arc::new(QueryPayload::default()));
        let (flush_sender, flush_receiver) = mpsc::unbounded_channel();

        // Spawn long-lived task that handles HTTP requests
        let runtime = get_io_runtime(false);
        let task_handle = runtime.spawn(Self::http_sender_task(receiver, flush_receiver));

        Self {
            plan_data: HashMap::new(),
            sender,
            flush_sender,
            sequence_counter: std::sync::atomic::AtomicU64::new(0),
            _task_handle: task_handle,
        }
    }

    async fn http_sender_task(
        mut receiver: watch::Receiver<Arc<QueryPayload>>,
        mut flush_receiver: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    ) {
        // Create the HTTP client once and reuse it for all requests
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            .build()
            .unwrap();

        // Build headers for the requests
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        // Add the auth token to the headers if it exists
        if let Ok(auth_token) = env::var("DAFT_DASHBOARD_AUTH_TOKEN") {
            let auth_value = format!("Bearer {}", auth_token);
            if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&auth_value) {
                headers.insert(reqwest::header::AUTHORIZATION, header_value);
            }
        }

        // Build the endpoint for the requests
        let endpoint = format!(
            "{}/queries",
            env::var("DAFT_DASHBOARD_URL").unwrap_or_else(|_| Self::DEFAULT_DASHBOARD_URL.into())
        );

        let mut pending_flush_signals: Vec<oneshot::Sender<()>> = Vec::new();

        loop {
            tokio::select! {
                // Handle regular query updates
                result = receiver.changed() => {
                    if result.is_err() {
                        break;
                    }

                    let query_payload = receiver.borrow_and_update().clone();

                    // Process HTTP request for non-empty payloads
                    if !query_payload.id.is_empty() {
                        tracing::info!(target: HTTP_LOG_TARGET, "HttpSubscriber sending request to: {}", endpoint);

                        // Send the HTTP request using the reused client
                        tracing::info!(target: HTTP_LOG_TARGET, "HttpSubscriber executing HTTP POST request");
                        let response = client
                            .post(&endpoint)
                            .headers(headers.clone())
                            .json(&*query_payload)
                            .send()
                            .await;

                        match response {
                            Ok(resp) => {
                                if resp.status().is_success() {
                                    tracing::debug!(target: HTTP_LOG_TARGET, "Successfully sent query information");
                                } else {
                                    tracing::warn!(target: HTTP_LOG_TARGET, "Failed to send query information: {}", resp.status());
                                }
                            }
                            Err(e) => {
                                tracing::warn!(target: HTTP_LOG_TARGET, "Failed to broadcast metrics over {}: {}", endpoint, e);
                            }
                        }
                    }

                    // Always signal completion to any pending flush requests after processing
                    for flush_tx in pending_flush_signals.drain(..) {
                        let _ = flush_tx.send(());
                    }
                }
                // Handle flush requests
                flush_tx = flush_receiver.recv() => {
                    if let Some(flush_tx) = flush_tx {
                        tracing::debug!(target: HTTP_LOG_TARGET, "Flush request received - will signal after next HTTP completion");
                        pending_flush_signals.push(flush_tx);
                    } else {
                        // Channel closed, exit
                        break;
                    }
                }
            }
        }
    }

    pub fn ingest_event(&mut self, event: &StatisticsEvent) {
        match event {
            StatisticsEvent::TaskSubmitted { context, name } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    let task_state = plan_data
                        .tasks
                        .entry(*context)
                        .or_insert_with(|| TaskState {
                            name: name.clone(),
                            status: TaskExecutionStatus::Created,
                            pending: 0,
                            completed: 0,
                            canceled: 0,
                            failed: 0,
                            total: 0,
                        });
                    task_state.total += 1;
                }
                // If plan doesn't exist yet, ignore the task - it will be processed when PlanSubmitted arrives
            }
            StatisticsEvent::TaskScheduled { context } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    if let Some(task_state) = plan_data.tasks.get_mut(context) {
                        task_state.status = TaskExecutionStatus::Running;
                        task_state.pending += 1;
                    }
                }
            }
            StatisticsEvent::TaskCompleted { context } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    if let Some(task_state) = plan_data.tasks.get_mut(context) {
                        task_state.status = TaskExecutionStatus::Completed;
                        if task_state.pending > 0 {
                            task_state.pending -= 1;
                        }
                        task_state.completed += 1;
                    }
                }
            }
            StatisticsEvent::TaskStarted { context } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    if let Some(task_state) = plan_data.tasks.get_mut(context) {
                        task_state.status = TaskExecutionStatus::Running;
                    }
                }
            }
            StatisticsEvent::TaskFailed { context, .. } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    if let Some(task_state) = plan_data.tasks.get_mut(context) {
                        task_state.status = TaskExecutionStatus::Failed;
                        if task_state.pending > 0 {
                            task_state.pending -= 1;
                        }
                        task_state.failed += 1;
                    }
                }
            }
            StatisticsEvent::TaskCancelled { context } => {
                let plan_id = context.plan_id;
                if let Some(plan_data) = self.plan_data.get_mut(&plan_id) {
                    if let Some(task_state) = plan_data.tasks.get_mut(context) {
                        task_state.status = TaskExecutionStatus::Canceled;
                        if task_state.pending > 0 {
                            task_state.pending -= 1;
                        }
                        task_state.canceled += 1;
                    }
                }
            }
            StatisticsEvent::PlanSubmitted {
                plan_id,
                query_id,
                logical_plan,
            } => {
                let plan_state = PlanState {
                    plan_id: *plan_id,
                    query_id: query_id.clone(),
                    logical_plan: logical_plan.clone(),
                };
                self.plan_data.insert(*plan_id, PlanData::new(plan_state));
            }
            StatisticsEvent::PlanStarted { .. } | StatisticsEvent::PlanFinished { .. } => {
                // Plan-level events don't update task state
            }
        }

        // Plan id and data should be populated now
        let plan_id = event.plan_id();
        let plan_data = self
            .plan_data
            .get(&plan_id)
            .expect("Plan ID not found in plan_data");

        // Build the query graph
        let query_graph = Self::build_query_graph(plan_data);
        let sequence = self
            .sequence_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let query_payload = Arc::new(QueryPayload {
            id: plan_data.plan_state.query_id.clone(),
            optimized_plan: serde_json::to_string(&query_graph)
                .unwrap_or_else(|_| "{}".to_string()),
            run_id: env::var("DAFT_DASHBOARD_RUN_ID").ok(),
            logs: String::new(),
            sequence,
        });

        // Send the query payload without flush
        let _ = self.sender.send(query_payload);
    }

    fn flush(&self) -> DaftResult<()> {
        let runtime = get_io_runtime(false);

        // Create a oneshot channel to signal when the flush is complete
        let (flush_tx, flush_rx) = oneshot::channel();

        // Send the flush signal
        if self.flush_sender.send(flush_tx).is_err() {
            return Err(common_error::DaftError::InternalError(
                "Failed to send flush signal to HTTP sender task".to_string(),
            ));
        }

        // Wait for the HTTP request to complete
        runtime.block_within_async_context(async {
            flush_rx.await.map_err(|_| {
                common_error::DaftError::InternalError(
                    "HTTP sender task closed before flush completed".to_string(),
                )
            })
        })?
    }

    pub fn build_query_graph(plan_data: &PlanData) -> QueryGraph {
        // Mapping from logical node ID to QueryGraphNode
        let mut nodes_map: HashMap<NodeID, QueryGraphNode> = HashMap::new();

        for (context, task_state) in &plan_data.tasks {
            let node_id = context
                .logical_node_id
                .expect("Logical node ID must be set for optimized logical plan");

            if let Some(existing_node) = nodes_map.get_mut(&node_id) {
                // Collect task progress per node
                existing_node.pending += task_state.pending;
                existing_node.completed += task_state.completed;
                existing_node.canceled += task_state.canceled;
                existing_node.failed += task_state.failed;
                existing_node.total += task_state.total;

                // Update status - prioritize Failed > Running > Completed > Canceled > Created
                let new_status = Self::convert_task_status(&task_state.status);
                existing_node.status = Self::merge_node_status(&existing_node.status, &new_status);
            } else {
                let node = QueryGraphNode {
                    id: node_id,
                    label: Self::extract_operation_name(&task_state.name),
                    description: task_state.name.clone(),
                    metadata: HashMap::from([
                        ("plan_id".to_string(), context.plan_id.to_string()),
                        ("stage_id".to_string(), context.stage_id.to_string()),
                        ("node_id".to_string(), context.node_id.to_string()),
                    ]),
                    status: Self::convert_task_status(&task_state.status),
                    pending: task_state.pending,
                    completed: task_state.completed,
                    canceled: task_state.canceled,
                    failed: task_state.failed,
                    total: task_state.total,
                    metrics: None,
                };
                nodes_map.insert(node_id, node);
            }
        }

        QueryGraph {
            version: "1.0.0".to_string(),
            plan_id: plan_data.plan_state.plan_id,
            nodes: nodes_map.into_values().collect(),
            adjacency_list: plan_data.adjacency_list.clone(),
            metrics: None,
        }
    }

    fn extract_operation_name(task_name: &str) -> String {
        if let Some(arrow_pos) = task_name.find("->") {
            task_name[..arrow_pos].to_string()
        } else {
            task_name.to_string()
        }
    }

    fn convert_task_status(status: &TaskExecutionStatus) -> NodeStatus {
        match status {
            TaskExecutionStatus::Created => NodeStatus::Created,
            TaskExecutionStatus::Running => NodeStatus::Running,
            TaskExecutionStatus::Completed => NodeStatus::Completed,
            TaskExecutionStatus::Failed => NodeStatus::Failed,
            TaskExecutionStatus::Canceled => NodeStatus::Canceled,
        }
    }

    fn merge_node_status(current: &NodeStatus, new: &NodeStatus) -> NodeStatus {
        // Priority: Failed > Running > Completed > Canceled > Created
        match (current, new) {
            (NodeStatus::Failed, _) | (_, NodeStatus::Failed) => NodeStatus::Failed,
            (NodeStatus::Running, _) | (_, NodeStatus::Running) => NodeStatus::Running,
            (NodeStatus::Completed, _) | (_, NodeStatus::Completed) => NodeStatus::Completed,
            (NodeStatus::Canceled, _) | (_, NodeStatus::Canceled) => NodeStatus::Canceled,
            _ => NodeStatus::Created,
        }
    }
}

impl StatisticsSubscriber for HttpSubscriber {
    fn handle_event(&mut self, event: &StatisticsEvent) -> DaftResult<()> {
        self.ingest_event(event);

        // Only flush HTTP requests on plan completion
        if let StatisticsEvent::PlanFinished { .. } = event {
            if let Err(e) = self.flush() {
                tracing::warn!(target: HTTP_LOG_TARGET, "Failed to flush pending HTTP work: {}", e);
            }
        }

        Ok(())
    }
}
