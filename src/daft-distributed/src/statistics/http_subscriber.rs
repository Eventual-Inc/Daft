use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeTask};
use common_treenode::TreeNode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{PlanState, StatisticsEvent, StatisticsSubscriber, TaskState};
use crate::scheduling::task::TaskContext;

const HTTP_LOG_TARGET: &str = "DaftHttpSubscriber";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraph {
    pub version: String,
    pub query_id: u32,
    pub nodes: Vec<QueryGraphNode>,
    pub adjacency_list: HashMap<usize, Vec<usize>>,
    pub metrics: Option<Vec<MetricDisplayInformation>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraphNode {
    pub id: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPayload {
    pub id: String,
    pub optimized_plan: String,
    pub run_id: Option<String>,
    pub logs: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDisplayInformation {
    pub name: String,
    pub description: String,
    pub value: f64,
    pub unit: String,
}

pub struct HttpSubscriber {
    pending_requests: Arc<Mutex<Vec<RuntimeTask<()>>>>,
    plans: Mutex<HashMap<u32, PlanState>>,
    tasks: Mutex<HashMap<TaskContext, TaskState>>,
}

impl HttpSubscriber {
    pub fn new() -> Self {
        Self {
            pending_requests: Arc::new(Mutex::new(Vec::new())),
            plans: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn ingest_event(&self, event: StatisticsEvent) {
        let mut plans = self.plans.lock().unwrap();
        let mut tasks = self.tasks.lock().unwrap();

        match event {
            StatisticsEvent::TaskSubmitted { context, name } => {
                let task_state = tasks.entry(context).or_insert_with(|| TaskState {
                    name: name.clone(),
                    status: super::TaskExecutionStatus::Created,
                    pending: 0,
                    completed: 0,
                    canceled: 0,
                    failed: 0,
                    total: 0,
                });
                task_state.total += 1;
            }
            StatisticsEvent::ScheduledTask { context } => {
                if let Some(task_state) = tasks.get_mut(&context) {
                    task_state.status = super::TaskExecutionStatus::Running;
                    task_state.pending += 1;
                }
            }
            StatisticsEvent::TaskCompleted { context } => {
                if let Some(task_state) = tasks.get_mut(&context) {
                    task_state.status = super::TaskExecutionStatus::Completed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.completed += 1;
                }
            }
            StatisticsEvent::TaskStarted { context } => {
                if let Some(task_state) = tasks.get_mut(&context) {
                    task_state.status = super::TaskExecutionStatus::Running;
                }
            }
            StatisticsEvent::TaskFailed { context, .. } => {
                if let Some(task_state) = tasks.get_mut(&context) {
                    task_state.status = super::TaskExecutionStatus::Failed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.failed += 1;
                }
            }
            StatisticsEvent::TaskCancelled { context } => {
                if let Some(task_state) = tasks.get_mut(&context) {
                    task_state.status = super::TaskExecutionStatus::Canceled;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.canceled += 1;
                }
            }
            StatisticsEvent::PlanSubmitted {
                plan_id,
                query_id,
                logical_plan,
            } => {
                plans.insert(
                    plan_id,
                    PlanState {
                        plan_id: plan_id as usize,
                        query_id,
                        logical_plan,
                    },
                );
            }
            StatisticsEvent::PlanStarted { .. } | StatisticsEvent::PlanFinished { .. } => {
                // Plan-level events don't update task state
            }
        }

        // Clone for reporting to avoid holding the locks during the HTTP request
        let plans_clone = plans.clone();
        let tasks_clone = tasks.clone();
        drop(plans);
        drop(tasks);

        if let Err(e) = self.report_state(&plans_clone, &tasks_clone) {
            tracing::warn!("Failed to report state: {}", e);
        }
    }

    pub fn report_state(
        &self,
        plans: &HashMap<u32, PlanState>,
        tasks: &HashMap<TaskContext, TaskState>,
    ) -> DaftResult<()> {
        let query_graph = self.build_query_graph(plans, tasks);
        let endpoint = format!(
            "{}/queries",
            env::var("DAFT_DASHBOARD_URL")
                .unwrap_or_else(|_| "http://localhost:3238/api/queries".into())
        );

        tracing::info!(target: HTTP_LOG_TARGET, "HttpSubscriber sending request to: {}", endpoint);

        // Build headers
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        if let Ok(auth_token) = env::var("DAFT_DASHBOARD_AUTH_TOKEN") {
            let auth_value = format!("Bearer {}", auth_token);
            if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&auth_value) {
                headers.insert(reqwest::header::AUTHORIZATION, header_value);
            }
        }

        let optimized_plan =
            serde_json::to_string(&query_graph).unwrap_or_else(|_| "{}".to_string());

        // Extract query ID from the first plan state, or generate a new UUID if no plans exist
        let query_id = plans
            .values()
            .next()
            .map(|plan| plan.query_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let payload = QueryPayload {
            id: query_id,
            optimized_plan,
            run_id: env::var("DAFT_DASHBOARD_RUN_ID").ok(),
            logs: String::new(),
        };

        // Send the HTTP request asynchronously and store the handle
        let runtime = get_io_runtime(false);
        tracing::info!(
            target: HTTP_LOG_TARGET,
            "HttpSubscriber spawning HTTP request task for endpoint: {}",
            endpoint
        );

        let task_handle = runtime.spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(1))
                .default_headers(headers)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());

            tracing::info!(target: HTTP_LOG_TARGET, "HttpSubscriber executing HTTP POST request");
            let response = client.post(&endpoint).json(&payload).send().await;

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
        });

        // Store the task handle for later cleanup/flushing
        if let Ok(mut pending) = self.pending_requests.lock() {
            pending.push(task_handle);
        }

        Ok(())
    }

    /// Exposed for tests only
    pub fn build_query_graph(
        &self,
        plans: &HashMap<u32, PlanState>,
        tasks: &HashMap<TaskContext, TaskState>,
    ) -> QueryGraph {
        // Mapping from query graph node ID to QueryGraphNode
        let mut nodes_map: HashMap<u32, QueryGraphNode> = HashMap::new();

        // Mapping from logical plan node ID to query graph node IDs
        let mut logical_to_query_map: HashMap<usize, usize> = HashMap::new();

        for (context, task_state) in tasks {
            let node_id = Self::generate_node_id(context);

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

                logical_to_query_map.insert(context.logical_node_id as usize, node_id as usize);
                nodes_map.insert(node_id, node);
            }
        }

        // For now, use the first plan's ID if available, otherwise use 0
        let query_id = plans.keys().next().copied().unwrap_or(0);

        // Build adjacency list from logical plan structure
        let adjacency_list: HashMap<usize, Vec<usize>> =
            self.build_adjacency_list(plans, &logical_to_query_map);

        QueryGraph {
            version: "1.0.0".to_string(),
            query_id,
            nodes: nodes_map.into_values().collect(),
            adjacency_list,
            metrics: None,
        }
    }

    pub fn generate_node_id(context: &TaskContext) -> u32 {
        ((context.plan_id as u32) << 16) | ((context.stage_id as u32) << 8) | context.node_id
    }

    fn extract_operation_name(task_name: &str) -> String {
        if let Some(arrow_pos) = task_name.find("->") {
            task_name[..arrow_pos].to_string()
        } else {
            task_name.to_string()
        }
    }

    fn convert_task_status(status: &super::TaskExecutionStatus) -> NodeStatus {
        match status {
            super::TaskExecutionStatus::Created => NodeStatus::Created,
            super::TaskExecutionStatus::Running => NodeStatus::Running,
            super::TaskExecutionStatus::Completed => NodeStatus::Completed,
            super::TaskExecutionStatus::Failed => NodeStatus::Failed,
            super::TaskExecutionStatus::Canceled => NodeStatus::Canceled,
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

    fn build_adjacency_list(
        &self,
        plans: &HashMap<u32, PlanState>,
        logical_to_query_map: &HashMap<usize, usize>,
    ) -> HashMap<usize, Vec<usize>> {
        let mut adjacency_list: HashMap<usize, Vec<usize>> = HashMap::new();

        if let Some((_, plan_state)) = plans.iter().next() {
            // Build logical plan adjacency list first
            let mut logical_adjacency: HashMap<usize, Vec<usize>> = HashMap::new();
            self.build_logical_plan_adjacency_list(
                &plan_state.logical_plan,
                &mut logical_adjacency,
            );

            // Convert logical adjacency to query graph adjacency
            for (logical_parent_id, logical_children) in logical_adjacency {
                if let Some(query_parent_id) = logical_to_query_map.get(&logical_parent_id) {
                    let mut query_graph_children = Vec::new();
                    for logical_child_id in &logical_children {
                        if let Some(query_child_node) = logical_to_query_map.get(logical_child_id) {
                            query_graph_children.push(*query_child_node);
                        }
                    }
                    adjacency_list.insert(*query_parent_id, query_graph_children);
                }
            }
        }

        adjacency_list
    }

    fn build_logical_plan_adjacency_list(
        &self,
        plan: &daft_logical_plan::LogicalPlan,
        adjacency: &mut HashMap<usize, Vec<usize>>,
    ) {
        let plan_arc: std::sync::Arc<daft_logical_plan::LogicalPlan> =
            std::sync::Arc::new(plan.clone());

        let _ = plan_arc.apply(|node| {
            if let Some(node_id) = node.node_id() {
                let parent_id = *node_id;
                let children = node.children();

                let mut child_ids: Vec<usize> = Vec::new();
                for child in children {
                    if let Some(child_node_id) = child.node_id() {
                        child_ids.push(*child_node_id);
                    }
                }

                adjacency.insert(parent_id, child_ids);
            }

            Ok(common_treenode::TreeNodeRecursion::Continue)
        });
    }

    pub async fn flush_pending_requests(&self) -> DaftResult<()> {
        let mut handles = {
            let mut pending = self.pending_requests.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        tracing::info!(
            target: HTTP_LOG_TARGET,
            "Flushing {} pending HTTP requests",
            handles.len()
        );

        // Wait for all handles to complete
        for handle in handles.drain(..) {
            if let Err(e) = handle.await {
                tracing::warn!(
                    target: HTTP_LOG_TARGET,
                    "Error waiting for HTTP request to complete: {}",
                    e
                );
            }
        }

        tracing::info!(target: HTTP_LOG_TARGET, "All pending HTTP requests completed");
        Ok(())
    }
}

impl StatisticsSubscriber for HttpSubscriber {
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()> {
        self.ingest_event(event.clone());
        Ok(())
    }

    fn flush(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>>>
    {
        Some(Box::pin(self.flush_pending_requests()))
    }
}
