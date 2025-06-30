use std::collections::HashMap;

use common_error::DaftResult;
use common_treenode::TreeNode;
use serde::{Deserialize, Serialize};

use super::{PlanState, StatisticsEvent, StatisticsSubscriber, TaskState};
use crate::scheduling::task::TaskContext;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Created,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDisplayInformation {
    pub name: String,
    pub description: String,
    pub value: f64,
    pub unit: String,
}

pub struct HttpSubscriber {
    endpoint: String,
    client: reqwest::Client,
}

impl HttpSubscriber {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new(),
        }
    }

    fn build_query_graph(
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

                logical_to_query_map.insert(context.node_id as usize, node_id as usize);
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

    fn generate_node_id(context: &TaskContext) -> u32 {
        ((context.plan_id as u32) << 16)
            | ((context.stage_id as u32) << 8)
            | context.node_id
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
            if let Some(plan_id) = node.plan_id() {
                let parent_id = *plan_id;
                let children = node.children();

                let mut child_ids: Vec<usize> = Vec::new();
                for child in children {
                    if let Some(child_plan_id) = child.plan_id() {
                        child_ids.push(*child_plan_id);
                    }
                }

                if !child_ids.is_empty() {
                    adjacency.insert(parent_id, child_ids);
                }
            }

            Ok(common_treenode::TreeNodeRecursion::Continue)
        });
    }
}

impl StatisticsSubscriber for HttpSubscriber {
    fn handle_event(
        &self,
        _event: &StatisticsEvent,
        plans: &HashMap<u32, PlanState>,
        tasks: &HashMap<TaskContext, TaskState>,
    ) -> DaftResult<()> {
        let query_graph = self.build_query_graph(plans, tasks);

        // Send update asynchronously (fire and forget)
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        tokio::spawn(async move {
            let response = client.post(&endpoint).json(&query_graph).send().await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        tracing::debug!("Successfully sent query graph update");
                    } else {
                        tracing::warn!("Failed to send query graph update: {}", resp.status());
                    }
                }
                Err(e) => {
                    tracing::error!("Error sending query graph update: {}", e);
                }
            }
        });

        Ok(())
    }
}
