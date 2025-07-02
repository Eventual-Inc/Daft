use std::collections::HashMap;

use common_error::DaftResult;
use serde::{Deserialize, Serialize};

use super::{PlanState, StatisticsEvent, StatisticsSubscriber, TaskState};
use crate::scheduling::task::TaskContext;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraph {
    pub version: String,
    pub query_id: u32,
    pub nodes: Vec<QueryGraphNode>,
    pub adjacency_list: HashMap<String, Vec<String>>,
    pub metrics: Option<Vec<MetricDisplayInformation>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGraphNode {
    pub id: u32,
    pub label: String,
    pub description: String,
    pub metadata: HashMap<String, String>,
    pub status: NodeStatus,
    pub pending: Option<u32>,
    pub completed: Option<u32>,
    pub canceled: Option<u32>,
    pub failed: Option<u32>,
    pub total: Option<u32>,
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
        let mut nodes = Vec::new();

        // Create nodes from task state
        for (context, task_state) in tasks {
            let node_id = Self::generate_node_id(context);

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
                pending: Some(task_state.pending),
                completed: Some(task_state.completed),
                canceled: Some(task_state.canceled),
                failed: Some(task_state.failed),
                total: Some(task_state.total),
                metrics: None,
            };
            nodes.push(node);
        }

        // For now, use the first plan's ID if available, otherwise use 0
        let query_id = plans.keys().next().copied().unwrap_or(0);

        QueryGraph {
            version: "1.0.0".to_string(),
            query_id,
            nodes,
            adjacency_list: HashMap::new(), // TODO: Build from plan structure
            metrics: None,
        }
    }

    fn generate_node_id(context: &TaskContext) -> u32 {
        ((context.plan_id as u32) << 16) | ((context.stage_id as u32) << 8) | (context.node_id)
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
}

impl StatisticsSubscriber for HttpSubscriber {
    fn handle_event(
        &self,
        event: &StatisticsEvent,
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
