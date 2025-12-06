use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use common_metrics::{NodeID, QueryID, QueryPlan, Stat};
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub(crate) enum OperatorStatus {
    Pending,
    Executing,
    Finished,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeInfo {
    pub id: NodeID,
    pub name: String,
    pub node_type: Arc<str>,
    pub node_category: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OperatorInfo {
    pub status: OperatorStatus,
    pub node_info: NodeInfo,
    pub stats: HashMap<String, Stat>,
}

pub(crate) type OperatorInfos = HashMap<NodeID, OperatorInfo>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PlanInfo {
    pub plan_start_sec: u64,
    pub plan_end_sec: u64,
    pub optimized_plan: QueryPlan,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExecInfo {
    pub exec_start_sec: u64,
    pub physical_plan: QueryPlan,
    pub operators: OperatorInfos,
    // TODO: Logs
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status")]
pub(crate) enum QueryStatus {
    Pending {
        start_sec: u64,
    },
    Optimizing {
        plan_start_sec: u64,
    },
    Setup,
    Executing {
        exec_start_sec: u64,
    },
    Finalizing,
    Finished {
        duration_sec: u64,
    },
    Canceled {
        duration_sec: u64,
        message: Option<String>,
    },
    Failed {
        duration_sec: u64,
        message: Option<String>,
    },
    /* TODO(void001): Implement dead state */
    Dead {},
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QuerySummary {
    pub id: QueryID,
    pub start_sec: u64,
    pub status: QueryStatus,
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
#[serde(tag = "status")]
pub(crate) enum QueryState {
    Pending,
    Optimizing {
        plan_start_sec: u64,
    },
    Setup {
        plan_info: PlanInfo,
    },
    Executing {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
    },
    Finalizing {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        exec_end_sec: u64,
    },
    Finished {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        exec_end_sec: u64,
        end_sec: u64,
        #[serde(skip_serializing)]
        results: Option<RecordBatch>,
    },
    Canceled {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        end_sec: u64,
        message: Option<String>,
    },
    Failed {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        end_sec: u64,
        message: Option<String>,
    },
    /* TODO(void001): Implement dead state */
    Dead {},
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueryInfo {
    pub id: QueryID,
    pub start_sec: u64,
    pub unoptimized_plan: QueryPlan,
    pub state: QueryState,
}

impl QueryInfo {
    pub fn status(&self) -> QueryStatus {
        match &self.state {
            QueryState::Pending => QueryStatus::Pending {
                start_sec: self.start_sec,
            },
            QueryState::Optimizing { plan_start_sec } => QueryStatus::Optimizing {
                plan_start_sec: *plan_start_sec,
            },
            // Pending between optimizing and execution
            QueryState::Setup { .. } => QueryStatus::Setup,
            QueryState::Executing { exec_info, .. } => QueryStatus::Executing {
                exec_start_sec: exec_info.exec_start_sec,
            },
            // Finalizing may take longer so just in case
            QueryState::Finalizing { .. } => QueryStatus::Finalizing,
            QueryState::Finished { end_sec, .. } => QueryStatus::Finished {
                duration_sec: end_sec - self.start_sec,
            },
            QueryState::Canceled {
                end_sec, message, ..
            } => QueryStatus::Canceled {
                duration_sec: end_sec - self.start_sec,
                message: message.clone(),
            },
            QueryState::Failed {
                end_sec, message, ..
            } => QueryStatus::Failed {
                duration_sec: end_sec - self.start_sec,
                message: message.clone(),
            },
            QueryState::Dead { .. } => QueryStatus::Dead {},
        }
    }

    pub fn summarize(&self) -> QuerySummary {
        QuerySummary {
            id: self.id.clone(),
            start_sec: self.start_sec,
            status: self.status(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DashboardState {
    // Mapping from query id to query info
    pub queries: DashMap<QueryID, QueryInfo>,
    pub dataframe_previews: DashMap<String, RecordBatch>,
    pub clients: broadcast::Sender<(usize, QuerySummary)>,
    pub query_clients: DashMap<QueryID, (watch::Sender<QueryInfo>, watch::Sender<OperatorInfos>)>,
    pub event_counter: AtomicUsize,
}

impl DashboardState {
    pub fn new() -> Self {
        Self {
            queries: Default::default(),
            dataframe_previews: Default::default(),
            // TODO: Ideally this should never drop events, we need an unbounded broadcast channel
            clients: broadcast::Sender::new(256),
            query_clients: Default::default(),
            event_counter: AtomicUsize::new(0),
        }
    }

    pub fn register_dataframe_preview(&self, record_batch: RecordBatch) -> String {
        let id = Uuid::new_v4().to_string();
        self.dataframe_previews.insert(id.clone(), record_batch);
        id
    }

    pub fn get_dataframe_preview(&self, id: &str) -> Option<RecordBatch> {
        self.dataframe_previews.get(id).map(|r| r.value().clone())
    }

    // -------------------- Updating Queries -------------------- //

    pub fn ping_clients_on_query_update(&self, query_info: &QueryInfo) {
        let id = self.event_counter.fetch_add(1, Ordering::SeqCst);

        if let Some(query_client) = self.query_clients.get(&query_info.id) {
            let _ = query_client.0.send(query_info.clone());
        }

        let _ = self.clients.send((id, query_info.summarize()));
    }

    pub fn ping_clients_on_operator_update(&self, query_info: &QueryInfo) {
        let query_id = &query_info.id;
        if let Some(query_client) = self.query_clients.get(query_id) {
            let QueryState::Executing { exec_info, .. } = &query_info.state else {
                tracing::error!("Query `{}` is not executing", query_id);
                panic!("Query `{}` is not executing", query_id);
            };

            let operator_infos = exec_info.operators.clone();
            let _ = query_client.1.send(operator_infos);
        }
    }
}

// Global shared dashboard state for this process.
pub static GLOBAL_DASHBOARD_STATE: LazyLock<Arc<DashboardState>> =
    LazyLock::new(|| Arc::new(DashboardState::new()));
