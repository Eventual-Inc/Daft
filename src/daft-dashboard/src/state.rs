use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use common_metrics::{Stat, ops::NodeInfo};
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub(crate) enum OperatorStatus {
    Pending,
    Executing,
    Finished,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OperatorInfo {
    pub status: OperatorStatus,
    pub node_info: NodeInfo,
    pub stats: HashMap<String, Stat>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PlanInfo {
    pub plan_start_sec: u64,
    pub plan_end_sec: u64,
    pub optimized_plan: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExecInfo {
    pub exec_start_sec: u64,
    // TODO: Replace usize with NodeID
    pub operators: HashMap<usize, OperatorInfo>,
    // TODO: Logs
}

#[derive(Debug, Clone, Serialize)]
pub(crate) enum QueryStatus {
    Pending,
    Planning,
    Executing,
    Finalizing,
    Finished,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) enum QueryState {
    Pending,
    Planning {
        plan_start_sec: u64,
    },
    Planned(PlanInfo),
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
        results: RecordBatch,
    },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueryInfo {
    pub id: Arc<str>,
    pub start_sec: u64,
    pub unoptimized_plan: Arc<str>,
    pub status: QueryState,
}

impl QueryInfo {
    pub fn summarize(&self) -> (QueryStatus, u64) {
        match &self.status {
            QueryState::Pending => (QueryStatus::Pending, self.start_sec),
            QueryState::Planning { plan_start_sec } => (QueryStatus::Planning, *plan_start_sec),
            // Pending between planning and execution
            QueryState::Planned(plan_info) => (QueryStatus::Pending, plan_info.plan_end_sec),
            QueryState::Executing { exec_info, .. } => {
                (QueryStatus::Executing, exec_info.exec_start_sec)
            }
            // Finalizing may take longer so just in case
            QueryState::Finalizing { exec_end_sec, .. } => (QueryStatus::Finalizing, *exec_end_sec),
            // Returns duration in seconds instead of start_sec
            QueryState::Finished { end_sec, .. } => {
                (QueryStatus::Finished, end_sec - self.start_sec)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct DashboardState {
    // Mapping from query id to query info
    pub queries: DashMap<String, QueryInfo>,
    pub dataframe_previews: DashMap<String, RecordBatch>,
}

impl DashboardState {
    pub fn new() -> Self {
        Self {
            queries: Default::default(),
            dataframe_previews: Default::default(),
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
}

// Global shared dashboard state for this process.
pub static GLOBAL_DASHBOARD_STATE: LazyLock<Arc<DashboardState>> =
    LazyLock::new(|| Arc::new(DashboardState::new()));
