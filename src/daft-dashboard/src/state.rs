use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Debug)]
pub enum QueryStatus {
    Running,
    Finished,
    #[allow(dead_code)]
    Failed,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct QueryInfo {
    /// Query ID from Runner
    name: Arc<str>,
    // Plan Details
    unoptimized_plan: Arc<str>,
    optimized_plan: Arc<str>,
    physical_plan: Arc<str>,
}

/// Metadata to return to clients
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryMetadata {
    /// Query Info
    info: Arc<QueryInfo>,
    /// Runner start time
    pub start_time: SystemTime,
    /// Query Status
    pub status: QueryStatus,
    /// Query Duration
    duration: Option<Duration>,
}

/// Actual Query State
struct Query {
    /// Query Info
    info: Arc<QueryInfo>,
    /// Runner start time
    start_time: SystemTime,
    /// Query Status
    duration: Option<Duration>,
    status: QueryStatus,

    // Additional Info
    // TODO: Logs
    // TODO: Metrics
    /// DataFrame Output
    df_output: Option<Arc<RecordBatch>>,
}

impl Query {
    pub fn to_metadata(&self) -> QueryMetadata {
        QueryMetadata {
            info: self.info.clone(),
            start_time: self.start_time,
            status: self.status.clone(),
            duration: self.duration,
        }
    }
}

pub struct AppState {
    queries: DashMap<Arc<str>, Query>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            queries: Default::default(),
        }
    }

    pub fn queries(&self) -> Vec<QueryMetadata> {
        self.queries
            .iter()
            .map(|x| x.value().to_metadata())
            .collect::<Vec<_>>()
    }

    pub fn add_query(&self, query: QueryInfo) {
        self.queries.insert(
            query.name.clone(),
            Query {
                info: Arc::new(query),
                start_time: SystemTime::now(),
                duration: None,
                status: QueryStatus::Running,
                df_output: None,
            },
        );
    }

    pub fn get_query(&self, query_id: Arc<str>) -> Option<QueryMetadata> {
        let query = self.queries.get(&query_id)?;

        let meta = QueryMetadata {
            info: query.info.clone(),
            start_time: SystemTime::now(),
            status: query.status.clone(),
            duration: query.duration,
        };

        Some(meta)
    }

    pub fn complete_query(
        &self,
        query_id: Arc<str>,
        duration: Duration,
        output: RecordBatch,
    ) -> Option<()> {
        let mut query = self.queries.get_mut(&query_id)?;

        query.status = QueryStatus::Finished;
        query.df_output = Some(Arc::new(output));
        query.duration = Some(duration);
        Some(())
    }

    pub fn get_query_dataframe(&self, query_id: Arc<str>) -> Option<Arc<RecordBatch>> {
        let query = self.queries.get(&query_id)?;
        query.df_output.clone()
    }
}
