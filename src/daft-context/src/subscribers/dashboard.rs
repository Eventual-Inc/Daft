use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, QueryID, QueryPlan, Stats};
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use reqwest::{Client, RequestBuilder};
use uuid::Uuid;

use crate::subscribers::{QueryMetadata, QueryResult, Subscriber};

/// Get the number of seconds from the current timesince the UNIX epoch
fn secs_from_epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub struct DashboardSubscriber {
    url: String,
    client: Client,
    runtime: RuntimeRef,
    preview_rows: DashMap<QueryID, MicroPartitionRef>,
    execution_ids: DashMap<QueryID, String>,
    worker_id: Option<String>,
}

impl std::fmt::Debug for DashboardSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DashboardSubscriber")
            .field("url", &self.url)
            .field("client", &self.client)
            .field("runtime", &self.runtime.runtime)
            .field("preview_rows", &self.preview_rows)
            .field("execution_ids", &self.execution_ids)
            .field("worker_id", &self.worker_id)
            .finish()
    }
}

impl DashboardSubscriber {
    pub fn try_new() -> DaftResult<Option<Self>> {
        let Ok(url) = std::env::var("DAFT_DASHBOARD_URL") else {
            return Ok(None);
        };

        const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

        let client = if url.contains("localhost") || url.contains("127.0.0.1") {
            Client::builder()
                // if it's a localhost uri we can skip ssl verification
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        } else {
            // TODO: Auth handling?
            Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        };

        let url_clone = url.clone();
        let client_clone = client.clone();
        let runtime = get_io_runtime(false);

        // Validate that we can connect to the dashboard
        runtime.block_within_async_context(async move {
            client
                .get(format!("{}/api/ping", url))
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })??;

        let worker_id = if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            Some(format!("worker-{}", Uuid::new_v4()))
        } else {
            None
        };

        Ok(Some(Self {
            url: url_clone,
            client: client_clone,
            runtime,
            preview_rows: DashMap::new(),
            execution_ids: DashMap::new(),
            worker_id,
        }))
    }

    async fn handle_request(request: RequestBuilder) -> DaftResult<()> {
        request
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }
}

const TOTAL_ROWS: usize = 10;

#[async_trait]
impl Subscriber for DashboardSubscriber {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }
        let url = self.url.clone();
        let client = self.client.clone();
        let unoptimized_plan = metadata.unoptimized_plan.clone();
        let runner = Some(metadata.runner.clone());
        let ray_dashboard_url = metadata.ray_dashboard_url.clone();
        let entrypoint = metadata.entrypoint.clone();
        let start_sec = secs_from_epoch();
        let qid = query_id.clone();

        self.runtime.block_within_async_context(async move {
            if let Err(e) = Self::handle_request(
                client
                    .post(format!("{}/engine/query/{}/start", url, qid))
                    .json(&daft_dashboard::engine::StartQueryArgs {
                        start_sec,
                        unoptimized_plan,
                        runner,
                        ray_dashboard_url,
                        entrypoint,
                    }),
            )
            .await
            {
                log::error!("Failed to notify query start: {}", e);
            }
        })?;

        self.preview_rows.insert(
            query_id,
            Arc::new(MicroPartition::empty(Some(metadata.output_schema.clone()))),
        );
        Ok(())
    }

    fn on_result_out(&self, query_id: QueryID, result: MicroPartitionRef) -> DaftResult<()> {
        // Limit to TOTAL_ROWS rows
        // TODO: Limit by X MB and # of rows
        let Some(mut entry) = self.preview_rows.get_mut(&query_id) else {
            return Err(DaftError::ValueError(format!(
                "Query `{}` not started or already ended in DashboardSubscriber",
                query_id
            )));
        };

        let all_results = entry.value_mut();
        let num_rows = all_results.len();
        if num_rows < TOTAL_ROWS && !result.is_empty() {
            let result = result.head(TOTAL_ROWS - num_rows)?;
            *all_results = Arc::new(MicroPartition::concat(vec![
                all_results.clone(),
                Arc::new(result),
            ])?);
        }
        Ok(())
    }

    fn on_query_end(&self, query_id: QueryID, end_result: QueryResult) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }
        let results = self.preview_rows.remove(&query_id);
        let results_ipc = if let Some((_, results)) = results {
            debug_assert!(results.len() <= TOTAL_ROWS);
            let result = results
                .concat_or_get()?
                .unwrap_or_else(|| RecordBatch::empty(Some(results.schema())));
            let results_ipc = result.to_ipc_stream()?;
            if results_ipc.len() > 1024 * 1024 * 2 {
                // 2MB, our dashboard cap
                None
            } else {
                Some(results_ipc)
            }
        } else {
            None
        };

        let end_sec = secs_from_epoch();
        let url = self.url.clone();
        let client = self.client.clone();
        let end_state = end_result.end_state;
        let error_message = end_result.error_message;

        self.runtime.block_within_async_context(async move {
            if let Err(e) = Self::handle_request(
                client
                    .post(format!("{}/engine/query/{}/end", url, query_id))
                    .json(&daft_dashboard::engine::FinalizeArgs {
                        end_sec,
                        end_state,
                        error_message,
                        results: results_ipc,
                    }),
            )
            .await
            {
                log::error!("Failed to notify query end: {}", e);
            }
        })?;
        Ok(())
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }
        let url = self.url.clone();
        let client = self.client.clone();
        let plan_start_sec = secs_from_epoch();

        self.runtime.block_within_async_context(async move {
            if let Err(e) = Self::handle_request(
                client
                    .post(format!("{}/engine/query/{}/plan_start", url, query_id))
                    .json(&daft_dashboard::engine::PlanStartArgs { plan_start_sec }),
            )
            .await
            {
                log::error!("Failed to notify optimization start: {}", e);
            }
        })?;
        Ok(())
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }
        let url = self.url.clone();
        let client = self.client.clone();
        let plan_end_sec = secs_from_epoch();

        self.runtime.block_within_async_context(async move {
            if let Err(e) = Self::handle_request(
                client
                    .post(format!("{}/engine/query/{}/plan_end", url, query_id))
                    .json(&daft_dashboard::engine::PlanEndArgs {
                        plan_end_sec,
                        optimized_plan,
                    }),
            )
            .await
            {
                log::error!("Failed to notify optimization end: {}", e);
            }
        })?;
        Ok(())
    }

    fn on_exec_start_with_id(
        &self,
        query_id: QueryID,
        execution_id: &str,
        physical_plan: QueryPlan,
    ) -> DaftResult<()> {
        self.execution_ids
            .insert(query_id.clone(), execution_id.to_string());

        let url = self.url.clone();
        let client = self.client.clone();
        let exec_start_sec = secs_from_epoch();

        self.runtime.block_within_async_context(async move {
            if let Err(e) = Self::handle_request(
                client
                    .post(format!("{}/engine/query/{}/exec/start", url, query_id))
                    .json(&daft_dashboard::engine::ExecStartArgs {
                        exec_start_sec,
                        physical_plan,
                    }),
            )
            .await
            {
                log::error!("Failed to notify exec start: {}", e);
            }
        })?;
        Ok(())
    }

    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()> {
        let execution_id = format!("{}-driver", query_id);
        self.on_exec_start_with_id(query_id, &execution_id, physical_plan)
    }

    async fn on_exec_operator_start(&self, _query_id: QueryID, _node_id: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn on_exec_emit_stats_with_id(
        &self,
        query_id: QueryID,
        execution_id: &str,
        stats: Arc<Vec<(NodeID, Stats)>>,
    ) -> DaftResult<()> {
        Self::handle_request(
            self.client
                .post(format!(
                    "{}/engine/query/{}/exec/emit_stats",
                    self.url, query_id
                ))
                .json(&daft_dashboard::engine::ExecEmitStatsArgsSend {
                    source_id: execution_id.to_string(),
                    stats: stats
                        .iter()
                        .map(|(node_id, snapshot)| {
                            (
                                *node_id,
                                snapshot
                                    .0
                                    .iter()
                                    .map(|(name, stat)| (name.to_string(), stat.clone()))
                                    .collect(),
                            )
                        })
                        .collect(),
                }),
        )
        .await?;
        Ok(())
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: Arc<Vec<(NodeID, Stats)>>,
    ) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        let source_id = if let Some(worker_id) = &self.worker_id {
            worker_id.clone()
        } else {
            self.execution_ids
                .get(&query_id)
                .map(|id| id.clone())
                .unwrap_or_else(|| "unknown".to_string())
        };
        self.on_exec_emit_stats_with_id(query_id, &source_id, stats)
            .await
    }

    async fn on_exec_operator_end(&self, _query_id: QueryID, _node_id: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn on_exec_end_with_id(&self, query_id: QueryID, _execution_id: &str) -> DaftResult<()> {
        self.execution_ids.remove(&query_id);

        let exec_end_sec = secs_from_epoch();
        let url = self.url.clone();
        let client = self.client.clone();

        let _ = Self::handle_request(
            client
                .post(format!("{}/engine/query/{}/exec/end", url, query_id))
                .json(&daft_dashboard::engine::ExecEndArgs { exec_end_sec }),
        )
        .await;
        Ok(())
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        self.on_exec_end_with_id(query_id, "unknown").await
    }
}
