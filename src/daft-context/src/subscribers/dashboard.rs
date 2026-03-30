use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, QueryID, QueryPlan, Stats};
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use reqwest::{Client, RequestBuilder};
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::subscribers::{QueryMetadata, QueryResult, Subscriber};

const TOTAL_ROWS: usize = 10;
const DASHBOARD_EVENT_LIMIT: usize = 512;
const DASHBOARD_SHUTDOWN_TIMEOUT_MS: u64 = 500;

/// Get the number of seconds from the current time since the UNIX epoch
fn secs_from_epoch() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

#[derive(Debug)]
struct DashboardEvent {
    path: String,
    context: &'static str,
    body: Option<Vec<u8>>,
}

pub struct DashboardSubscriber {
    url: String,
    client: Client,
    runtime: RuntimeRef,
    preview_rows: DashMap<QueryID, MicroPartitionRef>,
    execution_ids: DashMap<QueryID, String>,
    worker_id: Option<String>,

    dashboard_tx: Option<mpsc::Sender<DashboardEvent>>,
    dashboard_worker: Option<JoinHandle<()>>,
    dropped_events: AtomicU64,
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
            .field("dashboard_tx", &self.dashboard_tx.is_some())
            .field("dashboard_worker", &self.dashboard_worker.is_some())
            .field(
                "dropped_events",
                &self
                    .dropped_events
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
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
                .connect_timeout(std::time::Duration::from_secs(3))
                .timeout(std::time::Duration::from_secs(5))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        } else {
            // TODO: Auth handling?
            Client::builder()
                .connect_timeout(std::time::Duration::from_secs(3))
                .timeout(std::time::Duration::from_secs(5))
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| DaftError::External(Box::new(e)))?
        };

        let runtime = get_io_runtime(false);

        let (dashboard_tx, dashboard_rx) = mpsc::channel(DASHBOARD_EVENT_LIMIT);
        let worker_url = url.clone();
        let worker_client = client.clone();
        let dashboard_worker = runtime.runtime.handle().spawn(async move {
            Self::dashboard_worker_loop(worker_url, worker_client, dashboard_rx).await;
        });

        // Validate that we can connect to the dashboard
        // We log a warning if we can't connect, but we still return the subscriber
        // because the dashboard might come up later.
        let ping_url = format!("{}/api/ping", url);
        let ping_client = client.clone();
        if let Err(e) = runtime.block_within_async_context(async move {
            ping_client
                .get(ping_url)
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        }) {
            log::warn!("Failed to connect to dashboard at {}: {}", url, e);
        }

        let worker_id = if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            Some(format!("worker-{}", Uuid::new_v4()))
        } else {
            None
        };

        Ok(Some(Self {
            url,
            client,
            runtime,
            preview_rows: DashMap::new(),
            execution_ids: DashMap::new(),
            worker_id,
            dashboard_tx: Some(dashboard_tx),
            dashboard_worker: Some(dashboard_worker),
            dropped_events: AtomicU64::new(0),
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

    fn try_enqueue(&self, event: DashboardEvent) {
        let Some(tx) = &self.dashboard_tx else {
            return; // shutdown in progress
        };

        if tx.try_send(event).is_err() {
            let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_multiple_of(100) {
                log::warn!("Dropped {dropped} dashboard events because queue is full");
            }
        }
    }

    fn enqueue_json<T: serde::Serialize>(&self, path: String, context: &'static str, payload: &T) {
        match serde_json::to_vec(payload) {
            Ok(body) => self.try_enqueue(DashboardEvent {
                path,
                context,
                body: Some(body),
            }),
            Err(e) => log::error!("Failed to serialize dashboard payload for {context}: {e}"),
        }
    }

    fn enqueue_no_body(&self, path: String, context: &'static str) {
        self.try_enqueue(DashboardEvent {
            path,
            context,
            body: None,
        });
    }

    async fn dashboard_worker_loop(
        url: String,
        client: Client,
        mut rx: mpsc::Receiver<DashboardEvent>,
    ) {
        while let Some(event) = rx.recv().await {
            let mut req = client.post(format!("{}/{}", url, event.path));
            if let Some(body) = event.body {
                req = req
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body);
            }

            if let Err(e) = Self::handle_request(req).await {
                log::error!("Failed to notify {}: {}", event.context, e);
            }
        }
    }
}

impl Drop for DashboardSubscriber {
    fn drop(&mut self) {
        // Close channel so worker drains buffered events and exits.
        let _ = self.dashboard_tx.take();

        // Wait up to timeout for clean drain; then force abort.
        if let Some(mut worker) = self.dashboard_worker.take() {
            let shutdown = self.runtime.block_within_async_context(async move {
                if tokio::time::timeout(
                    std::time::Duration::from_millis(DASHBOARD_SHUTDOWN_TIMEOUT_MS),
                    &mut worker,
                )
                .await
                .is_err()
                {
                    log::warn!("Dashboard worker did not drain in time, aborting");
                    worker.abort();
                }
            });

            if let Err(e) = shutdown {
                log::warn!("Dashboard worker shutdown encountered an error: {e}");
            }
        }
    }
}

#[async_trait]
impl Subscriber for DashboardSubscriber {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.preview_rows.insert(
            query_id.clone(),
            Arc::new(MicroPartition::empty(Some(metadata.output_schema.clone()))),
        );

        self.enqueue_json(
            format!("engine/query/{}/start", query_id),
            "query_start",
            &daft_dashboard::engine::StartQueryArgs {
                start_sec: secs_from_epoch(),
                unoptimized_plan: metadata.unoptimized_plan.clone(),
                runner: Some(metadata.runner.clone()),
                ray_dashboard_url: metadata.ray_dashboard_url.clone(),
                entrypoint: metadata.entrypoint.clone(),
            },
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

        self.enqueue_json(
            format!("engine/query/{}/end", query_id),
            "query_end",
            &daft_dashboard::engine::FinalizeArgs {
                end_sec: secs_from_epoch(),
                end_state: end_result.end_state,
                error_message: end_result.error_message,
                results: results_ipc,
            },
        );
        Ok(())
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/plan_start", query_id),
            "optimization_start",
            &daft_dashboard::engine::PlanStartArgs {
                plan_start_sec: secs_from_epoch(),
            },
        );
        Ok(())
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.enqueue_json(
            format!("engine/query/{}/plan_end", query_id),
            "optimization_end",
            &daft_dashboard::engine::PlanEndArgs {
                plan_end_sec: secs_from_epoch(),
                optimized_plan,
            },
        );
        Ok(())
    }

    fn on_exec_start_with_id(
        &self,
        query_id: QueryID,
        execution_id: &str,
        physical_plan: QueryPlan,
    ) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.execution_ids
            .insert(query_id.clone(), execution_id.to_string());

        self.enqueue_json(
            format!("engine/query/{}/exec/start", query_id),
            "exec_start",
            &daft_dashboard::engine::ExecStartArgs {
                exec_start_sec: secs_from_epoch(),
                physical_plan,
            },
        );
        Ok(())
    }

    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()> {
        self.on_exec_start_with_id(query_id.clone(), &query_id, physical_plan)
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.enqueue_no_body(
            format!("engine/query/{}/exec/{}/start", query_id, node_id),
            "exec_operator_start",
        );
        Ok(())
    }

    async fn on_exec_emit_stats_with_id(
        &self,
        query_id: QueryID,
        execution_id: &str,
        stats: Arc<Vec<(NodeID, Stats)>>,
    ) -> DaftResult<()> {
        self.enqueue_json(
            format!("engine/query/{}/exec/emit_stats", query_id),
            "exec_emit_stats",
            &daft_dashboard::engine::ExecEmitStatsArgsSend {
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
            },
        );
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

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.enqueue_no_body(
            format!("engine/query/{}/exec/{}/end", query_id, node_id),
            "exec_operator_end",
        );
        Ok(())
    }

    async fn on_exec_end_with_id(&self, query_id: QueryID, _execution_id: &str) -> DaftResult<()> {
        if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            return Ok(());
        }

        self.execution_ids.remove(&query_id);

        self.enqueue_json(
            format!("engine/query/{}/exec/end", query_id),
            "exec_end",
            &daft_dashboard::engine::ExecEndArgs {
                exec_end_sec: secs_from_epoch(),
            },
        );
        Ok(())
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        self.on_exec_end_with_id(query_id, "unknown").await
    }
}
