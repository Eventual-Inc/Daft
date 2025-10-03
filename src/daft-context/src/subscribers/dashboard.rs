use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, QueryID, QueryPlan, StatSnapshotView, ops::NodeInfo};
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_io::IOStatsContext;
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use dashmap::DashMap;
use reqwest::{Client, RequestBuilder};

use crate::subscribers::Subscriber;

/// Get the number of seconds from the current timesince the UNIX epoch
fn secs_from_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug)]
pub struct DashboardSubscriber {
    url: String,
    client: Client,
    runtime: RuntimeRef,
    preview_rows: DashMap<QueryID, MicroPartitionRef>,
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

        let runtime = get_io_runtime(false);

        // Validate that we can connect to the dashboard
        runtime.block_on_current_thread(async {
            client
                .get(format!("{}/api/ping", url))
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })?;

        Ok(Some(Self {
            url,
            client,
            runtime,
            preview_rows: DashMap::new(),
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
    fn on_query_start(&self, query_id: QueryID, unoptimized_plan: QueryPlan) -> DaftResult<()> {
        self.runtime.block_on_current_thread(async {
            Self::handle_request(
                self.client
                    .post(format!("{}/engine/query/{}/start", self.url, query_id))
                    .json(&daft_dashboard::engine::StartQueryArgs {
                        start_sec: secs_from_epoch(),
                        unoptimized_plan,
                    }),
            )
            .await?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_result_out(&self, query_id: QueryID, result: MicroPartitionRef) -> DaftResult<()> {
        // Limit to TOTAL_ROWS rows
        // TODO: Limit by X MB and # of rows
        let entry = self.preview_rows.get_mut(&query_id);
        if entry.is_none() {
            let result = result.head(TOTAL_ROWS)?;
            self.preview_rows.insert(query_id, Arc::new(result));
            return Ok(());
        }

        let mut entry = entry.unwrap();
        let all_results = entry.value_mut();
        let num_rows = all_results.len();
        if num_rows < TOTAL_ROWS {
            let result = result.head(TOTAL_ROWS - num_rows)?;
            *all_results = Arc::new(MicroPartition::concat(vec![
                all_results.clone(),
                Arc::new(result),
            ])?);
        }
        Ok(())
    }

    fn on_query_end(&self, query_id: QueryID) -> DaftResult<()> {
        let result = self
            .preview_rows
            .view(&query_id, |_, v| v.clone())
            .unwrap_or_else(|| Arc::new(MicroPartition::empty(None)));

        debug_assert!(result.len() <= TOTAL_ROWS);
        let ipc_buffer = if result.is_empty() {
            vec![]
        } else {
            let result = result
                .concat_or_get(IOStatsContext::new("DashboardSubscriber::on_query_end"))?
                .expect("Results should not be empty");
            result.to_ipc_stream()?
        };

        let end_sec = secs_from_epoch();
        self.runtime.block_on_current_thread(async {
            Self::handle_request(
                self.client
                    .post(format!("{}/engine/query/{}/end", self.url, query_id))
                    .json(&daft_dashboard::engine::FinalizeArgs {
                        end_sec,
                        results: ipc_buffer,
                    }),
            )
            .await?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        self.runtime.block_on_current_thread(async {
            Self::handle_request(
                self.client
                    .post(format!("{}/engine/query/{}/plan_start", self.url, query_id))
                    .json(&daft_dashboard::engine::PlanStartArgs {
                        plan_start_sec: secs_from_epoch(),
                    }),
            )
            .await?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        let plan_end_sec = secs_from_epoch();
        self.runtime.block_on_current_thread(async {
            Self::handle_request(
                self.client
                    .post(format!("{}/engine/query/{}/plan_end", self.url, query_id))
                    .json(&daft_dashboard::engine::PlanEndArgs {
                        plan_end_sec,
                        optimized_plan,
                    }),
            )
            .await?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_exec_start(&self, query_id: QueryID, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        let exec_start_sec = secs_from_epoch();
        self.runtime.block_on_current_thread(async {
            Self::handle_request(
                self.client
                    .post(format!("{}/engine/query/{}/exec/start", self.url, query_id))
                    .json(&daft_dashboard::engine::ExecStartArgs {
                        exec_start_sec,
                        node_infos: node_infos
                            .iter()
                            .map(|info| info.as_ref().clone())
                            .collect(),
                    }),
            )
            .await?;
            Ok::<_, DaftError>(())
        })
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Self::handle_request(self.client.post(format!(
            "{}/engine/query/{}/exec/{}/start",
            self.url, query_id, node_id
        )))
        .await?;
        Ok(())
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        Self::handle_request(
            self.client
                .post(format!(
                    "{}/engine/query/{}/exec/emit_stats",
                    self.url, query_id
                ))
                .json(&daft_dashboard::engine::ExecEmitStatsArgsSend {
                    stats: stats
                        .iter()
                        .map(|(node_id, stats)| {
                            (
                                *node_id,
                                stats
                                    .into_iter()
                                    .map(|(name, stat)| (*name, stat.clone()))
                                    .collect(),
                            )
                        })
                        .collect::<Vec<_>>(),
                }),
        )
        .await?;
        Ok(())
    }

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Self::handle_request(self.client.post(format!(
            "{}/engine/query/{}/exec/{}/end",
            self.url, query_id, node_id
        )))
        .await?;
        Ok(())
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        let exec_end_sec = secs_from_epoch();

        Self::handle_request(
            self.client
                .post(format!("{}/engine/query/{}/exec/end", self.url, query_id))
                .json(&daft_dashboard::engine::ExecEndArgs { exec_end_sec }),
        )
        .await?;
        Ok(())
    }
}
