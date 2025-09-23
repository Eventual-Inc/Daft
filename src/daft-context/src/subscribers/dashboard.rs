use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, StatSnapshotView, ops::NodeInfo};
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_core::prelude::DataType;
use daft_io::IOStatsContext;
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use reqwest::Client;

use crate::subscribers::QuerySubscriber;

fn now() -> u64 {
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
                .unwrap()
        } else {
            // TODO: Auth handling?
            Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .build()
                .unwrap()
        };

        let runtime = get_io_runtime(false);

        // Validate that we can connect to the dashboard
        runtime.block_on_current_thread(async {
            client
                .get(format!("{}/ping", url))
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
        }))
    }
}

#[async_trait]
impl QuerySubscriber for DashboardSubscriber {
    fn on_query_start(&self, query_id: String, unoptimized_plan: String) -> DaftResult<()> {
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/start", self.url, query_id))
                .json(&daft_dashboard::api::StartQueryArgs {
                    start_sec: now(),
                    unoptimized_plan: unoptimized_plan.into(),
                })
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;

            Ok::<_, DaftError>(())
        })
    }

    fn on_query_end(&self, query_id: String, results: Vec<MicroPartitionRef>) -> DaftResult<()> {
        // Limit the results to max 10 rows
        // TODO: Limit by X MB and # of rows

        let (ipc_buffer, num_rows, total_bytes) = if results.is_empty() {
            (vec![], 0, 0)
        } else {
            let results = MicroPartition::concat(results)?;
            let num_rows = results.len();
            let total_bytes = results.size_bytes().unwrap_or(0);
            let schema = results.schema();

            // Clean up the data to:
            // - Limit to max 10 rows
            let io_stats = IOStatsContext::new("DashboardSubscriber::on_query_end");
            let results = results
                .head(10)?
                .concat_or_get(io_stats)?
                .expect("Results should not be empty");
            // - Filter out Python dtype columns
            let non_py_cols: Vec<usize>;
            #[cfg(feature = "python")]
            {
                non_py_cols = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.dtype == DataType::Python)
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
            }
            #[cfg(not(feature = "python"))]
            {
                non_py_cols = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
            }

            let result = results.get_columns(&non_py_cols);
            (result.to_ipc_stream()?, num_rows, total_bytes)
        };

        let end_sec = now();
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/end", self.url, query_id))
                .json(&daft_dashboard::api::FinalizeArgs {
                    end_sec,
                    results: ipc_buffer,
                    num_rows,
                    total_bytes,
                })
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_plan_start(&self, query_id: String) -> DaftResult<()> {
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/plan_start", self.url, query_id))
                .json(&daft_dashboard::api::PlanStartArgs {
                    plan_start_sec: now(),
                })
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_plan_end(&self, query_id: String, optimized_plan: String) -> DaftResult<()> {
        let plan_end_sec = now();
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/plan_end", self.url, query_id))
                .json(&daft_dashboard::api::PlanEndArgs {
                    plan_end_sec,
                    optimized_plan: optimized_plan.into(),
                })
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })
    }

    fn on_exec_start(&self, query_id: String, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        let exec_start_sec = now();
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/exec/start", self.url, query_id))
                .json(&daft_dashboard::api::ExecStartArgs {
                    exec_start_sec,
                    node_infos: node_infos
                        .iter()
                        .map(|info| info.as_ref().clone())
                        .collect(),
                })
                .send()
                .await
                .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
                .error_for_status()
                .map_err(|e| DaftError::External(Box::new(e)))?;
            Ok::<_, DaftError>(())
        })
    }

    async fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        self.client
            .post(format!(
                "{}/query/{}/exec/{}/start",
                self.url, query_id, node_id
            ))
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: String,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/emit_stats", self.url, query_id))
            .json(&daft_dashboard::api::ExecEmitStatsArgs {
                stats: stats
                    .iter()
                    .map(|(node_id, stats)| {
                        (
                            *node_id,
                            stats
                                .into_iter()
                                .map(|(name, stat)| ((*name).to_string(), stat.clone()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>(),
            })
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }

    async fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        self.client
            .post(format!(
                "{}/query/{}/exec/{}/end",
                self.url, query_id, node_id
            ))
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }

    async fn on_exec_end(&self, query_id: String) -> DaftResult<()> {
        let exec_end_sec = now();
        self.client
            .post(format!("{}/query/{}/exec/end", self.url, query_id))
            .json(&daft_dashboard::api::ExecEndArgs { exec_end_sec })
            .send()
            .await
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;
        Ok(())
    }
}
