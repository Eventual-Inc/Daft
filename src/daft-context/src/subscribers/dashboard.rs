use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, StatSnapshotView, ops::NodeInfo};
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_micropartition::MicroPartitionRef;
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
        // TODO: Limit to X MB and record # of rows

        let ipc_buffer = if results.is_empty() {
            vec![]
        } else {
            let total_bytes = results
                .iter()
                .map(|part| part.size_bytes().unwrap_or(0))
                .sum::<usize>();
            let buffer = Vec::with_capacity(total_bytes);
            let schema = results[0].schema().to_arrow()?;

            let options = arrow2::io::ipc::write::WriteOptions { compression: None };
            let mut writer = arrow2::io::ipc::write::StreamWriter::new(buffer, options);
            writer.start(&schema, None)?;

            let tables = results[0].head(5)?.get_tables()?;
            for table in tables.iter() {
                let chunk = table.to_chunk();
                writer.write(&chunk, None)?;
            }

            writer.finish()?;
            let mut finished_buffer = writer.into_inner();
            finished_buffer.shrink_to_fit();
            finished_buffer
        };

        let end_sec = now();
        self.runtime.block_on_current_thread(async {
            self.client
                .post(format!("{}/query/{}/end", self.url, query_id))
                .json(&daft_dashboard::api::FinalizeArgs {
                    end_sec,
                    results: ipc_buffer,
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
