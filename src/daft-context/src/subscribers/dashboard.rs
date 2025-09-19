use std::{sync::Arc, time::SystemTime};

use common_error::{DaftError, DaftResult};
use common_metrics::{StatSnapshotView, ops::NodeInfo};
use daft_micropartition::MicroPartitionRef;
use reqwest::blocking::Client;
use serde_json::json;

use crate::subscribers::{NodeID, QuerySubscriber};

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

        // Validate that we can connect to the dashboard
        client
            .get(format!("{}/ping", url))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(Some(Self { url, client }))
    }
}

impl QuerySubscriber for DashboardSubscriber {
    fn on_query_start(&self, query_id: String, unoptimized_plan: String) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/start", self.url, query_id))
            .json(&json!({
                "unoptimized_plan": unoptimized_plan,
                "timestamp": now()
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
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

            for part in &results {
                let tables = part.get_tables()?;
                for table in tables.iter() {
                    let chunk = table.to_chunk();
                    writer.write(&chunk, None)?;
                }
            }

            writer.finish()?;
            let mut finished_buffer = writer.into_inner();
            finished_buffer.shrink_to_fit();
            finished_buffer
        };

        self.client
            .post(format!("{}/query/{}/end", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "ipc_buffer": ipc_buffer,
                "num_rows": results.iter().map(|part| part.len()).sum::<usize>(),
                "num_bytes": ipc_buffer.len(),
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_plan_start(&self, query_id: String) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/start_plan", self.url, query_id))
            .json(&json!({
                "timestamp": now()
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_plan_end(&self, query_id: String, optimized_plan: String) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/end_plan", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "optimized_plan": optimized_plan,
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_exec_start(&self, query_id: String, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/start", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "node_infos": node_infos,
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/start_op", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "node_id": node_id,
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_exec_emit_stats(
        &self,
        query_id: String,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/emit_stats", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "stats": stats,
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/end_op", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
                "node_id": node_id,
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }

    fn on_exec_end(&self, query_id: String) -> DaftResult<()> {
        self.client
            .post(format!("{}/query/{}/exec/end", self.url, query_id))
            .json(&json!({
                "timestamp": now(),
            }))
            .send()
            .map_err(|e| DaftError::ConnectTimeout(Box::new(e)))?
            .error_for_status()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        Ok(())
    }
}
