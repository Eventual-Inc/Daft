use std::{sync::Arc, time::Duration};

use common_error::DaftResult;
use common_metrics::LocalPhysicalNodeMetrics;
use common_runtime::get_io_runtime;
use reqwest::Client;
use tokio::sync::mpsc;

use crate::{
    pipeline::NodeInfo,
    runtime_stats::{subscribers::RuntimeStatsSubscriber, StatSnapshot},
};

/// Subscriber that connects to an external RPC server and emits snapshots to it.
/// Intended for observability of distributed execution (Flotilla)
#[derive(Debug)]
pub struct RpcSubscriber {
    snapshot_tx: mpsc::UnboundedSender<(LocalPhysicalNodeMetrics, StatSnapshot)>,
    finish_tx: Option<tokio::sync::oneshot::Sender<()>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for RpcSubscriber {
    fn drop(&mut self) {
        // Ensure we signal the background task to shut down if not already done
        if let Some(finish_tx) = self.finish_tx.take() {
            let _ = finish_tx.send(()); // Ignore error if receiver already dropped
        }
    }
}

impl RpcSubscriber {
    /// Creates a new RPC subscriber that connects to the specified server
    pub fn new(server_url: String) -> DaftResult<Self> {
        let client = Arc::new(
            Client::builder()
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(10)
                .pool_idle_timeout(Duration::from_secs(30))
                .tcp_keepalive(Duration::from_secs(60))
                .build()
                .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?,
        );

        let (snapshot_tx, mut snapshot_rx) =
            mpsc::unbounded_channel::<(LocalPhysicalNodeMetrics, StatSnapshot)>();
        let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn background task to handle RPC communication
        let runtime = get_io_runtime(true);
        let handle = runtime.runtime.spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    // Handle incoming events
                    maybe_payload = snapshot_rx.recv() => {
                        match maybe_payload {
                            Some(payload) => {
                                log::debug!("Received payload from channel {:?}", payload);
                                if let Err(e) = Self::send_batch(&client, &server_url, payload).await {
                                    log::debug!("Failed to send RPC batch: {}", e);
                                    // Continue processing despite errors
                                }
                            }
                            None => {
                                log::debug!("RPC subscriber channel closed, shutting down background task");
                                break;
                            }
                        }
                    }

                    // Handle finish requests
                    result = &mut finish_rx => {
                        match result {
                            Ok(()) => {
                                log::debug!("RPC subscriber received finish signal, shutting down");
                                break;
                            }
                            Err(_) => {
                                log::debug!("RPC subscriber finish channel closed unexpectedly");
                                break;
                            }
                        }
                    }
                }
            }
            log::debug!("RPC subscriber background task ended");
        });

        Ok(Self {
            snapshot_tx,
            _handle: handle,
            finish_tx: Some(finish_tx),
        })
    }

    /// Sends a batch of events to the RPC server with compression
    async fn send_batch(
        client: &Arc<Client>,
        server_url: &str,
        payload: (LocalPhysicalNodeMetrics, StatSnapshot),
    ) -> DaftResult<()> {
        // Serialize the batch to bincode
        let serialized = bincode::serialize(&payload)
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        log::debug!(
            "Sending RPC batch to {}: {} bytes",
            server_url,
            serialized.len()
        );

        // Send the data to the server
        let response = client
            .post(format!("http://{}", server_url))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .header(
                reqwest::header::CONTENT_LENGTH,
                serialized.len().to_string(),
            )
            .body(serialized)
            .send()
            .await
            .map_err(|e| {
                log::debug!("HTTP request failed to {}: {}", server_url, e);
                common_error::DaftError::MiscTransient(Box::new(e))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let response_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string());
            log::debug!(
                "RPC server returned error status {}: {}",
                status,
                response_text
            );
            return Err(common_error::DaftError::MiscTransient(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "RPC server returned error status {}: {}",
                        status, response_text
                    ),
                ),
            )));
        }

        log::debug!("Successfully sent RPC batch to {}", server_url);

        Ok(())
    }

    /// Converts NodeInfo to LocalPhysicalNodeMetrics
    fn node_info_to_metrics(node_info: &NodeInfo) -> DaftResult<LocalPhysicalNodeMetrics> {
        // Extract plan_id from context
        let plan_id = node_info
            .context
            .get("plan_id")
            .ok_or_else(|| {
                common_error::DaftError::MiscTransient(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "plan_id not found in context",
                )))
            })?
            .parse::<u16>()
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        // Extract stage_id from context (may not be present in local execution)
        let stage_id = node_info
            .context
            .get("stage_id")
            .unwrap()
            .parse::<u16>()
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        // Extract task_id from context (may not be present in local execution)
        let task_id = node_info
            .context
            .get("task_id")
            .unwrap()
            .parse::<u32>()
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        // Use node_info.id as logical_node_id
        let logical_node_id = node_info
            .context
            .get("logical_node_id")
            .unwrap()
            .parse::<u32>()
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        let distributed_physical_node_type = node_info
            .context
            .get("node_name")
            .unwrap_or(&String::new())
            .clone();

        // Use node_info.node_type as local_physical_node_type
        let local_physical_node_type = node_info.node_type.to_string();

        Ok(LocalPhysicalNodeMetrics {
            plan_id,
            stage_id,
            task_id,
            logical_node_id,
            local_physical_node_type,
            distributed_physical_node_type,
        })
    }
}

#[async_trait::async_trait]
impl RuntimeStatsSubscriber for RpcSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn finalize_node(&self, _node_info: &NodeInfo) -> DaftResult<()> {
        Ok(())
    }

    fn handle_event(&self, event: &StatSnapshot, node_info: &NodeInfo) -> DaftResult<()> {
        let local_metrics = Self::node_info_to_metrics(node_info)?;

        self.snapshot_tx
            .send((local_metrics, event.clone()))
            .map_err(|e| {
                log::debug!(
                    "RPC subscriber channel is closed - background task may have exited: {}",
                    e
                );
                common_error::DaftError::MiscTransient(Box::new(e))
            })?;

        Ok(())
    }

    async fn flush(&self) -> DaftResult<()> {
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> DaftResult<()> {
        if let Some(finish_tx) = self.finish_tx.take() {
            finish_tx.send(()).map_err(|()| {
                common_error::DaftError::MiscTransient(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to send finish signal",
                )))
            })?;
        }
        Ok(())
    }
}
