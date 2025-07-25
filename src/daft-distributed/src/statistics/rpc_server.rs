use axum::{body::Bytes, routing::post, Router};
use common_error::DaftResult;
use common_metrics::RpcPayload;
use common_py_serde::bincode;
use common_runtime::RuntimeTask;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::{StatisticsEvent, StatisticsManagerRef};

const RPC_SERVER_LOG_TARGET: &str = "DaftRpcServer";

/// RPC server that receives runtime metrics from worker nodes
pub struct RpcServer {
    statistics_manager: StatisticsManagerRef,
    server_handle: Option<RuntimeTask<()>>,
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
}

impl RpcServer {
    /// Creates a new RPC server that will forward metrics to the statistics manager
    pub fn new(statistics_manager: StatisticsManagerRef) -> Self {
        Self {
            statistics_manager,
            server_handle: None,
            shutdown_tx: None,
        }
    }

    /// Starts the RPC server on the specified address
    pub async fn start(&mut self, addr: &str) -> DaftResult<()> {
        info!(target: RPC_SERVER_LOG_TARGET, "RPC server starting on {}", addr);

        let (shutdown_tx, mut _shutdown_rx) = mpsc::unbounded_channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);

        let statistics_manager = self.statistics_manager.clone();

        let addr_str = addr.to_string();
        let runtime = common_runtime::get_io_runtime(true);
        let server_handle = runtime.spawn(async move {
            let app = Router::new().route(
                "/",
                post(move |body: Bytes| Self::handle_connection(body, statistics_manager.clone())),
            );

            let listener = tokio::net::TcpListener::bind(addr_str.clone())
                .await
                .unwrap();
            debug!("RPC server listening on {}", addr_str);
            axum::serve(listener, app).await.unwrap();
        });

        self.server_handle = Some(server_handle);
        Ok(())
    }

    /// Handles an individual connection from a worker node
    async fn handle_connection(body: Bytes, statistics_manager: StatisticsManagerRef) {
        match bincode::deserialize::<RpcPayload>(&body) {
            Ok(payload) => {
                // Convert runtime metrics to statistics events and forward them
                if let Err(e) = Self::process_metrics_payload(&statistics_manager, payload).await {
                    warn!(target: RPC_SERVER_LOG_TARGET,
                        "Failed to process metrics payload: {}", e);
                }
            }
            Err(e) => {
                warn!(target: RPC_SERVER_LOG_TARGET,
                    "Failed to deserialize payload: {}", e);
            }
        }
    }

    /// Processes received metrics and forwards them to subscribers
    async fn process_metrics_payload(
        statistics_manager: &StatisticsManagerRef,
        payload: RpcPayload,
    ) -> DaftResult<()> {
        let (local_physical_node_metrics, snapshot) = payload;

        // Use the network snapshot directly with owned strings
        let event = StatisticsEvent::LocalPhysicalNodeMetrics {
            plan_id: local_physical_node_metrics.plan_id,
            stage_id: local_physical_node_metrics.stage_id,
            task_id: local_physical_node_metrics.task_id,
            logical_node_id: local_physical_node_metrics.logical_node_id,
            local_physical_node_type: local_physical_node_metrics.local_physical_node_type,
            distributed_physical_node_type: local_physical_node_metrics
                .distributed_physical_node_type,
            snapshot,
        };

        statistics_manager.handle_event(event)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn shutdown(&mut self) -> DaftResult<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(server_handle) = self.server_handle.take() {
            server_handle
                .await
                .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;
        }

        Ok(())
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}
