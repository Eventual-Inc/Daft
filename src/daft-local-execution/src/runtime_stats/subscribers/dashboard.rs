use std::{collections::HashMap, env, sync::Arc, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_metrics::{NodeID, StatSnapshotSend, ops::NodeInfo};
use common_runtime::get_io_runtime;
use reqwest::{Client, header};
use tokio::sync::{mpsc, oneshot};

use crate::runtime_stats::subscribers::RuntimeStatsSubscriber;

/// Similar to `RuntimeStatsEventHandler`, the `DashboardSubscriber` also has it's own internal mechanism to throttle events.
/// Since there could be many queries broadcasting to the dashboard at the same time, we want to be conscientious about how often we send updates.
#[derive(Debug)]
pub struct DashboardSubscriber {
    info_map: HashMap<NodeID, Arc<NodeInfo>>,
    event_tx: mpsc::UnboundedSender<(StatSnapshotSend, NodeInfo)>,
    flush_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl DashboardSubscriber {
    fn new_with_throttle_interval(node_infos: &[Arc<NodeInfo>], interval: Duration) -> Self {
        let Ok(url) = env::var("DAFT_DASHBOARD_METRICS_URL") else {
            panic!(
                "DashboardSubscriber::new must only be called after checking if it's enabled via `DashboardSubscriber::is_enabled`"
            )
        };

        static USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

        let client = if url.contains("localhost") || url.contains("127.0.0.1") {
            reqwest::Client::builder()
                // if it's a localhost uri we can skip ssl verification
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .build()
                .unwrap()
        } else {
            let default_headers = if let Ok(auth_token) = env::var("DAFT_DASHBOARD_AUTH_TOKEN") {
                let mut headers = header::HeaderMap::new();
                let mut auth_value = header::HeaderValue::from_str(&auth_token)
                    .expect("Failed to create auth header value");
                auth_value.set_sensitive(true);
                headers.insert(header::AUTHORIZATION, auth_value);
                headers
            } else {
                header::HeaderMap::new()
            };
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .user_agent(USER_AGENT)
                .default_headers(default_headers)
                .build()
                .unwrap()
        };
        let client = Arc::new(client);

        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();

        let rt = get_io_runtime(true);
        rt.runtime.spawn(async move {
            let mut tick_interval = tokio::time::interval(interval);
            let mut events_batch = Vec::new();

            loop {
                tokio::select! {
                    Some(event) = event_rx.recv() => {
                        events_batch.push(event);
                    }
                    Some(flush) = flush_rx.recv() => {
                        if !events_batch.is_empty() {
                            send_metrics_batch(&url, &client, &events_batch).await;
                            events_batch.clear();
                        }
                        let _ = flush.send(());
                    }
                    _ = tick_interval.tick() => {
                        if !events_batch.is_empty() {
                            send_metrics_batch(&url, &client, &events_batch).await;
                            events_batch.clear();
                        }
                    }
                }
            }
        });

        let info_map = node_infos
            .iter()
            .map(|info| (info.id, info.clone()))
            .collect::<HashMap<_, _>>();

        Self {
            info_map,
            event_tx,
            flush_tx,
        }
    }

    pub fn new(node_infos: &[Arc<NodeInfo>]) -> Self {
        Self::new_with_throttle_interval(node_infos, Duration::from_secs(1))
    }

    pub fn is_enabled() -> bool {
        env::var("DAFT_DASHBOARD_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
            && env::var("DAFT_DASHBOARD_METRICS_URL").is_ok()
    }
}

async fn send_metrics_batch(
    url: &str,
    client: &Arc<Client>,
    events: &[(StatSnapshotSend, NodeInfo)],
) {
    let mut batch_payload = Vec::new();

    for (snapshot, ctx) in events {
        let mut payload = ctx.context.clone();

        payload.insert("name".to_string(), ctx.name.to_string());
        payload.insert("id".to_string(), ctx.id.to_string());
        for (name, value) in snapshot.iter() {
            payload.insert((*name).to_string(), value.to_string());
        }

        if let Ok(run_id) = env::var("DAFT_DASHBOARD_RUN_ID") {
            payload.insert("run_id".to_string(), run_id);
        }

        batch_payload.push(payload);
    }

    let res = client.post(url).json(&batch_payload).send().await;

    if let Err(e) = res {
        #[cfg(debug_assertions)]
        {
            eprintln!("Failed to send metrics batch to dashboard: {}", e);
        }
        log::error!("Failed to send metrics batch to dashboard: {}", e);
    }
}

#[async_trait]
impl RuntimeStatsSubscriber for DashboardSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn initialize_node(&self, _: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn finalize_node(&self, _: NodeID) -> DaftResult<()> {
        Ok(())
    }

    async fn handle_event(&self, events: &[(NodeID, StatSnapshotSend)]) -> DaftResult<()> {
        for (node_id, event) in events {
            let node_info = &self.info_map[node_id];
            self.event_tx
                .send((event.clone(), node_info.as_ref().clone()))
                .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;
        }
        Ok(())
    }

    async fn finish(self: Box<Self>) -> DaftResult<()> {
        let (tx, rx) = oneshot::channel();
        self.flush_tx
            .send(tx)
            .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;

        rx.blocking_recv()
            .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;
        Ok(())
    }
}
