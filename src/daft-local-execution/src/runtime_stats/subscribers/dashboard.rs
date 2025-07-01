use std::{env, sync::Arc, time::Duration};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use reqwest::{header, Client};
use tokio::sync::{mpsc, oneshot};

use crate::runtime_stats::{subscribers::RuntimeStatsSubscriber, RuntimeStatsEvent};

/// Similar to `RuntimeStatsEventHandler`, the `DashboardSubscriber` also has it's own internal mechanism to throttle events.
/// Since there could be many queries broadcasting to the dashboard at the same time, we want to be conscientious about how often we send updates.
#[derive(Debug)]
pub struct DashboardSubscriber {
    event_tx: mpsc::UnboundedSender<RuntimeStatsEvent>,
    flush_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl DashboardSubscriber {
    fn new_with_throttle_interval(interval: Duration) -> Arc<Self> {
        let Ok(url) = env::var("DAFT_DASHBOARD_METRICS_URL") else {
            panic!("DashboardSubscriber::new must only be called after checking if it's enabled via `DashboardSubscriber::is_enabled`")
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

        Arc::new(Self { event_tx, flush_tx })
    }
    pub fn new() -> Arc<Self> {
        Self::new_with_throttle_interval(Duration::from_secs(1))
    }

    pub fn is_enabled() -> bool {
        env::var("DAFT_DASHBOARD_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
            && env::var("DAFT_DASHBOARD_METRICS_URL").is_ok()
    }
}

async fn send_metrics_batch(url: &str, client: &Arc<Client>, events: &[RuntimeStatsEvent]) {
    let mut batch_payload = Vec::new();

    for event in events {
        let context = &event.node_info;
        let mut payload = event.node_info.context.clone();

        payload.insert("name".to_string(), context.name.to_string());
        payload.insert("id".to_string(), context.id.to_string());
        payload.insert("rows_received".to_string(), event.rows_received.to_string());
        payload.insert("rows_emitted".to_string(), event.rows_emitted.to_string());
        payload.insert("cpu_usage".to_string(), event.cpu_us.to_string());

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

#[async_trait::async_trait]
impl RuntimeStatsSubscriber for DashboardSubscriber {
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn handle_event(&self, event: &RuntimeStatsEvent) -> DaftResult<()> {
        self.event_tx
            .send(event.clone())
            .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;
        Ok(())
    }

    async fn flush(&self) -> DaftResult<()> {
        let (tx, rx) = oneshot::channel();
        self.flush_tx
            .send(tx)
            .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;
        rx.await
            .map_err(|e| DaftError::MiscTransient(Box::new(e)))?;

        Ok(())
    }
}
